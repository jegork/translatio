import mtranslate
import os
from tqdm import tqdm_notebook as tqdm
from concurrent.futures import ProcessPoolExecutor
import json
import pandas as pd
from glob import glob
from typing import Dict, List, Any
import re
import time
import logging

__all__ = ["Translator"]
logging.basicConfig(
    format="[%(asctime)s] Translatio [%(levelname)s]: %(message)s", datefmt="%H:%M:%S"
)


class Translator:
    def __init__(
        self,
        target_lang: str,
        max_workers: int = 10,
        per_request: int = 10,
        checkpoint_folder: str = None,
        translate_columns: List = [],
        keep_columns: List = [],
    ):
        if target_lang is None:
            raise AttributeError("target_lang not set")
        if not isinstance(target_lang, str):
            raise AttributeError('target_lang is not a string')
        if not isinstance(max_workers, int):
            raise AttributeError('max_workers is not an integer')
        if not isinstance(per_request, int):
            raise AttributeError('per_request is not an integer')
        if not isinstance(checkpoint_folder, str):
            raise AttributeError("checkpoint_folder is not a string")
        if not isinstance(translate_columns, list):
            raise AttributeError("translate_columns is not a list")
        if not isinstance(keep_columns, list):
            raise AttributeError("keep_columns is not a list")
        if len(translate_columns) == 0:
            raise AttributeError("translate_columns is empty")

        cfg = {
            'target_lang': target_lang,
            'max_workers': max_workers,
            'per_request': per_request,
            'translate_columns': translate_columns,
            'keep_columns': keep_columns,
        }

        self.checkpoint_folder = checkpoint_folder

        if os.path.exists(os.path.join(self.checkpoint_folder, "config.json")):
            self.cfg = self._read_config()
        else:
            cfg["last_translated_batch_file"] = None
            self.cfg = cfg
            self._write_config()

    def generate_batches(
        self,
        filename: str,
        names: str,
        sep: str = "\t",
        batch_size: int = 1000,
        start_at: int = 0,
        rows: int = None,
    ):
        if not isinstance(filename, str):
            raise AttributeError("Provide filename!")

        df = pd.read_csv(filename, names=names, sep=sep, skiprows=start_at, nrows=rows)

        use_cols = []

        for r in df.columns:
            if r in self.cfg['translate_columns'] or r in self.cfg['keep_columns']:
                use_cols.append(r)

        self.use_cols = use_cols

        d = df[use_cols].to_dict("records")
        return [d[i : i + batch_size] for i in range(0, len(d), batch_size)]

    def _read_config(self):
        with open(os.path.join(self.checkpoint_folder, "config.json"), "r") as f:
            cfg = f.read()

        return json.loads(cfg)

    def _write_config(self):
        os.makedirs(self.checkpoint_folder, exist_ok=True)
        cfg = json.dumps(self.cfg)

        with open(os.path.join(self.checkpoint_folder, "config.json"), "w+") as f:
            f.write(cfg)

    def _update_config(self, **kwargs):
        cfg = self._read_config()

        for k, v in kwargs.items():
            if k in cfg:
                cfg[k] = v
            else:
                raise KeyError(f"No field {k} in config!")

        self.cfg = cfg
        self._write_config()

    def _merge_files(self):
        temp_files = glob(os.path.join(self.checkpoint_folder, "temp_*.tsv"))

        logging.info(f"Merging {len(temp_files)} files")

        translated_dfs = [pd.read_csv(f, sep="\t") for f in temp_files]
        translated_dfs = pd.concat(translated_dfs, ignore_index=True)

        return translated_dfs

    def _cleanup_folder(self):
        temp_files = glob(os.path.join(self.checkpoint_folder, "temp_*.tsv"))
        logging.info(f"Cleaning up {len(temp_files)} temporary files...")

        for f in temp_files:
            os.remove(f)

    def ready_batches(self):
        return len(glob(os.path.join(self.checkpoint_folder, "temp_*.tsv")))

    def translate(self, data: List[Dict]):
        translate = self.cfg['translate_columns']
        keep = self.cfg['keep_columns']

        translated_data = {}
        to_translate = {x: [d[x] for d in data] for x in translate}
        to_keep = {x: [d[x] for d in data] for x in keep}

        for k, v in to_translate.items():
            t = []
            for i in range(0, len(v), self.cfg["per_request"]):
                result = None
                data_packed = "\n".join(v[i : i + self.cfg["per_request"]])

                while result is None:
                    try:
                        result = mtranslate.translate(
                            data_packed, self.cfg["target_lang"], "en"
                        )
                    except Exception as e:
                        logging.exception(f"Error: {e}, retrying...")

                result = result.split("\n")

                if len(result) != len((v[i : i + self.cfg["per_request"]])):
                    raise Exception(
                        "Length of original and translated data is not the same! Try decreasing per_request variable."
                    )

                t.extend(result)

            translated_data[k] = t

        for k in to_keep.keys():
            translated_data[k] = to_keep[k]

        return translated_data

    def async_translate(self, d: List[Dict]):
        split = int(len(d) / self.cfg["max_workers"])
        submits = []
        results = []  # list of dicts

        with ProcessPoolExecutor(max_workers=self.cfg["max_workers"]) as executor:
            for i in range(self.cfg["max_workers"]):
                start_at = i * split
                stop_at = (i * split) + split

                if stop_at >= len(d) - 1:
                    submits.append(executor.submit(self.translate, d[start_at:]))
                    time.sleep(10)
                else:
                    submits.append(executor.submit(self.translate, d[start_at:stop_at]))

            for i in range(self.cfg["max_workers"]):
                results.append(submits[i].result())

        outputs = {}
        for k in results[0].keys():
            outputs[k] = []

        for r in results:
            for k, v in r.items():
                outputs[k].extend(v)

        return outputs

    def __call__(
        self, batches: List[List[Dict]], output_file: str, done_batches: int = None
    ):
        if not isinstance(output_file, str):
            raise AttributeError("Provide output_file!")
        if not (isinstance(done_batches, int) or done_batches is None):
            raise AttributeError("done_batches should be None or int!")

        if done_batches is None:
            if self.cfg["last_translated_batch_file"] is None:
                start_batch = 0
            else:
                start_batch = (
                    int(
                        re.search(
                            "[0-9]+", self.cfg["last_translated_batch_file"]
                        ).group()
                    )
                    + 1
                )
        else:
            start_batch = done_batches

        if start_batch != 0:
            logging.info(f"Skipping {start_batch} batches...")

        if len(batches) - start_batch != 0:
            for i, batch in tqdm(
                enumerate(batches),
                total=(len(batches) - start_batch),
                desc="Processing batches",
            ):
                if i < start_batch:
                    continue
                translated = self.async_translate(batch)

                pd.DataFrame(translated).to_csv(
                    os.path.join(self.checkpoint_folder, f"temp_{i}.tsv"),
                    sep="\t",
                    index=False,
                )

                self._update_config(last_translated_batch_file=f"temp_{i}.tsv")
                self._cleanup_folder()

                time.sleep(20)
        else:
            logging.info("Dataset already translated")

        merged_df = self._merge_files()
        merged_df = merged_df.reindex(self.use_cols, axis=1)
        merged_df.to_csv(output_file, sep="\t", index=False)

        logging.info("Done")

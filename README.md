# Translatio
Library for translation of big datasets in Python

## Usage

```python
from translatio import Translator

# translate 4,000,000 rows starting from row 2,000,000
# translating column 'text' and keep original column 'id' from collection.tsv file
translatio = Translator(
    cfg={'max_workers': 30, 'per_request': 5, 'target_lang': 'nl'},
    checkpoint_folder='./checkpoint_2/', translate_columns=['text'], keep_columns=['id'])

batches = translatio.generate_batches("collection.tsv", names=['id', 'text'], batch_size=10000, rows=4_000_000, start_at=2_000_000)
```
## TODO
- [ ] add batches generator for long files
- [ ] add variables in config for sleep time
- [ ] batch autogeneration
- [ ] write docs
- [ ] write tests
- [ ] consider not using pandas

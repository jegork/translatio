from setuptools import setup

setup(
    packages=['translatio', 'translatio.tests'],
    install_requires=[
        "tqdm",
        "mtranslate",
        "pandas",
        "pytest",
    ],
)

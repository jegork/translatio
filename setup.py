from setuptools import setup

setup(
    name='Translatio',
    version='0.1.0',
    author='Jegor Kitškerkin',
    author_email='jegor.kitskerin@gmail.com',
    packages=['translatio', 'translatio.tests'],
    url='http://pypi.python.org/pypi/translatio/',
    license='LICENSE',
    description='Translate big datasets effortlessly!',
    long_description=open('README.md').read(),
    install_requires=[
        "tqdm",
        "mtranslate",
        "pandas",
        "pytest",
    ],
)

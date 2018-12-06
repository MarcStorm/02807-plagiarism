# How to setup
Start by (optionally) setting up a virtual environment with python 3.5.x
```bash
virtualenv venv
source venv/bin/activate
```

Install python dependencies
```bash
pip install -r requirements.txt
```

Move into the module folder:
```bash
cd plagiarism
```

Setup configuration file
```bash
cp config.example.py config.py
```
(you may edit the configuration file as needed for your environment)

# Running
## Finding candidates
Using the included sample dataset you can test run the application of finding candidates:
```bash
./lookup.sh resources/test/test1.txt
```

## Generating datastructure
To generate a dataset of your own you will need to aqcuire the Wikipedia database from:
https://dumps.wikimedia.org/enwiki/

You will need to download the compressed `*.xml.bz2` file (around 15 GB) and the index `*.txt.bz2` file (around 200 MB). Put both files in the `resources/wiki` folder and decompress the index file (*NOT* the archive). The decompressed index file will be around 600 MB. Afterwards update the `config.py` file accordingly.

You can now generate articles from the Wikipedia dump to stdout using the `wiki.py` module. The following will output a snipped of the first 10 articles:
```bash
python wiki.py ls -O -d --limit 10 | cut -c-80
```

To generate a new dataset from the first 10 articles you can pipe the output to the `generate.sh` script:
```bash
python wiki.py ls -O -d --limit 10 | ./generate.sh
```

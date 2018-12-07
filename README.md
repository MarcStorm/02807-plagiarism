# Setting up
The following instructions are intended for unix/linux environments.

Start by (optionally) setting up a virtual environment with python 3.5.x or above
```bash
virtualenv venv && source venv/bin/activate
```

Run the makefile to install dependencies and setup the environment:
```bash
make setup
```

# Running
## Finding candidates
Move into the module folder:
```bash
cd plagiarism
```

Using the included sample dataset you can test run the application of finding candidates:
```bash
./lookup.sh resources/test/test1.txt
```

You may optionally try to play around with the supplied articles in `resources/samples`.

## Generating datastructure
To generate a dataset of your own you will need to aqcuire the Wikipedia database from:
https://dumps.wikimedia.org/enwiki/

You will need to download the compressed `*.xml.bz2` file (around 15 GB) and the index `*.txt.bz2` file (around 200 MB). Put both files in the `resources/wiki` folder and decompress the index file (*NOT* the archive). The decompressed index file will be around 600 MB. Afterwards update the `config.py` file accordingly.

You can now generate articles from the Wikipedia dump to stdout using the `wiki.py` module. The following will output a snippet of the first 10 articles:
```bash
python wiki.py ls -O -d --limit 10 | cut -c-80
```

To generate a new dataset from the first 10 articles you can pipe the output to the `generate.sh` script:
```bash
python wiki.py ls -O -d --limit 10 | ./generate.sh
```

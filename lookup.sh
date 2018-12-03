#!/bin/bash
python plagiarism/lookup.py -r local --cmdenv PYTHONPATH=$(pwd)/plagiarism "$@"
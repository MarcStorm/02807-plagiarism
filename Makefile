setup:
	python setup.py

generate:
	python plagiarism/wiki.py ls -O -d --limit 5 | python plagiarism/generate.py -r local --cmdenv PYTHONPATH=${CURDIR}/plagiarism

lookup:
	python plagiarism/lookup.py -r local --cmdenv PYTHONPATH=${CURDIR}/plagiarism << EOF
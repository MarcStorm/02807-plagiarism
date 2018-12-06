setup:
	pip install -r requirements.txt
	python setup.py
	cd plagiarism && ls config.py || cp config.example.py config.py
	cd plagiarism && chmod +x generate.sh lookup.sh

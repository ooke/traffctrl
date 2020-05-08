check:
	mypy --show-error-context --pretty --strict *.py

test:
	./tests.py

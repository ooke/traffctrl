SOURCES=accounts.py additionals.py filtering.py generator.py hosts.py limits.py \
	marks.py protocols.py reports.py storage.py usage_html.py utils.py \
	tests.py data_config.py data_repl.py iplog.py mikrotik.py \
	update_data.py update_firewall.py

check:
	python -m pyflakes $(SOURCES)
	mypy --show-error-context --pretty --strict $(SOURCES)

test:
	./tests.py

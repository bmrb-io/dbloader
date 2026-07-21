# dbloader

internal: scripts for loading the BMRB postgres database

Python 3 and `psycopg2`, plus the BMRB `starobj` library (Python 3 branch
`dbloader-py3-fixes`) and a `psql`/`pg_dump` client.

```sh
pip install -r requirements.txt
export STAROBJ_PATH=/path/to/starobj        # or put it on PYTHONPATH
python __main__.py -c loader.properties --dictdir <dictdir> -d <outdir>
```

Start from `loader.example.properties`. See `CLAUDE.md` for what each stage
does, `PORT_NOTES.md` for the Python 3 port, and `tests/README.md` for the
regression harness.

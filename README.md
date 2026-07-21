# dbloader

internal: scripts for loading the BMRB postgres database

Python 3, `psycopg2`, `pynmrstar`, and a `psql`/`pg_dump` client. No BMRB
libraries: `starobj` and `sas` are gone.

```sh
pip install -r requirements.txt
python __main__.py -c loader.properties --dictdir <dictdir> -d <outdir>
```

Start from `loader.example.properties`. See `CLAUDE.md` for what each stage
does, `PORT_NOTES.md` for the Python 3 port and the starobj removal, and
`tests/README.md` for the regression harness.

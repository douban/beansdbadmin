# coding: utf-8
from flask import Flask
from flask import render_template as tmpl

from beansdbadmin.tools.gc import GCRecord, SQLITE_DB_PATH

app = Flask(__name__)


@app.route('/')
@app.route('/index/')
def index():
    return tmpl('index.html')


@app.route('/gc/')
def gc():
    gc_record = GCRecord(SQLITE_DB_PATH)
    records = gc_record.get_all_record()
    return tmpl('gc.html', gc_records=sorted(records, reverse=True))


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p", "--port", type=int, default=5000,
        help="beansdbadmin agent port number."
    )
    args = parser.parse_args()
    app.run(debug=False, host="0.0.0.0", port=args.port)


if __name__ == '__main__':
    main()
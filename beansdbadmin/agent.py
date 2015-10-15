# coding: utf-8
from flask import Flask, jsonify
from beansdbadmin.disk import get_disks_info, get_buckets_info

app = Flask(__name__)


@app.route('/disks')
def disks():
    return jsonify(disks=get_disks_info())


@app.route('/buckets')
def buckets():
    return jsonify(buckets=get_buckets_info())


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p", "--port", type=int, default=5000,
        help="beansdbadmin agent port number."
    )
    args = parser.parse_args()
    app.run(debug=True, host="0.0.0.0", port=args.port)


if __name__ == '__main__':
    main()

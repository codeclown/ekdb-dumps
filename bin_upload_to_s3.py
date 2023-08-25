# ISC License
#
# Copyright 2023 Martti Laine <martti@marttilaine.com>
#
# Permission to use, copy, modify, and/or distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
# AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
# INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
# LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
# OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
# PERFORMANCE OF THIS SOFTWARE.

#
# DOCUMENTATION
#
# This script uploads generated database dumps to S3.
#
# Usage:
#   python3 -m pip install boto3
#   AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... python3 bin_upload_to_s3.py
#
# Related bucket policy:
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::ekdb-dumps"
    },
    {
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::ekdb-dumps/*"
    }
  ]
}
#
# Related IAM policy:
{
  "Version": "2012-10-17",
  "Statement": [
      {
        "Effect": "Allow",
        "Action": "s3:PutLifecycleConfiguration",
        "Resource": "arn:aws:s3:::ekdb-dumps"
      },
      {
        "Effect": "Allow",
        "Action": "s3:PutObject",
        "Resource": "arn:aws:s3:::ekdb-dumps/v1/*"
      }
  ]
}
#
# /End
#

import sqlite3
import os
import boto3
import json
import datetime

#
# HELPERS
#

def write_metadata(file_path, out_path):
  metadata = {
    'size_mb': 0,
    'tables': {}
  }

  file_stats = os.stat(file_path)
  size_in_mb = file_stats.st_size / (1024 * 1024)
  metadata['size_mb'] = size_in_mb

  db = sqlite3.connect(file_path)
  cursor = db.cursor()
  table_names = cursor.execute("""
    SELECT name
    FROM sqlite_schema
    WHERE type = "table"
    AND name NOT LIKE "sqlite_%"
  """).fetchall()
  for table_name, in table_names:
    row_count, = cursor.execute("""
      SELECT COUNT(*) as amount
      FROM """ + table_name + """
    """).fetchone()
    metadata['tables'][table_name] = {
      'row_count': row_count
    }
  db.close()

  with open(out_path, "w") as out:
    out.write(json.dumps(metadata, indent=2))


#
# MAIN
#

# boto3.set_stream_logger('', logging.DEBUG)

def run():
  s3 = boto3.client('s3')

  print("[   ] Starting")

  s3.put_bucket_lifecycle_configuration(
    Bucket='ekdb-dumps',
    LifecycleConfiguration={
      'Rules': [
        {
          'Expiration': {
            'Days': 30,
          },
          'Filter': {
            'Prefix': 'v1/eduskunta_data.*',
          },
          'ID': 'ExpireDailyDumps',
          'Status': 'Enabled',
        },
      ],
    },
  )

  print("[   ] Patched lifecycle")

  write_metadata("eduskunta_data.sqlite", "eduskunta_data.metadata.json")

  print("[   ] Wrote eduskunta_data.metadata.json")

  date = datetime.date.today()

  sqlite_object_name = f'v1/eduskunta_data.{date}.sqlite'
  metadata_object_name = f'v1/eduskunta_data.{date}.metadata.json'

  latest_sqlite_object_name = 'v1/latest.eduskunta_data.sqlite'
  latest_metadata_object_name = 'v1/latest.eduskunta_data.metadata.json'

  print("[   ] Uploading files")

  s3.upload_file("eduskunta_data.sqlite", "ekdb-dumps", sqlite_object_name)
  s3.upload_file("eduskunta_data.metadata.json", "ekdb-dumps", metadata_object_name)

  print("[   ] Copying to 'latest' path")

  s3.copy_object(Bucket='ekdb-dumps', CopySource=f'/ekdb-dumps/{sqlite_object_name}', Key=latest_sqlite_object_name)
  s3.copy_object(Bucket='ekdb-dumps', CopySource=f'/ekdb-dumps/{metadata_object_name}', Key=latest_metadata_object_name)

  print("[   ] Done!")

run()

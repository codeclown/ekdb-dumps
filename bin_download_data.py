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
# This self-contained script recreates all tables from avoindata.eduskunta.fi
# into a local sqlite file, and downloads all available rows into them.
#
# If the file already exists, the script will only download new rows that are
# not yet present in the tables.
#
# The script has no external dependencies. Only tested on Python 3.
#
# Usage:
#   python3 bin_download_data.py <filename>
#
# Examples:
#   python3 bin_download_data.py eduskunta_data.sqlite
#
# Thanks for using!
#
# Source: https://github.com/codeclown/ekdb-dumps
#

import sqlite3
import json
import re
import sys
import urllib.request
import ssl

#
# CONFIG
#

# Local sqlite file path (will be created if doesn't exist)
db_file = "eduskunta_data.sqlite"
if len(sys.argv) == 2:
  db_file = sys.argv[1]

# Amount of rows to download at once (maximum allowed is 100)
per_page = 100

# https://avoindata.eduskunta.fi/api/v1/tables/
tables_names = [
  "Attachment",
  "AttachmentGroup",
  "HetekaData",
  "MemberOfParliament",
  "SaliDBAanestys",
  # "SaliDBAanestysAsiakirja",
  "SaliDBAanestysEdustaja",
  "SaliDBAanestysJakauma",
  "SaliDBAanestysKieli",
  "SaliDBIstunto",
  "SaliDBKohta",
  "SaliDBKohtaAanestys",
  "SaliDBKohtaAsiakirja",
  "SaliDBMessageLog",
  "SaliDBPuheenvuoro",
  "SaliDBTiedote",
  # "SeatingOfParliament",
  "VaskiData",
]

# https://avoindata.eduskunta.fi/api/v1/tables/PrimaryKeys/rows?perPage=100
primary_keys = {
  "Attachment": "Id",
  "AttachmentGroup": "Id",
  "HetekaData": "Id",
  "MemberOfParliament": "personId",
  "SaliDBAanestys": "AanestysId",
  "SaliDBAanestysAsiakirja": "AsiakirjaId",
  "SaliDBAanestysEdustaja": "EdustajaId",
  "SaliDBAanestysJakauma": "JakaumaId",
  "SaliDBAanestysKieli": "KieliId",
  "SaliDBIstunto": "Id",
  "SaliDBKohta": "Id",
  "SaliDBKohtaAanestys": "Id",
  "SaliDBKohtaAsiakirja": "Id",
  "SaliDBMessageLog": "Id",
  "SaliDBPuheenvuoro": "Id",
  "SaliDBTiedote": "Id",
  "SeatingOfParliament": "seatNumber",
  "Statistics": "TableName",
  "Statistics": "TypeId",
  "VaskiData": "Id",
}

# Allow SSL to work (https://stackoverflow.com/a/49174340)
ssl._create_default_https_context = ssl._create_unverified_context

#
# HELPERS
#

def http_get(url):
  request = urllib.request.Request(url)
  response = urllib.request.urlopen(request)
  return {
    "body": response.read().decode('utf8')
  }

#
# MAIN
#

def run():
  db = sqlite3.connect(db_file)
  cursor = db.cursor()

  try:
    cursor.execute("""
      CREATE TABLE metadata (
        key PRIMARY KEY,
        content
      )
    """)
    cursor.execute("""
      INSERT INTO metadata (
        key,
        content
      ) VALUES (
        "license_and_source_information",
        "Data downloaded from https://avoindata.eduskunta.fi under license CC BY 4.0"
      )
    """)
    db.commit()
  except sqlite3.OperationalError as err:
    if "table source_and_license already exists" in str(err):
      # This is fine, continue
      ""
    else:
      raise

  for table_name in tables_names:
    print(f"[{table_name}] starting")

    primary_key = primary_keys[table_name]

    create_table = False
    pk_start_value = "1"

    try:
      last_row = cursor.execute("SELECT " + primary_key + " FROM " + table_name + " ORDER BY " + primary_key + " DESC LIMIT 1").fetchone()
      if last_row is not None:
        pk_start_value = int(last_row[0]) + 1
    except sqlite3.OperationalError as err:
      if "no such table: " in str(err):
        print(f"[{table_name}] table does not exist in sqlite, will be created")
        create_table = True
      else:
        raise

    while True:
      print(f"[{table_name}] fetching pk_start_value = {pk_start_value}")

      url = f"https://avoindata.eduskunta.fi/api/v1/tables/{table_name}/batch?pkStartValue={pk_start_value}&pkName={primary_key}&perPage={per_page}"
      json_data = http_get(url)["body"]
      data = json.loads(json_data)

      column_names = []
      for column_name in data['columnNames']:
        column_names.append(re.sub(r'[^A-Za-z]', '', column_name))

      if create_table:
        print(f"[{table_name}] creating table")
        column_sql_statements = []
        for column_name in column_names:
          extra = ''

          if column_name == primary_key:
            extra = ' INTEGER PRIMARY KEY'

          column_sql_statements.append(f'{column_name}{extra}')

        create_table_sql = f"CREATE TABLE {table_name} ({', '.join(column_sql_statements)})"
        cursor.execute(create_table_sql)
        create_table = False

      param_placeholders = []
      for column_name in column_names:
        param_placeholders.append('?')

      insert_sql = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({', '.join(param_placeholders)});"
      cursor.executemany(insert_sql, data['rowData'])
      db.commit()

      if not data['hasMore']:
        print(f"[{table_name}] hasMore = false, breaking loop (i.e. done!)")
        break

      pk_start_value = data['pkLastValue'] + 1

run()

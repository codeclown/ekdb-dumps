# ekdb-dumps

All tables from avoindata.eduskunta.fi as a daily updated sqlite database.

## Downloads

### Latest dump

The latest dump is available at a static path:

https://ekdb-dumps.s3.eu-north-1.amazonaws.com/v1/latest.eduskunta_data.sqlite

Its related metadata-file:

https://ekdb-dumps.s3.eu-north-1.amazonaws.com/v1/latest.eduskunta_data.metadata.json

### Datestamped dumps

Datestamped dumps are available in this format (they expire after 30 days):

https://ekdb-dumps.s3.eu-north-1.amazonaws.com/v1/eduskunta_data.YYYY-MM-DD.sqlite

Related metadata files similarly:

https://ekdb-dumps.s3.eu-north-1.amazonaws.com/v1/eduskunta_data.YYYY-MM-DD.metadata.json

### Listing of all files

You can find the list of all available files here:

https://ekdb-dumps.s3.eu-north-1.amazonaws.com/

## What is this?

Official public data by the Finnish Parliament is made available at https://avoindata.eduskunta.fi.

In order to use it you need to download each tables content individually 100 rows at a time.

To make it easier to explore and utilise this data, this repository makes it possible to just download the entire dataset as a single sqlite database. You can find the download links above under [Downloads](#downloads).

The database is updated daily (via the [daily_update.yml](.github/workflows/daily_update.yml) GitHub workflow).

Or if you want, you can copy the self-contained [bin_download_data.py](bin_download_data.py) script and use it yourself to download the data.

## About the tables

The script replicates the tables as they are. Some notes about the result:

- All columns are strings, except the primary key which is an integer
- Some columns contain massive XML blobs (e.g. XmlData-column in VaskiData-table)
- The script adds an additional metadata-table, which contains information like the data source and license

## Inspecting the data

Use a tool like [DB Browser for SQLite](https://sqlitebrowser.org/) to inspect the data.

## Running the script locally

See the header comment in the script ([bin_download_data.py](bin_download_data.py))

## License

Data in the sqlite databases is licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/deed.en), just like the original ([source](https://avoindata.eduskunta.fi/#/fi/about)).

Contents of this repository are licensed under the ISC license. See [LICENSE](LICENSE).

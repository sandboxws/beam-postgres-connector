"""Read from PostgreSQL pipeline example."""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from postgres_connector.splitters import NoSplitter
from postgres_connector.io import ReadFromPostgres

class ReadRecordsOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--host", dest="host", default="localhost")
        parser.add_argument("--port", dest="port", default=5438)
        parser.add_argument("--database", dest="database", default="dvdrental")
        parser.add_argument("--query", dest="query", default="SELECT * FROM actor;")
        parser.add_argument("--user", dest="user", default="postgres")
        parser.add_argument("--password", dest="password", default="postgres")

def run():
    options = ReadRecordsOptions()

    p = beam.Pipeline(options=options)

    read_from_postgres = ReadFromPostgres(
        query=options.query,
        host=options.host,
        database=options.database,
        user=options.user,
        password=options.password,
        port=options.port,
        splitter=NoSplitter(),
    )

    (
        p
        | "ReadFromPostgres" >> read_from_postgres
        | "NoTransform" >> beam.Map(lambda e: print(e))
    )

    p.run().wait_until_finish()


if __name__ == "__main__":
    run()

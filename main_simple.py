import os

from google.cloud.bigquery_storage_v1 import BigQueryReadClient
from google.cloud.bigquery_storage_v1 import types
from google.protobuf.internal.well_known_types import Timestamp

project_id = os.environ.get('PROJECT_ID')
snapshot_millis = 0
#
# The read session is created in this project. This project can be
# different from that which contains the table.

client = BigQueryReadClient()

# This example reads baby name data from the public datasets.
table = "projects/{}/datasets/{}/tables/{}".format(
    "bigquery-public-data", "usa_names", "usa_1910_current"
)

requested_session = types.ReadSession()
requested_session.table = table
# This API can also deliver data serialized in Apache Arrow format.
# This example leverages Apache Avro.
requested_session.data_format = types.DataFormat.AVRO

# We limit the output columns to a subset of those allowed in the table,
# and set a simple filter to only report names from the state of
# Washington (WA).
requested_session.read_options.selected_fields = ["name", "number", "state"]
requested_session.read_options.row_restriction = 'state = "WA"'

# Set a snapshot time if it's been specified.
if snapshot_millis > 0:
    snapshot_time = Timestamp()
    snapshot_time.FromMilliseconds(snapshot_millis)
    requested_session.table_modifiers.snapshot_time = snapshot_time

parent = "projects/{}".format(project_id)
session = client.create_read_session(
    parent=parent,
    read_session=requested_session,
    # We'll use only a single stream for reading data from the table. However,
    # if you wanted to fan out multiple readers you could do so by having a
    # reader process each individual stream.
    max_stream_count=1,
)
reader = client.read_rows(session.streams[0].name)

# The read stream contains blocks of Avro-encoded bytes. The rows() method
# uses the fastavro library to parse these blocks as an iterable of Python
# dictionaries. Install fastavro with the following command:
#
# pip install google-cloud-bigquery-storage[fastavro]
rows = reader.rows(session)

# Do any local processing by iterating over the rows. The
# google-cloud-bigquery-storage client reconnects to the API after any
# transient network errors or timeouts.
names = set()
states = set()

for row in rows:
    names.add(row["name"])
    states.add(row["state"])

print("Got {} unique names in states: {}".format(len(names), ", ".join(states)))

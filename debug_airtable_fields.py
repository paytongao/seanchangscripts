"""
Quick script to see what fields actually exist in Airtable VC Database
"""
from pyairtable import Api
import json

AIRTABLE_API_KEY = "patPnlxR05peVEnUc.e5a8cfe5a3f88676da4b3c124c99ed46026b4f869bb5b6a3f54cd45db17fd58f"
BASE_ID = "app768aQ07mCJoyu8"
VC_TABLE_NAME = "VC Database"

api = Api(AIRTABLE_API_KEY)
table = api.table(BASE_ID, VC_TABLE_NAME)

# Get first record to see what fields exist
records = table.all(max_records=1)

if records:
    print("=" * 60)
    print("ACTUAL FIELDS IN AIRTABLE VC DATABASE:")
    print("=" * 60)

    fields = records[0]["fields"]
    for field_name in sorted(fields.keys()):
        print(f'  "{field_name}"')

    print("=" * 60)
    print("\nFull first record:")
    print(json.dumps(records[0], indent=2))
else:
    print("No records found in table")

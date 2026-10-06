import psycopg2
import json
from ikos_config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)

with conn.cursor() as cur:
    # First check all columns in the document table
    cur.execute("SELECT * FROM document WHERE doc_id = 15 LIMIT 1")
    if cur.description:
        cols = [desc[0] for desc in cur.description]
        row = cur.fetchone()
        if row:
            print(f"Columns in document table: {cols}")
            print(f"\nDocument ID 15 data:")
            for col, val in zip(cols, row):
                if col == "extracted_attributes" and val:
                    print(f"  {col}:")
                    if isinstance(val, str):
                        attr_dict = json.loads(val)
                    else:
                        attr_dict = val
                    for key, subval in attr_dict.items():
                        val_str = str(subval)[:150]
                        print(f"    {key}: {val_str}")
                else:
                    val_str = str(val)[:150] if val else "(null)"
                    print(f"  {col}: {val_str}")
        else:
            print("Document 15 not found")
    else:
        print("No columns found")

conn.close()

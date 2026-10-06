import psycopg2
import json
from ikos_config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)

# Find objects linked to doc_id 15
with conn.cursor() as cur:
    cur.execute("""
        SELECT object_id, class_name, attributes, metadata
        FROM object_instance
        WHERE source_document_id = 15
        LIMIT 10
    """)
    rows = cur.fetchall()
    if rows:
        print(f"Found {len(rows)} objects for doc_id 15:\n")
        for obj_id, class_name, attrs, meta in rows:
            print(f"Object ID: {obj_id}")
            print(f"Class: {class_name}")
            if attrs:
                if isinstance(attrs, str):
                    attr_dict = json.loads(attrs)
                else:
                    attr_dict = attrs
                print("Attributes:")
                for key, val in attr_dict.items():
                    val_str = str(val)[:120]
                    if key.lower() in ['payment_term', 'payment_terms', 'payment_days', 'days_to_pay', 'payment_deadline', 'due_date']:
                        print(f"  *** {key}: {val_str} ***")
                    else:
                        print(f"  {key}: {val_str}")
            print()
    else:
        print("No objects found for doc_id 15")

conn.close()

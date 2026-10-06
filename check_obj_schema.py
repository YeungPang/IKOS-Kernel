import psycopg2
from ikos_config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)

with conn.cursor() as cur:
    # Find all objects for doc_id 15
    cur.execute("""
        SELECT object_id, object_name, class_name, metadata
        FROM object_instance
        WHERE metadata->>'doc_id' = '15'
        ORDER BY object_id
    """)
    rows = cur.fetchall()
    print(f"Found {len(rows)} objects for doc_id 15:\n")
    for obj_id, obj_name, class_name, metadata in rows:
        print(f"Object ID: {obj_id}")
        print(f"Name: {obj_name}")
        print(f"Class: {class_name}")
        if metadata:
            print("Metadata:")
            for key, val in metadata.items():
                val_str = str(val)[:120]
                if key.lower() in ['payment_term', 'payment_terms', 'payment_days', 'days_to_pay', 'payment_deadline', 'due_date', 'due', 'payment']:
                    print(f"  *** {key}: {val_str} ***")
                else:
                    print(f"  {key}: {val_str}")
        print()

conn.close()

import csv
import json
import struct
import sys
import os

# COLUMNAR_MAGIC = b"COL1"
MAGIC = b"COL1"

def ingest_csv(csv_path, output_path, column_types):
    """
    column_types: dict mapping col_name -> type_str ("i64", "f64", "string")
    """
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        rows = list(reader)

    row_count = len(rows)
    columns_data = {h: [] for h in headers}
    sums = {h: 0.0 for h in headers if column_types.get(h) in ("i64", "f64")}

    for row in rows:
        for h in headers:
            val = row[h]
            t = column_types.get(h, "string")
            if t == "i64":
                v = int(float(val)) # handle potential .0 in CSV
                columns_data[h].append(v)
                sums[h] += v
            elif t == "f64":
                v = float(val)
                columns_data[h].append(v)
                sums[h] += v
            else:
                columns_data[h].append(val)

    # Write to .aqe file
    with open(output_path, 'wb') as f:
        f.write(MAGIC)
        # Placeholder for metadata offset (8 bytes)
        f.write(struct.pack('<Q', 0))
        
        current_offset = 4 + 8
        col_metas = []

        for h in headers:
            t = column_types.get(h, "string")
            start_offset = current_offset
            
            if t == "i64":
                for v in columns_data[h]:
                    f.write(struct.pack('<q', v))
                    current_offset += 8
            elif t == "f64":
                for v in columns_data[h]:
                    f.write(struct.pack('<d', v))
                    current_offset += 8
            else:
                # String format: [lengths (u32)][data]
                lengths = [len(s.encode('utf-8')) for s in columns_data[h]]
                for l in lengths:
                    f.write(struct.pack('<I', l))
                    current_offset += 4
                for s in columns_data[h]:
                    b = s.encode('utf-8')
                    f.write(b)
                    current_offset += len(b)

            col_metas.append({
                "name": h,
                "data_type": t,
                "offset": start_offset,
                "length": current_offset - start_offset,
                "sum": sums.get(h, 0.0)
            })

        # Write Metadata (JSON)
        metadata = {
            "row_count": row_count,
            "columns": col_metas,
            "sums": sums
        }
        meta_json = json.dumps(metadata).encode('utf-8')
        meta_offset = current_offset
        f.write(meta_json)
        
        # Seek back to write meta_offset
        f.seek(4)
        f.write(struct.pack('<Q', meta_offset))

    print(f"Successfully ingested {row_count} rows into {output_path}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python tools/ingest_csv.py <input.csv> <output.aqe>")
        sys.exit(1)

    csv_fn = sys.argv[1]
    aqe_fn = sys.argv[2]
    
    # Hardcoded mapping for our 'users' table demo, or we could parse schema.toml
    # For now, let's just make it handle user_id, status, country, timestamp as i64
    types = {
        "user_id": "i64",
        "status": "i64",
        "country": "i64",
        "timestamp": "i64"
    }
    
    ingest_csv(csv_fn, aqe_fn, types)

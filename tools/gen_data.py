import csv
import random
import argparse
import re
import os

def parse_schema(schema_path):
    columns = []
    with open(schema_path, 'r') as f:
        content = f.read()
        
    # Simple regex to find [[columns]] blocks
    matches = re.finditer(r'\[\[columns\]\]\s*name\s*=\s*"([^"]+)"\s*type\s*=\s*"([^"]+)"', content)
    for match in matches:
        columns.append({
            'name': match.group(1),
            'type': match.group(2)
        })
    return columns

def generate_data(num_rows, schema, output_path, seed=42):
    random.seed(seed)
    
    start_ts = 1577836800000
    end_ts = 1735603200000
    
    headers = [col['name'] for col in schema]
    
    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        
        for i in range(num_rows):
            row = []
            for col in schema:
                name = col['name']
                col_type = col['type']
                
                if name == "user_id":
                    val = i
                elif name == "status":
                    r = random.randint(0, 99)
                    if r < 70: val = 0
                    elif r < 90: val = 1
                    else: val = 2
                elif name == "country":
                    val = random.randint(0, 4)
                elif name == "timestamp":
                    val = random.randint(start_ts, end_ts)
                else:
                    if col_type in ["u8", "i8"]:
                        val = random.randint(0, 255)
                    elif col_type in ["u64", "i64", "u32"]:
                        val = random.randint(0, 1000)
                    elif col_type in ["f64", "f32"]:
                        val = round(random.uniform(0, 1000), 2)
                    else:
                        val = "dynamic_val"
                row.append(val)
            writer.writerow(row)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate synthetic CSV data for AQP.')
    parser.add_argument('--num', type=int, default=100000, help='Number of rows to generate')
    parser.add_argument('--output', type=str, default='data.csv', help='Output CSV path')
    parser.add_argument('--schema', type=str, default='schema.toml', help='Path to schema.toml')
    parser.add_argument('--seed', type=int, default=42, help='Random seed')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.schema):
        print(f"Error: Schema file {args.schema} not found.")
        exit(1)
        
    print(f"Generating {args.num} rows using schema {args.schema}...")
    schema = parse_schema(args.schema)
    if not schema:
        print("Error: Could not parse columns from schema.toml.")
        exit(1)
        
    generate_data(args.num, schema, args.output, args.seed)
    print(f"Successfully generated data to {args.output}")

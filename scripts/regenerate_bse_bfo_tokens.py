#!/usr/bin/env python3
"""Regenerate allowed_tokens.txt from BSE and BFO CSV files."""
import csv
from pathlib import Path

def regenerate_tokens():
    repo_root = Path(__file__).parent.parent
    bse_csv = repo_root / "BSE_BFO" / "BSE.csv"
    bfo_csv = repo_root / "BSE_BFO" / "BFO.csv"
    output_file = repo_root / "allowed_tokens.txt"
    
    tokens = set()
    
    # Read BSE tokens
    print(f"Reading BSE tokens from {bse_csv}...")
    with open(bse_csv, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            token = row['exchange_token'].strip()
            if token and token.isdigit():
                tokens.add(token)
    
    bse_count = len(tokens)
    print(f"  Found {bse_count} BSE tokens")
    
    # Read BFO tokens
    print(f"Reading BFO tokens from {bfo_csv}...")
    with open(bfo_csv, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            token = row['exchange_token'].strip()
            if token and token.isdigit():
                tokens.add(token)
    
    bfo_count = len(tokens) - bse_count
    print(f"  Found {bfo_count} BFO tokens")
    
    # Write sorted tokens
    print(f"\nWriting {len(tokens)} unique tokens to {output_file}...")
    sorted_tokens = sorted(tokens, key=int)
    with open(output_file, 'w') as f:
        for token in sorted_tokens:
            f.write(f"{token}\n")
    
    print(f"✓ Generated {output_file}")
    print(f"  Total unique tokens: {len(tokens)}")
    print(f"  Token range: {min(sorted_tokens, key=int)} - {max(sorted_tokens, key=int)}")

if __name__ == "__main__":
    regenerate_tokens()

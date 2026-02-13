import re

def format_target_type(oracle_type, target_db_type='postgres'):
    """Convert Oracle type to target DB type based on target database."""
    t = oracle_type.upper().strip()
    
    if target_db_type == 'postgres':
        if 'NVARCHAR2' in t:
            m = re.search(r'\((\d+)\)', oracle_type)
            return f"VARCHAR({m.group(1)})" if m else 'VARCHAR(255)'
        elif 'VARCHAR2' in t:
            return oracle_type.upper().replace('VARCHAR2', 'VARCHAR')
        elif t.startswith('NCHAR'):
            return oracle_type.upper().replace('NCHAR', 'CHAR')
        elif t.startswith('CHAR'):
            return oracle_type.upper()
        elif t.startswith('NUMBER'):
            m = re.search(r'\((.+?)\)', oracle_type)
            return f"NUMERIC({m.group(1)})" if m else 'NUMERIC'
        elif t == 'INTEGER':
            return 'INTEGER'
        elif t.startswith('DECIMAL'):
            return oracle_type.upper().replace('DECIMAL', 'NUMERIC')
        elif t in ('FLOAT', 'BINARY_FLOAT'):
            return 'REAL'
        elif t == 'BINARY_DOUBLE':
            return 'DOUBLE PRECISION'
        elif t == 'DATE':
            return 'TIMESTAMP'
        elif t.startswith('TIMESTAMP'):
            return 'TIMESTAMP'
        elif t in ('CLOB', 'NCLOB', 'LONG'):
            return 'TEXT'
        elif t == 'BLOB':
            return 'BYTEA'
        elif t.startswith('RAW') or t == 'LONG RAW':
            return 'BYTEA'
        elif t == 'TEXT':
            return 'TEXT'
        else:
            return oracle_type
    else:
        if t.startswith('NCHAR'):
            return oracle_type.replace('NCHAR', 'CHAR')
        return oracle_type

# Test all Oracle types
test_types = [
    'NVARCHAR2(80)', 'NVARCHAR2(40)', 'NVARCHAR2(20)',
    'VARCHAR2(100)', 'VARCHAR2(50)',
    'NCHAR(10)', 'CHAR(20)',
    'NUMBER(10)', 'NUMBER(10,2)', 'NUMBER',
    'INTEGER', 'DECIMAL(10,2)',
    'FLOAT', 'BINARY_FLOAT', 'BINARY_DOUBLE',
    'DATE', 'TIMESTAMP',
    'CLOB', 'NCLOB', 'BLOB', 'RAW(100)',
    'TEXT'
]

print("=" * 60)
print(f"{'Oracle Type':<25} {'PostgreSQL Type':<25}")
print("=" * 60)
for t in test_types:
    result = format_target_type(t, 'postgres')
    status = "OK" if 'NVARCHAR' not in result else "FAIL"
    print(f"{t:<25} {result:<25} [{status}]")
print("=" * 60)


# verify_replacement.py
from datetime import datetime

def verify_dag_generation():
    # Mock data similar to what's in app.py
    m_data = {
        'source_sql': "SELECT * FROM HR.EMPLOYEES",
        'source_table': "HR.EMPLOYEES",
        'target_table': "employees_target",
        'source_conn_name': "Oracle_Prod",
        'target_conn_name': "Postgres_DW"
    }

    # Helper to mimic app.py logic
    safe_source = "".join([c if c.isalnum() or c in ('_', '-') else '_' for c in m_data['source_conn_name']])
    safe_table = "".join([c if c.isalnum() or c in ('_', '-') else '_' for c in m_data['source_table']])
    # We can't match exact timestamp, so we'll just check logic logic or mock it
    # For testing replacement, let's assume a known timestamp string or capture logic
    timestamp_str = "20230101_120000" 
    dag_name = f"dag_{safe_source}_{safe_table}_{timestamp_str}"

    # Templates to test
    templates = [
        ("Template 1: {{Source_SQL}}", "Template 1: SELECT * FROM HR.EMPLOYEES"),
        ("Template 2: {{SOURCE_SQL}}", "Template 2: SELECT * FROM HR.EMPLOYEES"),
        ("Template 3: {{ Source_SQL }}", "Template 3: SELECT * FROM HR.EMPLOYEES"),
        ("Template 4: {{TABLE_NAME}}", "Template 4: employees_target"),
        ("Template 5: {{ TABLE_NAME }}", "Template 5: employees_target"),
        ("Template 6: {{Dag_Name}}", f"Template 6: {dag_name}"),
        ("Template 7: {{DAG_NAME}}", f"Template 7: {dag_name}"),
        ("Mixed: {{DAG_NAME}} -> {{TABLE_NAME}}", f"Mixed: {dag_name} -> employees_target")
    ]

    # Replacement logic from app.py
    replacements = {
        '{{ source_sql }}': m_data['source_sql'],
        '{{ Source_SQL }}': m_data['source_sql'],
        '{{Source_SQL}}': m_data['source_sql'],
        '{{ SOURCE_SQL }}': m_data['source_sql'],
        '{{SOURCE_SQL}}': m_data['source_sql'],
        '{{ source_table }}': m_data['source_table'],
        '{{ target_table }}': m_data['target_table'],
        '{{ TABLE_NAME }}': m_data['target_table'],
        '{{TABLE_NAME}}': m_data['target_table'],
        '{{ source_conn }}': m_data['source_conn_name'],
        '{{ target_conn }}': m_data['target_conn_name'],
        '{{ Dag_Name }}': dag_name,
        '{{Dag_Name}}': dag_name,
        '{{DAG_NAME}}': dag_name
    }

    print("Starting Verification...")
    all_passed = True
    for tmpl_str, expected in templates:
        dag_code = tmpl_str
        for key, value in replacements.items():
            dag_code = dag_code.replace(key, value)
        
        if dag_code == expected:
            print(f"[PASS] Template: '{tmpl_str}' -> '{dag_code}'")
        else:
            print(f"[FAIL] Template: '{tmpl_str}'")
            print(f"   Expected: '{expected}'")
            print(f"   Actual:   '{dag_code}'")
            all_passed = False

    if all_passed:
        print("\nAll tests passed successfully!")
    else:
        print("\nSome tests failed.")

if __name__ == "__main__":
    verify_dag_generation()

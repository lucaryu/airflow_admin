
from app import app, db
from models import Mapping, MappingColumn

with app.app_context():
    print("Fixing ETL_DTM columns for all mappings...")
    
    # Update existing columns where source_column is ETL_DTM
    # Note: source_column might be 'ETL_DTM' (case sensitive in DB depending on collation, but we set it as upper)
    
    cols = MappingColumn.query.filter(MappingColumn.source_column.ilike('ETL_DTM')).all()
    
    count = 0
    for col in cols:
        print(f"Updating Mapping {col.mapping_id}: {col.source_column} -> SYSDATE")
        col.source_column = 'SYSDATE'
        col.source_type = 'SYSTEM'
        col.target_column = 'ETL_CRY_DTM'
        col.target_logical_name = 'ETL Creation Timestamp'
        count += 1
        
    db.session.commit()
    print(f"Updated {count} columns.")

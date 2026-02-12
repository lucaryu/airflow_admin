
from app import app, db
from models import Mapping, MappingColumn

with app.app_context():
    # Find mapping for GOSALES.PRODUCT_LINE
    mapping = Mapping.query.filter(Mapping.source_table.ilike('%PRODUCT_LINE%')).first()
    
    if not mapping:
        print("Mapping not found.")
        exit()

    print(f"Updating columns for mapping: {mapping.source_table}")
    
    # Delete existing columns
    MappingColumn.query.filter_by(mapping_id=mapping.id).delete()
    
    # Add correct columns for GOSALES.PRODUCT_LINE
    # Assuming typical columns based on name
    columns = [
        MappingColumn(
            mapping_id=mapping.id,
            column_order=1,
            source_column='PRODUCT_LINE_CODE',
            source_type='INTEGER',
            is_pk=True,
            is_nullable=False,
            target_column='product_line_code',
            target_type='INTEGER',
            target_logical_name='Product Line Code'
        ),
        MappingColumn(
            mapping_id=mapping.id,
            column_order=2,
            source_column='PRODUCT_LINE_EN',
            source_type='VARCHAR(255)',
            is_pk=False,
            is_nullable=True,
            target_column='product_line_en',
            target_type='VARCHAR(255)',
            target_logical_name='Product Line English'
        ),
        MappingColumn(
            mapping_id=mapping.id,
            column_order=3,
            source_column='PRODUCT_LINE_FR',
            source_type='VARCHAR(255)',
            is_pk=False,
            is_nullable=True,
            target_column='text_fr', # Example mapping
            target_type='VARCHAR(255)',
            target_logical_name='Text French'
        ),
        MappingColumn(
            mapping_id=mapping.id,
            column_order=4,
            source_column='PRODUCT_LINE_MB',
            source_type='VARCHAR(255)',
            is_pk=False,
            is_nullable=True,
            target_column='text_mb', # Example
            target_type='VARCHAR(255)',
            target_logical_name='Text Multi-byte'
        ),
         MappingColumn(
            mapping_id=mapping.id,
            column_order=5,
            source_column='ETL_DTM',
            source_type='TIMESTAMP',
            is_pk=False,
            is_nullable=True,
            target_column='etl_dtm',
            target_type='TIMESTAMP',
            target_logical_name='ETL Timestamp'
        )
    ]
    
    db.session.add_all(columns)
    db.session.commit()
    print("Columns updated successfully.")

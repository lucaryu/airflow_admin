
from app import app, db
from models import Mapping, MappingColumn

with app.app_context():
    # Find mapping for GOSALES.BRANCH
    mapping = Mapping.query.filter(Mapping.source_table.ilike('%BRANCH%')).first()
    
    if not mapping:
        print("Mapping for 'BRANCH' not found.")
        exit()

    print(f"Updating columns for mapping: {mapping.source_table}")
    
    # Delete existing columns
    MappingColumn.query.filter_by(mapping_id=mapping.id).delete()
    
    # Add correct columns for GOSALES.BRANCH
    columns = [
        MappingColumn(
            mapping_id=mapping.id,
            column_order=1,
            source_column='BRANCH_CODE',
            source_type='INTEGER',
            is_pk=True,
            is_nullable=False,
            target_column='branch_code',
            target_type='INTEGER',
            target_logical_name='Branch Code'
        ),
        MappingColumn(
            mapping_id=mapping.id,
            column_order=2,
            source_column='ADDRESS1',
            source_type='VARCHAR(255)',
            is_pk=False,
            is_nullable=True,
            target_column='address1',
            target_type='VARCHAR(255)',
            target_logical_name='Address Line 1'
        ),
        MappingColumn(
            mapping_id=mapping.id,
            column_order=3,
            source_column='ADDRESS2',
            source_type='VARCHAR(255)',
            is_pk=False,
            is_nullable=True,
            target_column='address2',
            target_type='VARCHAR(255)',
            target_logical_name='Address Line 2'
        ),
        MappingColumn(
            mapping_id=mapping.id,
            column_order=4,
            source_column='CITY',
            source_type='VARCHAR(100)',
            is_pk=False,
            is_nullable=True,
            target_column='city',
            target_type='VARCHAR(100)',
            target_logical_name='City'
        ),
        MappingColumn(
            mapping_id=mapping.id,
            column_order=5,
            source_column='PROV_STATE',
            source_type='VARCHAR(100)',
            is_pk=False,
            is_nullable=True,
            target_column='prov_state',
            target_type='VARCHAR(100)',
            target_logical_name='Province/State'
        ),
        MappingColumn(
            mapping_id=mapping.id,
            column_order=6,
            source_column='POSTAL_ZONE',
            source_type='VARCHAR(20)',
            is_pk=False,
            is_nullable=True,
            target_column='postal_zone',
            target_type='VARCHAR(20)',
            target_logical_name='Postal Zone'
        ),
        MappingColumn(
            mapping_id=mapping.id,
            column_order=7,
            source_column='COUNTRY_CODE',
            source_type='INTEGER',
            is_pk=False,
            is_nullable=True,
            target_column='country_code',
            target_type='INTEGER',
            target_logical_name='Country Code'
        ),
        MappingColumn(
            mapping_id=mapping.id,
            column_order=8,
            source_column='ORGANIZATION_CODE',
            source_type='VARCHAR(20)',
            is_pk=False,
            is_nullable=True,
            target_column='organization_code',
            target_type='VARCHAR(20)',
            target_logical_name='Organization Code'
        ),
        MappingColumn(
            mapping_id=mapping.id,
            column_order=9,
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

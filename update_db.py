from app import app, db
from models import TemplateVariable
from sqlalchemy import inspect

def update_database():
    with app.app_context():
        inspector = inspect(db.engine)
        if 'template_variable' not in inspector.get_table_names():
            print("Creating 'template_variable' table...")
            TemplateVariable.__table__.create(db.engine)
            
            # Add default variables
            defaults = [
                TemplateVariable(name='SQL Query', code='{{ sql_query }}', color='primary', icon='fas fa-database'),
                TemplateVariable(name='Table Name', code='{{ table_name }}', color='success', icon='fas fa-table'),
                TemplateVariable(name='Execution Date', code='{{ ds }}', color='warning', icon='fas fa-calendar'),
                TemplateVariable(name='DAG ID', code='{{ dag.dag_id }}', color='info', icon='fas fa-fingerprint')
            ]
            
            db.session.bulk_save_objects(defaults)
            db.session.commit()
            print("Default variables added.")
        else:
            print("'template_variable' table already exists.")

if __name__ == '__main__':
    update_database()

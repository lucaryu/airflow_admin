from app import app, db, GeneratedDAG

if __name__ == '__main__':
    with app.app_context():
        # Find and delete the error record
        error_dags = GeneratedDAG.query.filter_by(status='Error').all()
        print(f"Found {len(error_dags)} error records.")
        for dag in error_dags:
            print(f"Deleting DAG ID: {dag.id}, Filename: {dag.filename}")
            db.session.delete(dag)
        
        db.session.commit()
        print("Cleanup complete.")

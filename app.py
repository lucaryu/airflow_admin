from flask import Flask, redirect, url_for, render_template, request, jsonify, send_file
from flask_admin import Admin, BaseView, expose
from flask_admin.contrib.sqla import ModelView
from models import db, Connection, Mapping, MappingColumn, Template, GeneratedDAG
import os
from datetime import datetime

app = Flask(__name__)

# Configuration
app.config['SECRET_KEY'] = 'dev-key-123'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///toy_airflow.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Initialize DB
db.init_app(app)

@app.errorhandler(404)
def not_found_error(error):
    with open('dags_generation_debug.log', 'a') as log_file:
        log_file.write(f"404 Error triggered. Path: {request.path}\n")
        
    if request.path.startswith('/api/'):
        return jsonify({'status': 'error', 'message': 'Resource not found'}), 404
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_error(error):
    with open('dags_generation_debug.log', 'a') as log_file:
        log_file.write(f"500 Error triggered. Path: {request.path} - Error: {error}\n")
        
    if request.path.startswith('/api/'):
        return jsonify({'status': 'error', 'message': 'Internal server error'}), 500
    return render_template('500.html'), 500

@app.errorhandler(405)
def method_not_allowed_error(error):
    if request.path.startswith('/api/'):
        return jsonify({'status': 'error', 'message': 'Method not allowed'}), 405
    return render_template('404.html'), 405

class ConnectionView(ModelView):
    # Override the list view template
    list_template = 'connection_list.html'
    create_template = 'add_connection.html'
    edit_template = 'add_connection.html' # Reusing add template for edit for now

    def is_visible(self):
        return False # Hide from default menu since we have a custom sidebar

    @expose('/')
    def index_view(self):
        connections = Connection.query.all()
        return self.render('connection_list.html', connections=connections)

    @expose('/test', methods=['POST'])
    def test_connection(self):
        data = request.json or request.form
        host = data.get('host')
        port = data.get('port')
        
        # Simulation logic
        if host and port:
            return {'status': 'success', 'message': f'Successfully connected to {host}:{port}'}, 200
        else:
            return {'status': 'error', 'message': 'Host and Port are required for testing'}, 400

    @expose('/test/<int:id>', methods=['POST'])
    def test_connection_id(self, id):
        conn = Connection.query.get_or_404(id)
        # Simulation logic
        if conn.host and conn.port:
            return {'status': 'success', 'message': f'Successfully connected to {conn.name} ({conn.host}:{conn.port})'}, 200
        else:
            return {'status': 'error', 'message': 'Host and Port are missing'}, 400

    @expose('/new', methods=('GET', 'POST'))
    def create_view(self):
        if request.method == 'POST':
            conn = Connection(
                name=request.form.get('name'),
                conn_type=request.form.get('conn_type'),
                host=request.form.get('host'),
                port=request.form.get('port'),
                database=request.form.get('database'),
                username=request.form.get('username'),
                password=request.form.get('password'),
                status='Active'
            )
            db.session.add(conn)
            db.session.commit()
            return redirect(url_for('.index_view'))
        return self.render('add_connection.html', connection=None)

    @expose('/edit/<int:id>', methods=('GET', 'POST'))
    def edit_view(self, id):
        conn = Connection.query.get_or_404(id)
        if request.method == 'POST':
            conn.name = request.form.get('name')
            conn.conn_type = request.form.get('conn_type')
            conn.host = request.form.get('host')
            conn.port = request.form.get('port')
            conn.database = request.form.get('database')
            conn.username = request.form.get('username')
            if request.form.get('password'): # Only update password if provided
                conn.password = request.form.get('password')
            
            db.session.commit()
            return redirect(url_for('.index_view'))
        
        return self.render('add_connection.html', connection=conn)

    @expose('/delete/<int:id>', methods=['POST'])
    def delete_view(self, id):
        conn = Connection.query.get_or_404(id)
        db.session.delete(conn)
        db.session.commit()
        return {'status': 'success', 'message': 'Connection deleted successfully'}, 200

# Setup Admin
admin = Admin(app, name='Toy Airflow', url='/admin')

from models import db, Connection, Mapping, MappingColumn, Template, TemplateVariable, GeneratedDAG

class MappingView(BaseView):
    @expose('/')
    def index(self):
        mappings = Mapping.query.all()
        return self.render('mapping_list.html', mappings=mappings)

    @expose('/new')
    def new_mapping(self):
        connections = Connection.query.all()
        return self.render('table_selection.html', connections=connections)

    @expose('/fetch_tables/<int:conn_id>')
    def fetch_tables(self, conn_id):
        conn = Connection.query.get_or_404(conn_id)
        # Mock logic to return tables based on connection type
        tables = []
        # Implement real Oracle data fetching logic
        if conn.conn_type == 'oracle':
            import oracledb
            try:
                # Construct DSN properly. host:port/service_name
                # If conn.database is SID, use host:port:sid or DSN(sid=...)
                # Assuming conn.database is Service Name for consistency with modern setups
                dsn = f"{conn.host}:{conn.port}/{conn.database}"
                print(f"DEBUG: Connecting to Oracle DSN: {dsn}, User: {conn.username}") # Log connection attempt

                with oracledb.connect(user=conn.username, password=conn.password, dsn=dsn) as connection:
                    print("DEBUG: Connection successful")
                    with connection.cursor() as cursor:
                        # Fetch tables accessible to the user, excluding system tables
                        sql = """
                            SELECT owner || '.' || table_name 
                            FROM all_tables 
                            WHERE owner NOT IN ('SYS', 'SYSTEM', 'OUTLN', 'DBSNMP', 'CTXSYS', 'MDSYS', 
                                                'OLAPSYS', 'ORDSYS', 'XDB', 'WMSYS', 'LBACSYS', 
                                                'DVSYS', 'GSMADMIN_INTERNAL', 'APPQOSSYS', 'AUDSYS') 
                            AND table_name NOT LIKE 'BIN$%'
                            ORDER BY owner, table_name
                            FETCH FIRST 100 ROWS ONLY
                        """
                        cursor.execute(sql)
                        tables = [row[0] for row in cursor.fetchall()]
                        print(f"DEBUG: Found {len(tables)} tables")

            except oracledb.Error as e:
                error_obj, = e.args
                print(f"ERROR: Oracle connection failed: {error_obj.message}")
                return {'status': 'error', 'message': f'Oracle Error: {error_obj.message}'}, 500
            except Exception as e:
                 print(f"ERROR: Unexpected error: {str(e)}")
                 return {'status': 'error', 'message': f'Unexpected Error: {str(e)}'}, 500
        elif conn.conn_type == 'postgres':
            tables = ['public.users', 'public.orders', 'public.products', 'finance.transactions', 'analytics.daily_stats']
        elif conn.conn_type == 'mysql':
            tables = ['app_db.users', 'app_db.posts', 'app_db.comments', 'legacy.users']
        else:
            tables = [f'schema.table_{i}' for i in range(1, 6)]
            
        return {'tables': tables}, 200

    @expose('/generate', methods=['POST'])
    def generate_mapping(self):
        data = request.json
        source_conn_id = data.get('source_conn_id')
        target_conn_id = data.get('target_conn_id')
        selected_tables = data.get('tables') # List of table names

        if not source_conn_id or not target_conn_id or not selected_tables:
            return {'status': 'error', 'message': 'Missing required fields'}, 400

        # Get source connection for DB introspection
        source_conn = Connection.query.get(source_conn_id)
        if not source_conn:
            return {'status': 'error', 'message': 'Source connection not found'}, 404

        def get_oracle_columns(conn_obj, owner, table_name):
            """Fetch real column metadata from Oracle DB."""
            import oracledb
            dsn = f"{conn_obj.host}:{conn_obj.port}/{conn_obj.database}"
            result = {
                'table_comment': '',
                'columns': [],  # list of dicts
            }
            
            with oracledb.connect(user=conn_obj.username, password=conn_obj.password, dsn=dsn) as connection:
                with connection.cursor() as cursor:
                    # 1. Table COMMENTS
                    cursor.execute("""
                        SELECT comments FROM ALL_TAB_COMMENTS 
                        WHERE owner = :owner AND table_name = :table_name AND table_type = 'TABLE'
                    """, {'owner': owner, 'table_name': table_name})
                    row = cursor.fetchone()
                    result['table_comment'] = row[0] if row and row[0] else ''

                    # 2. PK columns
                    cursor.execute("""
                        SELECT cols.column_name
                        FROM ALL_CONSTRAINTS cons
                        JOIN ALL_CONS_COLUMNS cols 
                          ON cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner
                        WHERE cons.constraint_type = 'P'
                          AND cons.owner = :owner AND cons.table_name = :table_name
                    """, {'owner': owner, 'table_name': table_name})
                    pk_columns = set(row[0] for row in cursor.fetchall())

                    # 3. Partition key columns
                    cursor.execute("""
                        SELECT column_name
                        FROM ALL_PART_KEY_COLUMNS
                        WHERE owner = :owner AND name = :table_name AND object_type = 'TABLE'
                    """, {'owner': owner, 'table_name': table_name})
                    partition_columns = set(row[0] for row in cursor.fetchall())

                    # 4. Column COMMENTS
                    cursor.execute("""
                        SELECT column_name, comments FROM ALL_COL_COMMENTS
                        WHERE owner = :owner AND table_name = :table_name
                    """, {'owner': owner, 'table_name': table_name})
                    col_comments = {}
                    for row in cursor.fetchall():
                        col_comments[row[0]] = row[1] if row[1] else ''

                    # 5. Column metadata
                    cursor.execute("""
                        SELECT column_name, data_type, data_length, data_precision, 
                               data_scale, nullable
                        FROM ALL_TAB_COLUMNS
                        WHERE owner = :owner AND table_name = :table_name
                        ORDER BY column_id
                    """, {'owner': owner, 'table_name': table_name})
                    
                    for row in cursor.fetchall():
                        col_name = row[0]
                        data_type = row[1]
                        data_length = row[2]
                        data_precision = row[3]
                        data_scale = row[4]
                        nullable = row[5]  # 'Y' or 'N'
                        
                        # Format Oracle type to readable string
                        if data_type in ('VARCHAR2', 'CHAR', 'NVARCHAR2', 'NCHAR'):
                            type_str = f"{data_type}({data_length})"
                        elif data_type == 'NUMBER':
                            if data_precision and data_scale and data_scale > 0:
                                type_str = f"DECIMAL({data_precision},{data_scale})"
                            elif data_precision:
                                type_str = f"NUMBER({data_precision})"
                            else:
                                type_str = "INTEGER"
                        elif data_type in ('FLOAT', 'BINARY_FLOAT', 'BINARY_DOUBLE'):
                            type_str = data_type
                        elif data_type == 'DATE':
                            type_str = 'DATE'
                        elif 'TIMESTAMP' in data_type:
                            type_str = 'TIMESTAMP'
                        elif data_type in ('CLOB', 'NCLOB', 'LONG'):
                            type_str = 'TEXT'
                        elif data_type in ('BLOB', 'RAW', 'LONG RAW'):
                            type_str = data_type
                        else:
                            type_str = data_type
                        
                        result['columns'].append({
                            'name': col_name,
                            'type': type_str,
                            'is_pk': col_name in pk_columns,
                            'is_nullable': nullable == 'Y',
                            'is_partition': col_name in partition_columns,
                            'comment': col_comments.get(col_name, ''),
                        })
            
            return result

        def format_target_type(oracle_type):
            """Convert Oracle type to target DB type."""
            t = oracle_type.upper()
            if t.startswith('VARCHAR2') or t.startswith('NVARCHAR2'):
                return oracle_type.replace('VARCHAR2', 'VARCHAR').replace('NVARCHAR2', 'VARCHAR')
            elif t.startswith('NUMBER') or t == 'INTEGER':
                return oracle_type
            elif t.startswith('DECIMAL'):
                return oracle_type
            elif t == 'DATE':
                return 'DATE'
            elif t.startswith('TIMESTAMP'):
                return 'TIMESTAMP'
            elif t == 'TEXT':
                return 'TEXT'
            elif t.startswith('CHAR') or t.startswith('NCHAR'):
                return oracle_type.replace('NCHAR', 'CHAR')
            else:
                return oracle_type

        new_mappings = []
        for table in selected_tables:
            # Parse owner.table_name
            parts = table.split('.')
            if len(parts) == 2:
                owner = parts[0].upper()
                table_name = parts[1].upper()
            else:
                owner = source_conn.username.upper() if source_conn.username else ''
                table_name = table.upper()
            
            target_table_name = table.split('.')[-1].lower()
            
            # Try to fetch real columns from DB
            col_data = None
            table_comment = ''
            try:
                if source_conn.conn_type == 'oracle':
                    col_data = get_oracle_columns(source_conn, owner, table_name)
                    table_comment = col_data.get('table_comment', '')
                    print(f"DEBUG: Fetched {len(col_data['columns'])} real columns for {table}")
            except Exception as e:
                print(f"WARNING: Failed to fetch real columns for {table}: {e}")
                import traceback
                traceback.print_exc()
                col_data = None
            
            mapping = Mapping(
                source_conn_id=source_conn_id,
                target_conn_id=target_conn_id,
                source_table=table,
                target_table=target_table_name,
                source_table_desc=table_comment,
                status='Draft'
            )
            db.session.add(mapping)
            db.session.flush()

            if col_data and col_data['columns']:
                # Use real DB columns
                has_etl_col = False
                for i, col in enumerate(col_data['columns']):
                    col_name = col['name']
                    if col_name.upper() in ('ETL_DTM', 'ETL_CRY_DTM'):
                        has_etl_col = True
                    
                    mapping_col = MappingColumn(
                        mapping_id=mapping.id,
                        source_column=col_name.upper(),
                        source_type=col['type'],
                        is_pk=col['is_pk'],
                        is_nullable=col['is_nullable'],
                        is_partition=col['is_partition'],
                        column_order=i + 1,
                        target_column=col_name.lower(),
                        target_type=format_target_type(col['type']),
                        target_logical_name=col['comment'] if col['comment'] else col_name.replace('_', ' ').capitalize(),
                        source_column_desc=col['comment'],
                    )
                    db.session.add(mapping_col)
                
                # Add ETL_CRY_DTM if not present
                if not has_etl_col:
                    etl_col = MappingColumn(
                        mapping_id=mapping.id,
                        source_column='SYSDATE',
                        source_type='SYSTEM',
                        is_pk=False,
                        is_nullable=False,
                        is_partition=False,
                        column_order=len(col_data['columns']) + 1,
                        target_column='ETL_CRY_DTM',
                        target_type='TIMESTAMP',
                        target_logical_name='ETL Creation Time',
                        source_column_desc='ETL 생성 시간',
                    )
                    db.session.add(etl_col)
            else:
                # Fallback: minimal columns if DB fetch failed
                fallback_col = MappingColumn(
                    mapping_id=mapping.id,
                    source_column='*',
                    source_type='UNKNOWN',
                    is_pk=False,
                    is_nullable=True,
                    column_order=1,
                    target_column='*',
                    target_type='UNKNOWN',
                    target_logical_name='All Columns (DB fetch failed)',
                    source_column_desc='DB 연결 실패로 컬럼 정보를 가져올 수 없습니다.',
                )
                db.session.add(fallback_col)

            new_mappings.append(mapping)
        
        db.session.commit()
        return {'status': 'success', 'message': f'{len(new_mappings)} mappings generated'}, 200


    @expose('/delete/<int:id>', methods=['POST'])
    def delete_view(self, id):
        mapping = Mapping.query.get_or_404(id)
        db.session.delete(mapping)
        db.session.commit()
        return {'status': 'success', 'message': 'Mapping deleted successfully'}, 200

    @expose('/detail/<int:id>')
    def detail_view(self, id):
        # This renders the static page (can still be used as a fallback or direct link)
        mapping = Mapping.query.get_or_404(id)
        return self.render('mapping_detail.html', mapping=mapping)

    @expose('/api/detail/<int:id>')
    def api_detail_view(self, id):
        # API to return JSON data for the modal
        mapping = Mapping.query.get_or_404(id)
        columns = []
        for col in mapping.columns:
            columns.append({
                'id': col.id,
                'source_column': col.source_column,
                'source_type': col.source_type,
                'target_column': col.target_column,
                'target_type': col.target_type,
                'column_order': col.column_order,
                'is_pk': col.is_pk,
                'is_nullable': col.is_nullable, 
                'target_logical_name': col.target_logical_name,
                'is_extraction_condition': col.is_extraction_condition,
                'is_partition': col.is_partition,
                'trans_rule': col.trans_rule,
                'source_column_desc': col.source_column_desc or ''
            })
            
        return {
            'id': mapping.id,
            'source_table': mapping.source_table,
            'target_table': mapping.target_table,
            'source_conn': f"{mapping.source_conn.name} ({mapping.source_conn.conn_type})",
            'target_conn': f"{mapping.target_conn.name} ({mapping.target_conn.conn_type})",
            'source_table_desc': mapping.source_table_desc or '',
            'columns': columns
        }, 200


    @expose('/download_excel', methods=['POST'])
    def download_excel(self):
        import pandas as pd
        from io import BytesIO
        from flask import send_file

        data = request.json
        mapping_ids = data.get('mapping_ids', [])

        if not mapping_ids:
            return {'status': 'error', 'message': 'No mappings selected'}, 400

        mappings = Mapping.query.filter(Mapping.id.in_(mapping_ids)).all()
        
        all_data = []
        for mapping in mappings:
            for col in mapping.columns:
                all_data.append({
                    'Mapping Name': mapping.source_table,
                    'Source Connection': mapping.source_conn.name,
                    'Source Table': mapping.source_table,
                    'Target Connection': mapping.target_conn.name,
                    'Target Table': mapping.target_table,
                    'Source Column': col.source_column,
                    'Source Type': col.source_type,
                    'Target Column': col.target_column,
                    'Target Type': col.target_type,
                    'Logical Name': col.target_logical_name,
                    'Is PK': 'Y' if col.is_pk else 'N',
                    'Is Nullable': 'Y' if col.is_nullable else 'N',
                    'Order': col.column_order
                })
        
        if not all_data:
             return {'status': 'error', 'message': 'No data found for selected mappings'}, 404

        df = pd.DataFrame(all_data)
        
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Mapping Definitions')
        
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='mapping_definitions.xlsx'
        )

    @expose('/generate_ddl/<int:id>', methods=['GET'])
    def generate_ddl(self, id):
        mapping = Mapping.query.get_or_404(id)
        
        # Determine target database type (default to Postgres for now if not specified in connection)
        # In a real app, we'd check mapping.target_conn.conn_type
        target_db_type = mapping.target_conn.conn_type.lower()
        
        schema_name = 'public' # Default schema
        table_name = mapping.target_table
        
        # Start constructing DDL
        ddl_lines = []
        ddl_lines.append(f"-- DDL for Table: {table_name}")
        ddl_lines.append(f"-- Target Database: {target_db_type.upper()}")
        ddl_lines.append(f"-- Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        ddl_lines.append("")
        
        if target_db_type == 'postgres':
            ddl_lines.append(f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (")
            
            col_defs = []
            pks = []
            
            for col in sorted(mapping.columns, key=lambda x: x.column_order):
                # Basic type mapping (can be expanded)
                # Ensure target_type is valid for Postgres
                data_type = col.target_type
                if 'VARCHAR2' in data_type:
                    data_type = data_type.replace('VARCHAR2', 'VARCHAR')
                elif 'NUMBER' in data_type:
                    data_type = 'NUMERIC'
                elif 'DATE' in data_type:
                    data_type = 'TIMESTAMP'
                
                line = f"    {col.target_column} {data_type}"
                
                if not col.is_nullable:
                    line += " NOT NULL"
                
                col_defs.append(line)
                
                if col.is_pk:
                    pks.append(col.target_column)
            
            ddl_lines.append(",\n".join(col_defs))
            
            if pks:
                ddl_lines.append(f"    ,CONSTRAINT pk_{table_name} PRIMARY KEY ({', '.join(pks)})")
            
            ddl_lines.append(");")
            
            # Add comments for columns if logical name exists
            ddl_lines.append("")
            for col in mapping.columns:
                if col.target_logical_name:
                    comment = col.target_logical_name.replace("'", "''")
                    ddl_lines.append(f"COMMENT ON COLUMN {schema_name}.{table_name}.{col.target_column} IS '{comment}';")

        elif target_db_type == 'oracle':
             ddl_lines.append(f"CREATE TABLE {table_name} (")
             col_defs = []
             pks = []
             for col in sorted(mapping.columns, key=lambda x: x.column_order):
                 line = f"    {col.target_column} {col.target_type}"
                 if not col.is_nullable:
                     line += " NOT NULL"
                 col_defs.append(line)
                 if col.is_pk:
                     pks.append(col.target_column)
             
             ddl_lines.append(",\n".join(col_defs))
             if pks:
                 ddl_lines.append(f"    ,CONSTRAINT pk_{table_name} PRIMARY KEY ({', '.join(pks)})")
             ddl_lines.append(");")
             
        else:
            return {'status': 'error', 'message': f'DDL generation for {target_db_type} is not yet supported.'}, 400

        return {'status': 'success', 'ddl': "\n".join(ddl_lines)}, 200


# Register views
admin.add_view(ConnectionView(Connection, db.session, name='Connections', endpoint='connections'))
admin.add_view(MappingView(name='Mappings', endpoint='mappings'))

@app.route('/api/mappings/bulk_delete', methods=['POST'])
def bulk_delete_mappings():
    data = request.json
    mapping_ids = data.get('mapping_ids', [])

    if not mapping_ids:
        return jsonify({'status': 'error', 'message': 'No mappings selected'}), 400

    success_count = 0
    errors = []
    for mid in mapping_ids:
        try:
            mapping = Mapping.query.get(mid)
            if mapping:
                db.session.delete(mapping)
                success_count += 1
            else:
                errors.append(f'Mapping ID {mid} not found')
        except Exception as e:
            errors.append(f'Mapping ID {mid}: {str(e)}')

    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return jsonify({'status': 'error', 'message': f'Database error: {str(e)}'}), 500

    if errors:
        return jsonify({'status': 'partial_success', 'message': f'Deleted {success_count} mappings. Errors: {"; ".join(errors)}'}), 207
    return jsonify({'status': 'success', 'message': f'Successfully deleted {success_count} mappings'}), 200

@app.route('/mapping_list.html')
def mapping_list_redirect():
    return redirect('/admin/mappings/')

@app.route('/api/mappings/<int:id>/update', methods=['POST'])
def api_update_mapping(id):
    mapping = Mapping.query.get_or_404(id)
    data = request.json
    columns_data = data.get('columns', [])
    
    provided_ids = [c.get('id') for c in columns_data if c.get('id')]
    
    # Delete removed columns
    for col in mapping.columns:
        if col.id not in provided_ids:
            db.session.delete(col)
            
    for col_data in columns_data:
        col_id = col_data.get('id')
        if col_id:
            # Update existing column
            col = MappingColumn.query.get(col_id)
            if col and col.mapping_id == mapping.id:
                col.source_column = col_data.get('source_column')
                col.source_type = col_data.get('source_type')
                col.target_column = col_data.get('target_column')
                col.target_type = col_data.get('target_type')
                col.target_logical_name = col_data.get('target_logical_name')
                col.column_order = col_data.get('column_order')
                col.is_pk = col_data.get('is_pk', False)
                col.is_nullable = col_data.get('is_nullable', True)
                col.is_extraction_condition = col_data.get('is_extraction_condition', False)
                col.is_partition = col_data.get('is_partition', False)
                col.trans_rule = col_data.get('trans_rule')
        else:
            # Create new column
            new_col = MappingColumn(
                mapping_id=mapping.id,
                source_column=col_data.get('source_column'),
                source_type=col_data.get('source_type'),
                target_column=col_data.get('target_column'),
                target_type=col_data.get('target_type'),
                target_logical_name=col_data.get('target_logical_name'),
                column_order=col_data.get('column_order'),
                is_pk=col_data.get('is_pk', False),
                is_nullable=col_data.get('is_nullable', True),
                is_extraction_condition=col_data.get('is_extraction_condition', False),
                is_partition=col_data.get('is_partition', False),
                trans_rule=col_data.get('trans_rule')
            )
            db.session.add(new_col)
            
    try:
        db.session.commit()
        return jsonify({'status': 'success', 'message': 'Mapping updated successfully'}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({'status': 'error', 'message': str(e)}), 500
    
@app.route('/table_selection.html')
def table_selection_redirect():
    return redirect('/admin/mappings/new')

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/connection_list.html')
def connection_list_redirect():
    return redirect('/admin/connections/')

@app.route('/add_connection.html')
def add_connection_redirect():
    return redirect('/admin/connections/new')


@app.route('/dag_template.html')
def dag_template_view():
    templates = Template.query.order_by(Template.created_at.desc()).all()
    return render_template('dag_template.html', templates=templates)

# Serve other static templates for navigation continuity
@app.route('/<page_name>.html')
def serve_pages(page_name):
    if page_name == 'dag_generation_list':
        dags = GeneratedDAG.query.order_by(GeneratedDAG.created_at.desc()).all()
        return render_template(f'{page_name}.html', dags=dags)
    return render_template(f'{page_name}.html')

@app.route('/variable_list.html')
def variable_list_view():
    variables = TemplateVariable.query.all()
    return render_template('variable_list.html', variables=variables)

@app.route('/api/templates', methods=['GET'])
def get_templates():
    templates = Template.query.order_by(Template.created_at.desc()).all()
    return jsonify([{
        'id': t.id,
        'name': t.name,
        'source_type': t.source_type,
        'target_type': t.target_type,
        'comment': t.comment
         # code omitted for list to save bandwidth if not needed, but no harm including if small
    } for t in templates]), 200

@app.route('/api/templates', methods=['POST'])
def save_template():
    data = request.json
    name = data.get('name')
    source_type = data.get('source_type')
    target_type = data.get('target_type')
    comment = data.get('comment')
    code = data.get('code')

    if not name or not code:
        return {'status': 'error', 'message': 'Name and Code are required'}, 400

    try:
        new_template = Template(
            name=name,
            source_type=source_type,
            target_type=target_type,
            comment=comment,
            code=code
        )
        db.session.add(new_template)
        db.session.commit()
        return {'status': 'success', 'message': 'Template saved successfully', 'id': new_template.id}, 201
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500

@app.route('/api/templates/<int:id>', methods=['GET'])
def get_template(id):
    template = Template.query.get_or_404(id)
    return {
        'id': template.id,
        'name': template.name,
        'source_type': template.source_type,
        'target_type': template.target_type,
        'comment': template.comment,
        'code': template.code
    }, 200

@app.route('/api/templates/<int:id>', methods=['PUT'])
def update_template(id):
    template = Template.query.get_or_404(id)
    data = request.json
    
    name = data.get('name')
    source_type = data.get('source_type')
    target_type = data.get('target_type')
    comment = data.get('comment')
    code = data.get('code')

    if not name or not code:
        return {'status': 'error', 'message': 'Name and Code are required'}, 400

    try:
        template.name = name
        template.source_type = source_type
        template.target_type = target_type
        template.comment = comment
        template.code = code
        
        db.session.commit()
        return {'status': 'success', 'message': 'Template updated successfully'}, 200
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500

@app.route('/api/templates/<int:id>', methods=['DELETE'])
def delete_template(id):
    template = Template.query.get_or_404(id)
    try:
        db.session.delete(template)
        db.session.commit()
        return {'status': 'success', 'message': 'Template deleted successfully'}, 200
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500

# Template Variable APIs
@app.route('/api/template-variables', methods=['GET'])
def get_template_variables():
    variables = TemplateVariable.query.all()
    return jsonify([{
        'id': v.id,
        'name': v.name,
        'code': v.code,
        'color': v.color,
        'icon': v.icon
    } for v in variables]), 200

@app.route('/api/template-variables', methods=['POST'])
def create_template_variable():
    data = request.json
    name = data.get('name')
    code = data.get('code')
    color = data.get('color') or 'secondary'
    icon = data.get('icon', 'fas fa-cube')

    if not name or not code:
        return {'status': 'error', 'message': 'Name and Code are required'}, 400

    # Check for duplicate name
    existing_var = TemplateVariable.query.filter_by(name=name).first()
    if existing_var:
        return {'status': 'error', 'message': f'Variable "{name}" already exists.'}, 400

    try:
        new_var = TemplateVariable(name=name, code=code, color=color, icon=icon)
        db.session.add(new_var)
        db.session.commit()
        return {'status': 'success', 'message': 'Variable created successfully', 'id': new_var.id}, 201
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500

@app.route('/api/template-variables/<int:id>', methods=['PUT'])
def update_template_variable(id):
    var = TemplateVariable.query.get_or_404(id)
    data = request.json
    
    name = data.get('name')
    code = data.get('code')
    
    if not name or not code:
        return {'status': 'error', 'message': 'Name and Code are required'}, 400
        
    # Check if name is taken by another variable
    existing_var = TemplateVariable.query.filter(TemplateVariable.name == name, TemplateVariable.id != id).first()
    if existing_var:
        return {'status': 'error', 'message': f'Variable "{name}" already exists.'}, 400

    try:
        var.name = name
        var.code = code
        var.color = data.get('color', var.color)
        var.icon = data.get('icon', var.icon)
        
        db.session.commit()
        return {'status': 'success', 'message': 'Variable updated successfully'}, 200
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500

@app.route('/api/template-variables/<int:id>', methods=['DELETE'])
def delete_template_variable(id):
    var = TemplateVariable.query.get_or_404(id)
    try:
        db.session.delete(var)
        db.session.commit()
        return {'status': 'success', 'message': 'Variable deleted successfully'}, 200
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500

@app.route('/meta_db_settings.html')
def meta_db_settings_view():
    return render_template('meta_db_settings.html')

def generate_formatted_sql(mapping):
    """Generates a formatted SQL query for a given mapping."""
    columns = MappingColumn.query.filter_by(mapping_id=mapping.id).order_by(MappingColumn.column_order).all()
    
    if not columns:
        return f"SELECT *\n  FROM {mapping.source_table}"
    
    # Format:
    # SELECT COL1
    #      , COL2
    #      , COL3
    #   FROM TABLE
    
    formatted_cols = []
    # First column
    first_col_source = columns[0].source_column
    if first_col_source == 'SYSDATE':
        first_col_source = 'SYSDATE AS ETL_CRY_DTM'
    formatted_cols.append(f"SELECT {first_col_source}")
    
    # Subsequent columns
    for col in columns[1:]:
        source_col = col.source_column
        if source_col == 'SYSDATE':
            source_col = 'SYSDATE AS ETL_CRY_DTM'
        formatted_cols.append(f"     , {source_col}")
        
    query = "\n".join(formatted_cols)
    query += f"\n  FROM {mapping.source_table}"
    
    return query

@app.route('/api/dags/preview', methods=['POST'])
def preview_dags():
    data = request.json
    mapping_ids = data.get('mapping_ids', [])

    if not mapping_ids:
        return {'status': 'error', 'message': 'Mapping IDs are required'}, 400

    previews = []
    try:
        for map_id in mapping_ids:
            mapping = Mapping.query.get(map_id)
            if not mapping:
                continue

            source_sql = generate_formatted_sql(mapping)
            previews.append({
                'mapping_name': f"{mapping.source_table} -> {mapping.target_table}",
                'source_sql': source_sql
            })
            
        return {'status': 'success', 'previews': previews}, 200

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'status': 'error', 'message': str(e)}, 500

@app.route('/api/dags/<int:id>', methods=['DELETE'])
def delete_dag(id):
    with open('dags_generation_debug.log', 'a') as log_file:
        log_file.write(f"DELETE request for DAG ID: {id}\n")

    dag = GeneratedDAG.query.get(id)
    if not dag:
        return {'status': 'error', 'message': 'DAG not found'}, 404
        
    try:
        # Try to remove the file if it exists
        if dag.filepath and os.path.exists(dag.filepath):
            try:
                os.remove(dag.filepath)
            except Exception as e:
                print(f"Warning: Could not delete file {dag.filepath}: {e}")
        
        db.session.delete(dag)
        db.session.commit()
        return {'status': 'success', 'message': 'DAG deleted successfully'}, 200
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500

@app.route('/api/dags/bulk_delete', methods=['POST'])
def bulk_delete_dags():
    data = request.json
    dag_ids = data.get('dag_ids', [])
    
    if not dag_ids:
        return {'status': 'error', 'message': 'No DAG IDs provided'}, 400

    success_count = 0
    errors = []

    for dag_id in dag_ids:
        try:
            dag = GeneratedDAG.query.get(dag_id)
            if not dag:
                continue
            
            # Try to remove the file if it exists
            if dag.filepath and os.path.exists(dag.filepath):
                try:
                    os.remove(dag.filepath)
                except Exception as e:
                    print(f"Warning: Could not delete file {dag.filepath}: {e}")
            
            db.session.delete(dag)
            success_count += 1
        except Exception as e:
            errors.append(f"Error deleting DAG {dag_id}: {str(e)}")
    
    try:
        db.session.commit()
        if errors:
            return {'status': 'partial_success', 'message': f'Deleted {success_count} DAGs. Errors: {"; ".join(errors)}'}, 207
        return {'status': 'success', 'message': f'Successfully deleted {success_count} DAGs'}, 200
    except Exception as e:
        db.session.rollback()
        return {'status': 'error', 'message': f'Database commit failed: {str(e)}'}, 500

@app.before_request
def log_request_info():
    if request.path.startswith('/api/'):
        with open('dags_generation_debug.log', 'a') as log_file:
            log_file.write(f"API Request: {request.method} {request.path}\n")

@app.route('/api/dags/<string:id_str>/code', methods=['GET'])
def get_dag_code_string(id_str):
    with open('dags_generation_debug.log', 'a') as log_file:
        log_file.write(f"GET CODE request for STRING ID: {id_str}\n")
    return jsonify({'status': 'error', 'message': 'Invalid DAG ID format. ID must be an integer.'}), 400

@app.route('/api/dags/<int:id>/code', methods=['GET'])
def get_dag_code(id):
    with open('dags_generation_debug.log', 'a') as log_file:
        log_file.write(f"GET CODE request for DAG ID: {id}\n")

    dag = GeneratedDAG.query.get(id)
    if not dag:
        return {'status': 'error', 'message': 'DAG not found'}, 404
    
    if not dag.filepath or not os.path.exists(dag.filepath):
        return {'status': 'error', 'message': 'DAG file not found on server'}, 404
        
    try:
        with open(dag.filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        return {'filename': dag.filename, 'code': content}, 200
    except Exception as e:
        return {'status': 'error', 'message': f'Error reading file: {str(e)}'}, 500

@app.route('/api/dags/<int:id>/download', methods=['GET'])
def download_dag(id):
    dag = GeneratedDAG.query.get(id)
    if not dag:
        return {'status': 'error', 'message': 'DAG not found'}, 404
    
    if not dag.filepath or not os.path.exists(dag.filepath):
            return {'status': 'error', 'message': 'DAG file not found on server'}, 404
            
    return send_file(
        dag.filepath,
        as_attachment=True,
        download_name=dag.filename,
        mimetype='application/x-python-code'
    )

@app.route('/api/dags/generate', methods=['POST'])
def generate_dags():
    # Debug Logging
    with open('dags_generation_debug.log', 'a') as log_file:
        log_file.write(f"Received generate request at {datetime.now()}\n")
        
    try:
        data = request.json
        # ... (no changes to data validation) ...
        template_id = data.get('template_id')
        mapping_ids = data.get('mapping_ids', [])

        if not template_id or not mapping_ids:
            return {'status': 'error', 'message': 'Template ID and Mapping IDs are required'}, 400

        # ---------------------------------------------------------
        # 1. READ PHASE: Fetch all necessary data into memory
        # ---------------------------------------------------------
        with open('dags_generation_debug.log', 'a') as log_file:
            log_file.write("Starting Read Phase\n")
            
            
    
        template = Template.query.get(template_id)
        if not template:
            return {'status': 'error', 'message': 'Template not found'}, 404
        
        template_code = template.code
        template_name = template.name
        template_id_val = template.id

        mappings_data = []
        for map_id in mapping_ids:
            mapping = Mapping.query.get(map_id)
            if not mapping:
                continue
            
            source_sql = generate_formatted_sql(mapping)
            
            mapping_info = {
                'id': mapping.id,
                'source_table': mapping.source_table,
                'target_table': mapping.target_table,
                'source_conn_name': mapping.source_conn.name if mapping.source_conn else 'Unknown',
                'target_conn_name': mapping.target_conn.name if mapping.target_conn else 'Unknown',
                'source_sql': source_sql
            }
            mappings_data.append(mapping_info)
        
        # Use rollback to release locks without destroying the session factory
        db.session.rollback()

        with open('dags_generation_debug.log', 'a') as log_file:
            log_file.write(f"Data fetch complete. Found {len(mappings_data)} mappings. Locks released.\n")

        # ---------------------------------------------------------
        # 2. GENERATE & WRITE PHASE
        # ---------------------------------------------------------
        generated_files = []
        success_count = 0
        output_dir = os.path.join(app.root_path, 'dags_output')
        os.makedirs(output_dir, exist_ok=True)
        
        with open('dags_generation_debug.log', 'a') as log_file:
            log_file.write(f"Output dir validated. Starting loop.\n")

        for m_data in mappings_data:
            with open('dags_generation_debug.log', 'a') as log_file:
                log_file.write(f"Generating code for mapping {m_data['id']}\n")
                
            try:
                # Generate Code
                dag_code = template_code
                
                # Filename & Dag Name Generation
                safe_source = "".join([c if c.isalnum() or c in ('_', '-') else '_' for c in m_data['source_conn_name']])
                safe_table = "".join([c if c.isalnum() or c in ('_', '-') else '_' for c in m_data['source_table']])
                timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                dag_name = f"dag_{safe_source}_{safe_table}_{timestamp_str}"
                filename = f"{dag_name}.py"
                filepath = os.path.join(output_dir, filename)

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
                
                for key, value in replacements.items():
                    dag_code = dag_code.replace(key, value)
                
                with open('dags_generation_debug.log', 'a') as log_file:
                        log_file.write(f"About to write file: {filepath}\n")

                # Write File
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(dag_code)
                
                with open('dags_generation_debug.log', 'a') as log_file:
                    log_file.write(f"File created successfully: {filepath}\n")

                # ---------------------------------------------------------
                # 3. DB UPDATE PHASE
                # ---------------------------------------------------------
                try:
                    with open('dags_generation_debug.log', 'a') as log_file:
                        log_file.write(f"Attempting DB save for {m_data['id']}\n")
                        
                    new_dag = GeneratedDAG(
                        filename=filename,
                        filepath=filepath,
                        template_id=template_id_val,
                        mapping_id=m_data['id'],
                        status='Generated'
                    )
                    db.session.add(new_dag)
                    db.session.commit()
                    
                    with open('dags_generation_debug.log', 'a') as log_file:
                        log_file.write(f"Committed DB for mapping {m_data['id']}\n")
                        
                except Exception as commit_e:
                    db.session.rollback()
                    with open('dags_generation_debug.log', 'a') as log_file:
                        log_file.write(f"WARNING: DB Commit failed for mapping {m_data['id']}: {commit_e}\n")
                
                generated_files.append(filename)
                success_count += 1

            except Exception as inner_e:
                error_msg = str(inner_e)
                with open('dags_generation_debug.log', 'a') as log_file:
                    log_file.write(f"ERROR processing mapping {m_data['id']}: {error_msg}\n")
                
                try:
                    error_dag = GeneratedDAG(
                        filename='Error',
                        filepath='Error',
                        template_id=template_id_val,
                        mapping_id=m_data['id'],
                        status='Error',
                        error_message=error_msg
                    )
                    db.session.add(error_dag)
                    db.session.commit()
                except:
                    pass
                continue

        return {'status': 'success', 'message': f'Successfully generated {success_count} DAGs.', 'generated_files': generated_files}, 200

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"CRITICAL ERROR in generate_dags: {e}")
        with open('dags_generation_debug.log', 'a') as log_file:
            log_file.write(f"CRITICAL ERROR: {e}\n{tb}\n")
            
        return {'status': 'error', 'message': f'Server Error: {str(e)}'}, 500

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
        # Create dummy data if empty
        if not Connection.query.first():
            dummy1 = Connection(name='Oracle_Prod_DB', conn_type='oracle', host='192.168.1.10', port=1521, database='ORCL', status='Active')
            dummy2 = Connection(name='Postgres_DW', conn_type='postgres', host='10.0.0.5', port=5432, database='warehouse', status='Active')
            db.session.add_all([dummy1, dummy2])
            db.session.commit()
            
    app.run(debug=True, use_reloader=False, port=5000)

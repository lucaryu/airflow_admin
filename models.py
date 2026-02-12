from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

class Connection(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    conn_type = db.Column(db.String(50), nullable=False)
    host = db.Column(db.String(255))
    port = db.Column(db.Integer)
    database = db.Column(db.String(100))
    username = db.Column(db.String(100))
    password = db.Column(db.String(255)) # In production, this should be encrypted
    status = db.Column(db.String(20), default='Inactive')
    last_used = db.Column(db.DateTime, nullable=True)

    def __repr__(self):
        return f'<Connection {self.name}>'

class Mapping(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    source_conn_id = db.Column(db.Integer, db.ForeignKey('connection.id'), nullable=False)
    target_conn_id = db.Column(db.Integer, db.ForeignKey('connection.id'), nullable=False)
    source_table = db.Column(db.String(255), nullable=False)
    target_table = db.Column(db.String(255), nullable=False)
    source_table_desc = db.Column(db.String(500), nullable=True)  # Table COMMENTS from DB
    status = db.Column(db.String(50), default='Draft')
    created_at = db.Column(db.DateTime, default=db.func.current_timestamp())

    source_conn = db.relationship('Connection', foreign_keys=[source_conn_id], backref='source_mappings')
    target_conn = db.relationship('Connection', foreign_keys=[target_conn_id], backref='target_mappings')
    columns = db.relationship('MappingColumn', backref='mapping', cascade='all, delete-orphan')

    def __repr__(self):
        return f'<Mapping {self.source_table} -> {self.target_table}>'

class MappingColumn(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    mapping_id = db.Column(db.Integer, db.ForeignKey('mapping.id'), nullable=False)
    source_column = db.Column(db.String(255), nullable=False)
    source_type = db.Column(db.String(50))
    is_pk = db.Column(db.Boolean, default=False)
    is_nullable = db.Column(db.Boolean, default=True)
    column_order = db.Column(db.Integer)
    target_column = db.Column(db.String(255))
    target_type = db.Column(db.String(50))
    target_logical_name = db.Column(db.String(255)) # Added logical name
    source_column_desc = db.Column(db.String(500), nullable=True)  # Column COMMENTS from DB
    is_extraction_condition = db.Column(db.Boolean, default=False)
    is_partition = db.Column(db.Boolean, default=False)
    trans_rule = db.Column(db.String(500), nullable=True)

    def __repr__(self):
        return f'<MappingColumn {self.source_column} -> {self.target_column}>'

class Template(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    source_type = db.Column(db.String(50), nullable=False)
    target_type = db.Column(db.String(50), nullable=False)
    comment = db.Column(db.Text, nullable=True)
    code = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, default=db.func.current_timestamp())

    def __repr__(self):
        return f'<Template {self.name}>'

class TemplateVariable(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), nullable=False)
    code = db.Column(db.String(100), nullable=False)
    color = db.Column(db.String(20), default='secondary') # e.g., 'primary', 'secondary', 'success', 'danger', 'warning', 'info', or hex code
    icon = db.Column(db.String(50), default='fas fa-cube') # FontAwesome class

    def __repr__(self):
        return f'<TemplateVariable {self.name}>'

class GeneratedDAG(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(255), nullable=False)
    filepath = db.Column(db.String(500), nullable=False)
    template_id = db.Column(db.Integer, db.ForeignKey('template.id'), nullable=True) # Nullable in case template is deleted
    mapping_id = db.Column(db.Integer, db.ForeignKey('mapping.id'), nullable=True) # Link to source mapping
    status = db.Column(db.String(50), default='Generated') # Generated, Deployed, Error
    error_message = db.Column(db.Text, nullable=True) # To store error details if failed
    created_at = db.Column(db.DateTime, default=db.func.current_timestamp())

    template = db.relationship('Template', backref='generated_dags')
    mapping = db.relationship('Mapping', backref='generated_dags')

    def __repr__(self):
        return f'<GeneratedDAG {self.filename}>'

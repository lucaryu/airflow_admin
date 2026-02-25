import queue
import os
import sys
from jupyter_client import KernelManager

SETUP_CODE = """
import os
os.environ['AIRFLOW__CORE__LOAD_EXAMPLES'] = 'False'
os.environ['AIRFLOW__CORE__UNIT_TEST_MODE'] = 'True'
os.environ['AIRFLOW_HOME'] = os.path.abspath(os.path.join(os.getcwd(), 'airflow_home'))

import sqlite3
import json

DB_PATH = os.path.abspath(os.path.join(os.getcwd(), 'instance/toy_airflow.db'))

class FakeConnection:
    def __init__(self, row):
        self.conn_id = row[0]
        self.conn_type = row[1]
        self.host = row[2]
        self.port = row[3]
        self.schema = row[4]
        self.login = row[5]
        self.password = row[6]

class MockBaseHook:
    @classmethod
    def get_connection(cls, conn_id):
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT name, conn_type, host, port, database, username, password FROM connection WHERE name=?", (conn_id,))
        row = cur.fetchone()
        conn.close()
        if not row:
            raise ValueError(f"Connection '{conn_id}' not found in Toy Airflow DB")
        return FakeConnection(row)

class MockVariable:
    @classmethod
    def get(cls, key, default_var=None, deserialize_json=False):
        # We can simulate variables from a specific table if needed, using default for now
        if default_var is not None:
            return default_var
        raise KeyError(f"Variable '{key}' does not exist")

try:
    import airflow.hooks.base
    import airflow.models
    airflow.hooks.base.BaseHook = MockBaseHook
    airflow.models.Variable = MockVariable
except ImportError:
    pass # If airflow isn't fully loaded, ignore
"""

class JupyterKernelManager:
    def __init__(self):
        import asyncio
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            
        self.km = KernelManager(kernel_name='python3')
        
        # Merge with existing OS environment so Python can find its paths
        env = os.environ.copy()
        env['AIRFLOW_HOME'] = os.path.abspath(os.path.join(os.getcwd(), 'airflow_home'))
        
        self.km.start_kernel(env=env)
        self.kc = self.km.client()
        self.kc.start_channels()
        self.kc.wait_for_ready(timeout=10)
        self.execute_code(SETUP_CODE)  # Inject context

    def execute_code(self, code, timeout=30):
        # Execute the code in the kernel
        msg_id = self.kc.execute(code)
        
        reply_content = None
        outputs = []
        
        while True:
            try:
                # Poll for messages from the iopub channel
                msg = self.kc.get_iopub_msg(timeout=timeout)
                msg_type = msg['header']['msg_type']
                content = msg['content']
                
                if msg_type == 'stream':
                    outputs.append({
                        'type': 'stream',
                        'name': content.get('name', 'stdout'),
                        'text': content.get('text', '')
                    })
                elif msg_type == 'execute_result':
                    outputs.append({
                        'type': 'execute_result',
                        'data': content.get('data', {}).get('text/plain', '')
                    })
                elif msg_type == 'display_data':
                    outputs.append({
                        'type': 'display_data',
                        'data': content.get('data', {}).get('text/plain', '')
                    })
                elif msg_type == 'error':
                    outputs.append({
                        'type': 'error',
                        'ename': content.get('ename', ''),
                        'evalue': content.get('evalue', ''),
                        'traceback': content.get('traceback', [])
                    })
                elif msg_type == 'status':
                    if content['execution_state'] == 'idle':
                        break
            except queue.Empty:
                outputs.append({'type': 'error', 'ename': 'Timeout', 'evalue': 'Execution timed out.', 'traceback': []})
                break
                
        # Also get the reply from the shell channel to check if execution was successful
        try:
            while True:
                reply = self.kc.get_shell_msg(timeout=1)
                if reply['parent_header'].get('msg_id') == msg_id:
                    reply_content = reply['content']
                    break
        except queue.Empty:
            pass

        # Parse outputs back into stdout/stderr/returncode to be somewhat compatible,
        # but also provide structured outputs for the cells
        stdout = ""
        stderr = ""
        for out in outputs:
            if out['type'] == 'stream':
                if out['name'] == 'stdout':
                    stdout += out['text']
                else:
                    stderr += out['text']
            elif out['type'] == 'error':
                stderr += "\n".join(out['traceback'])
            elif out['type'] in ('execute_result', 'display_data'):
                stdout += str(out['data']) + "\n"
                
        status = reply_content.get('status', 'ok') if reply_content else 'unknown'
        
        return {
            'stdout': stdout,
            'stderr': stderr,
            'returncode': 0 if status == 'ok' else 1,
            'outputs': outputs,
            'status': status
        }

    def restart_kernel(self):
        self.km.restart_kernel()
        self.kc = self.km.client()
        self.kc.start_channels()
        self.kc.wait_for_ready(timeout=10)
        self.execute_code(SETUP_CODE)

    def shutdown(self):
        self.kc.stop_channels()
        self.km.shutdown_kernel()

_kernel_manager_instance = None

def get_kernel_manager():
    global _kernel_manager_instance
    if _kernel_manager_instance is None:
        _kernel_manager_instance = JupyterKernelManager()
    return _kernel_manager_instance

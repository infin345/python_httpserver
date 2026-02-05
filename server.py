import http.server
import socketserver
from string import Template
import urllib.request
import urllib.error
import urllib.parse
import csv
import json
import base64
from datetime import datetime

# Airflow Configuration
# Airflow Configuration
AIRFLOW_API_URL = "http://localhost:8080/api/v1"
AIRFLOW_USER = "airflow"
AIRFLOW_PASS = "airflow"
AIRFLOW_AUTH_URL = "http://localhost:8080/api/v1/security/oauth/token" 

# 1. HTML 템플릿
html_layout = """
<html>
<head>
    <title>Airflow Monitor</title>
    <style>
        body { font-family: sans-serif; padding: 20px; }
        table { border-collapse: collapse; width: 100%; margin-top: 20px; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        tr.dag-row:hover { background-color: #f5f5f5; cursor: pointer; }
        .row-success { background-color: #d4edda; }
        .row-failed { background-color: #f8d7da; }
        .detail-row { display: none; background-color: #fafafa; }
        .task-table { width: 95%; margin: 10px auto; border: 1px solid #ccc; }
        .task-table th { background-color: #e0e0e0; }
        /* Modal styles */
        .modal { display: none; position: fixed; z-index: 1; left: 0; top: 0; width: 100%; height: 100%; overflow: auto; background-color: rgba(0,0,0,0.4); }
        .modal-content { background-color: #fefefe; margin: 10% auto; padding: 20px; border: 1px solid #888; width: 80%; max-height: 70vh; overflow-y: auto; }
        .close { color: #aaa; float: right; font-size: 28px; font-weight: bold; cursor: pointer; }
        .close:hover { color: black; }
        pre { white-space: pre-wrap; word-wrap: break-word; background: #f4f4f4; padding: 10px; }
    </style>
    <script>
        async function fetchTasks(dagId, dagRunId, rowId) {
            const detailRow = document.getElementById(rowId);
            const contentDiv = document.getElementById(rowId + '-content');
            
            if (detailRow.style.display === 'table-row') {
                detailRow.style.display = 'none';
                return;
            }
            console.log(dagId, dagRunId)
            detailRow.style.display = 'table-row';
            contentDiv.innerHTML = 'Loading tasks...';
            
            try {
                const response = await fetch(`/api/tasks?dag_id=$${encodeURIComponent(dagId)}&dag_run_id=$${encodeURIComponent(dagRunId)}`);
                const tasks = await response.json();
                
                let html = '<table class="task-table"><thead><tr><th>Task ID</th><th>State</th><th>Try Number</th><th>Action</th></tr></thead><tbody>';
                if (tasks.length === 0) {
                     html += '<tr><td colspan="4">No tasks found</td></tr>';
                } else {
                    tasks.forEach(task => {
                        let rowClass = "";
                        if (task.state === "success") {
                            rowClass = "row-success";
                        } else if (task.state === "failed") {
                            rowClass = "row-failed";
                        }
                        html += `<tr class="$${rowClass}">
                            <td>$${task.task_id}</td>
                            <td>$${task.state}</td>
                            <td>$${task.try_number}</td>
                            <td><button onclick="event.stopPropagation(); fetchLog('$${dagId}', '$${dagRunId}', '$${task.task_id}', $${task.try_number})">View Log</button></td>
                        </tr>`;
                    });
                }
                html += '</tbody></table>';
                contentDiv.innerHTML = html;
            } catch (error) {
                console.error(error);
                contentDiv.innerHTML = 'Error loading tasks: ' + error;
            }
        }

        async function fetchLog(dagId, dagRunId, taskId, tryNumber) {
            const modal = document.getElementById('logModal');
            const logContent = document.getElementById('logContent');
            
            modal.style.display = "block";
            logContent.textContent = "Loading logs...";
            
            try {
                const response = await fetch(`/api/logs?dag_id=$${encodeURIComponent(dagId)}&dag_run_id=$${encodeURIComponent(dagRunId)}&task_id=$${encodeURIComponent(taskId)}&try_number=$${encodeURIComponent(tryNumber)}`);
                const data = await response.json();
                logContent.textContent = data.content;
            } catch (error) {
                console.error(error);
                logContent.textContent = 'Error loading logs: ' + error;
            }
        }

        function closeModal() {
            document.getElementById('logModal').style.display = "none";
        }
        
        window.onclick = function(event) {
            const modal = document.getElementById('logModal');
            if (event.target == modal) {
                modal.style.display = "none";
            }
        }
    </script>
</head>
<body>
    <h1>Airflow On-prem Status</h1>
    <table border="1">
        <tr><th>DAG ID</th><th>State</th><th>Execution Date</th></tr>
        ${table_rows}
    </table>
    
    <!-- Log Modal -->
    <div id="logModal" class="modal">
        <div class="modal-content">
            <span class="close" onclick="closeModal()">&times;</span>
            <h2>Task Log</h2>
            <pre id="logContent"></pre>
        </div>
    </div>
</body>
</html>
"""

def get_airflow_token(username, password):
    try:
        credentials = f"{username}:{password}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        return encoded_credentials
    except Exception as e:
        print(f"Error fetching token: {e}")
        return None

def get_latest_dag_status(dag_id, token):
    try:
        url = f"{AIRFLOW_API_URL}/dags/{dag_id}/dagRuns?limit=1&order_by=-execution_date"
        headers = { "Authorization": f"Basic {token}", "Content-Type": "application/json" }
        req = urllib.request.Request(url, headers=headers)
        
        with urllib.request.urlopen(req, timeout=5) as response:
            if response.status == 200:
                data = json.loads(response.read().decode())
                dag_runs = data.get("dag_runs", [])
                if dag_runs:
                    latest_run = dag_runs[0]
                    return {
                        "dag_id": dag_id,
                        "dag_run_id": latest_run.get("dag_run_id"),
                        "state": latest_run.get("state"),
                        "execution_date": latest_run.get("execution_date")
                    }
    except Exception as e:
        print(f"Error fetching dag status for {dag_id}: {e}")
    
    return {"dag_id": dag_id, "state": "N/A", "execution_date": "N/A", "dag_run_id": None}

def get_dag_tasks(dag_id, dag_run_id, token):
    try:
        # URL parsing to handle special chars in dag_run_id if necessary, but urllib.parse.quote helps
        safe_dag_run_id = urllib.parse.quote(dag_run_id)
        url = f"{AIRFLOW_API_URL}/dags/{dag_id}/dagRuns/{safe_dag_run_id}/taskInstances"
        headers = { "Authorization": f"Basic {token}", "Content-Type": "application/json" }
        req = urllib.request.Request(url, headers=headers)
        
        with urllib.request.urlopen(req, timeout=5) as response:
            if response.status == 200:
                data = json.loads(response.read().decode())
                return data.get("task_instances", [])
    except Exception as e:
        print(f"Error fetching tasks for {dag_id}: {e}")
    return []

def get_task_log(dag_id, dag_run_id, task_id, try_number, token):
    try:
        safe_dag_run_id = urllib.parse.quote(dag_run_id)
        # Note: endpoint for logs might be different versions. Assuming /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{task_try_number}
        url = f"{AIRFLOW_API_URL}/dags/{dag_id}/dagRuns/{safe_dag_run_id}/taskInstances/{task_id}/logs/{try_number}"
        headers = { "Authorization": f"Basic {token}", "Content-Type": "application/json" }
        req = urllib.request.Request(url, headers=headers)
        
        with urllib.request.urlopen(req, timeout=5) as response:
            if response.status == 200:
                data = json.loads(response.read().decode())
                # Log response format depends on config, sometimes it's text/plain, sometimes json
                return data.get("content", str(data))
    except Exception as e:
        print(f"Error fetching log: {e}")
        return str(e)

class MyHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        token = get_airflow_token(AIRFLOW_USER, AIRFLOW_PASS)
        parsed_path = urllib.parse.urlparse(self.path)
        path = parsed_path.path
        query = urllib.parse.parse_qs(parsed_path.query)
        
        if path == "/api/tasks":
            dag_id = query.get("dag_id", [None])[0]
            dag_run_id = query.get("dag_run_id", [None])[0]
            if dag_id and dag_run_id:
                tasks = get_dag_tasks(dag_id, dag_run_id, token)
                response_data = []
                for t in tasks:
                    response_data.append({
                        "task_id": t.get("task_id"),
                        "state": t.get("state"),
                        "try_number": t.get("try_number")
                    })
                self.send_json(response_data)
            else:
                self.send_error(400, "Missing dag_id or dag_run_id")
            return

        if path == "/api/logs":
            dag_id = query.get("dag_id", [None])[0]
            dag_run_id = query.get("dag_run_id", [None])[0]
            task_id = query.get("task_id", [None])[0]
            try_number = query.get("try_number", [None])[0]
            
            if dag_id and dag_run_id and task_id:
                content = get_task_log(dag_id, dag_run_id, task_id, try_number, token)
                self.send_json({"content": content})
            else:
                self.send_error(400, "Missing parameters")
            return

        # Default: Render HTML Table
        rows_html = ""
        try:
            with open('dags.csv', mode='r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for i, row in enumerate(reader):
                    dag_id = row.get('dag_id')
                    if dag_id:
                        status_info = get_latest_dag_status(dag_id, token)
                        row_id = f"row-{i}"
                        dag_run_id = status_info['dag_run_id']
                        
                        row_class = "dag-row"
                        if status_info['state'] == 'success':
                            row_class += " row-success"
                        elif status_info['state'] == 'failed':
                            row_class += " row-failed"
                        
                        onclick = ""
                        if dag_run_id:
                            onclick = f"onclick=\"fetchTasks('{dag_id}', '{dag_run_id}', '{row_id}-detail')\""
                        
                        rows_html += f"""
                        <tr class="{row_class}" {onclick}>
                            <td>{status_info['dag_id']}</td>
                            <td>{status_info['state']}</td>
                            <td>{status_info['execution_date']}</td>
                        </tr>
                        <tr id="{row_id}-detail" class="detail-row">
                            <td colspan="3">
                                <div id="{row_id}-detail-content"></div>
                            </td>
                        </tr>
                        """
        except Exception as e:
            rows_html = f"<tr><td colspan='3'>Error: {str(e)}</td></tr>"
        
        t = Template(html_layout)
        display_html = t.substitute(table_rows=rows_html)

        self.send_response(200)
        self.send_header("Content-type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(display_html.encode('utf-8'))

    def send_json(self, data):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode('utf-8'))

# 5. 서버 실행 (포트 8000)
PORT = 8000
socketserver.TCPServer.allow_reuse_address = True
with socketserver.TCPServer(("", PORT), MyHandler) as httpd:
    print(f"Serving at port {PORT}")

    httpd.serve_forever()
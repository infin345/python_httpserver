import http.server
import socketserver
# from string import Template # Not really needed anymore as we don't do server-side template substitution
import urllib.request
import urllib.error
import urllib.parse
import json
import base64
from datetime import datetime

# Airflow Configuration
AIRFLOW_API_URL = "http://localhost:8080/api/v1"
AIRFLOW_USER = "airflow"
AIRFLOW_PASS = "airflow"

# HTML Template with Client-Side Logic
html_layout = """
<html>
<head>
    <title>Airflow Monitor (Remote)</title>
    <style>
        body { font-family: sans-serif; padding: 20px; }
        .controls { margin-bottom: 20px; padding: 10px; background: #f0f0f0; border-radius: 5px; }
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
        pre { white-space: pre-wrap; word-wrap: break-word; background: #2d2d2d; color: #f8f8f2; padding: 15px; border-radius: 5px; font-family: monospace; overflow-x: auto; max-height: 60vh; }
    </style>
    <script>
        // --- 1. Client-Side DAG Management ---
        const STORE_KEY = 'my_dags';

        window.onload = function() {
            loadDags();
        };

        function importCsv() {
            const fileInput = document.getElementById('csvInput');
            const file = fileInput.files[0];
            if (!file) {
                alert("Please select a dags.csv file.");
                return;
            }

            const reader = new FileReader();
            reader.onload = function(e) {
                const text = e.target.result;
                const lines = text.split('\\n');
                const dags = [];
                
                // Simple CSV parsing (assuming 'dag_id' header or just list)
                lines.forEach(line => {
                    const cleanLine = line.trim();
                    if (cleanLine && cleanLine !== 'dag_id' && !cleanLine.startsWith('#')) {
                        // Handle possible CSV format (dag_id, ...) - take first column
                        const dagId = cleanLine.split(',')[0].trim();
                        if (dagId) dags.push(dagId);
                    }
                });

                if (dags.length > 0) {
                    localStorage.setItem(STORE_KEY, JSON.stringify(dags));
                    alert(`Imported ${dags.length} DAGs.`);
                    loadDags();
                } else {
                    alert("No valid DAG IDs found in file.");
                }
            };
            reader.readAsText(file);
        }

        function clearDags() {
            if (confirm("Clear all tracked DAGs?")) {
                localStorage.removeItem(STORE_KEY);
                loadDags();
            }
        }

        async function loadDags() {
            const stored = localStorage.getItem(STORE_KEY);
            const tableBody = document.getElementById('dagTableBody');
            tableBody.innerHTML = ''; // Clear table

            if (!stored) {
                tableBody.innerHTML = '<tr><td colspan="3">No DAGs tracked. Import CSV to start.</td></tr>';
                return;
            }

            const dags = JSON.parse(stored);
            
            // Render rows first (loading state)
            dags.forEach((dagId, index) => {
                const rowId = `row-${index}`;
                const html = `
                    <tr id="${rowId}" class="dag-row">
                        <td>${dagId}</td>
                        <td id="${rowId}-state">Loading...</td>
                        <td id="${rowId}-date">-</td>
                    </tr>
                    <tr id="${rowId}-detail" class="detail-row">
                        <td colspan="3"><div id="${rowId}-detail-content"></div></td>
                    </tr>
                `;
                tableBody.insertAdjacentHTML('beforeend', html);
                
                // Fetch status individually (could be optimized to bulk if needed)
                fetchStatus(dagId, rowId);
            });
        }

        async function fetchStatus(dagId, rowId) {
            try {
                const response = await fetch(`/api/status?dag_id=${encodeURIComponent(dagId)}`);
                const data = await response.json();
                
                const row = document.getElementById(rowId);
                const stateCell = document.getElementById(`${rowId}-state`);
                const dateCell = document.getElementById(`${rowId}-date`);
                
                stateCell.textContent = data.state;
                dateCell.textContent = data.execution_date;
                
                // Color coding
                row.classList.remove('row-success', 'row-failed');
                if (data.state === 'success') row.classList.add('row-success');
                else if (data.state === 'failed') row.classList.add('row-failed');

                // Add click handler for details
                if (data.dag_run_id) {
                    row.onclick = function() { fetchRuns(dagId, `${rowId}-detail`); };
                }
                
            } catch (e) {
                console.error(`Failed to fetch status for ${dagId}`, e);
                document.getElementById(`${rowId}-state`).textContent = "Error";
            }
        }

        // --- 2. Detail View Logic (Runs/Tasks/Logs) ---
        
        async function fetchRuns(dagId, rowId) {
            const detailRow = document.getElementById(rowId);
            const contentDiv = document.getElementById(rowId + '-content');
            
            if (detailRow.style.display === 'table-row') {
                detailRow.style.display = 'none';
                return;
            }
            
            detailRow.style.display = 'table-row';
            contentDiv.innerHTML = 'Loading runs...';
            
            try {
                const response = await fetch(`/api/runs?dag_id=${encodeURIComponent(dagId)}`);
                const runs = await response.json();
                
                let html = '<table class="run-table" style="width:90%; margin:auto;"><thead><tr><th>Run ID</th><th>State</th><th>Execution Date</th></tr></thead><tbody>';
                if (runs.length === 0) {
                     html += '<tr><td colspan="3">No runs found</td></tr>';
                } else {
                    runs.forEach((run, index) => {
                        let rowClass = "";
                        if (run.state === "success") {
                            rowClass = "row-success";
                        } else if (run.state === "failed") {
                            rowClass = "row-failed";
                        }
                        
                        const uniqueRunId = rowId + '-run-' + index;
                        
                        // Note: Using $$ for Python template compatibility NOT needed here since we are not using Template()
                        // But we are in a raw string, so straightforward JS syntax is fine.
                        html += `<tr class="${rowClass}" onclick="fetchTasks('${dagId}', '${run.dag_run_id}', '${uniqueRunId}')" style="cursor:pointer;">
                            <td>${run.dag_run_id}</td>
                            <td>${run.state}</td>
                            <td>${run.execution_date}</td>
                        </tr>
                        <tr id="${uniqueRunId}" class="detail-row">
                            <td colspan="3"><div id="${uniqueRunId}-content"></div></td>
                        </tr>`;
                    });
                }
                html += '</tbody></table>';
                contentDiv.innerHTML = html;
            } catch (error) {
                console.error(error);
                contentDiv.innerHTML = 'Error loading runs: ' + error;
            }
        }

        async function fetchTasks(dagId, dagRunId, rowId) {
            const detailRow = document.getElementById(rowId);
            const contentDiv = document.getElementById(rowId + '-content');
            
            if (detailRow.style.display === 'table-row') {
                detailRow.style.display = 'none';
                return;
            }
            
            // Prevent event bubbling from closing parent
            window.event.stopPropagation();

            detailRow.style.display = 'table-row';
            contentDiv.innerHTML = 'Loading tasks...';
            
            try {
                const response = await fetch(`/api/tasks?dag_id=${encodeURIComponent(dagId)}&dag_run_id=${encodeURIComponent(dagRunId)}`);
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
                        html += `<tr class="${rowClass}">
                            <td>${task.task_id}</td>
                            <td>${task.state}</td>
                            <td>${task.try_number}</td>
                            <td><button onclick="event.stopPropagation(); fetchLog('${dagId}', '${dagRunId}', '${task.task_id}', ${task.try_number})">View Log</button></td>
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
                const response = await fetch(`/api/logs?dag_id=${encodeURIComponent(dagId)}&dag_run_id=${encodeURIComponent(dagRunId)}&task_id=${encodeURIComponent(taskId)}&try_number=${encodeURIComponent(tryNumber)}`);
                const data = await response.json();
                
                let cleanContent = data.content;
                if (typeof cleanContent === 'string') {
                    // Replace literal \\n with actual newline
                    cleanContent = cleanContent.replace(/\\\\n/g, '\\n');
                }
                logContent.textContent = cleanContent;
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
    <h1>Airflow On-prem Status (Client Managed)</h1>
    
    <div class="controls">
        <label>Import dags.csv: <input type="file" id="csvInput" accept=".csv" onchange="importCsv()"></label>
        <button onclick="clearDags()">Clear List</button>
    </div>

    <table border="1">
        <thead>
            <tr><th>DAG ID</th><th>State</th><th>Execution Date</th></tr>
        </thead>
        <tbody id="dagTableBody">
            <!-- Rows generated by JS -->
        </tbody>
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

def get_recent_dag_runs(dag_id, token, limit=5):
    try:
        url = f"{AIRFLOW_API_URL}/dags/{dag_id}/dagRuns?limit={limit}&order_by=-execution_date"
        headers = { "Authorization": f"Basic {token}", "Content-Type": "application/json" }
        req = urllib.request.Request(url, headers=headers)
        
        with urllib.request.urlopen(req, timeout=5) as response:
            if response.status == 200:
                data = json.loads(response.read().decode())
                return data.get("dag_runs", [])
    except Exception as e:
        print(f"Error fetching recent runs for {dag_id}: {e}")
    return []

def get_dag_tasks(dag_id, dag_run_id, token):
    try:
        # URL parsing to handle special chars in dag_run_id if necessary
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
        url = f"{AIRFLOW_API_URL}/dags/{dag_id}/dagRuns/{safe_dag_run_id}/taskInstances/{task_id}/logs/{try_number}"
        headers = { "Authorization": f"Basic {token}", "Content-Type": "application/json" }
        req = urllib.request.Request(url, headers=headers)
        
        with urllib.request.urlopen(req, timeout=5) as response:
            if response.status == 200:
                data = json.loads(response.read().decode())
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
        
        # New Endpoint: /api/status?dag_id=...
        if path == "/api/status":
            dag_id = query.get("dag_id", [None])[0]
            if dag_id:
                status = get_latest_dag_status(dag_id, token)
                self.send_json(status)
            else:
                self.send_error(400, "Missing dag_id")
            return

        if path == "/api/runs":
            dag_id = query.get("dag_id", [None])[0]
            if dag_id:
                runs = get_recent_dag_runs(dag_id, token)
                response_data = []
                for r in runs:
                    response_data.append({
                        "dag_run_id": r.get("dag_run_id"),
                        "state": r.get("state"),
                        "execution_date": r.get("execution_date")
                    })
                self.send_json(response_data)
            else:
                self.send_error(400, "Missing dag_id")
            return

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

        # Default: Render Client-Side HTML (No CSV reading)
        self.send_response(200)
        self.send_header("Content-type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(html_layout.encode('utf-8'))

    def send_json(self, data):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode('utf-8'))

# 5. Server Start
PORT = 8000
socketserver.TCPServer.allow_reuse_address = True
with socketserver.TCPServer(("", PORT), MyHandler) as httpd:
    print(f"Serving at port {PORT}")
    httpd.serve_forever()

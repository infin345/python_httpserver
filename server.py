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
AIRFLOW_API_URL = "http://localhost:8080/api/v1"
AIRFLOW_USER = "admin"
AIRFLOW_PASS = "admin"
# 토큰 발급 엔드포인트가 따로 있다면 설정
AIRFLOW_AUTH_URL = "http://localhost:8080/api/v1/security/oauth/token" 

# 1. HTML 템플릿
html_layout = """
<html>
<head><title>Airflow Monitor</title></head>
<body>
    <h1>Airflow On-prem Status</h1>
    <table border="1">
        <tr><th>DAG ID</th><th>State</th><th>Execution Date</th></tr>
        ${table_rows}
    </table>
</body>
</html>
"""

def get_airflow_token(username, password):
    """
    Airflow (또는 연동된 인증 서버)에서 ID/PW를 이용해 Bearer 토큰을 받아오는 함수.
    """
    try:
        # 임시: Basic Auth를 Base64로 인코딩하여 토큰처럼 사용 (Basic Auth 구조)
        credentials = f"{username}:{password}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        return encoded_credentials
        
        # 실제 Token 발급 요청 구현 예시 (urllib 사용)
        # data = json.dumps({"username": username, "password": password, "provider": "db"}).encode('utf-8')
        # req = urllib.request.Request(AIRFLOW_AUTH_URL, data=data, headers={'Content-Type': 'application/json'})
        # with urllib.request.urlopen(req) as response:
        #     result = json.loads(response.read().decode())
        #     return result.get("access_token")

    except Exception as e:
        print(f"Error fetching token: {e}")
        return None

def get_latest_dag_status(dag_id, token):
    """
    특정 DAG의 가장 최신 실행 상태를 가져오는 함수
    """
    try:
        url = f"{AIRFLOW_API_URL}/dags/{dag_id}/dagRuns?limit=1&order_by=-execution_date"
        
        headers = {
            "Authorization": f"Basic {token}", 
            "Content-Type": "application/json"
        }
        
        req = urllib.request.Request(url, headers=headers)
        
        with urllib.request.urlopen(req, timeout=5) as response:
            if response.status == 200:
                data = json.loads(response.read().decode())
                dag_runs = data.get("dag_runs", [])
                if dag_runs:
                    latest_run = dag_runs[0]
                    return {
                        "dag_id": dag_id,
                        "state": latest_run.get("state"),
                        "execution_date": latest_run.get("execution_date")
                    }
    except urllib.error.URLError as e:
        print(f"Failed to fetch dag run for {dag_id}: {e}")
    except Exception as e:
        print(f"Error fetching dag status for {dag_id}: {e}")
    
    return {"dag_id": dag_id, "state": "N/A", "execution_date": "N/A"}

class MyHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        # 2. 토큰 및 DAG 상태 조회
        token = get_airflow_token(AIRFLOW_USER, AIRFLOW_PASS)
        rows_html = ""
        
        try:
            with open('dags.csv', mode='r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    dag_id = row.get('dag_id')
                    if dag_id:
                        status_info = get_latest_dag_status(dag_id, token)
                        rows_html += f"<tr><td>{status_info['dag_id']}</td><td>{status_info['state']}</td><td>{status_info['execution_date']}</td></tr>"
        except FileNotFoundError:
            rows_html = "<tr><td colspan='3'>dags.csv not found</td></tr>"
        except Exception as e:
            rows_html = f"<tr><td colspan='3'>Error: {str(e)}</td></tr>"
        
        # 3. 데이터 바인딩
        t = Template(html_layout)
        display_html = t.substitute(table_rows=rows_html)

        # 4. HTTP 응답 전송
        self.send_response(200)
        self.send_header("Content-type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(display_html.encode('utf-8'))

# 5. 서버 실행 (포트 8000)
PORT = 8000
socketserver.TCPServer.allow_reuse_address = True
with socketserver.TCPServer(("", PORT), MyHandler) as httpd:
    print(f"Serving at port {PORT}")
    httpd.serve_forever()
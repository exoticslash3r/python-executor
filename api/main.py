import os
import time
import subprocess
import tempfile
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1.base_query import FieldFilter

# Initialize Firebase Admin
cred = credentials.Certificate({
    "type": "service_account",
    "project_id": "hoster-88bb3",
    "private_key_id": os.getenv("FIREBASE_PRIVATE_KEY_ID", ""),
    "private_key": os.getenv("FIREBASE_PRIVATE_KEY", "").replace('\\n', '\n'),
    "client_email": os.getenv("FIREBASE_CLIENT_EMAIL", ""),
    "client_id": os.getenv("FIREBASE_CLIENT_ID", ""),
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": os.getenv("FIREBASE_CLIENT_CERT_URL", "")
})

firebase_admin.initialize_app(cred)
db = firestore.client()

def execute_python_code(code, job_id):
    """Execute Python code and return output"""
    try:
        # Create temporary Python file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write(code)
            temp_file = f.name
        
        # Execute with timeout
        result = subprocess.run(
            ['python3', temp_file],
            capture_output=True,
            text=True,
            timeout=60  # 60 second timeout
        )
        
        # Clean up
        os.unlink(temp_file)
        
        return {
            'output': result.stdout,
            'error': result.stderr,
            'exit_code': result.returncode
        }
        
    except subprocess.TimeoutExpired:
        return {
            'output': '',
            'error': 'Execution timed out (60 seconds)',
            'exit_code': -1
        }
    except Exception as e:
        return {
            'output': '',
            'error': str(e),
            'exit_code': -1
        }

def job_worker():
    """Background worker that processes jobs"""
    print("🎯 Job Worker Started...")
    
    while True:
        try:
            # Query for pending jobs that are running
            jobs_ref = db.collection('python_jobs')
            query = jobs_ref.where(filter=FieldFilter('status', '==', 'pending')).where(
                filter=FieldFilter('is_running', '==', True)
            ).limit(5)
            
            docs = query.stream()
            
            for doc in docs:
                job = doc.to_dict()
                job_id = doc.id
                
                print(f"🔄 Processing job: {job_id}")
                
                # Update status to running
                doc.reference.update({
                    'status': 'running',
                    'started_at': firestore.SERVER_TIMESTAMP
                })
                
                # Execute the code
                result = execute_python_code(job.get('code', ''), job_id)
                
                # Update with results
                update_data = {
                    'status': 'completed' if result['exit_code'] == 0 else 'error',
                    'completed_at': firestore.SERVER_TIMESTAMP,
                    'output': result['output'],
                    'error': result['error'],
                    'is_running': False
                }
                
                doc.reference.update(update_data)
                print(f"✅ Completed job: {job_id}")
            
            # Wait before checking again
            time.sleep(2)
            
        except Exception as e:
            print(f"❌ Error in job worker: {e}")
            time.sleep(5)

if __name__ == "__main__":
    print("🚀 Starting Python Execution Service...")
    print("Service will run 24/7, processing jobs from Firestore")
    job_worker()

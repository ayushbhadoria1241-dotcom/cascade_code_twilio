from flask import Flask, request, jsonify
from twilio.twiml.voice_response import VoiceResponse
import requests
import json
import time
from datetime import datetime
from threading import Thread
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Twilio configuration for CASCADE CALLING
TWILIO_ACCOUNT_SID = "AC091da57b9e4c8866d047628437f43804"
TWILIO_AUTH_TOKEN = "e7a4867b2b413a0eaad1573a7ca41ade"
TWILIO_PHONE_NUMBER = "+13366003611"

# Cascade call list - Priority order (all set to your number for testing)
ALERT_PHONE_NUMBERS = [
    {
        "name": "Person 1 - Primary",
        "number": "+917568735073",
        "wait_time": 60  # Wait 60 seconds (1 minute) before trying next person
    },
    {
        "name": "Person 2 - Secondary", 
        "number": "+917568735073",
        "wait_time": 60  # Wait 60 seconds (1 minute)
    },
    {
        "name": "Person 3 - Tertiary",
        "number": "+917568735073",
        "wait_time": 60  # Wait 60 seconds (1 minute)
    }
]

# Store call status for tracking
call_status_tracker = {}

# TwiML Server URL - will use deployed URL when available
TWIML_BASE_URL = os.getenv('TWIML_SERVER_URL', 'https://twilio-mv1t.onrender.com')


def make_twilio_call(to_number, dag_id, task_id, state):
    """Make a call using Twilio API"""
    try:
        url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Calls.json"
        
        # Build the TwiML URL with parameters (using your deployed Render server)
        twiml_url = f"{TWIML_BASE_URL}/airflow-alert?dag_id={dag_id}&task_id={task_id}&state={state}"
        
        data = {
            "From": TWILIO_PHONE_NUMBER,
            "To": to_number,
            "Url": twiml_url
        }
        
        response = requests.post(
            url,
            auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
            data=data,
            timeout=10
        )
        
        if response.status_code == 201:
            call_data = response.json()
            logger.info(f"✅ Call initiated to {to_number}: SID={call_data['sid']}")
            return {
                "success": True,
                "sid": call_data["sid"],
                "status": call_data["status"]
            }
        else:
            logger.error(f"❌ Failed to call {to_number}: {response.text}")
            return {"success": False, "error": response.text}
            
    except Exception as e:
        logger.error(f"❌ Error making call to {to_number}: {str(e)}")
        return {"success": False, "error": str(e)}


def check_call_status(call_sid):
    """Check if call was answered"""
    try:
        url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Calls/{call_sid}.json"
        
        response = requests.get(
            url,
            auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
            timeout=10
        )
        
        if response.status_code == 200:
            call_data = response.json()
            status = call_data.get("status")
            duration = call_data.get("duration", "0")
            
            logger.info(f"📞 Call {call_sid} status: {status}, duration: {duration}s")
            
            # Call was answered if status is 'completed' or 'in-progress' and duration > 0
            if status in ["completed", "in-progress"] and int(duration) > 0:
                return True
            # Call failed or no answer
            elif status in ["busy", "failed", "no-answer", "canceled"]:
                return False
            # Still in progress, need to wait
            else:
                return None
                
        return None
        
    except Exception as e:
        logger.error(f"Error checking call status: {str(e)}")
        return None


def cascade_calling_logic(dag_id, task_id, state, alert_id):
    """Cascade calling logic - calls each person in sequence until someone answers"""
    logger.info(f"🚨 Starting cascade call for Alert ID: {alert_id}")
    logger.info(f"📋 DAG: {dag_id}, Task: {task_id}, State: {state}")
    
    for idx, contact in enumerate(ALERT_PHONE_NUMBERS, 1):
        logger.info(f"\n{'='*60}")
        logger.info(f"📞 Attempt {idx}/{len(ALERT_PHONE_NUMBERS)}: Calling {contact['name']}")
        logger.info(f"📱 Number: {contact['number']}")
        logger.info(f"{'='*60}")
        
        # Make the call
        call_result = make_twilio_call(
            contact["number"],
            dag_id,
            task_id,
            state
        )
        
        if not call_result["success"]:
            logger.warning(f"⚠️ Failed to initiate call to {contact['name']}, trying next person...")
            continue
        
        call_sid = call_result["sid"]
        wait_time = contact["wait_time"]
        
        logger.info(f"⏳ Waiting {wait_time} seconds to check if call is answered...")
        
        # Wait and check call status
        time.sleep(wait_time)
        
        # Check if call was answered
        answered = check_call_status(call_sid)
        
        if answered:
            logger.info(f"✅ SUCCESS! {contact['name']} answered the call!")
            call_status_tracker[alert_id] = {
                "answered_by": contact["name"],
                "phone": contact["number"],
                "attempt": idx,
                "timestamp": datetime.now().isoformat(),
                "call_sid": call_sid
            }
            break
        elif answered is False:
            logger.warning(f"❌ {contact['name']} did not answer. Moving to next person...")
            continue
        else:
            logger.info(f"⏰ Call still in progress, assuming no answer. Moving to next person...")
            continue
    else:
        # No one answered
        logger.error(f"🔴 CRITICAL: No one answered after {len(ALERT_PHONE_NUMBERS)} attempts!")
        call_status_tracker[alert_id] = {
            "answered_by": "NO ONE",
            "attempts": len(ALERT_PHONE_NUMBERS),
            "timestamp": datetime.now().isoformat(),
            "status": "FAILED - No response"
        }
    
    logger.info(f"\n{'='*60}")
    logger.info(f"🏁 Cascade calling completed for Alert ID: {alert_id}")
    logger.info(f"{'='*60}\n")


@app.route('/')
def home():
    return "Twilio TwiML Server with CASCADE CALLING is running!"

@app.route('/airflow-alert', methods=['GET', 'POST'])
def airflow_alert():
    # Get parameters from URL query string (GET) or form data (POST)
    dag_id = request.values.get('dag_id', 'unknown DAG')
    task_id = request.values.get('task_id', 'unknown task')
    state = request.values.get('state', 'failed')
    
    # Clean up underscores for better speech
    dag_id_spoken = dag_id.replace('_', ' ')
    task_id_spoken = task_id.replace('_', ' ')
    
    # Create TwiML response
    response = VoiceResponse()
    
    # Attention getter
    response.say(
        "Attention! This is a critical alert from Grafana.",
        voice='alice',
        language='en-US'
    )
    response.pause(length=1)
    
    # Main alert with details
    message = (
        f"An Airflow task has failed. "
        f"The DAG name is {dag_id_spoken}. "
        f"The task name is {task_id_spoken}. "
        f"Current status is {state}. "
        f"Please check your monitoring dashboard immediately."
    )
    
    response.say(message, voice='alice', language='en-US')
    response.pause(length=2)
    
    # Repeat key info
    response.say(
        f"I repeat, task {task_id_spoken} in DAG {dag_id_spoken} has failed.",
        voice='alice',
        language='en-US'
    )
    response.pause(length=1)
    
    # Instructions
    response.say(
        "Check Microsoft Teams for full details, or login to Grafana dashboard.",
        voice='alice',
        language='en-US'
    )
    response.pause(length=1)
    
    # Closing
    response.say(
        "This is an automated alert from A 360 data platform team. Thank you.",
        voice='alice',
        language='en-US'
    )
    
    return str(response), 200, {'Content-Type': 'text/xml'}

@app.route('/test-twiml', methods=['GET', 'POST'])
def test_twiml():
    """Test endpoint to see what TwiML is generated"""
    # Get parameters from either query string (GET) or form data (POST)
    dag_id = request.values.get('dag_id', 'test_dag')
    task_id = request.values.get('task_id', 'test_task')
    state = request.values.get('state', 'failed')
    
    dag_id_spoken = dag_id.replace('_', ' ')
    task_id_spoken = task_id.replace('_', ' ')
    
    response = VoiceResponse()
    response.say(
        "Attention! This is a critical alert from Airflow.",
        voice='alice',
        language='en-US'
    )
    response.pause(length=1)
    
    message = (
        f"An Airflow task has failed. "
        f"The DAG name is {dag_id_spoken}. "
        f"The task name is {task_id_spoken}. "
        f"Current status is {state}. "
        f"Please check your monitoring dashboard immediately."
    )
    
    response.say(message, voice='alice', language='en-US')
    
    return str(response), 200, {'Content-Type': 'text/xml'}


# ======================== CASCADE CALLING ENDPOINTS ========================

@app.route('/grafana-cascade-call', methods=['POST'])
def grafana_cascade_call_webhook():
    """Grafana webhook endpoint that triggers cascade calling"""
    try:
        data = request.json
        logger.info(f"📨 Received Grafana alert: {json.dumps(data, indent=2)}")
        
        # Extract alert information
        alerts = data.get("alerts", [])
        if not alerts:
            return jsonify({"error": "No alerts in payload"}), 400
        
        alert = alerts[0]
        labels = alert.get("labels", {})
        
        dag_id = labels.get("dag_id", "unknown_dag")
        task_id = labels.get("task_id", "unknown_task")
        state = labels.get("state", "failed")
        
        # Generate unique alert ID
        alert_id = f"{dag_id}_{task_id}_{int(time.time())}"
        
        logger.info(f"🎯 Alert ID: {alert_id}")
        logger.info(f"📋 DAG: {dag_id}, Task: {task_id}, State: {state}")
        
        # Start cascade calling in background thread
        thread = Thread(
            target=cascade_calling_logic,
            args=(dag_id, task_id, state, alert_id)
        )
        thread.start()
        
        return jsonify({
            "status": "Cascade calling initiated",
            "alert_id": alert_id,
            "dag_id": dag_id,
            "task_id": task_id,
            "contacts_to_call": len(ALERT_PHONE_NUMBERS),
            "message": "Calls will be made in sequence until someone answers"
        }), 200
        
    except Exception as e:
        logger.error(f"❌ Webhook error: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "TwiML Server with Cascade Calling",
        "contact_count": len(ALERT_PHONE_NUMBERS),
        "contacts": [c["name"] for c in ALERT_PHONE_NUMBERS]
    }), 200


@app.route('/call-history', methods=['GET'])
def call_history():
    """View call history and who answered"""
    return jsonify({
        "call_history": call_status_tracker,
        "total_alerts": len(call_status_tracker)
    }), 200


@app.route('/test-cascade', methods=['POST'])
def test_cascade():
    """Test cascade calling with sample data"""
    sample_alert = {
        "alerts": [{
            "labels": {
                "dag_id": "test_customer_etl",
                "task_id": "validate_records",
                "state": "failed"
            }
        }]
    }
    
    # Manually trigger cascade
    alerts = sample_alert.get("alerts", [])
    alert = alerts[0]
    labels = alert.get("labels", {})
    
    dag_id = labels.get("dag_id", "test_dag")
    task_id = labels.get("task_id", "test_task")
    state = labels.get("state", "failed")
    
    alert_id = f"TEST_{dag_id}_{task_id}_{int(time.time())}"
    
    logger.info(f"🧪 TEST MODE: Starting cascade call test")
    
    # Start cascade calling in background
    thread = Thread(
        target=cascade_calling_logic,
        args=(dag_id, task_id, state, alert_id)
    )
    thread.start()
    
    return jsonify({
        "status": "TEST - Cascade calling initiated",
        "alert_id": alert_id,
        "dag_id": dag_id,
        "task_id": task_id,
        "contacts_to_call": len(ALERT_PHONE_NUMBERS),
        "message": "This is a TEST. Calls will be made to demonstrate cascade functionality."
    }), 200


if __name__ == '__main__':
    import sys
    print("=" * 70, flush=True)
    print("*** Twilio TwiML Server with CASCADE CALLING ***", flush=True)
    print("=" * 70, flush=True)
    print("Cascade contact list:", flush=True)
    for idx, contact in enumerate(ALERT_PHONE_NUMBERS, 1):
        print(f"   {idx}. {contact['name']} - {contact['number']} (wait: {contact['wait_time']}s)", flush=True)
    print("=" * 70, flush=True)
    print("Server endpoints:", flush=True)
    print("   - Home: http://localhost:5001/", flush=True)
    print("   - TwiML Alert: http://localhost:5001/airflow-alert", flush=True)
    print("   - Grafana Cascade: http://localhost:5001/grafana-cascade-call", flush=True)
    print("   - Health Check: http://localhost:5001/health", flush=True)
    print("   - Test Cascade: http://localhost:5001/test-cascade (POST)", flush=True)
    print("   - Call History: http://localhost:5001/call-history", flush=True)
    print("=" * 70, flush=True)
    sys.stdout.flush()
    app.run(host='0.0.0.0', port=5001, debug=False)
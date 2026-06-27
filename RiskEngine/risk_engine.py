import time
import json
import requests
import psycopg2
from google import genai
from google.genai import types

# 1. Configuration Credentials
UPSTASH_POP_URL = "https://flying-wombat-79982.upstash.io/lpop/transaction_queue"
UPSTASH_TOKEN = "gQAAAAAAAThuAAIgcDIyNGYyNjcyNzgyOGI0OTZiYTZjMTc3ZWVhMDY1YTE2NQ"

DB_CONNECTION_STRING = "postgresql://neondb_owner:npg_se27jIEhkWwz@ep-withered-mouse-aobvm0q3-pooler.c-2.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
GEMINI_API_KEY = "AQ.Ab8RN6LWZbGQZfGMOVP5IxNrWzQwS5GIc6R6CgUa7NJll-qiSw"

# Initialize the official Gemini Client
ai_client = genai.Client(api_key=GEMINI_API_KEY)


def analyze_with_gemini(transaction_data):
    """
    Triggers the autonomous compliance agent when a transaction breaks a rule.
    Forces Gemini to output structured JSON matching our dashboard's schema.
    """
    prompt = f"""
    You are an automated corporate financial compliance auditor.
    Analyze this suspicious transaction payload for potential fraud or high-risk activity:
    {json.dumps(transaction_data)}
    
    You must output a structured mitigation blueprint strictly in JSON format.
    Do not include markdown blocks like ```json. Output raw JSON only matching this schema:
    {{
        "flag_reason": "Clear explanation of why this transaction is dangerous",
        "recommended_action": "FREEZE_ACCOUNT, SUSPEND_TRANSACTION, or ALLOW_WITH_MONITORING",
        "risk_severity_score": 0-100 integer
    }}
    """
    
    response = ai_client.models.generate_content(
        model='gemini-2.5-flash',
        contents=prompt
    )
    
    # Parse the clean JSON string from the model output
    return json.loads(response.text.strip())


def process_queue():
    print("🚀 Sentinel-Ledger Risk Engine background worker is running...")
    headers = {"Authorization": f"Bearer {UPSTASH_TOKEN}"}
    
    while True:
        try:
            # Print a dot to console to show active polling pulse
            print(".", end="", flush=True)
            response = requests.post(UPSTASH_POP_URL, headers=headers)
            
            if response.status_code != 200:
                print(f"\n❌ Error communicating with Upstash: {response.status_code} - {response.text}")
                time.sleep(2)
                continue
                
            redis_data = response.json()
            
            # Upstash REST responses hold the pulled list item inside the "result" key
            if not redis_data or "result" not in redis_data or redis_data["result"] is None:
                time.sleep(1)
                continue
                
            raw_payload = redis_data["result"]
            
            # Safely handle potential double-serialization from ingestion layers
            try:
                transaction = json.loads(raw_payload)
                if isinstance(transaction, str):
                    transaction = json.loads(transaction)
            except Exception:
                print(f"\n❌ Failed to parse JSON payload: {raw_payload}")
                continue
            
            print(f"\n📥 Processing transaction for Account: {transaction.get('AccountId')} | Amount: ${transaction.get('Amount')}")
            
            # 3. Deterministic Velocity/Rule Check Layer
            # Rule: Any single transaction over $4,000 is automatically routed to Gemini AI agent
            is_high_risk = float(transaction.get('Amount', 0)) > 4000.00
            
            status = "LOW"
            risk_score = 10
            ai_summary = "Cleared by deterministic rules."
            mitigation_json = {}
            
            if is_high_risk:
                print("⚠️ Rule Triggered (Amount > $4000)! Activating Gemini Compliance Agent...")
                try:
                    ai_blueprint = analyze_with_gemini(transaction)
                    status = "HIGH" if ai_blueprint.get("risk_severity_score", 50) < 85 else "CRITICAL"
                    risk_score = ai_blueprint.get("risk_severity_score", 75)
                    ai_summary = ai_blueprint.get("flag_reason", "AI flagged high amount transaction.")
                    mitigation_json = ai_blueprint
                except Exception as ai_err:
                    print(f"❌ Gemini parsing issue: {ai_err}")
                    status = "HIGH"
                    risk_score = 80
                    ai_summary = "AI processing error. Flagged high amount for manual review."
                    mitigation_json = {"error": "Failed to generate AI blueprint"}

            # 4. Persistence Layer (Write to PostgreSQL)
            conn = psycopg2.connect(DB_CONNECTION_STRING)
            cursor = conn.cursor()
            
            # Insert transaction record
            insert_tx_query = """
                INSERT INTO transactions (account_id, amount, currency, merchant_category, risk_score, status)
                VALUES (%s, %s, %s, %s, %s, %s) RETURNING transaction_id;
            """
            cursor.execute(insert_tx_query, (
                transaction.get('AccountId'),
                transaction.get('Amount'),
                transaction.get('Currency', 'USD'),
                transaction.get('MerchantCategory'),
                risk_score,
                status
            ))
            
            generated_id = cursor.fetchone()[0]
            
            # If AI evaluated it, insert into compliance audits table
            if is_high_risk:
                insert_audit_query = """
                    INSERT INTO compliance_audits (transaction_id, ai_summary, structured_mitigation)
                    VALUES (%s, %s, %s);
                """
                cursor.execute(insert_audit_query, (
                    generated_id,
                    ai_summary,
                    json.dumps(mitigation_json)
                ))
                
            conn.commit()
            cursor.close()
            conn.close()
            print(f"✅ Transaction successfully saved to DB Ledger. ID: {generated_id}")
            
            # 5. AUTOMATED WEBHOOK ACTION DETECTOR (Phase 3 & 4 Real-Time Hook)
            # Instantly hits the .NET SignalR worker receiver route to push alerts over live WebSockets
            try:
                webhook_payload = {
                    "transaction_id": str(generated_id),
                    "account_id": transaction.get('AccountId'),
                    "amount": float(transaction.get('Amount', 0)),
                    "merchant_category": transaction.get('MerchantCategory'),
                    "risk_score": risk_score,
                    "status": status,
                    "ai_summary": ai_summary,
                    "is_high_risk": is_high_risk
                }
                # Dispatches webhook payload execution back into .NET
                requests.post("http://localhost:5015/api/worker/action", json=webhook_payload)
                print(f"📢 Automation Webhook dispatched to .NET Gateway for Real-Time Streaming.")
            except Exception as webhook_err:
                print(f"⚠️ Webhook transmission delay or port 5015 down: {webhook_err}")
            
        except Exception as e:
            print(f"\n💥 Worker exception occurred: {e}")
            time.sleep(2)


if __name__ == "__main__":
    process_queue()
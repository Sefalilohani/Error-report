"""
SVIN Ops Error Report - GitHub Actions
Runs at 9AM, 12PM, 4PM, 6PM IST
Posts to #sv-in-ops (CF0RH10M8)
"""

import os
import time
import requests
from datetime import datetime, timezone, timedelta, date as date_type

# ── Config ───────────────────────────────────────────────────────────────────
SLACK_TOKEN     = os.environ["SLACK_BOT_TOKEN"]
REDASH_API_KEY  = os.environ["REDASH_API_KEY"]
REDASH_BASE_URL = os.environ.get("REDASH_BASE_URL", "https://redash.springworks.in")
REDASH_QUERY_ID = 1528
OPS_CHANNEL_ID  = "CF0RH10M8"
SUBTEAM_ID      = "S08T66C76CS"
CC_USERS        = "<@UPAMYUZAS> <@U06T72TD4BD>"
REPORT_TYPE     = os.environ.get("REPORT_TYPE", "9am")

IST = timezone(timedelta(hours=5, minutes=30))

# ── Helpers ──────────────────────────────────────────────────────────────────
def ordinal(n):
    if 11 <= n % 100 <= 13:
        return f"{n}th"
    return f"{n}{['th','st','nd','rd','th','th','th','th','th','th'][n % 10]}"

def format_date(dt):
    months = ["Jan","Feb","March","April","May","June","July","Aug","Sept","Oct","Nov","Dec"]
    return f"{ordinal(dt.day)} {months[dt.month - 1]} {dt.year}"

def get_earliest_error_date():
    """Get the earliest pending error date via ad-hoc query using query 1528's API key."""
    url = f"{REDASH_BASE_URL}/api/queries/{REDASH_QUERY_ID}/results"
    today = datetime.now(IST).date()
    payload = {
        "parameters": {
            "created_at": {"start": "2025-01-01 00:00:00", "end": f"{today} 23:59:59"},
            "error_status": "NEW,UNDER_DISCUSSION",
            "department": "OPERATIONS",
            "check_type": "ALL",
            "user_email": "ALL"
        },
        "max_age": 0
    }
    resp = requests.post(
        url,
        json=payload,
        headers={"Authorization": f"Key {REDASH_API_KEY}"},
        timeout=60
    )
    resp.raise_for_status()
    data = resp.json()

    # Poll if job
    if "job" in data:
        job_id = data["job"]["id"]
        for _ in range(30):
            time.sleep(2)
            job_resp = requests.get(
                f"{REDASH_BASE_URL}/api/jobs/{job_id}",
                headers={"Authorization": f"Key {REDASH_API_KEY}"},
                timeout=30
            )
            job_data = job_resp.json()["job"]
            if job_data["status"] == 3:
                qr_id = job_data["query_result_id"]
                qr_resp = requests.get(
                    f"{REDASH_BASE_URL}/api/query_results/{qr_id}",
                    headers={"Authorization": f"Key {REDASH_API_KEY}"},
                    timeout=30
                )
                return qr_resp.json()["query_result"]["data"]["rows"]
            elif job_data["status"] == 4:
                raise Exception(f"Redash job failed: {job_data.get('error')}")
        raise Exception("Redash job timed out")
    return data["query_result"]["data"]["rows"]

def get_error_counts(start_date, end_date):
    """Run query 1528 with parameters to get error counts per agent."""
    url = f"{REDASH_BASE_URL}/api/queries/{REDASH_QUERY_ID}/results"
    payload = {
        "parameters": {
            "created_at": {"start": start_date, "end": end_date},
            "error_status": "NEW,UNDER_DISCUSSION",
            "department": "OPERATIONS",
            "check_type": "ALL",
            "user_email": "ALL"
        },
        "max_age": 0
    }
    resp = requests.post(
        url,
        json=payload,
        headers={"Authorization": f"Key {REDASH_API_KEY}"},
        timeout=60
    )
    resp.raise_for_status()
    data = resp.json()

    # Poll if job
    if "job" in data:
        job_id = data["job"]["id"]
        for _ in range(30):
            time.sleep(2)
            job_resp = requests.get(
                f"{REDASH_BASE_URL}/api/jobs/{job_id}",
                headers={"Authorization": f"Key {REDASH_API_KEY}"},
                timeout=30
            )
            job_data = job_resp.json()["job"]
            if job_data["status"] == 3:
                qr_id = job_data["query_result_id"]
                qr_resp = requests.get(
                    f"{REDASH_BASE_URL}/api/query_results/{qr_id}",
                    headers={"Authorization": f"Key {REDASH_API_KEY}"},
                    timeout=30
                )
                return qr_resp.json()["query_result"]["data"]["rows"]
            elif job_data["status"] == 4:
                raise Exception(f"Redash job failed: {job_data.get('error')}")
        raise Exception("Redash job timed out")
    return data["query_result"]["data"]["rows"]

def get_slack_user_id(email):
    resp = requests.get(
        "https://slack.com/api/users.lookupByEmail",
        params={"email": email},
        headers={"Authorization": f"Bearer {SLACK_TOKEN}"},
        timeout=15
    )
    data = resp.json()
    if data.get("ok"):
        return data["user"]["id"]
    return None

def slack_post(channel, text, thread_ts=None):
    payload = {"channel": channel, "text": text}
    if thread_ts:
        payload["thread_ts"] = thread_ts
    resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        json=payload,
        headers={"Authorization": f"Bearer {SLACK_TOKEN}", "Content-Type": "application/json"},
        timeout=15
    )
    data = resp.json()
    if not data.get("ok"):
        raise Exception(f"Slack post failed: {data.get('error')}")
    return data["ts"]

def get_todays_9am_thread():
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    resp = requests.get(
        "https://slack.com/api/conversations.history",
        params={"channel": OPS_CHANNEL_ID, "limit": 20},
        headers={"Authorization": f"Bearer {SLACK_TOKEN}"},
        timeout=15
    )
    data = resp.json()
    for msg in data.get("messages", []):
        if "Daily Error Report" in msg.get("text", ""):
            ts = float(msg["ts"])
            if datetime.fromtimestamp(ts, tz=IST).strftime("%Y-%m-%d") == today_str:
                return msg["ts"]
    return None

# ── Main ─────────────────────────────────────────────────────────────────────
def run_report():
    now = datetime.now(IST)
    today = now.date()
    end_date = f"{today} 23:59:59"
    print(f"[{now.isoformat()}] Running {REPORT_TYPE} report...")

    # Use fixed start date (earliest known errors)
    start_date = "2025-09-25 00:00:00"
    sd = date_type.fromisoformat("2025-09-25")
    start_display = format_date(sd)
    end_display = format_date(today)

    # Get agent error counts from query 1528
    rows = get_error_counts(start_date, end_date)
    print(f"Got {len(rows)} rows from Redash")

    # Aggregate by email (sum across check types)
    from collections import defaultdict
    agent_totals = defaultdict(lambda: {"name": "", "count": 0})
    for row in rows:
        email = row.get("Email", "")
        name = row.get("Name", email)
        count = row.get("Error Count", 0) or 0
        agent_totals[email]["name"] = name
        agent_totals[email]["count"] += count

    # Sort by count desc
    sorted_agents = sorted(agent_totals.items(), key=lambda x: -x[1]["count"])

    lines = []
    total = 0
    for i, (email, info) in enumerate(sorted_agents, 1):
        count = info["count"]
        total += count
        uid = get_slack_user_id(email)
        tag = f"<@{uid}>" if uid else info["name"]
        lines.append(f"{i}. {tag} - {count}")

    # Build Redash URL
    start_enc = start_date.replace(" ", "%20").replace(":", "%3A")
    end_enc = f"{today}%2023%3A59%3A59"
    redash_url = (
        f"https://redash.springworks.in/queries/1528"
        f"?p_check_type=%5B%22ALL%22%5D"
        f"&p_created_at={start_enc}--{end_enc}"
        f"&p_department=%5B%22OPERATIONS%22%5D"
        f"&p_error_status=%5B%22NEW%22%2C%22UNDER_DISCUSSION%22%5D"
        f"&p_user_email=%5B%22ALL%22%5D#2204"
    )

    heading = (":rotating_light: _Daily Error Report_" if REPORT_TYPE == "9am"
               else ":rotating_light: _Updated Error Report | Status: NEW & UNDER DISCUSSION_")

    body = "\n".join(lines) if lines else "No open errors found!"
    message = (
        f"{heading}\n"
        f"_{end_display}_\n"
        f"_{start_display} to {end_display}_\n"
        f"_Status: NEW & UNDER DISCUSSION | Department: OPERATIONS_\n"
        f"{body}\n"
        f"_Total - {total}_\n\n"
        f":bar_chart: <{redash_url}|View Full Report on Redash>\n\n"
        f"_Tagged agents: Please review and resolve/rectify your open errors at the earliest and acknowledge the message :pray:_\n\n"
        f"CC: <!subteam^{SUBTEAM_ID}> {CC_USERS}\n"
        f"*Sent using* <@U0A7P44QK39|Claude>"
    )

    if REPORT_TYPE == "9am":
        ts = slack_post(OPS_CHANNEL_ID, message)
        print(f"9AM report posted. ts={ts}")
    else:
        thread_ts = get_todays_9am_thread()
        if not thread_ts:
            print("Could not find today's 9AM thread - posting as new message")
            slack_post(OPS_CHANNEL_ID, message)
        else:
            slack_post(OPS_CHANNEL_ID, message, thread_ts=thread_ts)
            print(f"{REPORT_TYPE} report posted in thread {thread_ts}")

if __name__ == "__main__":
    run_report()

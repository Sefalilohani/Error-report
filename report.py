"""
SVIN Ops Error Report — Render Cron Job
Runs at 9AM, 12PM, 4PM, 6PM IST
Posts to #sv-in-ops (CF0RH10M8)
"""

import os
import sys
import requests
from datetime import datetime, timezone, timedelta

# ── Config ────────────────────────────────────────────────────────────────────
SLACK_TOKEN       = os.environ["SLACK_BOT_TOKEN"]
REDASH_API_KEY    = os.environ["REDASH_API_KEY"]
REDASH_BASE_URL   = os.environ.get("REDASH_BASE_URL", "https://redash.springworks.in")
OPS_CHANNEL_ID    = "CF0RH10M8"          # #sv-in-ops
SUBTEAM_ID        = "S08T66C76CS"        # svin-ops-teamspocs
CC_USERS          = "<@UPAMYUZAS> <@U06T72TD4BD>"
REPORT_TYPE       = os.environ.get("REPORT_TYPE", "9am")  # 9am | 12pm | 4pm | 6pm

IST = timezone(timedelta(hours=5, minutes=30))

# ── Helpers ───────────────────────────────────────────────────────────────────
def ordinal(n):
    if 11 <= n % 100 <= 13:
        return f"{n}th"
    return f"{n}{['th','st','nd','rd','th','th','th','th','th','th'][n % 10]}"

def format_date(dt):
    months = ["Jan","Feb","March","April","May","June","July","Aug","Sept","Oct","Nov","Dec"]
    return f"{ordinal(dt.day)} {months[dt.month - 1]} {dt.year}"

def redash_query(sql):
    """Execute ad-hoc query on Redash datasource 5"""
    url = f"{REDASH_BASE_URL}/api/query_results"
    resp = requests.post(
        url,
        json={"query": sql, "data_source_id": 5, "max_age": 0},
        headers={"Authorization": f"Key {REDASH_API_KEY}"},
        timeout=60
    )
    resp.raise_for_status()
    data = resp.json()
    # Redash may return a job reference — poll until done
    if "job" in data:
        job_id = data["job"]["id"]
        for _ in range(30):
            import time; time.sleep(2)
            job_resp = requests.get(
                f"{REDASH_BASE_URL}/api/jobs/{job_id}",
                headers={"Authorization": f"Key {REDASH_API_KEY}"},
                timeout=30
            )
            job_data = job_resp.json()["job"]
            if job_data["status"] == 3:  # success
                qr_id = job_data["query_result_id"]
                qr_resp = requests.get(
                    f"{REDASH_BASE_URL}/api/query_results/{qr_id}",
                    headers={"Authorization": f"Key {REDASH_API_KEY}"},
                    timeout=30
                )
                return qr_resp.json()["query_result"]["data"]["rows"]
            elif job_data["status"] == 4:
                raise Exception(f"Redash query failed: {job_data.get('error')}")
        raise Exception("Redash query timed out")
    return data["query_result"]["data"]["rows"]

def get_slack_user_id(email):
    """Look up Slack user ID by email"""
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
        headers={
            "Authorization": f"Bearer {SLACK_TOKEN}",
            "Content-Type": "application/json"
        },
        timeout=15
    )
    data = resp.json()
    if not data.get("ok"):
        raise Exception(f"Slack post failed: {data.get('error')}")
    return data["ts"]

def get_todays_9am_thread():
    """Find today's 9AM report thread_ts in #sv-in-ops"""
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    resp = requests.get(
        "https://slack.com/api/conversations.history",
        params={"channel": OPS_CHANNEL_ID, "limit": 20},
        headers={"Authorization": f"Bearer {SLACK_TOKEN}"},
        timeout=15
    )
    data = resp.json()
    for msg in data.get("messages", []):
        text = msg.get("text", "")
        if "Daily Error Report" in text:
            # Check if it was posted today
            ts = float(msg["ts"])
            msg_date = datetime.fromtimestamp(ts, tz=IST).strftime("%Y-%m-%d")
            if msg_date == today_str:
                return msg["ts"]
    return None

# ── Core Report Logic ─────────────────────────────────────────────────────────
def run_report():
    now = datetime.now(IST)
    today = now.date()
    end_date = f"{today} 23:59:59"

    print(f"[{now.isoformat()}] Running {REPORT_TYPE} report...")

    # Step 1: Get earliest pending error date
    start_rows = redash_query("""
        SELECT DATE(MIN(e.created_at)) AS earliest_error_date
        FROM errors e
        WHERE e.deleted_at IS NULL
          AND FIND_IN_SET(UPPER(e.status), 'NEW,UNDER_DISCUSSION') > 0
    """)
    start_date_raw = start_rows[0]["earliest_error_date"]
    start_date = f"{start_date_raw} 00:00:00"

    # Format dates for display
    from datetime import date as date_type
    if isinstance(start_date_raw, str):
        sd = date_type.fromisoformat(start_date_raw)
    else:
        sd = start_date_raw
    start_display = format_date(sd)
    end_display = format_date(today)

    # Step 2: Get agent error counts
    rows = redash_query(f"""
        WITH department_filter AS (
            SELECT (
                SELECT id FROM enums
                WHERE type = 'DEPARTMENT' AND value = 'OPERATIONS' AND deleted_at IS NULL
                LIMIT 1
            ) AS department_id
        )
        SELECT
            u.name AS Name,
            u.email AS Email,
            COUNT(DISTINCT e.id) AS ErrorCount
        FROM errors e
        INNER JOIN users u ON u.id = e.agent_user_id_fk
        LEFT JOIN teams_user_mapping tum ON tum.user_id_fk = u.id
        LEFT JOIN teams t ON t.id = tum.team_id_fk AND t.deleted_at IS NULL
        LEFT JOIN enums dept_enum ON dept_enum.id = t.department_enum_fk AND dept_enum.deleted_at IS NULL
        CROSS JOIN department_filter df
        WHERE e.deleted_at IS NULL
          AND e.created_at >= '{start_date}'
          AND e.created_at <= '{end_date}'
          AND FIND_IN_SET(UPPER(e.status), 'NEW,UNDER_DISCUSSION') > 0
          AND (df.department_id IS NULL OR t.department_enum_fk = df.department_id)
        GROUP BY u.id, u.name, u.email
        ORDER BY COUNT(DISTINCT e.id) DESC, u.name
    """)

    # Step 3: Resolve Slack IDs
    lines = []
    total = 0
    for i, row in enumerate(rows, 1):
        email = row.get("Email", "")
        name = row.get("Name", email)
        count = row.get("ErrorCount", 0)
        total += count
        uid = get_slack_user_id(email)
        tag = f"<@{uid}>" if uid else name
        lines.append(f"{i}. {tag} - {count}")

    # URL-encode dates for Redash link
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

    # Step 4: Format message
    if REPORT_TYPE == "9am":
        heading = f"🚨 *Daily Error Report — {end_display}*"
    else:
        heading = f"🚨 *Updated Error Report | Status: NEW & UNDER DISCUSSION — {end_display}*"

    body = "\n".join(lines) if lines else "✅ No open errors found!"
    message = (
        f"{heading}\n"
        f"*{start_display} to {end_display}*\n"
        f"*Status: NEW & UNDER DISCUSSION | Department: OPERATIONS*\n\n"
        f"{body}\n"
        f"*Total - {total}*\n\n"
        f"📊 <{redash_url}|View Full Report on Redash>\n\n"
        f"_Tagged agents: Please review and resolve/rectify your open errors at the earliest and acknowledge the message 🙏_\n\n"
        f"CC: <!subteam^{SUBTEAM_ID}> {CC_USERS}"
    )

    # Step 5: Post to Slack
    if REPORT_TYPE == "9am":
        ts = slack_post(OPS_CHANNEL_ID, message)
        print(f"✅ 9AM report posted. ts={ts}")
    else:
        thread_ts = get_todays_9am_thread()
        if not thread_ts:
            print("⚠️  Could not find today's 9AM thread — posting as new message")
            slack_post(OPS_CHANNEL_ID, message)
        else:
            slack_post(OPS_CHANNEL_ID, message, thread_ts=thread_ts)
            print(f"✅ {REPORT_TYPE} report posted as thread reply under {thread_ts}")

if __name__ == "__main__":
    run_report()

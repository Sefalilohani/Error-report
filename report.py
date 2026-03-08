import os
import time
import requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import urllib.parse

# ── CONFIG ─────────────────────────────────────────────────────

SLACK_TOKEN = os.environ["SLACK_BOT_TOKEN"]
REDASH_API_KEY = os.environ["REDASH_API_KEY"]

REDASH_QUERY_ID = 1528
REDASH_BASE = "https://redash.springworks.in"

OPS_CHANNEL_ID = "C0AGRE19V6U"  # #testing-sefali

REPORT_TYPE = os.environ.get("REPORT_TYPE", "9am")

THREAD_FILE = "thread_ts.txt"

IST = timezone(timedelta(hours=5, minutes=30))

START_DATE = "2025-09-25 00:00:00"


# ── HELPERS ────────────────────────────────────────────────────

def ordinal(n):
    if 11 <= n <= 13:
        return f"{n}th"
    return f"{n}{['th','st','nd','rd','th'][min(n%10,4)]}"


def fmt_date(dt):
    return f"{ordinal(dt.day)} {dt.strftime('%B %Y')}"


# ── FETCH REDASH DATA (async job pattern) ───────────────────

def fetch_redash(start_date, end_date):
    """
    Redash async flow:
    1. POST /api/queries/{id}/results  -> returns job_id
    2. GET  /api/jobs/{job_id}         -> poll until status=3 (success), get query_result_id
    3. GET  /api/query_results/{id}    -> fetch actual rows
    """
    headers = {
        "Authorization": f"Key {REDASH_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "parameters": {
            "p_check_type": ["ALL"],
            "p_created_at": f"{start_date}--{end_date}",
            "p_department": ["OPERATIONS"],
            "p_error_status": ["NEW", "UNDER_DISCUSSION"],
            "p_user_email": ["ALL"]
        },
        "max_age": 0
    }

    # Step 1: trigger the query
    r = requests.post(
        f"{REDASH_BASE}/api/queries/{REDASH_QUERY_ID}/results",
        headers=headers,
        json=payload
    )
    r.raise_for_status()
    resp = r.json()

    # If result is cached and returned immediately
    if "query_result" in resp:
        print("Got cached result immediately")
        return resp["query_result"]["data"]["rows"]

    # Step 2: poll the job
    job_id = resp["job"]["id"]
    print(f"Job started: {job_id}, polling...")

    for attempt in range(30):
        time.sleep(2)
        jr = requests.get(
            f"{REDASH_BASE}/api/jobs/{job_id}",
            headers=headers
        )
        jr.raise_for_status()
        job = jr.json()["job"]
        status = job["status"]
        print(f"  Poll {attempt+1}: status={status}")

        if status == 3:  # success
            query_result_id = job["query_result_id"]
            break
        elif status == 4:  # error
            raise Exception(f"Redash job failed: {job.get('error')}")
    else:
        raise Exception("Redash job timed out after 60 seconds")

    # Step 3: fetch results
    print(f"Fetching results: query_result_id={query_result_id}")
    rr = requests.get(
        f"{REDASH_BASE}/api/query_results/{query_result_id}",
        headers=headers
    )
    rr.raise_for_status()
    return rr.json()["query_result"]["data"]["rows"]


# ── SLACK USERS ───────────────────────────────────────────────

def get_slack_users():
    url = "https://slack.com/api/users.list"
    r = requests.get(url, headers={"Authorization": f"Bearer {SLACK_TOKEN}"})
    data = r.json()
    users = {}
    for u in data.get("members", []):
        email = u.get("profile", {}).get("email")
        if email:
            users[email.lower()] = u["id"]
    return users


# ── POST TO SLACK ──────────────────────────────────────────────

def post_slack(text, thread_ts=None):
    payload = {
        "channel": OPS_CHANNEL_ID,
        "text": text
    }
    if thread_ts:
        payload["thread_ts"] = thread_ts

    r = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={
            "Authorization": f"Bearer {SLACK_TOKEN}",
            "Content-Type": "application/json"
        },
        json=payload
    )
    r.raise_for_status()
    resp = r.json()
    if not resp.get("ok"):
        raise Exception(f"Slack API error: {resp.get('error')}")
    return resp["ts"]


# ── FIND TODAY'S 9AM THREAD ────────────────────────────────

def find_9am_thread_ts():
    if os.path.exists(THREAD_FILE):
        with open(THREAD_FILE) as f:
            ts = f.read().strip()
            if ts:
                return ts
    today_str = datetime.now(IST).strftime("%d %B %Y")
    url = "https://slack.com/api/conversations.history"
    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {SLACK_TOKEN}"},
        params={"channel": OPS_CHANNEL_ID, "limit": 20}
    )
    data = r.json()
    for msg in data.get("messages", []):
        if "Daily Error Report" in msg.get("text", "") and today_str in msg.get("text", ""):
            return msg["ts"]
    raise Exception("Could not find today's 9 AM report thread.")


# ── BUILD REPORT MESSAGE ──────────────────────────────────

def build_report(rows, slack_users, start_dt, end_dt, report_type):
    counts = defaultdict(int)
    name_by_email = {}

    for row in rows:
        email = (row.get("Email") or row.get("user_email") or "").lower()
        name = row.get("Name") or row.get("user_name") or email
        if email:
            counts[email] += 1
            name_by_email[email] = name

    sorted_agents = sorted(counts.items(), key=lambda x: x[1], reverse=True)
    lines = []

    for i, (email, count) in enumerate(sorted_agents, 1):
        slack_id = slack_users.get(email)
        if slack_id:
            mention = f"<@{slack_id}>"
        else:
            mention = name_by_email.get(email, email)
        lines.append(f"{i}. {mention} - {count}")

    total = sum(counts.values())
    start_str = fmt_date(start_dt)
    end_str = fmt_date(end_dt)

    if report_type == "9am":
        heading = f"U0001f6a8 *Daily Error Report — {end_str}*"
    else:
        heading = f"U0001f6a8 *Updated Error Report | Status: NEW & UNDER DISCUSSION — {end_str}*"

    start_param = urllib.parse.quote(f"{start_dt.strftime('%Y-%m-%d')} 00:00:00")
    end_param = urllib.parse.quote(f"{end_dt.strftime('%Y-%m-%d')} 23:59:59")
    redash_url = (
        f"https://redash.springworks.in/queries/1528"
        f"?p_check_type=%5B%22ALL%22%5D"
        f"&p_created_at={start_param}--{end_param}"
        f"&p_department=%5B%22OPERATIONS%22%5D"
        f"&p_error_status=%5B%22NEW%22%2C%22UNDER_DISCUSSION%22%5D"
        f"&p_user_email=%5B%22ALL%22%5D#2204"
    )

    text = (
        f"{heading}\n"
        f"*{start_str} to {end_str}*\n"
        f"*Status: NEW & UNDER DISCUSSION | Department: OPERATIONS*\n\n"
        + "\n".join(lines)
        + f"\n*Total - {total}*\n\n"
        + f"U0001f4ca <{redash_url}|View Full Report on Redash>\n\n"
        + "_Tagged agents: Please review and resolve/rectify your open errors at the earliest and acknowledge the message U0001f64f_\n\n"
        + "CC: <!subteam^S08T66C76CS> <@UPAMYUZAS> <@U06T72TD4BD>"
    )
    return text


# ── MAIN REPORT LOGIC ──────────────────────────────────────

def run_report():
    now = datetime.now(IST)
    end_date_str = now.strftime("%Y-%m-%d 23:59:59")
    start_date_str = START_DATE

    print(f"Date range: {start_date_str} to {end_date_str}")

    print("Fetching Redash data...")
    rows = fetch_redash(start_date_str, end_date_str)
    print(f"Got {len(rows)} rows from Redash")

    print("Fetching Slack users...")
    slack_users = get_slack_users()

    start_dt = datetime.strptime(start_date_str.split()[0], "%Y-%m-%d").replace(tzinfo=IST)

    message = build_report(rows, slack_users, start_dt, now, REPORT_TYPE)

    if REPORT_TYPE == "9am":
        print("Posting new Slack message")
        ts = post_slack(message)
        with open(THREAD_FILE, "w") as f:
            f.write(ts)
        print(f"Posted. Thread ts: {ts}")
    else:
        print("Replying in Slack thread")
        ts = find_9am_thread_ts()
        post_slack(message, ts)
        print(f"Replied in thread: {ts}")


# ── ENTRY ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    run_report()

import os
import time
import requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import urllib.parse

# ── CONFIG ─────────────────────────────────────────────────────

_raw_token = os.environ["SLACK_BOT_TOKEN"]
# Browser lowercases all uppercase letters in the token suffix when saving to GitHub Secrets
# Numeric part (positions 4-30) is unaffected; restore prefix and correct suffix
SLACK_TOKEN = "xoxb" + _raw_token[4:31] + "bFqMGfkmHBzvLRtU1It2ptnt"

REDASH_API_KEY = "sMdXlebHKozPGyJjOfAhRpH0S7ggmsSNE8GR5zc7"

REDASH_QUERY_ID = 1528
REDASH_BASE = "https://redash.springworks.in"

OPS_CHANNEL_ID = "CF0RH10M8"  #sv-in-ops (change to CF0RH10M8 for production)

REPORT_TYPE = os.environ.get("REPORT_TYPE", "9am")

THREAD_FILE = "thread_ts.txt"

IST = timezone(timedelta(hours=5, minutes=30))

START_DATE = "2025-09-25 00:00:00"


# ── HELPERS ────────────────────────────────────────────────────

def ordinal(n):
    if 11 <= n <= 13:
        return f"{n}th"
    return f"{n}{['th','st','nd','rd','th'][min(n % 10, 4)]}"


def fmt_date(dt):
    return f"{ordinal(dt.day)} {dt.strftime('%B %Y')}"


# ── FETCH REDASH DATA ─────────────────────────────────────────

def fetch_redash(start_date, end_date):
    """
    POST to /api/queries/{id}/results with max_age=0 to trigger fresh execution.
    If result is cached, it returns immediately.
    If a job is queued, re-POST with max_age=60 until the result is ready.
    Works entirely with API key auth — no session cookie or /api/jobs polling needed.
    """
    headers = {
        "Authorization": f"Key {REDASH_API_KEY}",
        "Content-Type": "application/json"
    }

    start_dt_str = start_date.split()[0] + " 00:00:00"
    end_dt_str = end_date.split()[0] + " 23:59:59"

    payload = {
        "parameters": {
            "error_status": ["NEW", "UNDER_DISCUSSION"],
            "department": ["OPERATIONS"],
            "created_at": {
                "start": start_dt_str,
                "end": end_dt_str
            },
            "check_type": ["ALL"],
            "user_email": ["ALL"]
        },
        "max_age": 0  # Force fresh execution
    }

    url = f"{REDASH_BASE}/api/queries/{REDASH_QUERY_ID}/results"

    print(f"Triggering Redash query: {start_dt_str} → {end_dt_str}")
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    print(f"POST status: {r.status_code}")
    if r.status_code not in (200, 201):
        print(f"Response: {r.text[:500]}")
    r.raise_for_status()
    resp = r.json()

    # Immediate cached result
    if "query_result" in resp:
        rows = resp["query_result"]["data"]["rows"]
        print(f"Got immediate result: {len(rows)} rows")
        return rows

    # Job queued — poll by re-POSTing with max_age=60
    job_id = resp.get("job", {}).get("id", "unknown")
    print(f"Query job queued (id={job_id}), polling for result...")

    poll_payload = {**payload, "max_age": 60}

    for attempt in range(20):
        time.sleep(3)
        print(f"  Poll attempt {attempt + 1}/20...")
        r2 = requests.post(url, headers=headers, json=poll_payload, timeout=30)
        if r2.status_code not in (200, 201):
            print(f"  Poll status {r2.status_code}: {r2.text[:200]}")
            continue
        resp2 = r2.json()
        if "query_result" in resp2:
            rows = resp2["query_result"]["data"]["rows"]
            print(f"  Got result: {len(rows)} rows")
            return rows
        new_job = resp2.get("job", {})
        print(f"  Still running, job status={new_job.get('status')}")

    raise Exception("Timed out waiting for Redash query result after 60 seconds")


# ── SLACK USERS ───────────────────────────────────────────────

def get_slack_users():
    users = {}
    cursor = None
    while True:
        params = {"limit": 200}
        if cursor:
            params["cursor"] = cursor
        r = requests.get(
            "https://slack.com/api/users.list",
            headers={"Authorization": f"Bearer {SLACK_TOKEN}"},
            params=params
        )
        data = r.json()
        if not data.get("ok"):
            raise Exception(f"Slack users.list error: {data.get('error')}")
        for u in data.get("members", []):
            email = u.get("profile", {}).get("email")
            if email:
                users[email.lower()] = u["id"]
        cursor = data.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break
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


# ── FIND TODAY'S 9AM THREAD ────────────────────────────────────

def find_9am_thread_ts():
    # Check local file first (persists within same workflow run)
    if os.path.exists(THREAD_FILE):
        with open(THREAD_FILE) as f:
            ts = f.read().strip()
            if ts:
                print(f"Found thread ts from file: {ts}")
                return ts

    # Search Slack for today's report message
    now = datetime.now(IST)
    today_start = datetime(now.year, now.month, now.day, 0, 0, 0, tzinfo=IST).timestamp()

    r = requests.get(
        "https://slack.com/api/conversations.history",
        headers={"Authorization": f"Bearer {SLACK_TOKEN}"},
        params={
            "channel": OPS_CHANNEL_ID,
            "oldest": str(today_start),
            "limit": 50
        }
    )
    data = r.json()
    if not data.get("ok"):
        raise Exception(f"Slack history error: {data.get('error')}")

    for msg in data.get("messages", []):
        text = msg.get("text", "")
        if "Daily Error Report" in text or "Error Report" in text:
            ts = msg["ts"]
            print(f"Found today's report thread in Slack: {ts}")
            with open(THREAD_FILE, "w") as f:
                f.write(ts)
            return ts

    raise Exception(
        "Could not find today's 9 AM report thread. "
        "Run with REPORT_TYPE=9am first to create the thread."
    )


# ── BUILD REPORT MESSAGE ───────────────────────────────────────

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
        mention = f"<@{slack_id}>" if slack_id else name_by_email.get(email, email)
        lines.append(f"{i}. {mention} - {count}")

    total = sum(counts.values())

    # Find oldest error date from actual Redash data
    oldest_dt = None
    for row in rows:
        raw = row.get("created_at") or row.get("Created At") or row.get("date") or row.get("Date")
        if raw:
            try:
                dt = datetime.strptime(str(raw)[:10], "%Y-%m-%d").replace(tzinfo=IST)
                if oldest_dt is None or dt < oldest_dt:
                    oldest_dt = dt
            except ValueError:
                pass
    if oldest_dt is None:
        oldest_dt = start_dt  # fallback to configured start date

    start_str = fmt_date(oldest_dt)
    end_str = fmt_date(end_dt)

    if report_type == "9am":
        heading = f"\U0001f6a8 *Daily Error Report \u2014 {end_str}*"
    else:
        heading = f"\U0001f6a8 *Updated Error Report | Status: NEW & UNDER DISCUSSION \u2014 {end_str}*"

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
        f"*Error pending from {start_str} to {end_str}*\n"
        f"*Status: NEW & UNDER DISCUSSION | Department: OPERATIONS*\n\n"
        + "\n".join(lines)
        + f"\n*Total - {total}*\n\n"
        + f"\U0001f4ca <{redash_url}|View Full Report on Redash>\n\n"
        + "_Tagged agents: Please review and resolve/rectify your open errors at the earliest and acknowledge the message \U0001f64f_\n\n"
        + "CC: <!subteam^S08T66C76CS> <@UPAMYUZAS> <@U06T72TD4BD>"
    )
    return text


# ── MAIN ───────────────────────────────────────────────────────

def run_report():
    now = datetime.now(IST)
    end_date_str = now.strftime("%Y-%m-%d 23:59:59")
    start_date_str = START_DATE

    print(f"Date range: {start_date_str} to {end_date_str}")
    print(f"Report type: {REPORT_TYPE}")

    print("Fetching Redash data...")
    rows = fetch_redash(start_date_str, end_date_str)
    print(f"Got {len(rows)} rows from Redash")

    print("Fetching Slack users...")
    slack_users = get_slack_users()

    start_dt = datetime.strptime(start_date_str.split()[0], "%Y-%m-%d").replace(tzinfo=IST)

    message = build_report(rows, slack_users, start_dt, now, REPORT_TYPE)

    if REPORT_TYPE == "9am":
        print("Posting new Slack message (9am report)")
        ts = post_slack(message)
        with open(THREAD_FILE, "w") as f:
            f.write(ts)
        print(f"Posted. Thread ts: {ts}")
    else:
        print(f"Replying in Slack thread ({REPORT_TYPE} report)")
        ts = find_9am_thread_ts()
        post_slack(message, ts)
        print(f"Replied in thread: {ts}")


if __name__ == "__main__":
    run_report()

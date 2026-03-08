"""
SVIN Ops Error Report - GitHub Actions
Runs at 9AM, 12PM, 4PM, 6PM IST
Posts to #testing-sefali
"""

import os
import requests
from datetime import datetime, timezone, timedelta


# ── Config ─────────────────────────────────────────────
SLACK_TOKEN = os.environ["SLACK_BOT_TOKEN"]
REDASH_API_KEY = os.environ["REDASH_API_KEY"]

REDASH_BASE_URL = os.environ.get(
    "REDASH_BASE_URL",
    "https://redash.springworks.in"
)

REDASH_QUERY_ID = 1528

OPS_CHANNEL_ID = "C0AGRE19V6U"   # testing-sefali
SUBTEAM_ID = "S08T66C76CS"

CC_USERS = "<@UPAMYUZAS> <@U06T72TD4BD>"

REPORT_TYPE = os.environ.get("REPORT_TYPE", "9am")

IST = timezone(timedelta(hours=5, minutes=30))


# ── Helpers ────────────────────────────────────────────
def ordinal(n):
    if 11 <= n % 100 <= 13:
        return f"{n}th"
    return f"{n}{['th','st','nd','rd','th','th','th','th','th','th'][n % 10]}"


def format_date(dt):
    months = ["Jan","Feb","March","April","May","June","July","Aug","Sept","Oct","Nov","Dec"]
    return f"{ordinal(dt.day)} {months[dt.month - 1]} {dt.year}"


# ── Redash Query ───────────────────────────────────────
def redash_query(params):

    url = f"{REDASH_BASE_URL}/api/query_results"

    headers = {
        "Authorization": f"Key {REDASH_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "query_id": REDASH_QUERY_ID,
        "parameters": params
    }

    resp = requests.post(
        url,
        headers=headers,
        json=payload
    )

    resp.raise_for_status()

    data = resp.json()

    rows = data["query_result"]["data"]["rows"]

    return rows


# ── Slack Post ─────────────────────────────────────────
def post_to_slack(message, thread_ts=None):

    url = "https://slack.com/api/chat.postMessage"

    headers = {
        "Authorization": f"Bearer {SLACK_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "channel": OPS_CHANNEL_ID,
        "text": message
    }

    if thread_ts:
        payload["thread_ts"] = thread_ts

    response = requests.post(url, headers=headers, json=payload)

    response.raise_for_status()

    return response.json()


# ── Build Message ──────────────────────────────────────
def build_message(rows):

    today = datetime.now(IST)
    report_date = format_date(today)

    counts = {}

    for row in rows:
        agent = row.get("user_email", "Unknown")
        counts[agent] = counts.get(agent, 0) + 1

    sorted_agents = sorted(counts.items(), key=lambda x: x[1], reverse=True)

    lines = []

    for i, (agent, count) in enumerate(sorted_agents, start=1):
        lines.append(f"{i}. @{agent} - {count}")

    total = sum(counts.values())

    message = (
        f"🚨 Daily Error Report – {report_date}\n\n"
        f"Status: NEW & UNDER_DISCUSSION | Department: OPERATIONS\n\n"
        + "\n".join(lines)
        + f"\n\nTotal - {total}\n\n"
        f"Tagged agents: Please review and resolve/rectify your open errors\n"
        f"at the earliest and acknowledge the message 🙏\n\n"
        f"CC: <!subteam^{SUBTEAM_ID}> {CC_USERS}"
    )

    return message


# ── Run Report ─────────────────────────────────────────
def run_report():

    print(f"Running {REPORT_TYPE} report...")

    params = {
        "p_check_type": ["ALL"],
        "p_created_at": "2025-09-25 00:00:00--2026-03-08 23:59:59",
        "p_department": ["OPERATIONS"],
        "p_error_status": ["NEW", "UNDER_DISCUSSION"],
        "p_user_email": ["ALL"]
    }

    rows = redash_query(params)

    message = build_message(rows)

    post_to_slack(message)

    print("Report sent successfully.")


# ── Entry ──────────────────────────────────────────────
if __name__ == "__main__":
    run_report()

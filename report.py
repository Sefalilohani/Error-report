"""
SVIN Ops Error Report - GitHub Actions
Runs at 9AM, 12PM, 4PM, 6PM IST
Posts to Slack
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

OPS_CHANNEL_ID = "CF0RH10M8"
SUBTEAM_ID = "S08T66C76CS"
CC_USERS = "<@UPAMYUZAS> <@U06T72TD4BD>"

REPORT_TYPE = os.environ.get("REPORT_TYPE", "9am")

IST = timezone(timedelta(hours=5, minutes=30))


# ── Date Helpers ───────────────────────────────────────
def ordinal(n):
    if 11 <= n % 100 <= 13:
        return f"{n}th"
    return f"{n}{['th','st','nd','rd','th','th','th','th','th','th'][n % 10]}"


def format_date(dt):
    months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    return f"{ordinal(dt.day)} {months[dt.month - 1]} {dt.year}"


# ── Fetch Redash Data ───────────────────────────────────
def fetch_redash_data():

    url = f"{REDASH_BASE_URL}/api/queries/{REDASH_QUERY_ID}/results"

    headers = {
        "Authorization": f"Key {REDASH_API_KEY}"
    }

    params = {
        "p_check_type": '["ALL"]',
        "p_created_at": "2026-02-01 00:00:00--2026-03-08 23:59:59",
        "p_department": '["OPERATIONS"]',
        "p_error_status": '["NEW","UNDER_DISCUSSION"]',
        "p_user_email": '["ALL"]'
    }

    response = requests.get(url, headers=headers, params=params)

    response.raise_for_status()

    data = response.json()

    rows = data["query_result"]["data"]["rows"]

    return rows


# ── Slack Message ───────────────────────────────────────
def send_to_slack(message):

    url = "https://slack.com/api/chat.postMessage"

    headers = {
        "Authorization": f"Bearer {SLACK_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "channel": OPS_CHANNEL_ID,
        "text": message
    }

    response = requests.post(url, headers=headers, json=payload)

    response.raise_for_status()


# ── Main Report Logic ───────────────────────────────────
def run_report():

    print(f"Running {REPORT_TYPE} report...")

    rows = fetch_redash_data()

    total_errors = len(rows)

    now = datetime.now(IST)

    report_date = format_date(now)

    message = (
        f"*SVIN Ops Error Report*\n\n"
        f"Date: {report_date}\n"
        f"Pending Errors: *{total_errors}*\n\n"
        f"<!subteam^{SUBTEAM_ID}>\n"
        f"{CC_USERS}"
    )

    send_to_slack(message)

    print("Report sent successfully.")


# ── Run ─────────────────────────────────────────────────
if __name__ == "__main__":
    run_report()

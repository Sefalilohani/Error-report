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

 import os                                                                                                                                                                                                                                                                      import time                                                                                                                                                                                                                                                                    import requests
  from datetime import datetime, timezone, timedelta

  # ── CONFIG ─────────────────────────────────────────────────────

  _raw_token = os.environ["SLACK_BOT_TOKEN"]
  SLACK_TOKEN = "xoxb" + _raw_token[4:31] + "bFqMGfkmHBzvLRtU1It2ptnt"

  ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]

  REDASH_API_KEY = "CWcvNsz8fkzifFJPD6r7kc2T6TCU6pbhxa0z0nRm"
  REDASH_BASE     = "https://redash.springworks.in"
  REDASH_QUERY_ID = 590

  PACKAGE_TEXT_CHANNEL    = "C08DG4H2NG2"   # sv-package-text-request
  PRODUCT_UPDATES_CHANNEL = "C08FZLDSEEA"   # sv-in-product-updates
  WORKFLOW_BOT_ID         = "B0661F6DEKD"   # Package text Request bot
  SEFALI_USER_ID          = "UNX1PPM3M"

  IST = timezone(timedelta(hours=5, minutes=30))

  # ── SYSTEM PROMPT ──────────────────────────────────────────────

  SYSTEM_PROMPT = """You are the SpringVerify Package Text Skill bot. You help draft or confirm package texts for SpringVerify client verification packages.

  What is a Package Text?
  A package text is a short instruction stored in Redash for how a specific check should be processed for a given client sub-package. It tells the verification team exactly how to handle edge cases or special client requirements.

  Format:
  CHECK_TYPE: Instruction in plain English.

  Examples:
  EMP: Client to Client Mark N/A
  EDU: Mark the check as N/A
  ADD: Process without documents. Capturing selfie is not mandatory. || ADD: Grade the check as verified with the selfie and ID proof images.
  UAN: If LWD missing, refer to EPFO for wage month and year. Grade as Yellow if LWD missing in both UAN and EPFO
  MISC: Resume Review Required

  Rules for package text format:
  - Start with the check type abbreviation in CAPS: EMP, EDU, ADD, CRT, ID, REF, UAN, WCK, CC, DRG, CSM, MISC, OVERLAP, etc.
  - Follow with a colon and a space
  - Keep instructions concise and actionable
  - Use || to separate multiple instructions for the same check
  - For multiple checks, write each on a new line

  Key process rules:
  - EMP Physical Visit (PV): Supported as fallback only. Initiated Day 9 after min 5 attempts, no verifier response, UAN unavailable. Not standalone — ADC raised in SV.
  - EMP UAN grading: Grade immediately if UAN data is available and sufficient.
  - ADD: Digital verification first; physical fallback after SLA.
  - EDU: Standard affiliation and document-based verification.

  Key product constraints:
  - PV for EMP is not a separate product check — package text + ADC manually raised in SV
  - Bank Statement / Penny Drop: custom add-on (released Feb 2026)
  - UAN Employment History: integrated via SVD (released Feb 2026)
  - Court checks, Drug tests, World Check, CIBIL etc. are separate check types in product

  Tips for drafting:
  - Be specific and actionable
  - Use standard grading language: "Grade as Red", "Grade as Yellow", "Grade as Green", "Mark as N/A", "Place on Hold"
  - Mirror style of similar existing package texts
  - Keep it concise — clear, direct instructions
  - If a suggested package text was provided, use it as base and refine for clarity
  - For UAN: LWD instructions, overlap rules, EPFO instructions
  - For EMP: C2C instructions, insufficiency rules, resume matching, PV fallback
  - For EDU: affiliation rules, N/A conditions, hold conditions
  - For ADD: document-free processing, selfie/ID proof grading, DAV/physical fallback

  You will receive:
  1. The request details (company, check type, client requirement, suggested text if any)
  2. Existing package texts from Redash for reference (may be empty)
  3. Recent product updates from Slack

  Based on this, respond with ONE of:

  Case A — NOT feasible (process or product issue):
  ⚠️ Before we proceed, there's something to clarify:
  *Process / Product concern:* [explain what's not supported or needs clarification]
  *Suggested next step:* [what to do]
  *Sent using @Claude*

  Case B — Existing package text already found for this company + scenario:
  ✅ Package text already exists for [Company Name]:
  `[existing package text]`
  Let me know if this needs to be updated!
  *Sent using @Claude*

  Case C — Feasible, no existing text, draft new:
  📝 _No existing package text found for this specific scenario._ Here's a suggested package text:
  `[CHECK_TYPE]: [drafted instruction]`
  _Based on:_ [brief explanation of client requirement]
  ✅ _Process check:_ [confirm alignment with ops process]
  ✅ _Product check:_ [confirm product support or flag ADC/workaround]
  Please review and approve! ✅
  *Sent using @Claude*"""


  # ── REDASH ─────────────────────────────────────────────────────

  def fetch_redash_package_texts():
      headers = {
          "Authorization": f"Key {REDASH_API_KEY}",
          "Content-Type": "application/json"
      }
      payload = {"parameters": {}, "max_age": 3600}
      url = f"{REDASH_BASE}/api/queries/{REDASH_QUERY_ID}/results"

      print("Fetching Redash package texts...")
      r = requests.post(url, headers=headers, json=payload, timeout=30)
      r.raise_for_status()
      resp = r.json()

      if "query_result" in resp:
          rows = resp["query_result"]["data"]["rows"]
          print(f"Got {len(rows)} rows from Redash")
          return rows

      # Poll if job queued
      poll_payload = {**payload, "max_age": 60}
      for attempt in range(20):
          time.sleep(3)
          r2 = requests.post(url, headers=headers, json=poll_payload, timeout=30)
          if r2.status_code not in (200, 201):
              continue
          resp2 = r2.json()
          if "query_result" in resp2:
              rows = resp2["query_result"]["data"]["rows"]
              print(f"Got {len(rows)} rows from Redash (after polling)")
              return rows

      print("Redash timed out, proceeding without DB data")
      return []


  def filter_redash_results(rows, company_name, check_type):
      company_lower = company_name.lower()
      check_upper = check_type.upper()

      # Company-specific matches
      company_matches = [
          r for r in rows
          if company_lower in (r.get("Company") or "").lower()
          and r.get("Sub Package text")
      ]

      # Similar check type across all companies
      check_matches = [
          r for r in rows
          if check_upper in (r.get("Sub Package text") or "").upper()
          and r.get("Sub Package text")
      ][:10]

      return company_matches, check_matches


  # ── SLACK ───────────────────────────────────────────────────────

  def get_channel_messages(channel_id, limit=15):
      r = requests.get(
          "https://slack.com/api/conversations.history",
          headers={"Authorization": f"Bearer {SLACK_TOKEN}"},
          params={"channel": channel_id, "limit": limit}
      )
      data = r.json()
      if not data.get("ok"):
          raise Exception(f"conversations.history error: {data.get('error')}")
      return data.get("messages", [])


  def get_thread_replies(channel_id, thread_ts):
      r = requests.get(
          "https://slack.com/api/conversations.replies",
          headers={"Authorization": f"Bearer {SLACK_TOKEN}"},
          params={"channel": channel_id, "ts": thread_ts}
      )
      data = r.json()
      if not data.get("ok"):
          raise Exception(f"conversations.replies error: {data.get('error')}")
      return data.get("messages", [])


  def post_to_thread(channel_id, thread_ts, text):
      r = requests.post(
          "https://slack.com/api/chat.postMessage",
          headers={
              "Authorization": f"Bearer {SLACK_TOKEN}",
              "Content-Type": "application/json"
          },
          json={"channel": channel_id, "thread_ts": thread_ts, "text": text}
      )
      r.raise_for_status()
      resp = r.json()
      if not resp.get("ok"):
          raise Exception(f"chat.postMessage error: {resp.get('error')}")
      print(f"Posted reply to thread {thread_ts}")
      return resp["ts"]


  # ── CLAUDE ──────────────────────────────────────────────────────

  def call_claude(request_details, company_matches, check_matches, product_updates):
      user_message = f"""Here is a new package text request. Please respond using the appropriate case format.

  --- REQUEST DETAILS ---
  {request_details}

  --- EXISTING PACKAGE TEXTS FOR THIS COMPANY (from Redash) ---
  {format_redash_rows(company_matches) if company_matches else "No existing package text found for this company."}

  --- SIMILAR PACKAGE TEXTS FOR THIS CHECK TYPE (reference only) ---
  {format_redash_rows(check_matches) if check_matches else "No similar package texts found for this check type."}

  --- RECENT PRODUCT UPDATES ---
  {product_updates if product_updates else "No recent product updates available."}
  """

      r = requests.post(
          "https://api.anthropic.com/v1/messages",
          headers={
              "x-api-key": ANTHROPIC_API_KEY,
              "anthropic-version": "2023-06-01",
              "content-type": "application/json"
          },
          json={
              "model": "claude-sonnet-4-6",
              "max_tokens": 1024,
              "system": SYSTEM_PROMPT,
              "messages": [{"role": "user", "content": user_message}]
          },
          timeout=60
      )
      r.raise_for_status()
      resp = r.json()
      return resp["content"][0]["text"]


  def format_redash_rows(rows):
      lines = []
      for row in rows:
          company = row.get("Company", "")
          package = row.get("Package name", "")
          sub_pkg = row.get("Sub Package Name", "")
          text = row.get("Sub Package text", "")
          lines.append(f"- {company} | {package} | {sub_pkg}: {text}")
      return "\n".join(lines)


  # ── PARSE REQUEST ───────────────────────────────────────────────

  def parse_request(text):
      lines = text.split("\n")
      details = {}
      for line in lines:
          if "Name of the company" in line:
              details["company"] = line.split("-", 1)[-1].strip()
          elif "Checks required" in line:
              details["checks"] = line.split("-", 1)[-1].strip()
          elif "Comments from the team" in line:
              details["comments"] = line.split("-", 1)[-1].strip()
          elif "Suggested Package Text" in line:
              details["suggested"] = line.split("-", 1)[-1].strip()
      return details


  def extract_check_type(checks_str):
      if not checks_str:
          return "EMP"
      checks_lower = checks_str.lower()
      mapping = {
          "employment": "EMP", "education": "EDU", "address": "ADD",
          "uan": "UAN", "court": "CRT", "drug": "DRG", "reference": "REF",
          "identity": "ID", "world check": "WCK", "credit": "CC",
          "cibil": "CSM", "misc": "MISC"
      }
      for keyword, abbr in mapping.items():
          if keyword in checks_lower:
              return abbr
      return checks_str.split()[0].upper()[:3]


  # ── MAIN ────────────────────────────────────────────────────────

  def main():
      print(f"Running package text bot at {datetime.now(IST).strftime('%Y-%m-%d %H:%M IST')}")

      # Fetch all data upfront
      messages = get_channel_messages(PACKAGE_TEXT_CHANNEL, limit=15)
      redash_rows = fetch_redash_package_texts()
      product_update_msgs = get_channel_messages(PRODUCT_UPDATES_CHANNEL, limit=30)
      product_updates = "\n".join([
          f"[{m.get('username', 'unknown')}]: {m.get('text', '')[:300]}"
          for m in product_update_msgs if m.get("text")
      ])

      handled = 0
      for msg in messages:
          bot_id = msg.get("bot_id", "")
          text = msg.get("text", "")
          ts = msg.get("ts", "")

          # Only workflow bot messages
          if bot_id != WORKFLOW_BOT_ID:
              continue

          # Only if Sefali is tagged
          if f"<@{SEFALI_USER_ID}>" not in text:
              print(f"Skipping message {ts} — Sefali not tagged")
              continue

          # Only package text requests
          if "New package text request" not in text:
              continue

          print(f"Found workflow request: {ts}")

          # Check if already replied
          replies = get_thread_replies(PACKAGE_TEXT_CHANNEL, ts)
          already_replied = any(
              "Sent using @Claude" in r.get("text", "")
              for r in replies
              if r.get("ts") != ts
          )
          if already_replied:
              print(f"Already replied to {ts}, skipping")
              continue

          # Parse request
          parsed = parse_request(text)
          company = parsed.get("company", "Unknown")
          checks = parsed.get("checks", "")
          comments = parsed.get("comments", "")
          suggested = parsed.get("suggested", "")
          check_type = extract_check_type(checks)

          request_details = f"""Company: {company}
  Check Type(s): {checks}
  Client Requirement / Comments: {comments}
  Suggested Package Text: {suggested if suggested else "None provided"}"""

          print(f"Processing: {company} | {checks}")

          # Filter Redash results
          company_matches, check_matches = filter_redash_results(redash_rows, company, check_type)
          print(f"Redash: {len(company_matches)} company matches, {len(check_matches)} check type matches")

          # Call Claude
          print("Calling Claude API...")
          response_text = call_claude(request_details, company_matches, check_matches, product_updates)

          # Post reply
          post_to_thread(PACKAGE_TEXT_CHANNEL, ts, response_text)
          handled += 1

      print(f"Done. Handled {handled} new request(s).")


  if __name__ == "__main__":
      main()

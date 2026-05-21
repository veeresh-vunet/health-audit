import imaplib
import email
import re
import argparse
import time
import threading
import json
import os
import signal
import sys
import requests
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
import pytz

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

# ─────────────────────────────────────────────
#  EMAIL CONFIG
# ─────────────────────────────────────────────
EMAIL_ADDRESS  = os.environ.get("EMAIL_ADDRESS", "care-notifications@vunetsystems.com")
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD", "trro ervb uyws jytu")
IMAP_SERVER    = os.environ.get("IMAP_SERVER", "imap.gmail.com")

_SUBJECTS_FILE = os.environ.get(
    "SUBJECTS_FILE",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "subjects.txt"),
)

def _load_subjects(path: str) -> list[str]:
    with open(path) as f:
        return [line.strip() for line in f if line.strip() and not line.startswith("#")]

TARGET_SUBJECTS = _load_subjects(_SUBJECTS_FILE)

DRY_RUN = False

# ─────────────────────────────────────────────
#  GREEN API (WhatsApp) CONFIG
# ─────────────────────────────────────────────
INSTANCE_ID   = os.environ.get("GREEN_INSTANCE_ID", "7103523858")
API_TOKEN     = os.environ.get("GREEN_API_TOKEN", "a29efb612cb842eab150ee88ee29ca1b21086a6bce344dcea3")
GROUP_CHAT_ID = os.environ.get("GREEN_GROUP_CHAT_ID", "120363424154615368@g.us")

# ─────────────────────────────────────────────
#  DASHBOARD CONFIG
# ─────────────────────────────────────────────
DASHBOARD_URL = os.environ.get(
    "DASHBOARD_URL",
    "http://bit.ly/4nc5oiI",
)

# ─────────────────────────────────────────────
#  CLICKHOUSE CONFIG
# ─────────────────────────────────────────────
CH_HOST     = os.environ.get("CH_HOST",     "clickhouse.vsmaps.svc.cluster.local")
CH_PORT     = int(os.environ.get("CH_PORT", "8123"))
CH_USER     = os.environ.get("CH_USER",     "vusmartmanager")
CH_PASSWORD = os.environ.get("CH_PASSWORD", "Vunet#1234")

# ─────────────────────────────────────────────
#  CLIENT NAME — derived from email subject
# ─────────────────────────────────────────────
SUBJECT_PREFIX = "NG Audit Check - "

def client_name_from_subject(subject: str) -> str:
    """Strip 'NG Audit Check - ' prefix → client name."""
    s = subject.strip()
    if s.startswith(SUBJECT_PREFIX):
        return s[len(SUBJECT_PREFIX):]
    return s or "Unknown"

# ─────────────────────────────────────────────
#  LIVENESS / HEALTH CONFIG
# ─────────────────────────────────────────────
HEALTH_PORT             = int(os.environ.get("HEALTH_PORT", "8083"))
POLL_INTERVAL_SECONDS   = int(os.environ.get("POLL_INTERVAL_SECONDS", "120"))
HEARTBEAT_FILE          = os.environ.get("HEARTBEAT_FILE", "/tmp/ng_audit_heartbeat")
HEARTBEAT_MAX_AGE       = POLL_INTERVAL_SECONDS * 2
IMAP_TIMEOUT            = int(os.environ.get("IMAP_TIMEOUT", "30"))

PERCENT_CUSTOM_ALERT_THRESHOLD = 80.0

# ─────────────────────────────────────────────
#  KAFKA LAG CONFIG
# ─────────────────────────────────────────────
KAFKA_LAG_ALERT_DIGITS = int(os.environ.get("KAFKA_LAG_ALERT_DIGITS", "8"))

# ─────────────────────────────────────────────
#  CORE POD PATTERNS  (P1 – Critical)
# ─────────────────────────────────────────────
import re as _re
CORE_POD_PATTERNS = _re.compile(
    r"^(?:"
    r"chi-clickhouse-vusmart-"
    r"|kafka-cluster-cp-kafka-"
    r"|vuinterface-cairo-"
    r"|keycloak-deployment-"
    r"|nairobi-1-"
    r"|postgresql-0(?:$|-)"
    r")",
    _re.IGNORECASE,
)
del _re

IST = pytz.timezone("Asia/Kolkata")

PROCESSED_MAIL_IDS_FILE = os.environ.get(
    "PROCESSED_MAIL_IDS_FILE",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "ng_processed_mail_ids.json"),
)
MAX_PROCESSED_MAIL_IDS = 500

MAX_EMAILS_PER_CYCLE = 20
MAX_MESSAGES_PER_SUBJECT = 5


# ═══════════════════════════════════════════════════════════════
#  HEARTBEAT
# ═══════════════════════════════════════════════════════════════

def write_heartbeat():
    os.makedirs(os.path.dirname(HEARTBEAT_FILE) or ".", exist_ok=True)
    with open(HEARTBEAT_FILE, "w") as f:
        f.write(str(time.time()))

def read_heartbeat_age() -> float:
    try:
        with open(HEARTBEAT_FILE, "r") as f:
            ts = float(f.read().strip())
        return time.time() - ts
    except Exception:
        return float("inf")

# ═══════════════════════════════════════════════════════════════
#  PROCESSED MAIL IDs
# ═══════════════════════════════════════════════════════════════

def read_processed_mail_ids() -> list[str]:
    if not os.path.exists(PROCESSED_MAIL_IDS_FILE):
        return []
    try:
        with open(PROCESSED_MAIL_IDS_FILE, "r") as f:
            data = json.load(f)
        if isinstance(data, list):
            return [str(x) for x in data if x]
        return []
    except Exception:
        return []


def is_mail_processed(mail_id: str) -> bool:
    return mail_id in set(read_processed_mail_ids())


def mark_processed_mail_id(mail_id: str) -> None:
    if not mail_id:
        return
    os.makedirs(os.path.dirname(PROCESSED_MAIL_IDS_FILE) or ".", exist_ok=True)
    ids = read_processed_mail_ids()
    if mail_id in ids:
        return
    ids.append(mail_id)
    if len(ids) > MAX_PROCESSED_MAIL_IDS:
        ids = ids[-MAX_PROCESSED_MAIL_IDS:]
    tmp = PROCESSED_MAIL_IDS_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(ids, f)
    os.replace(tmp, PROCESSED_MAIL_IDS_FILE)

# ═══════════════════════════════════════════════════════════════
#  SHIFT HELPER
# ═══════════════════════════════════════════════════════════════

def get_shift(mail_date) -> str:
    return "BOD" if 6 <= mail_date.hour < 13 else "EOD"


# ═══════════════════════════════════════════════════════════════
#  STEP 1 – FETCH UNPROCESSED AUDIT EMAILS
# ═══════════════════════════════════════════════════════════════

def fetch_unprocessed_audit_emails() -> list[dict]:
    print(f"[*] Connecting to {IMAP_SERVER} (timeout={IMAP_TIMEOUT}s) ...")
    mail = imaplib.IMAP4_SSL(IMAP_SERVER, timeout=IMAP_TIMEOUT)
    try:
        mail.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        mail.select("inbox")

        processed_ids = set(read_processed_mail_ids())

        today_str = datetime.now(IST).strftime("%d-%b-%Y")
        yesterday_str = (datetime.now(IST) - timedelta(days=1)).strftime("%d-%b-%Y")
        since_candidates = [today_str, yesterday_str]

        candidate_ids: set[bytes] = set()
        for subj in TARGET_SUBJECTS:
            found_for_subject = False
            for since_str in since_candidates:
                criteria = f'(SUBJECT "{subj}" SINCE "{since_str}")'
                print(f"[*] IMAP search: {criteria}")
                st, msgs = mail.search(None, criteria)
                if st == "OK" and msgs and msgs[0]:
                    ids = msgs[0].split()
                    ids_sorted = sorted(ids, key=lambda b: int(b))
                    candidate_ids.update(ids_sorted[-MAX_MESSAGES_PER_SUBJECT:])
                    found_for_subject = True
                    break

            if found_for_subject:
                continue

            criteria = f'(SUBJECT "{subj}")'
            print(f"[*] IMAP search fallback: {criteria}")
            st, msgs = mail.search(None, criteria)
            if st == "OK" and msgs and msgs[0]:
                ids = msgs[0].split()
                ids_sorted = sorted(ids, key=lambda b: int(b))
                candidate_ids.update(ids_sorted[-MAX_MESSAGES_PER_SUBJECT:])

        if not candidate_ids:
            return []

        candidate_ids_sorted = sorted(candidate_ids, key=lambda b: int(b), reverse=True)
        results: list[dict] = []
        email_re = re.compile(r"[\w.\-+]+@[\w.\-]+")

        for mid in candidate_ids_sorted:
            if len(results) >= MAX_EMAILS_PER_CYCLE:
                break

            st, data = mail.fetch(mid, "(RFC822.HEADER)")
            if st != "OK" or not data or not data[0]:
                continue

            raw_header = data[0][1]
            header_msg = email.message_from_bytes(raw_header)
            mail_id = header_msg.get("Message-ID", "") or ""
            if not mail_id:
                mail_id = f"NO_MESSAGE_ID::{mid.decode(errors='ignore')}"

            if mail_id in processed_ids:
                continue

            st, data = mail.fetch(mid, "(RFC822)")
            if st != "OK" or not data or not data[0]:
                continue

            raw_email = data[0][1]
            msg = email.message_from_bytes(raw_email)

            mail_date_raw = msg.get("Date", "")
            mail_date = email.utils.parsedate_to_datetime(mail_date_raw)
            if mail_date.tzinfo is None:
                mail_date = mail_date.replace(tzinfo=pytz.UTC)
            mail_date = mail_date.astimezone(IST)

            from_header = msg.get("From", "")
            m = email_re.search(from_header)
            sender_email = m.group(0).lower() if m else from_header.lower()

            # Decode subject (may be RFC2047-encoded)
            raw_subject = msg.get("Subject", "")
            decoded_parts = email.header.decode_header(raw_subject)
            subject = ""
            for part, charset in decoded_parts:
                if isinstance(part, bytes):
                    subject += part.decode(charset or "utf-8", errors="replace")
                else:
                    subject += part

            body_plain = ""
            body_html = ""
            if msg.is_multipart():
                for part in msg.walk():
                    ct = part.get_content_type()
                    if ct == "text/plain" and not body_plain:
                        body_plain = part.get_payload(decode=True).decode("utf-8", errors="replace")
                    elif ct == "text/html" and not body_html:
                        body_html = part.get_payload(decode=True).decode("utf-8", errors="replace")
            else:
                raw = msg.get_payload(decode=True).decode("utf-8", errors="replace")
                if msg.get_content_type() == "text/html":
                    body_html = raw
                else:
                    body_plain = raw

            if body_plain:
                body = body_plain
                print("[+] Body source: text/plain")
            else:
                body = re.sub(r"<[^>]+>", " ", body_html)
                body = re.sub(r"&nbsp;", " ", body)
                body = re.sub(r"&amp;", "&", body)
                body = re.sub(r"&[a-z]+;", "", body)
                body = re.sub(r"[ \t]{2,}", " ", body)
                body = "\n".join(line.strip() for line in body.splitlines() if line.strip())
                print("[+] Body source: text/html (tags stripped)")

            results.append({
                "mail_id":      mail_id,
                "sender_email": sender_email,
                "subject":      subject,
                "body":         body,
                "mail_date":    mail_date,
            })

        return results

    finally:
        try:
            mail.logout()
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════
#  STEP 2 – PARSE AUDIT REPORT  (WhatsApp alert logic)
# ═══════════════════════════════════════════════════════════════

def parse_audit_report(body: str) -> dict:
    def _slice_between_lines(text: str, start_line_re: str, end_line_re: str) -> str:
        lines = text.splitlines()
        start_re = re.compile(start_line_re, re.IGNORECASE)
        end_re = re.compile(end_line_re, re.IGNORECASE)
        start_i = end_i = None
        for i, line in enumerate(lines):
            if start_i is None and start_re.match(line.strip()):
                start_i = i
                continue
            if start_i is not None and end_re.match(line.strip()):
                end_i = i
                break
        if start_i is None:
            return ""
        return "\n".join(lines[start_i + 1:] if end_i is None else lines[start_i + 1:end_i])

    high_usage_pod_re = re.compile(
        r"High Usage Pod\s*\|\s*[^|]+\s*\|\s*([^|]+?)\s*\|\s*([0-9]+(?:\.[0-9]+)?)\s*%",
        re.IGNORECASE,
    )
    cpu_re = re.compile(r"CPU:\s*([0-9]+(?:\.[0-9]+)?)\s*%", re.IGNORECASE)
    mem_re = re.compile(r"Memory:\s*([0-9]+(?:\.[0-9]+)?)\s*%", re.IGNORECASE)

    disk_section        = _slice_between_lines(body, r"^Disk Summary Checks\s*$",           r"^Enrichment & Pod Restart Check\s*$")
    enrichment_section  = _slice_between_lines(body, r"^Enrichment & Pod Restart Check\s*$", r"^Kafka Lag Checks\s*$")
    kafka_lag_section   = _slice_between_lines(body, r"^Kafka Lag Checks\s*$",               r"^Data Retention Check\s*$")
    retention_section   = _slice_between_lines(body, r"^Data Retention Check\s*$",           r"^License Check\s*$")
    license_section     = _slice_between_lines(body, r"^License Check\s*$",                  r"^User Engagement Check\s*$")

    # 1) Disk
    disk_alert_rows = []
    disk_row_re = re.compile(
        r"^\|\s*(Node|Pod)\s*\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|\s*([0-9]+(?:\.[0-9]+)?)%\s*\|\s*([0-9]+(?:\.[0-9]+)?)\s*\|\s*(Pass|Fail|Warn|Info)\s*\|",
        re.IGNORECASE,
    )
    for line in disk_section.splitlines():
        m = disk_row_re.match(line.strip())
        if not m:
            continue
        usage_pct = float(m.group(4).strip())
        status = m.group(6).strip().upper()
        if status != "FAIL" or usage_pct < PERCENT_CUSTOM_ALERT_THRESHOLD:
            continue
        disk_alert_rows.append({
            "target":     m.group(2).strip(),
            "mount_path": m.group(3).strip(),
            "usage_pct":  usage_pct,
        })

    # 2) Enrichment
    pod_resource_usage_parent_fail = False
    node_resource_usage_parent_fail = False
    pod_ns = "pods"
    enrichment_high_usage_pods: list[tuple[str, float]] = []
    enrichment_high_usage_nodes: list[tuple[str, float]] = []
    enrichment_core_unhealthy_pods: list[tuple[str, str]] = []
    pod_ns_re = re.compile(r"Pod Resource Usage\s*\(([^)]+)\)", re.IGNORECASE)

    for line in enrichment_section.splitlines():
        parts = [p.strip() for p in line.split("|")]
        if len(parts) == 5 and parts[0] == "" and parts[-1] == "":
            component = parts[1]
            status = parts[2].upper()
            if component.lower().startswith("pod resource usage (") and status == "FAIL":
                pod_resource_usage_parent_fail = True
                ns_m = pod_ns_re.search(component)
                if ns_m:
                    pod_ns = ns_m.group(1).strip()
            if component.lower().startswith("node resource usage") and status == "FAIL":
                node_resource_usage_parent_fail = True

        if "High Usage Pod" in line:
            m = high_usage_pod_re.search(line)
            if m:
                enrichment_high_usage_pods.append((m.group(1).strip(), float(m.group(2))))

        if "High Usage Node" in line:
            node_parts = [p.strip() for p in line.split("|")]
            if len(node_parts) >= 5:
                node_host = node_parts[3]
                desc      = node_parts[4]
                cpu_m = cpu_re.search(desc)
                mem_m = mem_re.search(desc)
                cpu_pct = float(cpu_m.group(1)) if cpu_m else 0.0
                mem_pct = float(mem_m.group(1)) if mem_m else 0.0
                enrichment_high_usage_nodes.append((node_host, max(cpu_pct, mem_pct)))

        if "Unhealthy Pod" in line:
            pod_desc_parts = [p.strip() for p in line.split("|")]
            if len(pod_desc_parts) >= 4:
                desc = pod_desc_parts[3]
                pm = re.match(
                    r"([\w][\w\-]*)/([\w][\w\-\.]*)\s*-\s*(crashing|pending|unknown|error|failed)",
                    desc, re.IGNORECASE,
                )
                if pm:
                    ns = pm.group(1)
                    pod_name = pm.group(2)
                    state = pm.group(3).lower()
                    if CORE_POD_PATTERNS.match(pod_name):
                        enrichment_core_unhealthy_pods.append((f"{ns}/{pod_name}", state))

    pod_alert_usages = [
        (n, p) for n, p in enrichment_high_usage_pods
        if pod_resource_usage_parent_fail and p >= PERCENT_CUSTOM_ALERT_THRESHOLD
    ]
    node_alert_usages = [
        (n, p) for n, p in enrichment_high_usage_nodes
        if node_resource_usage_parent_fail and p >= PERCENT_CUSTOM_ALERT_THRESHOLD
    ]

    # 3) Kafka lag
    kafka_lag_alert_rows: list[dict] = []
    kafka_lag_row_re = re.compile(
        r"^\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|\s*([0-9]+)\s*\|\s*(Pass|Fail|Warn|Info)\s*\|",
        re.IGNORECASE,
    )
    for line in kafka_lag_section.splitlines():
        m = kafka_lag_row_re.match(line.strip())
        if not m:
            continue
        lag_count = int(m.group(3).strip())
        if m.group(4).strip().upper() != "FAIL" or len(str(lag_count)) < KAFKA_LAG_ALERT_DIGITS:
            continue
        kafka_lag_alert_rows.append({
            "group": m.group(1).strip(),
            "topic": m.group(2).strip(),
            "lag":   lag_count,
        })

    # 4) Retention — Default + Custom + Transactions subsections
    retention_alert_rows: list[dict] = []
    retention_row_re = re.compile(
        r"^\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|\s*([0-9]+)\s*\|\s*([0-9]+)\s*\|\s*([^|]+?)\s*\|",
        re.IGNORECASE,
    )
    for line in retention_section.splitlines():
        m = retention_row_re.match(line.strip())
        if not m:
            continue
        table_name = m.group(1).strip()
        if table_name.lower() == "table name":
            continue
        status = m.group(6).strip()
        if not status.lower().startswith("fail"):
            continue
        days_diff      = int(m.group(4).strip())
        retention_days = int(m.group(5).strip())
        if 0 < days_diff - retention_days <= 1:
            continue
        retention_alert_rows.append({
            "table_name":     table_name,
            "days_diff":      days_diff,
            "retention_days": retention_days,
            "status":         status,
        })

    # 5) License
    license_data: dict[str, str] = {}
    kv_re_lic = re.compile(r"^\|\s*([^|]+?)\s*\|\s*([^|]*?)\s*\|", re.IGNORECASE)
    for line in license_section.splitlines():
        m = kv_re_lic.match(line.strip())
        if not m:
            continue
        key = m.group(1).strip().lower()
        val = m.group(2).strip()
        if key == "details":
            continue
        license_data[key] = val

    # Build result
    result: dict = {"summary": [], "has_failures": False}

    if disk_alert_rows:
        result["summary"].append({
            "check": "Disk Summary Check", "priority": "P1", "status": "FAIL",
            "description": (
                "Disk usage failures: "
                + ", ".join(f"{r['target']} {r['mount_path']} ({r['usage_pct']:.1f}%)" for r in disk_alert_rows[:4])
                + (f" (+{max(0, len(disk_alert_rows)-4)} more)" if len(disk_alert_rows) > 4 else "")
            ),
        })
        result["has_failures"] = True

    if enrichment_core_unhealthy_pods:
        seen_core: set[str] = set()
        ordered_core: list[tuple[str, str]] = []
        for (fullname, state) in enrichment_core_unhealthy_pods:
            if fullname not in seen_core:
                seen_core.add(fullname)
                ordered_core.append((fullname, state))
        result["summary"].append({
            "check": "Unhealthy Pod Check (Critical)", "priority": "P1", "status": "FAIL",
            "description": (
                f"{len(ordered_core)} core pod(s) unhealthy: "
                + ", ".join(f"{name} [{state}]" for (name, state) in ordered_core[:6])
                + (f" (+{max(0, len(ordered_core)-6)} more)" if len(ordered_core) > 6 else "")
            ),
        })
        result["has_failures"] = True

    if retention_alert_rows:
        parts = []
        for r in retention_alert_rows:
            reason_m = re.search(r"\((.+?)\)", r["status"])
            if reason_m:
                parts.append(f"{r['table_name']} — {reason_m.group(1)}")
            else:
                parts.append(f"{r['table_name']} ({r['days_diff']}d / {r['retention_days']}d limit)")
        result["summary"].append({
            "check": "Data Retention Check", "priority": "P1", "status": "FAIL",
            "description": "Retention failures: " + ", ".join(parts),
        })
        result["has_failures"] = True

    enrichment_fail_count = (1 if pod_alert_usages else 0) + (1 if node_alert_usages else 0)
    if enrichment_fail_count:
        parts: list[str] = []
        if node_alert_usages:
            ordered_n = list(dict.fromkeys(node_alert_usages))
            parts.append("Node Resource Usage high: " + ", ".join(f"{n} ({p:.1f}%)" for n, p in ordered_n[:6]))
        if pod_alert_usages:
            ordered_p = list(dict.fromkeys(pod_alert_usages))
            parts.append(f"Pod Resource Usage ({pod_ns}) high: " + ", ".join(f"{n} ({p:.1f}%)" for n, p in ordered_p[:6]))
        result["summary"].append({
            "check": "Pod/Node resource check", "priority": "P2", "status": "FAIL",
            "description": "; ".join(parts) if parts else "Enrichment failures detected",
        })
        result["has_failures"] = True

    if kafka_lag_alert_rows:
        lag_map: dict[tuple[str, str], int] = {}
        for row in kafka_lag_alert_rows:
            key = (row["group"], row["topic"])
            lag_map[key] = max(lag_map.get(key, 0), row["lag"])
        lag_items = sorted(lag_map.items(), key=lambda x: x[1], reverse=True)
        result["summary"].append({
            "check": "Kafka Lag Check", "priority": "P2", "status": "FAIL",
            "description": (
                "High consumer lag: "
                + ", ".join(f"{grp}/{topic} ({lag:,})" for (grp, topic), lag in lag_items[:5])
                + (f" (+{max(0, len(lag_items)-5)} more)" if len(lag_items) > 5 else "")
            ),
        })
        result["has_failures"] = True

    if license_data:
        try:
            lic_days = int(license_data.get("days remaining", "999"))
        except ValueError:
            lic_days = 999
        if lic_days < 10:
            lic_priority, lic_fail = "P1", True
        elif lic_days < 15:
            lic_priority, lic_fail = "P2", True
        else:
            lic_fail = False
        if lic_fail:
            expiry = license_data.get("expiry date", "")
            desc = f"License expiring in {lic_days} days"
            if expiry:
                desc += f" (Expiry: {expiry})"
            result["summary"].append({
                "check": "License Check", "priority": lic_priority, "status": "FAIL",
                "description": desc,
            })
            result["has_failures"] = True

    return result


# ═══════════════════════════════════════════════════════════════
#  STEP 3 – BUILD WHATSAPP MESSAGE
# ═══════════════════════════════════════════════════════════════

def _desc_to_bullets(description: str) -> list[str]:
    """Split a description into individual bullet items, stripping label prefixes."""
    bullets = []
    for section in description.split("; "):
        colon_idx = section.find(": ")
        if colon_idx != -1:
            prefix = section[:colon_idx]
            # Only strip as label if colon is not inside parentheses
            paren_depth = prefix.count("(") - prefix.count(")")
            remainder = section[colon_idx + 2:] if paren_depth == 0 else section
        else:
            remainder = section
        for item in remainder.split(", "):
            item = item.strip()
            if item:
                bullets.append(f"  • {item}")
    return bullets


def build_whatsapp_message(client_name: str, report: dict, mail_date) -> str:
    shift    = get_shift(mail_date)
    time_str = mail_date.strftime("%d %b %Y, %I:%M %p IST")
    p1_items = [s for s in report["summary"] if s.get("priority") == "P1" and s["status"] == "FAIL"]
    p2_items = [s for s in report["summary"] if s.get("priority") == "P2" and s["status"] == "FAIL"]

    sev_icon, sev_label = ("🔴", "CRITICAL") if p1_items else ("🟡", "IMPORTANT")

    lines = [
        f"{sev_icon} *{sev_label} ALERT — {client_name}*",
        f"🕐 {shift}  |  {time_str}",
        f"📊 View Dashboard",
        DASHBOARD_URL,
    ]

    if p1_items:
        lines += ["", "━━━━━━━━━━━━━━━━━━━━", "🔴 *P1 — CRITICAL*"]
        for item in p1_items:
            lines += ["", f"🚨 *{item['check']}*"]
            lines += _desc_to_bullets(item["description"])

    if p2_items:
        lines += ["", "━━━━━━━━━━━━━━━━━━━━", "🟡 *P2 — IMPORTANT*"]
        for item in p2_items:
            lines += ["", f"⚠️ *{item['check']}*"]
            lines += _desc_to_bullets(item["description"])

    lines += ["", "━━━━━━━━━━━━━━━━━━━━", "_Please investigate and resolve._"]
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
#  STEP 4 – SEND WHATSAPP
# ═══════════════════════════════════════════════════════════════

def check_whatsapp_status() -> bool:
    url = f"https://api.green-api.com/waInstance{INSTANCE_ID}/getStateInstance/{API_TOKEN}"
    try:
        resp  = requests.get(url, timeout=10)
        state = resp.json().get("stateInstance", "")
        print(f"[*] WhatsApp instance state: {state}")
        return state == "authorized"
    except Exception as e:
        print(f"[!] Could not check WhatsApp status: {e}")
        return False


def send_whatsapp_message(message: str) -> bool:
    if not check_whatsapp_status():
        print("[!] WhatsApp instance not authorized. Aborting send.")
        return False

    url     = f"https://api.green-api.com/waInstance{INSTANCE_ID}/sendMessage/{API_TOKEN}"
    headers = {"Content-Type": "application/json"}
    payload = {"chatId": GROUP_CHAT_ID, "message": message}

    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=15)
        resp.raise_for_status()
        print(f"[+] WhatsApp message sent! idMessage: {resp.json().get('idMessage')}")
        return True
    except requests.exceptions.HTTPError as e:
        print(f"[!] HTTP error: {e} — {resp.text}")
    except Exception as e:
        print(f"[!] Error sending WhatsApp message: {e}")
    return False


# ═══════════════════════════════════════════════════════════════
#  STEP 5 – PUBLISH TO KAFKA  (new)
# ═══════════════════════════════════════════════════════════════

def publish_to_clickhouse(client_name: str, body: str, mail_date, report: dict) -> None:
    """
    Write all audit data directly to ClickHouse via HTTP.
    - Always writes 1 row to ng_audit_summary (even on full pass)
    - Writes threshold-breach rows to ng_audit_checks (P1 + P2)
    - Writes raw metric rows to 4 detail tables
    Errors are logged but never crash the cycle.
    """
    from ng_audit_parser import get_threshold_records, get_summary_record, get_raw_rows

    def _insert(table: str, records: list[dict]) -> None:
        if not records:
            return
        ndjson = "\n".join(json.dumps(r) for r in records).encode("utf-8")
        resp = requests.post(
            f"http://{CH_HOST}:{CH_PORT}/",
            params={
                "query":    f"INSERT INTO {table} FORMAT JSONEachRow",
                "user":     CH_USER,
                "password": CH_PASSWORD,
            },
            data=ndjson,
            timeout=15,
        )
        resp.raise_for_status()
        print(f"[CH] Inserted {len(records)} row(s) into {table}")

    try:
        # 1. Threshold breach records — compute first so summary counts match exactly
        checks = get_threshold_records(client_name, body, mail_date)

        # 2. Summary — always write so the matrix always has a row per email
        summ = get_summary_record(client_name, mail_date, report, checks)
        _insert("vusmart.ng_audit_summary_distributed", [summ])
        if checks:
            _insert("vusmart.ng_audit_checks_distributed", checks)
        else:
            print("[CH] No P1/P2 threshold breaches — skipping ng_audit_checks.")

        # 3. Raw detail rows (all rows, pass + fail)
        raw = get_raw_rows(client_name, body, mail_date)
        _insert("vusmart.ng_audit_disk_metrics_distributed",          raw["disk"])
        _insert("vusmart.ng_audit_pod_metrics_distributed",           raw["pods"])
        _insert("vusmart.ng_audit_kafka_metrics_distributed",         raw["kafka"])
        _insert("vusmart.ng_audit_retention_metrics_distributed",     raw["retention"])
        _insert("vusmart.ng_audit_license_metrics_distributed",       raw["license"])
        _insert("vusmart.ng_audit_user_engagement_distributed",       raw["user_engagement"])

    except Exception as exc:
        print(f"[CH] Error writing to ClickHouse: {exc}")


def run_once():
    emails = fetch_unprocessed_audit_emails()
    if not emails:
        print("[*] No new audit emails found.")
        return

    for e in emails:
        sender_email = e["sender_email"]
        body         = e["body"]
        mail_date    = e["mail_date"]
        mail_id      = e["mail_id"]

        client_name = client_name_from_subject(e["subject"])
        print(f"[+] Client resolved: {client_name}  (subject: {e['subject']})")

        # ── Parse for WhatsApp alert logic ──────────────────────
        report = parse_audit_report(body)
        print(f"[+] Failures detected: {report['has_failures']}")

        # ── Write all audit data to ClickHouse ──────────────────
        publish_to_clickhouse(client_name, body, mail_date, report)

        # ── WhatsApp alert (only on failures) ───────────────────
        if not report["has_failures"]:
            print("[✓] All checks passed. No WhatsApp alert needed.")
            mark_processed_mail_id(mail_id)
            continue

        message = build_whatsapp_message(client_name, report, mail_date)
        print("\n── WhatsApp message preview ──────────────────────────────")
        print(message)
        print("──────────────────────────────────────────────────────────\n")

        if DRY_RUN:
            print("[DRY RUN] Skipping WhatsApp delivery.")
            mark_processed_mail_id(mail_id)
            continue

        success = send_whatsapp_message(message)
        print("[✓] Alert delivered." if success else "[✗] Alert delivery failed.")
        if success:
            mark_processed_mail_id(mail_id)

    return True


# ═══════════════════════════════════════════════════════════════
#  HEALTH SERVER  (inline – no external module needed)
# ═══════════════════════════════════════════════════════════════

class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/healthz":
            age = read_heartbeat_age()
            ok  = age <= HEARTBEAT_MAX_AGE
            body = json.dumps({
                "status":          "ok" if ok else "unhealthy",
                "heartbeat_age_s": round(age, 1),
                "max_age_s":       HEARTBEAT_MAX_AGE,
                **({"reason": "main loop appears hung or has not started yet"} if not ok else {}),
            }).encode()
            self._respond(200 if ok else 503, body)
        else:
            self._respond(200, b'{"status":"alive"}')

    def _respond(self, code: int, body: bytes, content_type: str = "application/json"):
        self.send_response(code)
        self.send_header("Content-Type",   content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass


def start_health_server():
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), _HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"[*] Health server started on port {HEALTH_PORT} → /healthz")


def main():
    global DRY_RUN

    parser = argparse.ArgumentParser(description="NG Audit Check - WhatsApp alert + ClickHouse publisher")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse email and print WhatsApp message, but do not send it.")
    args = parser.parse_args()
    DRY_RUN = bool(args.dry_run)

    print("=" * 60)
    print("  NG Audit Check — WhatsApp Alert + ClickHouse Publisher")
    print(f"  CH host       : {CH_HOST}:{CH_PORT}")
    print(f"  Poll interval : {POLL_INTERVAL_SECONDS}s")
    print(f"  Health port   : {HEALTH_PORT}")
    print(f"  Heartbeat file: {HEARTBEAT_FILE}")
    print("=" * 60)

    shutdown = threading.Event()
    def _handle_signal(sig, frame):
        print(f"\n[*] Received signal {sig}. Shutting down...")
        shutdown.set()
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT,  _handle_signal)

    start_health_server()

    while not shutdown.is_set():
        cycle_start = time.time()
        print(f"\n[{datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}] ── Starting audit cycle ──")
        try:
            run_once()
        except Exception as e:
            print(f"[!] Unhandled exception in audit cycle: {e}")

        write_heartbeat()
        print(f"[*] Heartbeat written. Next cycle in {POLL_INTERVAL_SECONDS}s.")

        elapsed = time.time() - cycle_start
        remaining = max(0, POLL_INTERVAL_SECONDS - elapsed)
        shutdown.wait(timeout=remaining)

    pass

    print("[*] Service stopped cleanly.")
    sys.exit(0)


if __name__ == "__main__":
    main()

"""
ng_audit_parser.py
──────────────────
Parses NG Audit email bodies and returns structured records for ClickHouse.

Public API
──────────
  get_threshold_records(client_name, body, mail_date) -> list[dict]
      One record per breached check → vusmart.ng_audit_checks_distributed

  get_summary_record(client_name, mail_date, report) -> dict
      Single summary record → vusmart.ng_audit_summary_distributed
      `report` is the dict returned by parse_audit_report() in aduit-check.py

  get_raw_rows(client_name, body, mail_date) -> dict
      All raw metric rows → 4 detail tables:
      {"disk": [...], "pods": [...], "kafka": [...], "retention": [...]}

Check classification
─────────────────────
  P1 (Critical) – Disk Summary Check, Unhealthy Pod Check, Data Retention Check
  P2 (Important) – Pod/Node resource check, Kafka Lag Check
"""

from __future__ import annotations

import os
import re

import pytz

# ── Config ────────────────────────────────────────────────────────────────────
IST = pytz.timezone("Asia/Kolkata")

PERCENT_CUSTOM_ALERT_THRESHOLD = 80.0
KAFKA_LAG_ALERT_DIGITS = int(os.environ.get("KAFKA_LAG_ALERT_DIGITS", "8"))

CORE_POD_PATTERNS = re.compile(
    r"^(?:"
    r"chi-clickhouse-vusmart-"
    r"|kafka-cluster-cp-kafka-"
    r"|vuinterface-cairo-"
    r"|keycloak-deployment-"
    r"|nairobi-1-"
    r"|postgresql-0(?:$|-)"
    r")",
    re.IGNORECASE,
)


# ── Private helpers ───────────────────────────────────────────────────────────

def _get_shift(mail_date) -> str:
    return "BOD" if 6 <= mail_date.astimezone(IST).hour < 13 else "EOD"


def _ts(mail_date) -> str:
    # Store as UTC — ClickHouse DateTime is UTC internally.
    # The dashboard converts to IST using formatDateTime(..., 'Asia/Kolkata').
    return mail_date.astimezone(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S")


def _date(mail_date) -> str:
    return mail_date.astimezone(IST).strftime("%Y-%m-%d")


def _slice(text: str, start_re: str, end_re: str) -> str:
    """Extract lines between two regex-matched header lines (exclusive)."""
    lines = text.splitlines()
    s_re  = re.compile(start_re, re.IGNORECASE)
    e_re  = re.compile(end_re,   re.IGNORECASE)
    start = end = None
    for i, line in enumerate(lines):
        stripped = line.strip()
        if start is None and s_re.match(stripped):
            start = i
            continue
        if start is not None and e_re.match(stripped):
            end = i
            break
    if start is None:
        return ""
    return "\n".join(lines[start + 1 : end])


def _base(client_name: str, ts: str, shift: str, mail_date) -> dict:
    return {
        "timestamp":   ts,
        "date":        _date(mail_date),
        "shift":       shift,
        "client_name": client_name,
    }


def _check_rec(client_name: str, ip: str, metrics_type: str, status: str,
               ts: str, shift: str, mail_date, description: str = "",
               priority: str = "P1") -> dict:
    """Build a record for ng_audit_checks."""
    return {
        "timestamp":    ts,
        "client_name":  client_name,
        "ip":           ip,
        "metrics_type": metrics_type,
        "status":       status,
        "description":  description,
        "shift":        shift,
        "date":         _date(mail_date),
        "priority":     priority,
    }


# ═══════════════════════════════════════════════════════════════
#  THRESHOLD CHECK FUNCTIONS  (P1 / P2 — mirrors WhatsApp logic)
# ═══════════════════════════════════════════════════════════════

def _check_disk(body: str, client_name: str, ts: str, shift: str, mail_date) -> list[dict]:
    """P1: Any disk row FAIL and usage >= threshold."""
    section = _slice(body,
                     r"^Disk Summary Checks\s*$",
                     r"^Enrichment & Pod Restart Check\s*$")
    row_re = re.compile(
        r"^\|\s*(Node|Pod)\s*"
        r"\|\s*([^|]+?)\s*"
        r"\|\s*([^|]+?)\s*"
        r"\|\s*([0-9]+(?:\.[0-9]+)?)%\s*"
        r"\|\s*[^|]+?\s*"
        r"\|\s*(Pass|Fail|Warn|Info)\s*\|",
        re.IGNORECASE,
    )
    failing: list[tuple[str, str, float]] = []
    for line in section.splitlines():
        m = row_re.match(line.strip())
        if not m:
            continue
        usage_pct = float(m.group(4))
        if m.group(5).strip().upper() == "FAIL" and usage_pct >= PERCENT_CUSTOM_ALERT_THRESHOLD:
            failing.append((m.group(2).strip(), m.group(3).strip(), usage_pct))

    if not failing:
        return []

    desc = "Disk usage failures: " + ", ".join(
        f"{t} {mp} ({p:.1f}%)" for t, mp, p in failing[:4]
    ) + (f" (+{len(failing) - 4} more)" if len(failing) > 4 else "")

    return [_check_rec(client_name, failing[0][0], "Disk Summary Check", "Fail",
                       ts, shift, mail_date, desc, priority="P1")]


def _check_core_pods(body: str, client_name: str, ts: str, shift: str, mail_date) -> list[dict]:
    """P1: Any core pod is unhealthy."""
    section = _slice(body,
                     r"^Enrichment & Pod Restart Check\s*$",
                     r"^Kafka Lag Checks\s*$")
    core_unhealthy: list[str] = []
    for line in section.splitlines():
        if "Unhealthy Pod" not in line:
            continue
        pod_parts = [p.strip() for p in line.split("|")]
        if len(pod_parts) >= 4:
            pm = re.match(
                r"([\w][\w\-]*)/([\w][\w\-\.]*)\s*-\s*(crashing|pending|unknown|error|failed)",
                pod_parts[3], re.IGNORECASE,
            )
            if pm and CORE_POD_PATTERNS.match(pm.group(2)):
                core_unhealthy.append(f"{pm.group(1)}/{pm.group(2)}")

    if not core_unhealthy:
        return []

    seen: set[str] = set()
    ordered: list[str] = []
    for name in core_unhealthy:
        if name not in seen:
            seen.add(name)
            ordered.append(name)

    desc = f"{len(ordered)} core pod(s) unhealthy: " + ", ".join(ordered[:6])
    if len(ordered) > 6:
        desc += f" (+{len(ordered) - 6} more)"

    return [_check_rec(client_name, ordered[0], "Unhealthy Pod Check (Critical)", "Fail",
                       ts, shift, mail_date, desc, priority="P1")]


def _check_retention(body: str, client_name: str, ts: str, shift: str, mail_date) -> list[dict]:
    """P1: Any retention row (Default + Custom + Transactions) with Fail status."""
    section = _slice(body, r"^Data Retention Check\s*$", r"^License Check\s*$")
    row_re = re.compile(
        r"^\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*"
        r"\|\s*([0-9]+)\s*\|\s*([0-9]+)\s*\|\s*([^|]+?)\s*\|",
        re.IGNORECASE,
    )
    failing: list[tuple[str, int, int, str]] = []
    for line in section.splitlines():
        m = row_re.match(line.strip())
        if not m:
            continue
        table_name = m.group(1).strip()
        if table_name.lower() == "table name":
            continue
        status = m.group(6).strip()
        if not status.lower().startswith("fail"):
            continue
        days_diff      = int(m.group(4))
        retention_days = int(m.group(5))
        if 0 < days_diff - retention_days <= 1:
            continue
        failing.append((table_name, days_diff, retention_days, status))

    if not failing:
        return []

    parts = []
    for t, d, r, s in failing:
        reason_match = re.search(r"\((.+?)\)", s)
        if reason_match:
            parts.append(f"{t} — {reason_match.group(1)}")
        else:
            parts.append(f"{t} ({d}d / {r}d limit)")
    desc = "Retention failures: " + ", ".join(parts)
    return [_check_rec(client_name, failing[0][0], "Data Retention Check", "Fail",
                       ts, shift, mail_date, desc, priority="P1")]


def _check_resource_usage(body: str, client_name: str, ts: str, shift: str, mail_date) -> list[dict]:
    """P2: Node/pod CPU or memory >= threshold when parent row is FAIL."""
    section = _slice(body,
                     r"^Enrichment & Pod Restart Check\s*$",
                     r"^Kafka Lag Checks\s*$")
    high_pod_re = re.compile(
        r"High Usage Pod\s*\|\s*[^|]+\s*\|\s*([^|]+?)\s*\|\s*([0-9]+(?:\.[0-9]+)?)\s*%",
        re.IGNORECASE,
    )
    pod_ns_re = re.compile(r"Pod Resource Usage\s*\(([^)]+)\)", re.IGNORECASE)
    cpu_re = re.compile(r"CPU:\s*([0-9]+(?:\.[0-9]+)?)\s*%", re.IGNORECASE)
    mem_re = re.compile(r"Memory:\s*([0-9]+(?:\.[0-9]+)?)\s*%", re.IGNORECASE)

    pod_fail = node_fail = False
    pod_ns = "pods"
    high_pods:  list[tuple[str, float]] = []
    high_nodes: list[tuple[str, float]] = []

    for line in section.splitlines():
        parts = [p.strip() for p in line.split("|")]
        if len(parts) == 5 and parts[0] == "" and parts[-1] == "":
            comp   = parts[1].lower()
            status = parts[2].upper()
            if comp.startswith("pod resource usage (") and status == "FAIL":
                pod_fail = True
                ns_m = pod_ns_re.search(parts[1])
                if ns_m:
                    pod_ns = ns_m.group(1).strip()
            if comp.startswith("node resource usage") and status == "FAIL":
                node_fail = True

        if "High Usage Pod" in line:
            m = high_pod_re.search(line)
            if m:
                high_pods.append((m.group(1).strip(), float(m.group(2))))

        if "High Usage Node" in line:
            # Format: | ↳ High Usage Node | Info | hostname | CPU: XX%, Memory: YY% |
            node_parts = [p.strip() for p in line.split("|")]
            if len(node_parts) >= 5:
                node_host = node_parts[3]
                desc      = node_parts[4]
                cpu_m = cpu_re.search(desc)
                mem_m = mem_re.search(desc)
                cpu_pct = float(cpu_m.group(1)) if cpu_m else 0.0
                mem_pct = float(mem_m.group(1)) if mem_m else 0.0
                high_nodes.append((node_host, max(cpu_pct, mem_pct)))

    pod_alert  = [(n, p) for n, p in high_pods  if pod_fail  and p >= PERCENT_CUSTOM_ALERT_THRESHOLD]
    node_alert = [(n, p) for n, p in high_nodes if node_fail and p >= PERCENT_CUSTOM_ALERT_THRESHOLD]

    if not (pod_alert or node_alert):
        return []

    first_target = ""
    parts_list: list[str] = []
    if node_alert:
        first_target = node_alert[0][0]
        ordered_n = list(dict.fromkeys(node_alert))
        parts_list.append("Node Resource Usage high: " + ", ".join(
            f"{n} ({p:.1f}%)" for n, p in ordered_n[:6]))
    if pod_alert:
        if not first_target:
            first_target = pod_alert[0][0]
        ordered_p = list(dict.fromkeys(pod_alert))
        parts_list.append(f"Pod Resource Usage ({pod_ns}) high: " + ", ".join(
            f"{n} ({p:.1f}%)" for n, p in ordered_p[:6]))

    if node_alert and pod_alert:
        check_type = "Pod/Node Resource Check"
    elif node_alert:
        check_type = "Node Resource Check"
    else:
        check_type = "Pod Resource Check"

    return [_check_rec(client_name, first_target, check_type, "Fail",
                       ts, shift, mail_date, "; ".join(parts_list), priority="P2")]


def _check_license(body: str, client_name: str, ts: str, shift: str, mail_date) -> list[dict]:
    """P1 if days_remaining < 10; P2 if days_remaining < 15."""
    rows = _parse_license_rows(body, client_name, ts, shift, mail_date)
    if not rows:
        return []
    days = rows[0]["days_remaining"]
    if days < 10:
        priority = "P1"
    elif days < 15:
        priority = "P2"
    else:
        return []
    desc = f"License expiring in {days} days"
    if rows[0]["expiry_date"]:
        desc += f" (Expiry: {rows[0]['expiry_date']})"
    return [_check_rec(client_name, client_name, "License Check", "Fail",
                       ts, shift, mail_date, desc, priority=priority)]


def _check_kafka_lag(body: str, client_name: str, ts: str, shift: str, mail_date) -> list[dict]:
    """P2: Any lag row FAIL and digit count >= threshold."""
    section = _slice(body,
                     r"^Kafka Lag Checks\s*$",
                     r"^Data Retention Check\s*$")
    row_re = re.compile(
        r"^\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|\s*([0-9]+)\s*\|\s*(Pass|Fail|Warn|Info)\s*\|",
        re.IGNORECASE,
    )
    failing: list[tuple[str, str, int]] = []
    for line in section.splitlines():
        m = row_re.match(line.strip())
        if not m:
            continue
        lag = int(m.group(3))
        if m.group(4).strip().upper() == "FAIL" and len(str(lag)) >= KAFKA_LAG_ALERT_DIGITS:
            failing.append((m.group(1).strip(), m.group(2).strip(), lag))

    if not failing:
        return []

    first_target = f"{failing[0][0]}/{failing[0][1]}"
    desc = "High consumer lag: " + ", ".join(
        f"{g}/{t} ({l:,})" for g, t, l in failing[:5]
    ) + (f" (+{len(failing) - 5} more)" if len(failing) > 5 else "")

    return [_check_rec(client_name, first_target, "Kafka Lag Check", "Fail",
                       ts, shift, mail_date, desc, priority="P2")]


# ═══════════════════════════════════════════════════════════════
#  RAW ROW PARSERS  (all rows from each section, not just failures)
# ═══════════════════════════════════════════════════════════════

def _parse_disk_rows(body: str, client_name: str, ts: str, shift: str, mail_date) -> list[dict]:
    """All disk rows → ng_audit_disk_metrics."""
    section = _slice(body,
                     r"^Disk Summary Checks\s*$",
                     r"^Enrichment & Pod Restart Check\s*$")
    row_re = re.compile(
        r"^\|\s*(Node|Pod)\s*"
        r"\|\s*([^|]+?)\s*"
        r"\|\s*([^|]+?)\s*"
        r"\|\s*([0-9]+(?:\.[0-9]+)?)%\s*"
        r"\|\s*([0-9]+(?:\.[0-9]+)?)\s*"
        r"\|\s*(Pass|Fail|Warn|Info)\s*\|",
        re.IGNORECASE,
    )
    base = _base(client_name, ts, shift, mail_date)
    rows = []
    for line in section.splitlines():
        m = row_re.match(line.strip())
        if not m:
            continue
        rows.append({
            **base,
            "target_type":   m.group(1).strip(),
            "target":        m.group(2).strip(),
            "mount_path":    m.group(3).strip(),
            "usage_pct":     float(m.group(4)),
            "threshold_pct": float(m.group(5)),
            "status":        m.group(6).strip().capitalize(),
        })
    return rows


def _parse_pod_rows(body: str, client_name: str, ts: str, shift: str, mail_date) -> list[dict]:
    """Unhealthy pods + high-usage pods/nodes → ng_audit_pod_metrics."""
    section = _slice(body,
                     r"^Enrichment & Pod Restart Check\s*$",
                     r"^Kafka Lag Checks\s*$")
    high_pod_re = re.compile(
        r"High Usage Pod\s*\|\s*[^|]+\s*\|\s*([^|]+?)\s*\|\s*([0-9]+(?:\.[0-9]+)?)\s*%",
        re.IGNORECASE,
    )
    pod_ns_re = re.compile(r"Pod Resource Usage\s*\(([^)]+)\)", re.IGNORECASE)
    cpu_re = re.compile(r"CPU:\s*([0-9]+(?:\.[0-9]+)?)\s*%", re.IGNORECASE)
    mem_re = re.compile(r"Memory:\s*([0-9]+(?:\.[0-9]+)?)\s*%", re.IGNORECASE)
    base = _base(client_name, ts, shift, mail_date)
    rows = []
    current_pod_ns = "unknown"

    for line in section.splitlines():
        # Track the namespace from the parent "Pod Resource Usage (vsmaps)" row
        parts = [p.strip() for p in line.split("|")]
        if len(parts) == 5 and parts[0] == "" and parts[-1] == "":
            ns_m = pod_ns_re.search(parts[1])
            if ns_m:
                current_pod_ns = ns_m.group(1).strip()

        if "Unhealthy Pod" in line:
            pod_parts = [p.strip() for p in line.split("|")]
            if len(pod_parts) >= 4:
                pm = re.match(
                    r"([\w][\w\-]*)/([\w][\w\-\.]*)\s*-\s*(crashing|pending|unknown|error|failed)",
                    pod_parts[3], re.IGNORECASE,
                )
                if pm and CORE_POD_PATTERNS.match(pm.group(2)):
                    rows.append({
                        **base,
                        "namespace":        pm.group(1),
                        "pod_name":         pm.group(2),
                        "pod_status":       pm.group(3).lower(),
                        "restart_count":    0,
                        "cpu_usage_pct":    0.0,
                        "memory_usage_pct": 0.0,
                        "ip":               "",
                    })

        elif "High Usage Pod" in line:
            m = high_pod_re.search(line)
            if m:
                rows.append({
                    **base,
                    "namespace":        current_pod_ns,
                    "pod_name":         m.group(1).strip(),
                    "pod_status":       "high_usage",
                    "restart_count":    0,
                    "cpu_usage_pct":    float(m.group(2)),
                    "memory_usage_pct": 0.0,
                    "ip":               "",
                })

        elif "High Usage Node" in line:
            # Format: | ↳ High Usage Node | Info | hostname | CPU: XX%, Memory: YY% |
            node_parts = [p.strip() for p in line.split("|")]
            if len(node_parts) >= 5:
                node_host = node_parts[3]
                desc      = node_parts[4]
                cpu_m = cpu_re.search(desc)
                mem_m = mem_re.search(desc)
                rows.append({
                    **base,
                    "namespace":        "node",
                    "pod_name":         node_host,
                    "pod_status":       "high_usage",
                    "restart_count":    0,
                    "cpu_usage_pct":    float(cpu_m.group(1)) if cpu_m else 0.0,
                    "memory_usage_pct": float(mem_m.group(1)) if mem_m else 0.0,
                    "ip":               "",
                })

    return rows


def _parse_license_rows(body: str, client_name: str, ts: str, shift: str, mail_date) -> list[dict]:
    """Single license record from key-value table → ng_audit_license_metrics."""
    section = _slice(body, r"^License Check\s*$", r"^User Engagement Check\s*$")
    kv_re = re.compile(r"^\|\s*([^|]+?)\s*\|\s*([^|]*?)\s*\|", re.IGNORECASE)
    kv: dict[str, str] = {}
    for line in section.splitlines():
        m = kv_re.match(line.strip())
        if not m:
            continue
        key = m.group(1).strip().lower()
        val = m.group(2).strip()
        if key == "details":
            continue
        kv[key] = val

    if not kv:
        return []

    expiry_date = kv.get("expiry date", "")
    try:
        days_remaining = int(kv.get("days remaining", "0"))
    except ValueError:
        days_remaining = 0
    try:
        usage_pct = float(kv.get("usage", "0").rstrip("%"))
    except ValueError:
        usage_pct = 0.0
    status = kv.get("status", "Unknown")

    return [{
        **_base(client_name, ts, shift, mail_date),
        "expiry_date":    expiry_date,
        "days_remaining": days_remaining,
        "usage_pct":      usage_pct,
        "status":         status,
    }]


def _parse_user_engagement_rows(body: str, client_name: str, ts: str, shift: str, mail_date) -> list[dict]:
    """User engagement rows → ng_audit_user_engagement."""
    section = _slice(body,
                     r"^User Engagement Check\s*$",
                     r"^[A-Z][A-Za-z &]+Checks?\s*$")
    row_re = re.compile(
        r"^\|\s*([^|]+?)\s*\|\s*([0-9]+)\s*\|\s*([^|]+?)\s*\|",
        re.IGNORECASE,
    )
    base = _base(client_name, ts, shift, mail_date)
    rows = []
    for line in section.splitlines():
        m = row_re.match(line.strip())
        if not m:
            continue
        username = m.group(1).strip()
        if username.lower() == "username":
            continue
        rows.append({
            **base,
            "username": username,
            "logins":   int(m.group(2)),
            "duration": m.group(3).strip(),
        })
    if not rows and "no user engagement data" in section.lower():
        rows.append({
            **base,
            "username": "No user engagement data found for today",
            "logins":   0,
            "duration": "N/A",
        })
    return rows


def _parse_kafka_rows(body: str, client_name: str, ts: str, shift: str, mail_date) -> list[dict]:
    """All kafka lag rows → ng_audit_kafka_metrics."""
    section = _slice(body,
                     r"^Kafka Lag Checks\s*$",
                     r"^Data Retention Check\s*$")
    row_re = re.compile(
        r"^\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|\s*([0-9]+)\s*\|\s*(Pass|Fail|Warn|Info)\s*\|",
        re.IGNORECASE,
    )
    base = _base(client_name, ts, shift, mail_date)
    rows = []
    for line in section.splitlines():
        m = row_re.match(line.strip())
        if not m:
            continue
        rows.append({
            **base,
            "consumer_group": m.group(1).strip(),
            "topic":          m.group(2).strip(),
            "lag_count":      int(m.group(3)),
            "status":         m.group(4).strip().capitalize(),
        })
    return rows


def _parse_retention_rows(body: str, client_name: str, ts: str, shift: str, mail_date) -> list[dict]:
    """All retention rows (Default + Custom + Transactions) → ng_audit_retention_metrics."""
    section = _slice(body, r"^Data Retention Check\s*$", r"^License Check\s*$")
    row_re = re.compile(
        r"^\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*"
        r"\|\s*([0-9]+)\s*\|\s*([0-9]+)\s*\|\s*([^|]+?)\s*\|",
        re.IGNORECASE,
    )
    base = _base(client_name, ts, shift, mail_date)
    rows = []
    for line in section.splitlines():
        m = row_re.match(line.strip())
        if not m:
            continue
        table_name = m.group(1).strip()
        if table_name.lower() == "table name":
            continue
        rows.append({
            **base,
            "table_name":     table_name,
            "days_diff":      int(m.group(4)),
            "retention_days": int(m.group(5)),
            "status":         m.group(6).strip(),
        })
    return rows


# ═══════════════════════════════════════════════════════════════
#  PUBLIC API
# ═══════════════════════════════════════════════════════════════

def _ensure_aware(mail_date):
    if mail_date.tzinfo is None:
        return mail_date.replace(tzinfo=pytz.UTC)
    return mail_date


def get_threshold_records(client_name: str, body: str, mail_date) -> list[dict]:
    """
    Return threshold-breached check records for ng_audit_checks.
    One record per breached check category (P1 first, then P2).
    """
    mail_date = _ensure_aware(mail_date)
    ts    = _ts(mail_date)
    shift = _get_shift(mail_date)
    records: list[dict] = []
    records.extend(_check_disk(body, client_name, ts, shift, mail_date))
    records.extend(_check_core_pods(body, client_name, ts, shift, mail_date))
    records.extend(_check_retention(body, client_name, ts, shift, mail_date))
    records.extend(_check_license(body, client_name, ts, shift, mail_date))
    records.extend(_check_resource_usage(body, client_name, ts, shift, mail_date))
    records.extend(_check_kafka_lag(body, client_name, ts, shift, mail_date))
    return records


def get_summary_record(client_name: str, mail_date, report: dict,
                       threshold_records: list | None = None) -> dict:
    """
    Return a single summary record for ng_audit_summary.

    Parameters
    ----------
    client_name        : resolved client name
    mail_date          : aware datetime of the email
    report             : dict returned by parse_audit_report() in aduit-check.py
    threshold_records  : list returned by get_threshold_records() — when supplied,
                         p1/p2 counts are derived from it so the KPI stats always
                         match the Threshold Breaches table exactly.
    """
    mail_date = _ensure_aware(mail_date)
    ts    = _ts(mail_date)
    shift = _get_shift(mail_date)
    if threshold_records is not None:
        p1_fail = sum(1 for r in threshold_records if r.get("priority") == "P1")
        p2_fail = sum(1 for r in threshold_records if r.get("priority") == "P2")
    else:
        summary = report.get("summary", [])
        p1_fail = sum(1 for s in summary if s.get("priority") == "P1" and s.get("status") == "FAIL")
        p2_fail = sum(1 for s in summary if s.get("priority") == "P2" and s.get("status") == "FAIL")
    return {
        **_base(client_name, ts, shift, mail_date),
        "overall_status": "FAIL" if report.get("has_failures") else "PASS",
        "p1_fail_count":  p1_fail,
        "p2_fail_count":  p2_fail,
    }


def get_raw_rows(client_name: str, body: str, mail_date) -> dict:
    """
    Return all raw metric rows for the 4 detail tables.

    Returns
    -------
    {
      "disk":      list[dict],   # → ng_audit_disk_metrics_distributed
      "pods":      list[dict],   # → ng_audit_pod_metrics_distributed
      "kafka":     list[dict],   # → ng_audit_kafka_metrics_distributed
      "retention": list[dict],   # → ng_audit_retention_metrics_distributed
    }
    """
    mail_date = _ensure_aware(mail_date)
    ts    = _ts(mail_date)
    shift = _get_shift(mail_date)
    return {
        "disk":             _parse_disk_rows(body, client_name, ts, shift, mail_date),
        "pods":             _parse_pod_rows(body, client_name, ts, shift, mail_date),
        "kafka":            _parse_kafka_rows(body, client_name, ts, shift, mail_date),
        "retention":        _parse_retention_rows(body, client_name, ts, shift, mail_date),
        "license":          _parse_license_rows(body, client_name, ts, shift, mail_date),
        "user_engagement":  _parse_user_engagement_rows(body, client_name, ts, shift, mail_date),
    }

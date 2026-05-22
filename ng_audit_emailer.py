"""
ng_audit_emailer.py — HTML email builder + SMTP sender for NG audit alerts.
Imported by aduit-check.py.
"""

import io
import os
import random
import smtplib
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT   = 587
DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "http://bit.ly/4nc5oiI")

_CARE_TAGLINES = [
    "CARE is caring for your platform, around the clock.",
    "Your platform has someone who truly CAREs.",
    "Behind every healthy platform is a team that CAREs.",
    "CARE — Keeping your platform alive, so you don't have to.",
    "CARE never sleeps, so your platform can.",
    "We catch issues before you even notice them. That's CARE.",
    "CARE: Because your platform deserves someone who gives a damn.",
    "When your platform needs attention, CARE is already on it.",
    "We CARE about uptime as much as you do. Probably more. 😄",
    "Monitored with CARE.",
    "Powered by people who CARE.",
    "Every alert sent with CARE.",
]

# ── CC lists — override via env vars ────────────────────────────
# Normal alert emails: always CC this list
ALERT_CC = [
    a.strip() for a in
    os.environ.get("ALERT_CC", "care-team@vunetsystems.com").split(",")
    if a.strip()
]

# Missing-audit emails: expanded CC list (escalation)
MISSING_AUDIT_CC = [
    a.strip() for a in
    os.environ.get(
        "MISSING_AUDIT_CC",
        "care-team@vunetsystems.com,mathew@vunetsystems.com,"
        "bharat@vunetsystems.com,ramshankar@vunetsystems.com",
    ).split(",")
    if a.strip()
]


# ════════════════════════════════════════════════════════════════
#  GIF generators
# ════════════════════════════════════════════════════════════════

_TAGLINE_FONT_PATHS = [
    "/usr/share/fonts/truetype/lato/Lato-Medium.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
]

def _get_tagline_font(size: int = 15):
    from PIL import ImageFont
    for path in _TAGLINE_FONT_PATHS:
        try:
            return ImageFont.truetype(path, size)
        except Exception:
            pass
    try:
        return ImageFont.load_default(size=size)
    except TypeError:
        return ImageFont.load_default()


def _make_header_sweep_with_tagline(text: str, bg_color: tuple) -> bytes:
    """28px strip: sweep spotlight behind + per-letter wave-bounce text on top."""
    import math
    from PIL import Image, ImageDraw

    W, H, N = 560, 28, 24
    font = _get_tagline_font(13)
    is_grey = (bg_color[0] == bg_color[1] == bg_color[2])

    char_advances: list[float] = []
    for ch in text:
        try:
            char_advances.append(font.getlength(ch))
        except AttributeError:
            try:
                bb = font.getbbox(ch)
                char_advances.append(float(bb[2] - bb[0]))
            except Exception:
                char_advances.append(7.0)

    total_w  = sum(char_advances)
    start_x  = max((W - total_w) / 2, 4.0)

    frames = []
    for f in range(N):
        img  = Image.new("RGB", (W, H), bg_color)
        draw = ImageDraw.Draw(img)

        # Sweep spotlight (two staggered beams, same logic as _make_header_sweep)
        for offset in [0, W // 2]:
            cx = int((f / N) * (W + 140) - 70 + offset) % (W + 140) - 70
            for dx in range(-65, 66):
                x = cx + dx
                if 0 <= x < W:
                    t = (1.0 - abs(dx) / 65.0) ** 1.8
                    if is_grey:
                        v = int(bg_color[0] + (120 - bg_color[0]) * t)
                        draw.line([(x, 0), (x, H - 1)], fill=(v, v, v))
                    else:
                        r = int(bg_color[0] + (220 - bg_color[0]) * t)
                        draw.line([(x, 0), (x, H - 1)], fill=(r, 0, 0))

        # Per-letter wave bounce on top
        x = start_x
        for i, (ch, adv) in enumerate(zip(text, char_advances)):
            phase      = 2 * math.pi * f / N + i * 0.38
            dy         = math.sin(phase) * 3
            brightness = int(185 + 70 * (math.sin(phase) + 1) / 2)
            draw.text((x, H / 2 - 7 + dy), ch,
                      fill=(brightness, brightness, brightness), font=font)
            x += adv

        frames.append(img.convert("P", palette=Image.ADAPTIVE, colors=48))
    return _save_gif(frames, [60] * N)

def _save_gif(frames: list, durations) -> bytes:
    buf = io.BytesIO()
    frames[0].save(buf, format="GIF", save_all=True,
                   append_images=frames[1:], loop=0, duration=durations)
    return buf.getvalue()


def _make_header_sweep() -> bytes:
    from PIL import Image, ImageDraw
    W, H, BG, N = 560, 18, (20, 0, 0), 20
    frames = []
    for f in range(N):
        img  = Image.new("RGB", (W, H), BG)
        draw = ImageDraw.Draw(img)
        for offset in [0, W // 2]:
            cx = int((f / N) * (W + 140) - 70 + offset) % (W + 140) - 70
            for dx in range(-65, 66):
                x = cx + dx
                if 0 <= x < W:
                    t = (1.0 - abs(dx) / 65.0) ** 1.8
                    draw.line([(x, 0), (x, H - 1)],
                              fill=(int(BG[0] + (220 - BG[0]) * t), 0, 0))
        frames.append(img.convert("P", palette=Image.ADAPTIVE, colors=24))
    return _save_gif(frames, [60] * N)


def _make_alert_tape() -> bytes:
    from PIL import Image, ImageDraw
    W, H     = 580, 5
    BG, DOT  = (140, 0, 0), (255, 200, 195)
    SP, HALF, N = 52, 18, 12
    frames = []
    for f in range(N):
        img  = Image.new("RGB", (W, H), BG)
        draw = ImageDraw.Draw(img)
        shift = int(f / N * SP)
        for di in range(14):
            cx = shift + di * SP
            for dx in range(-HALF, HALF + 1):
                x = cx + dx
                if 0 <= x < W:
                    t = (1.0 - abs(dx) / HALF) ** 1.4
                    draw.line([(x, 0), (x, H - 1)], fill=(
                        int(BG[0] + (DOT[0] - BG[0]) * t),
                        int(BG[1] + (DOT[1] - BG[1]) * t),
                        int(BG[2] + (DOT[2] - BG[2]) * t),
                    ))
        frames.append(img.convert("P", palette=Image.ADAPTIVE, colors=16))
    return _save_gif(frames, [75] * N)


def _make_p1_alarm() -> bytes:
    from PIL import Image, ImageDraw
    S = 46
    CX = CY = S // 2
    BG, RED, N = (255, 245, 245), (200, 0, 0), 12
    frames = []
    for f in range(N):
        img  = Image.new("RGB", (S, S), BG)
        draw = ImageDraw.Draw(img)
        rr, ao = 20, f * 30
        for arc in range(6):
            a0 = ao + arc * 60
            draw.arc([CX-rr, CY-rr, CX+rr, CY+rr], start=a0, end=a0+30, fill=RED, width=3)
        ir = 13
        draw.ellipse([CX-ir, CY-ir, CX+ir, CY+ir], fill=RED)
        draw.rectangle([CX-2, CY-ir+4, CX+2, CY+1], fill="white")
        draw.ellipse([CX-2, CY+4, CX+2, CY+8],      fill="white")
        frames.append(img.convert("P", palette=Image.ADAPTIVE, colors=16))
    return _save_gif(frames, [70] * N)


def _make_p2_glow() -> bytes:
    import math
    from PIL import Image, ImageDraw
    S = 46
    CX = CY = S // 2
    BG, AMBER, N = (255, 251, 240), (214, 137, 16), 16

    def blend(fg, bg, a):
        return tuple(int(bg[i] + (fg[i] - bg[i]) * max(0.0, min(1.0, a))) for i in range(3))

    frames = []
    for f in range(N):
        img  = Image.new("RGB", (S, S), BG)
        draw = ImageDraw.Draw(img)
        pulse = (1 + math.sin(2 * math.pi * f / N - math.pi / 2)) / 2
        rr, ra = int(9 + pulse * 11), (1.0 - pulse) * 0.75
        for w, wa in [(4, ra * 0.3), (3, ra * 0.65), (2, ra)]:
            draw.ellipse([CX-rr, CY-rr, CX+rr, CY+rr],
                         outline=blend(AMBER, BG, wa), width=w)
        dr = int(7 + pulse * 2)
        draw.ellipse([CX-dr, CY-dr, CX+dr, CY+dr], fill=AMBER)
        draw.rectangle([CX-1, CY-dr+3, CX+1, CY],     fill="white")
        draw.ellipse([CX-1, CY+3,       CX+1, CY+5],   fill="white")
        frames.append(img.convert("P", palette=Image.ADAPTIVE, colors=16))
    return _save_gif(frames, [95] * N)


# ════════════════════════════════════════════════════════════════
#  HTML builders
# ════════════════════════════════════════════════════════════════

def _bullets(description: str) -> str:
    items = []
    for section in description.split("; "):
        colon = section.find(": ")
        remainder = section[colon + 2:] if colon != -1 else section
        for item in remainder.split(", "):
            item = item.strip()
            if item and not item.startswith("(+"):
                items.append(
                    f'<li style="margin:3px 0;font-size:13px;color:#333;">{item}</li>'
                )
    return ("<ul style='margin:6px 0 0;padding-left:18px;'>"
            + "".join(items) + "</ul>") if items else ""


def _p1_card(item: dict) -> str:
    tape  = ('<img src="cid:alert_tape" width="580" height="5" '
             'style="display:block;width:100%;"/>')
    alarm = ('<img src="cid:p1_alarm" width="46" height="46" '
             'style="display:inline-block;vertical-align:middle;margin-right:10px;"/>')
    badge = ('<span style="background:#cc0000;color:white;padding:2px 8px;'
             'border-radius:3px;font-size:10px;font-weight:700;'
             'margin-left:8px;vertical-align:middle;">CRITICAL</span>')
    return f"""
    <tr><td style="padding:10px 16px 0;">
      <table width="100%" cellpadding="0" cellspacing="0" border="0"
             style="border-radius:8px;overflow:hidden;border:1px solid #f0b0b0;">
        <tr><td colspan="2" style="padding:0;line-height:0;">{tape}</td></tr>
        <tr>
          <td width="4" style="background:#cc0000;"></td>
          <td style="background:#fff5f5;padding:10px 14px;">
            {alarm}
            <span style="font-size:13px;font-weight:700;color:#1a0000;vertical-align:middle;">
              {item['check']}</span>{badge}
            {_bullets(item['description'])}
          </td>
        </tr>
      </table>
    </td></tr>"""


def _p2_card(item: dict) -> str:
    glow  = ('<img src="cid:p2_glow" width="46" height="46" '
             'style="display:inline-block;vertical-align:middle;margin-right:10px;"/>')
    badge = ('<span style="background:#d68910;color:white;padding:2px 8px;'
             'border-radius:3px;font-size:10px;font-weight:700;'
             'margin-left:8px;vertical-align:middle;">IMPORTANT</span>')
    return f"""
    <tr><td style="padding:6px 16px 0;">
      <table width="100%" cellpadding="0" cellspacing="0" border="0"
             style="border-radius:8px;overflow:hidden;border:1px solid #f5dfa0;">
        <tr>
          <td width="4" style="background:#e67e22;"></td>
          <td style="background:#fffbf0;padding:10px 14px;">
            {glow}
            <span style="font-size:13px;font-weight:700;color:#3d1c00;vertical-align:middle;">
              {item['check']}</span>{badge}
            {_bullets(item['description'])}
          </td>
        </tr>
      </table>
    </td></tr>"""


def build_alert_html(client_name: str, report: dict, shift: str, date_str: str) -> tuple[str, str]:
    p1 = [i for i in report["summary"] if i.get("priority") == "P1" and i["status"] == "FAIL"]
    p2 = [i for i in report["summary"] if i.get("priority") == "P2" and i["status"] == "FAIL"]
    has_p1 = bool(p1)

    hdr_bg = "#1a0000" if has_p1 else "#3d1c00"
    btn_bg = "#a30000" if has_p1 else "#ca6f1e"

    counts = ", ".join(filter(None, [
        f"{len(p1)} P1" if p1 else "",
        f"{len(p2)} P2" if p2 else "",
    ]))
    icon = "🔴" if has_p1 else "🟡"

    p1_badge = (f'<span style="background:#cc0000;color:white;padding:3px 11px;'
                f'border-radius:12px;font-size:12px;font-weight:700;">{len(p1)} P1</span>&nbsp;') if p1 else ""
    p2_badge = (f'<span style="background:rgba(255,255,255,0.15);color:rgba(255,255,255,0.85);'
                f'padding:3px 11px;border-radius:12px;font-size:12px;">{len(p2)} P2</span>') if p2 else ""

    action_row = (
        '<tr><td style="background:#cc0000;padding:9px 20px;text-align:center;'
        'color:white;font-size:11px;font-weight:800;letter-spacing:2px;'
        'text-transform:uppercase;">⚡&nbsp; Immediate Action Required &nbsp;⚡</td></tr>'
    ) if has_p1 else ""

    body = ""
    if p1:
        body += ('<tr><td style="padding:14px 16px 4px;"><span style="color:#cc0000;'
                 'font-size:10px;font-weight:800;letter-spacing:2px;'
                 'text-transform:uppercase;">🚨 P1 — CRITICAL</span></td></tr>')
        for item in p1:
            body += _p1_card(item)
        body += '<tr><td style="padding:4px 0;"></td></tr>'
    if p2:
        body += ('<tr><td style="padding:14px 16px 4px;"><span style="color:#d68910;'
                 'font-size:10px;font-weight:800;letter-spacing:2px;'
                 'text-transform:uppercase;">⚠️ P2 — IMPORTANT</span></td></tr>')
        for item in p2:
            body += _p2_card(item)
        body += '<tr><td style="padding:4px 0;"></td></tr>'

    tagline = random.choice(_CARE_TAGLINES)
    html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>{icon} {client_name} — {counts}</title></head>
<body style="margin:0;padding:0;background:#f0f2f5;font-family:Arial,Helvetica,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" border="0">
  <tr><td style="background:{hdr_bg};padding:0;line-height:0;text-align:center;">
    <img src="cid:hdr_sweep" width="560" height="28"
         style="display:block;margin:0 auto;max-width:100%;"/>
  </td></tr>
  <tr><td style="background:{hdr_bg};padding:10px 20px 12px;">
    <table width="100%" cellpadding="0" cellspacing="0" border="0"><tr>
      <td style="vertical-align:middle;">
        <span style="color:#fff;font-size:16px;font-weight:800;">{client_name}</span>
        <span style="color:rgba(255,255,255,0.55);font-size:12px;margin-left:10px;">
          {shift} &nbsp;·&nbsp; {date_str}</span>
      </td>
      <td style="text-align:right;vertical-align:middle;padding-left:12px;white-space:nowrap;">
        {p1_badge}{p2_badge}
      </td>
    </tr></table>
  </td></tr>
  {action_row}
  <tr><td>
    <table width="100%" cellpadding="0" cellspacing="0" border="0"
           style="max-width:620px;margin:0 auto;">
      {body}
      <tr><td style="padding:12px 16px 8px;text-align:center;">
        <a href="{DASHBOARD_URL}"
           style="display:inline-block;background:{btn_bg};color:#fff;
                  padding:12px 40px;border-radius:7px;font-size:14px;
                  font-weight:700;text-decoration:none;letter-spacing:0.4px;">
          View Dashboard &rarr;
        </a>
      </td></tr>
      <tr><td style="padding:10px 16px 24px;text-align:center;color:#bbb;font-size:11px;">
        VuNet Systems &nbsp;&middot;&nbsp; NG Audit Monitoring
        &nbsp;&middot;&nbsp; Auto-generated alert
      </td></tr>
    </table>
  </td></tr>
</table>
</body></html>"""
    return html, tagline


def build_missing_html(client_name: str, shift: str, date_str: str) -> tuple[str, str]:
    tagline = random.choice(_CARE_TAGLINES)
    html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>⚠️ No {shift} audit — {client_name}</title></head>
<body style="margin:0;padding:0;background:#f0f2f5;font-family:Arial,Helvetica,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" border="0">
  <tr><td style="background:#2c2c2c;padding:0;line-height:0;text-align:center;">
    <img src="cid:hdr_sweep" width="560" height="28"
         style="display:block;margin:0 auto;max-width:100%;"/>
  </td></tr>
  <tr><td style="background:#2c2c2c;padding:10px 20px 12px;">
    <table width="100%" cellpadding="0" cellspacing="0" border="0"><tr>
      <td style="vertical-align:middle;">
        <span style="color:#fff;font-size:16px;font-weight:800;">{client_name}</span>
        <span style="color:rgba(255,255,255,0.5);font-size:12px;margin-left:10px;">
          {shift} &nbsp;·&nbsp; {date_str}</span>
      </td>
      <td style="text-align:right;vertical-align:middle;padding-left:12px;">
        <span style="background:#555;color:#ccc;padding:3px 11px;
                     border-radius:12px;font-size:12px;font-weight:600;">NO REPORT</span>
      </td>
    </tr></table>
  </td></tr>
  <tr><td style="background:#7a7a7a;padding:9px 20px;text-align:center;
                  color:white;font-size:11px;font-weight:800;letter-spacing:2px;
                  text-transform:uppercase;">
    ⚠️&nbsp; No Audit Report Received &nbsp;⚠️
  </td></tr>
  <tr><td>
    <table width="100%" cellpadding="0" cellspacing="0" border="0"
           style="max-width:620px;margin:0 auto;">
      <tr><td style="padding:32px 24px 20px;text-align:center;">
        <div style="font-size:42px;margin-bottom:14px;">📭</div>
        <div style="font-size:16px;font-weight:700;color:#2c2c2c;margin-bottom:10px;">
          The <strong>{shift}</strong> health audit report for
          <span style="color:#cc0000;">{client_name}</span> has not been received
        </div>
        <div style="font-size:13px;color:#666;line-height:1.7;max-width:400px;margin:0 auto;">
          Kindly check and update.
        </div>
      </td></tr>
      <tr><td style="padding:8px 16px 8px;text-align:center;">
        <a href="{DASHBOARD_URL}"
           style="display:inline-block;background:#555;color:#fff;
                  padding:12px 40px;border-radius:7px;font-size:14px;
                  font-weight:700;text-decoration:none;">
          View Dashboard &rarr;
        </a>
      </td></tr>
      <tr><td style="padding:10px 16px 24px;text-align:center;color:#bbb;font-size:11px;">
        VuNet Systems &nbsp;&middot;&nbsp; NG Audit Monitoring
        &nbsp;&middot;&nbsp; Auto-generated alert
      </td></tr>
    </table>
  </td></tr>
</table>
</body></html>"""
    return html, tagline


# ════════════════════════════════════════════════════════════════
#  SMTP sender
# ════════════════════════════════════════════════════════════════

def send_email(
    from_addr:  str,
    password:   str,
    to_addr:    str,
    cc_addrs:   list[str],
    subject:    str,
    html:       str,
    has_p1:     bool,
    has_p2:     bool = False,
    is_missing: bool = False,
    tagline:    str  = "",
) -> bool:
    try:
        to_list = [a.strip() for a in to_addr.split(",") if a.strip()]

        msg = MIMEMultipart("related")
        msg["Subject"] = subject
        msg["From"]    = from_addr
        msg["To"]      = ", ".join(to_list)
        if cc_addrs:
            msg["Cc"] = ", ".join(cc_addrs)

        alt = MIMEMultipart("alternative")
        alt.attach(MIMEText(html, "html", "utf-8"))
        msg.attach(alt)

        def _gif(data: bytes, cid: str, fname: str):
            p = MIMEImage(data, _subtype="gif")
            p.add_header("Content-ID",          f"<{cid}>")
            p.add_header("Content-Disposition", "inline", filename=fname)
            msg.attach(p)

        if tagline:
            if is_missing:
                tl_bg = (44, 44, 44)
            elif has_p1:
                tl_bg = (26, 0, 0)
            else:
                tl_bg = (61, 28, 0)
            _gif(_make_header_sweep_with_tagline(tagline, tl_bg), "hdr_sweep", "sweep.gif")
        else:
            _gif(_make_header_sweep(), "hdr_sweep", "sweep.gif")
        if not is_missing:
            if has_p1:
                _gif(_make_alert_tape(), "alert_tape", "tape.gif")
                _gif(_make_p1_alarm(),   "p1_alarm",   "alarm.gif")
            if has_p2:
                _gif(_make_p2_glow(),    "p2_glow",    "glow.gif")
        all_rcpt = list(set(to_list) | set(cc_addrs))
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=25) as s:
            s.ehlo(); s.starttls(); s.login(from_addr, password)
            s.sendmail(from_addr, all_rcpt, msg.as_string())
        print(f"[Email] Sent → {to_list}  cc={cc_addrs}  | {subject}")
        return True
    except Exception as exc:
        print(f"[Email] Error sending to {to_addr}: {exc}")
        return False

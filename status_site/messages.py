"""Everything the service says to users, independent of the channel it goes out on."""

import os
import random

REJECTED_REGION = "מצטערים, השירות זמין רק כרגע למספרי טלפון מישראל, אירופה וצפון אמריקה."
ONLY_RECORDINGS = "נכון להיום אני יודע לתמלל הקלטות, לא מעבר לזה."
DURATION_FAILED = "אירעה שגיאה בבדיקת אורך הקובץ."
TOO_LONG = "אני מתנצל, אך קיבלתי הנחיה שלא לתמלל קבצים שארוכים מ-10 דקות."


def rate_limited(minutes):
    return (
        f"מצטערים, אך הגעת למגבלת השימוש של השירות. "
        f"ניתן לנסות שוב בעוד כ-{minutes} דקות."
    )


TOO_LARGE = "הקובץ גדול מדי. אפשר לשלוח קבצים של עד 20MB."

# Sent after every transcript delivered on WhatsApp, as its own message so the transcript
# stays clean to copy.
WA_PRICING_NOTICE = """שלום,

וואטסאפ מתחילה לגבות תשלום מאתנו עבור הודעות התמלול שאליעזר שולח אליכם.
בהתאם לכך, אנו מבצעים שינויים שיאפשרו את המשך הפעלת השירות.

בימים הקרובים נפרסם פרטים נוספים באתר status.eliezer.ivrit.ai.
מוזמנים לעקוב שם בכדי שתוכלו להמשיך להשתמש באליעזר.

צוות ivrit.ai"""

# --- linking (Telegram <-> WhatsApp)

TG_WELCOME = (
    "שלום! אני אליעזר, בוט התמלול של ivrit.ai.\n"
    "שלחו לי כאן הקלטה ואתמלל אותה.\n\n"
    "רוצים לקבל כאן גם את התמלולים של הקלטות שאתם שולחים לי בוואטסאפ? "
    "לחצו על הכפתור, ובוואטסאפ שייפתח שלחו את ההודעה כמו שהיא."
)
LINK_BUTTON = "קישור לוואטסאפ"


def tg_link_code_hint(token):
    return f"\n\nאם הכפתור לא עובד, שלחו לאליעזר בוואטסאפ את ההודעה: link {token}"


def tg_linked(masked):
    return (
        f"✅ המספר {masked} מקושר לצ'אט הזה. "
        f"מעכשיו התמלולים של ההקלטות שתשלחו בוואטסאפ יגיעו לכאן.\n"
        f"לביטול הקישור: /unlink"
    )


def tg_already_linked(masked_numbers):
    return (
        f"הצ'אט הזה מקושר ל-{', '.join(masked_numbers)}.\n"
        f"לקישור מספר נוסף לחצו על הכפתור. לביטול הקישור: /unlink"
    )


TG_LINK_EXPIRED = "תוקף קוד הקישור פג. שלחו /link לקבלת קוד חדש."
WA_LINK_UNKNOWN = "קוד הקישור לא תקין. שלחו /link לבוט בטלגרם כדי לקבל קוד חדש."


def tg_moved_away(masked):
    return f"המספר {masked} קושר לצ'אט טלגרם אחר, והתמלולים שלו לא יגיעו לכאן יותר."


def tg_unlinked(masked_numbers):
    return f"הקישור בוטל. {', '.join(masked_numbers)} לא מקושר יותר לצ'אט הזה."


TG_NOTHING_TO_UNLINK = "הצ'אט הזה לא מקושר לאף מספר וואטסאפ."
TG_HELP = (
    "שלחו לי הקלטה ואתמלל אותה.\n\n"
    "/link – קישור לוואטסאפ: התמלולים של הקלטות מוואטסאפ יגיעו לכאן\n"
    "/unlink – ביטול הקישור\n"
    "/status – סטטוס השירות\n\n"
    "בקבוצה: הוסיפו אותי לקבוצה, והשיבו /transcribe להקלטה כדי לקבל את התמלול שלה."
)

# Appended to a transcript with probability 1/NUDGE_INTERVAL.
NUDGE_INTERVAL = int(os.environ.get("NUDGE_INTERVAL", "100"))
with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "nudge.txt"), encoding="utf-8") as f:
    NUDGE = f.read().strip()


def maybe_nudge():
    return NUDGE if random.random() < 1.0 / NUDGE_INTERVAL else None


# Linked from /status; the public dashboard, not whatever host served the request.
SITE_URL = os.environ.get("PUBLIC_SITE_URL", "https://status.eliezer.ivrit.ai")


def _fmt_uptime(seconds):
    """Compact uptime, e.g. '2d 3h 5m 10s'."""
    days, remainder = divmod(int(seconds), 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, secs = divmod(remainder, 60)
    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    parts.append(f"{secs}s")
    return " ".join(parts)


def _fmt_transcribed(seconds):
    """Longform transcribed time, e.g. '1 days, 2 hours, 30 minutes'."""
    td_days, rem = divmod(int(seconds), 86400)
    td_hours, rem = divmod(rem, 3600)
    td_minutes, _ = divmod(rem, 60)
    parts = []
    if td_days > 0:
        parts.append(f"{td_days} days")
    parts.append(f"{td_hours} hours")
    parts.append(f"{td_minutes} minutes")
    return ", ".join(parts)


def status_text(stats):
    """/status: fleet-wide numbers from compute_stats()."""
    totals = stats.get('totals') or {}
    msgs = stats.get('messages') or {}
    tr = stats.get('transcriptions_1h') or {}
    uptimes = [i.get('uptime_seconds', 0) for i in (stats.get('instances') or [])]
    queue_depth = stats.get('queue_depth')
    return (
        f"*Eliezer Status*\n\n"
        f"*Uptime:* {_fmt_uptime(max(uptimes) if uptimes else 0)}\n"
        f"*Live instances:* {stats.get('live_instances')}\n"
        f"*Unique users (24h):* {stats.get('unique_users_24h')}\n"
        f"*Messages handled:* {totals.get('messages', 0)}\n"
        f"*Queue depth (est.):* {'unknown' if queue_depth is None else queue_depth}\n"
        f"*Total time transcribed:* {_fmt_transcribed(totals.get('duration_seconds', 0) or 0)}\n\n"
        f"*Messages/min (1m):* {float(msgs.get('last_1m', 0)):.1f}\n"
        f"*Messages/min (5m):* {(msgs.get('last_5m', 0) or 0) / 5.0:.1f}\n"
        f"*Messages/min (1h):* {msgs.get('per_min_1h', 0) or 0:.2f}\n"
        f"*Messages/min (24h):* {(msgs.get('last_24h', 0) or 0) / 1440.0:.2f}\n\n"
        f"*Avg message length (1h):* {tr.get('avg_duration') or 0:.1f}s\n"
        f"*Median message length (1h):* {tr.get('median_duration') or 0:.1f}s"
        f"\n\n📊 {SITE_URL}"
    )


def detailed_status_text(stats):
    """/detailed-status: fleet totals, then each live instance on its own."""
    lines = ["*Eliezer Detailed Status*"]
    totals = stats.get('totals') or {}
    td = int(totals.get('duration_seconds', 0) or 0)
    qd = stats.get('queue_depth')
    lines.append(
        f"\n*Fleet totals:* {int(totals.get('messages', 0))} msgs, "
        f"{int(totals.get('transcriptions', 0))} transcriptions, "
        f"{td // 3600}h {(td % 3600) // 60}m transcribed")
    lines.append(f"*Queue depth (shared):* {'unknown' if qd is None else qd}")

    instances = sorted(stats.get('instances') or [], key=lambda i: i.get('instance_id', ''))
    for inst in instances:
        up = int(float(inst.get('uptime_seconds', 0) or 0))
        age = float(inst.get('age_seconds', 0) or 0)
        lines.append(
            f"\n*{inst.get('instance_id', '?')}* — live ({int(age)}s ago)\n"
            f"  Uptime: {up // 3600}h {(up % 3600) // 60}m")
    if not instances:
        lines.append("\n_No live instances reporting._")

    lines.append(f"\n📊 {SITE_URL}")
    return "\n".join(lines)

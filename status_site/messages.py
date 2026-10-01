"""Everything the service says to users, independent of the channel it goes out on."""

import os
import random

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

ההוראות להמשך השימוש באליעזר נמצאות באתר:
https://status.eliezer.ivrit.ai

צוות ivrit.ai"""

# Sent once, and only once, to an unlinked WhatsApp number while WhatsApp replies are
# off: the one billed message that tells it where its transcripts went.
WA_DROPPED_NOTICE = """שלום,

בגלל שינוי המחירים בוואטסאפ, אנחנו משנים את הדרך שבה אליעזר, בוט התמלול של מיזם ivrit.ai, מחזיר את התמלולים: הם כבר לא נשלחים כאן בוואטסאפ.

אליעזר משרת עשרות אלפי משתמשים ביום ומתמלל יותר ממיליון הודעות בחודש, ללא תשלום.

כדי להמשיך לקבל תמלולים מאליעזר, קשרו את הוואטסאפ שלכם לאפליקציה ייעודית שפיתחנו, Communicator, או לטלגרם. את ההקלטות ממשיכים לשלוח לכאן, כרגיל. ההוראות:
https://status.eliezer.ivrit.ai

לשאלות נוספות: info@ivrit.ai

זו ההודעה היחידה שתקבלו מאתנו כאן ❤️

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


def tg_moved_away(masked):
    return f"המספר {masked} קושר לצ'אט טלגרם אחר, והתמלולים שלו לא יגיעו לכאן יותר."


def tg_unlinked(masked_numbers):
    return f"הקישור בוטל. {', '.join(masked_numbers)} לא מקושר יותר לצ'אט הזה."


TG_NOTHING_TO_UNLINK = "הצ'אט הזה לא מקושר לאף מספר וואטסאפ."
TG_CHAT_UNLINKED = (
    "הצ'אט הזה נותק, והתמלולים לא יגיעו אליו יותר. "
    "הם ממשיכים להגיע לאפליקציית Communicator; את הקישור אליה מבטלים מתוך האפליקציה."
)

# Legacy WhatsApp link replies (successful linking is now confirmed via thumbs-up emoji reaction).
WA_LINKED_TELEGRAM = (
    "✅ הוואטסאפ שלכם מקושר לטלגרם.\n"
    "אפשר להמשיך לשלוח לאליעזר הקלטות כאן בוואטסאפ, כרגיל. התמלולים יגיעו אליכם בטלגרם."
)
WA_LINKED_COMMUNICATOR = (
    "✅ הוואטסאפ שלכם מקושר לאפליקציית Communicator.\n"
    "אפשר להמשיך לשלוח לאליעזר הקלטות כאן בוואטסאפ, כרגיל. התמלולים יגיעו אליכם באפליקציה."
)

# --- linking the Notifier app

def notifier_welcome(channel, masked):
    where = "בוואטסאפ" if channel == "whatsapp" else "בטלגרם"
    return (
        f"✅ אליעזר מקושר ({masked}). "
        f"מעכשיו התמלולים של ההקלטות שתשלחו לאליעזר {where} יגיעו לכאן, כהתראות."
    )


TG_NOTIFIER_LINKED = "✅ אפליקציית Communicator מקושרת. התמלולים יגיעו גם אליה."
TG_NOTIFIER_CODE_FAILED = "הקוד לא תקין או שתוקפו פג. צרו קוד חדש באפליקציית Communicator ונסו שוב."


def transcript_subtitle(seconds):
    """What a transcript notification says under the name: that it is one, and how long
    the recording was."""
    if not seconds:
        return "תמלול הקלטה"
    seconds = int(round(seconds))
    return f"תמלול הקלטה · {seconds // 60}:{seconds % 60:02d}"
TG_HELP = (
    "שלחו לי הקלטה ואתמלל אותה.\n\n"
    "/link – קישור לוואטסאפ: התמלולים של הקלטות מוואטסאפ יגיעו לכאן\n"
    "/unlink – ביטול הקישור\n"
    "/status – סטטוס השירות\n\n"
    "בקבוצה: הוסיפו אותי לקבוצה, ואתמלל כל הקלטה שתישלח בה."
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

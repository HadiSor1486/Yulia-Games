"""
╔══════════════════════════════════════════════════════════════════════════╗
║                  SOREX PROFILE BOT  —  Kyodo (BULLETPROOF)              ║
║                                                                          ║
║  USER COMMANDS                                                           ║
║   /signup          — create your profile (step-by-step)                 ║
║   /profile         — view your own profile card                         ║
║   /view <name>     — view by profile name OR Kyodo nickname             ║
║   /buy <type> <n>  — buy an asset (frame 1 / bubble 2 / bg 3 /          ║
║                                    colour white)                        ║
║   /use <type> <n>  — equip a purchased asset (card shown automatically) ║
║   /remove bio      — clear your bio (shows "Empty")                     ║
║   /set bio <text>  — set or update your bio                             ║
║   /set name <name> — change your profile name                           ║
║   /balance         — check your sorex balance                           ║
║   /shop            — list all purchasable assets & prices               ║
║                                                                          ║
║  GAMES                                                                   ║
║   Reply to someone with a game trigger phrase to challenge them          ║
║   Winners earn sorex automatically (must be signed up)                  ║
║   انهاء اللعبة / end game  — host can cancel a running game             ║
║                                                                          ║
║  OWNER COMMANDS (Sor only)                                               ║
║   /give <n> sorex  — reply to transfer sorex to someone                 ║
║   /ban             — reply to ban a user from the bot                   ║
║   /unban           — reply to unban a user                              ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import asyncio
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import signal
import tempfile
import time
import unicodedata
from contextlib import suppress
from io import BytesIO
from typing import Any

import httpx
from PIL import Image, ImageDraw, ImageFont
from loguru import logger
from keep_alive import keep_alive
from kyodo import ChatMessage, Client, EventType
from kyodo.objects.args import ChatMessageTypes, MediaTarget

import json as _json

from .assets import (
    ASSETS, COLOR_MAP, GAME_MAP,
    FRAME_LAYOUTS,
    get_price, get_reward,
)
from .games import GAME_CLASSES
from .github_storage import pull_all, schedule_push, close as close_github

with suppress(ImportError):
    import uvloop
    uvloop.install()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def _p(*parts):
    return os.path.join(BASE_DIR, *parts)


# ══════════════════════════════════════════════════════════════════════════════
# 1.  CONFIG
# ══════════════════════════════════════════════════════════════════════════════
class Config:
    EMAIL     = os.getenv("BOT_EMAIL",     "hadidaoud.ha@gmail.com")
    PASSWORD  = os.getenv("BOT_PASSWORD",  "yulia123")
    DEVICE_ID = os.getenv("BOT_DEVICE_ID", "870d649515ce700797d6a56965689f3aaa7d5e82dfdce994b239e00e37238184")

    CHAT_ID   = os.getenv("BOT_CHAT_ID",   "cmh2gy89r01pvt33exijh1wr3")
    CIRCLE_ID = os.getenv("BOT_CIRCLE_ID", "cm9bylrbn00hmux6t43mczt2o")
    SOR_ID    = os.getenv("BOT_SOR_ID",    "cmgxsk2b30nalpx3ffm07h9i9")

    TEMPLATE_PATH    = _p("template.png")
    FONT_LATIN       = _p("font1.ttf")
    FONT_ARABIC      = _p("fontar.ttf")
    OUTPUT_QUALITY   = 95
    MAX_OUTPUT_WIDTH = 1100


    # ── Welcome image settings ────────────────────────────────────
    WELCOME_BACKGROUND       = _p("shbg.jpg")
    WELCOME_PROFILE_SIZE     = 303
    WELCOME_PROFILE_POSITION = (389, 291)

    # Name text position — tested values for gothic gray style
    WELCOME_NAME_X           = 540
    WELCOME_NAME_Y           = 950
    WELCOME_NAME_SIZE        = 55
    WELCOME_NAME_FILL        = (192, 192, 192, 255)   # silver-gray
    WELCOME_NAME_SHADOW      = (40, 40, 40, 180)      # dark shadow
    WELCOME_NAME_SHADOW_OFF  = 2                      # px offset

    ACCOUNTS_FILE = _p("accounts.json")
    BANNED_FILE   = _p("banned.json")
    LOG_FILE      = _p("profile_bot.log")
    DEDUP_FILE    = _p("dedup.json")

    DEDUP_MAX_AGE = 300
    DEDUP_MAX_ENTRIES = 5000

    C4_EMOJI_TIMEOUT_S = 180
    C4_GAME_TIMEOUT_S  = 1800

    RESTART_BACKOFF_MIN = 5
    RESTART_BACKOFF_MAX = 120
    HTTP_TIMEOUT        = 15


# ══════════════════════════════════════════════════════════════════════════════
# 2.  LOGGING
# ══════════════════════════════════════════════════════════════════════════════
logger.remove()
logger.add(
    lambda m: print(m, end=""),
    colorize=True,
    format="<green>{time:HH:mm:ss}</green> | <level>{level:<7}</level> | "
           "<cyan>{name}:{function}:{line}</cyan> — <level>{message}</level>",
    level="INFO",
)
logger.add(
    Config.LOG_FILE,
    rotation="5 MB", retention="14 days",
    encoding="utf-8", level="DEBUG",
    backtrace=True, diagnose=True,
)


# ══════════════════════════════════════════════════════════════════════════════
# 3.  TEXT NORMALIZATION  (fancy fonts, Arabic, Unicode variations)
# ══════════════════════════════════════════════════════════════════════════════

# ── Fancy-font → plain ASCII mapping tables ─────────────────────────────────

_FANCY_MAP: dict[int, str] = {}

# Mathematical Alphanumeric Symbols block (U+1D400–U+1D7FF)
# Bold A-Z        U+1D400-U+1D419  → A-Z
_FANCY_MAP.update({0x1D400 + i: chr(0x41 + i) for i in range(26)})
# Bold a-z        U+1D41A-U+1D433  → a-z
_FANCY_MAP.update({0x1D41A + i: chr(0x61 + i) for i in range(26)})
# Italic A-Z      U+1D434-U+1D44D  → A-Z
_FANCY_MAP.update({0x1D434 + i: chr(0x41 + i) for i in range(26)})
# Italic a-z      U+1D44E-U+1D467  → a-z
_FANCY_MAP.update({0x1D44E + i: chr(0x61 + i) for i in range(26)})
# Bold Italic A-Z U+1D468-U+1D481  → A-Z
_FANCY_MAP.update({0x1D468 + i: chr(0x41 + i) for i in range(26)})
# Bold Italic a-z U+1D482-U+1D49B  → a-z
_FANCY_MAP.update({0x1D482 + i: chr(0x61 + i) for i in range(26)})
# Script A-Z      U+1D49C-U+1D4B5  → A-Z  (with a gap at U+1D4A2)
for i in range(26):
    cp = 0x1D49C + i
    if cp >= 0x1D4A2:  # skip the hole at U+1D4A2 (Script B)
        cp += 1
    _FANCY_MAP[cp] = chr(0x41 + i)
# Script a-z      U+1D4B6-U+1D4CF  → a-z
_FANCY_MAP.update({0x1D4B6 + i: chr(0x61 + i) for i in range(26)})
# Bold Script A-Z U+1D4D0-U+1D4E9  → A-Z
_FANCY_MAP.update({0x1D4D0 + i: chr(0x41 + i) for i in range(26)})
# Bold Script a-z U+1D4EA-U+1D503  → a-z
_FANCY_MAP.update({0x1D4EA + i: chr(0x61 + i) for i in range(26)})
# Fraktur A-Z     U+1D504-U+1D51D  → A-Z
_FANCY_MAP.update({0x1D504 + i: chr(0x41 + i) for i in range(26)})
# Fraktur a-z     U+1D51E-U+1D537  → a-z
_FANCY_MAP.update({0x1D51E + i: chr(0x61 + i) for i in range(26)})
# Double-struck A-Z U+1D538-U+1D551 → A-Z
_FANCY_MAP.update({0x1D538 + i: chr(0x41 + i) for i in range(26)})
# Double-struck a-z U+1D552-U+1D56B → a-z
_FANCY_MAP.update({0x1D552 + i: chr(0x61 + i) for i in range(26)})
# Bold Fraktur A-Z  U+1D56C-U+1D585 → A-Z
_FANCY_MAP.update({0x1D56C + i: chr(0x41 + i) for i in range(26)})
# Bold Fraktur a-z  U+1D586-U+1D59F → a-z
_FANCY_MAP.update({0x1D586 + i: chr(0x61 + i) for i in range(26)})
# Sans A-Z        U+1D5A0-U+1D5B9  → A-Z
_FANCY_MAP.update({0x1D5A0 + i: chr(0x41 + i) for i in range(26)})
# Sans a-z        U+1D5BA-U+1D5D3  → a-z
_FANCY_MAP.update({0x1D5BA + i: chr(0x61 + i) for i in range(26)})
# Sans Bold A-Z   U+1D5D4-U+1D5ED  → A-Z
_FANCY_MAP.update({0x1D5D4 + i: chr(0x41 + i) for i in range(26)})
# Sans Bold a-z   U+1D5EE-U+1D607  → a-z
_FANCY_MAP.update({0x1D5EE + i: chr(0x61 + i) for i in range(26)})
# Sans Italic A-Z U+1D608-U+1D621  → A-Z
_FANCY_MAP.update({0x1D608 + i: chr(0x41 + i) for i in range(26)})
# Sans Italic a-z U+1D622-U+1D63B  → a-z
_FANCY_MAP.update({0x1D622 + i: chr(0x61 + i) for i in range(26)})
# Sans Bold Italic A-Z U+1D63C-U+1D655 → A-Z
_FANCY_MAP.update({0x1D63C + i: chr(0x41 + i) for i in range(26)})
# Sans Bold Italic a-z U+1D656-U+1D66F → a-z
_FANCY_MAP.update({0x1D656 + i: chr(0x61 + i) for i in range(26)})
# Monospace A-Z   U+1D670-U+1D689  → A-Z
_FANCY_MAP.update({0x1D670 + i: chr(0x41 + i) for i in range(26)})
# Monospace a-z   U+1D68A-U+1D6A3  → a-z
_FANCY_MAP.update({0x1D68A + i: chr(0x61 + i) for i in range(26)})

# Mathematical digits
# Bold digits      U+1D7CE-U+1D7D7 → 0-9
_FANCY_MAP.update({0x1D7CE + i: str(i) for i in range(10)})
# Double-struck    U+1D7D8-U+1D7E1 → 0-9
_FANCY_MAP.update({0x1D7D8 + i: str(i) for i in range(10)})
# Sans digits      U+1D7E2-U+1D7EB → 0-9
_FANCY_MAP.update({0x1D7E2 + i: str(i) for i in range(10)})
# Sans Bold digits U+1D7EC-U+1D7F5 → 0-9
_FANCY_MAP.update({0x1D7EC + i: str(i) for i in range(10)})
# Monospace digits U+1D7F6-U+1D7FF → 0-9
_FANCY_MAP.update({0x1D7F6 + i: str(i) for i in range(10)})

# Fullwidth ASCII variants (U+FF01–U+FF5E) → U+0021–U+007E
_FANCY_MAP.update({0xFF01 + i: chr(0x21 + i) for i in range(94)})
_FANCY_MAP[0xFF0D] = '-'   # fullwidth hyphen → regular hyphen
_FANCY_MAP[0xFF5F] = '('   # fullwidth left parenthesis
_FANCY_MAP[0xFF60] = ')'   # fullwidth right parenthesis
_FANCY_MAP[0xFF61] = '.'   # halfwidth ideographic full stop
_FANCY_MAP[0xFF62] = '['   # halfwidth left corner bracket
_FANCY_MAP[0xFF63] = ']'   # halfwidth right corner bracket
_FANCY_MAP[0xFF64] = ','   # halfwidth ideographic comma
_FANCY_MAP[0xFF65] = ':'   # halfwidth katakana middle dot

# Enclosed alphanumerics  ① → 1, ⑵ → 2, etc. (best-effort)
# Circled digits  ①-⑳  U+2460-U+2473
_FANCY_MAP.update({0x2460 + i: str(i + 1) for i in range(20)})
# Circled 21-35   U+3251-U+325F
_FANCY_MAP.update({0x3251 + i: str(i + 21) for i in range(15)})
# Parenthesized digits ⑴-⒇ U+2474-U+2487
_FANCY_MAP.update({0x2474 + i: str(i + 1) for i in range(20)})
# Parenthesized 21-32  U+2488-U+2493 (period digits, but similar)
_FANCY_MAP.update({0x2488 + i: str(i + 1) for i in range(10)})  # ⒈-⒑
# Circled letters Ⓐ-Ⓩ, ⓐ-ⓩ  U+24B6-U+24CF, U+24D0-U+24E9
_FANCY_MAP.update({0x24B6 + i: chr(0x41 + i) for i in range(26)})
_FANCY_MAP.update({0x24D0 + i: chr(0x61 + i) for i in range(26)})

# Letterlike Symbols (™ → TM, ℠ → SM, ℻ → Fax, etc. are handled by NFKD)
# But some are mapped explicitly:
_FANCY_MAP[0x2100] = 'a/c'   # Account of
_FANCY_MAP[0x2101] = 'a/s'   # Addressed to the subject
_FANCY_MAP[0x2103] = 'C'     # Degree Celsius → just C (or keep ℃ if NFKD handles)
_FANCY_MAP[0x2105] = 'c/o'   # Care of
_FANCY_MAP[0x2106] = 'c/u'   # Cada una
_FANCY_MAP[0x2109] = 'F'     # Degree Fahrenheit
_FANCY_MAP[0x2116] = 'No'    # Numero sign

# Greek-ish "lookalike" letters often used in fancy names
# K (U+212A Kelvin sign) → K, Å (U+212B Angstrom) → A
_FANCY_MAP[0x212A] = 'K'
_FANCY_MAP[0x212B] = 'A'

# Compatibility characters that NFKD often misses
_FANCY_MAP[0x2B55] = 'O'     # Heavy large circle → O

# Strike-through / underline / overline characters that people paste
_FANCY_MAP.update({0x0332: '', 0x0333: '', 0x0336: '', 0x0337: '', 0x0338: '',
                   0x0335: '', 0x0334: '', 0x0330: '', 0x0331: '', 0x033F: ''})

# Combining enclosing keycap / circle / square (people use these for "badges")
_FANCY_MAP.update({0x20E3: '', 0x20DD: '', 0x20DE: '', 0x20DF: '', 0x20E0: '',
                   0x20E2: '', 0x20E4: '', 0x20E5: '', 0x20E6: '', 0x20E7: '',
                   0x20E8: '', 0x20E9: '', 0x20EA: '', 0x20EB: '', 0x20EC: '',
                   0x20ED: '', 0x20EE: '', 0x20EF: '', 0x20F0: ''})


def normalize_text(text: str) -> str:
    """
    Normalize text for matching.
    - NFKD decomposes fancy font characters to base + modifier
    - Strips combining characters (font stylers)
    - Lowercases
    """
    if not text:
        return ""
    decomposed = unicodedata.normalize('NFKD', text)
    stripped = ''.join(c for c in decomposed if not unicodedata.combining(c))
    return stripped.lower().strip()


def normalize_display_name(text: str) -> str:
    """
    Convert fancy/stylized Unicode names to plain readable text.
    Handles mathematical alphanumeric symbols, fullwidth, enclosed forms,
    and combining marks that fonts commonly lack. Keeps Arabic intact.
    """
    if not text:
        return ""

    # 1. Map known fancy blocks to plain ASCII
    result = []
    for ch in text:
        cp = ord(ch)
        if cp in _FANCY_MAP:
            result.append(_FANCY_MAP[cp])
        else:
            result.append(ch)
    text = ''.join(result)

    # 2. NFKD decomposition + strip combining diacritics
    #    (catches remaining stylers: accents, strike-through, circled combos, etc.)
    decomposed = unicodedata.normalize('NFKD', text)
    text = ''.join(c for c in decomposed if not unicodedata.combining(c))

    # 3. Strip remaining non-printable / control / special-purpose chars
    #    Keep: letters, numbers, basic punctuation, spaces, Arabic range
    cleaned = []
    for c in text:
        cp = ord(c)
        # Keep printable ASCII and common spacing
        if 0x20 <= cp <= 0x7E:
            cleaned.append(c)
        # Keep Arabic and Arabic Presentation Forms-B
        elif (0x0600 <= cp <= 0x06FF) or (0xFE70 <= cp <= 0xFEFF) or (0xFB50 <= cp <= 0xFDFF):
            cleaned.append(c)
        # Keep Arabic Presentation Forms-A (extended)
        elif 0xFB50 <= cp <= 0xFDFF:
            cleaned.append(c)
        # Keep general punctuation that might be used
        elif cp in (0x2018, 0x2019, 0x201C, 0x201D, 0x2026, 0x2013, 0x2014):
            # smart quotes → straight quotes for safety, keep others
            if cp == 0x2018 or cp == 0x2019:
                cleaned.append("'")
            elif cp == 0x201C or cp == 0x201D:
                cleaned.append('"')
            else:
                cleaned.append(c)
        # Keep extended Latin letters (accents stripped already, but base remains)
        elif 0x00A1 <= cp <= 0x024F:   # Latin-1 Supplement + Extended-A/B
            cleaned.append(c)
        # Keep zero-width joiner/non-joiner for Arabic shaping context
        elif cp in (0x200C, 0x200D):
            cleaned.append(c)
        # Everything else (symbols, emoji components, remaining math chars, etc.) → drop

    text = ''.join(cleaned)

    # 4. Collapse multiple spaces
    while '  ' in text:
        text = text.replace('  ', ' ')

    return text.strip()


def names_match(a: str, b: str) -> bool:
    """Check if two names match (handles fancy fonts, case, whitespace)."""
    return normalize_text(a) == normalize_text(b)


# ══════════════════════════════════════════════════════════════════════════════
# 4.  ARABIC TEXT RESHAPING
# ══════════════════════════════════════════════════════════════════════════════
try:
    from PIL import features as _pil_features
    _RAQM = _pil_features.check("raqm")
except Exception:
    _RAQM = False

_AR = {
    0x0621:('\u0621','\u0621','\u0621','\u0621'),
    0x0622:('\u0622','\uFE82',None,None),0x0623:('\u0623','\uFE84',None,None),
    0x0624:('\u0624','\uFE86',None,None),0x0625:('\u0625','\uFE88',None,None),
    0x0626:('\u0626','\uFE8A','\uFE8B','\uFE8C'),
    0x0627:('\u0627','\uFE8E',None,None),
    0x0628:('\u0628','\uFE90','\uFE91','\uFE92'),
    0x0629:('\u0629','\uFE94',None,None),
    0x062A:('\u062A','\uFE96','\uFE97','\uFE98'),
    0x062B:('\u062B','\uFE9A','\uFE9B','\uFE9C'),
    0x062C:('\u062C','\uFE9E','\uFE9F','\uFEA0'),
    0x062D:('\u062D','\uFEA2','\uFEA3','\uFEA4'),
    0x062E:('\u062E','\uFEA6','\uFEA7','\uFEA8'),
    0x062F:('\u062F','\uFEAA',None,None),0x0630:('\u0630','\uFEAC',None,None),
    0x0631:('\u0631','\uFEAE',None,None),0x0632:('\u0632','\uFEB0',None,None),
    0x0633:('\u0633','\uFEB2','\uFEB3','\uFEB4'),
    0x0634:('\u0634','\uFEB6','\uFEB7','\uFEB8'),
    0x0635:('\u0635','\uFEBA','\uFEBB','\uFEBC'),
    0x0636:('\u0636','\uFEBE','\uFEBF','\uFEC0'),
    0x0637:('\u0637','\uFEC2','\uFEC3','\uFEC4'),
    0x0638:('\u0638','\uFEC6','\uFEC7','\uFEC8'),
    0x0639:('\u0639','\uFECA','\uFECB','\uFECC'),
    0x063A:('\u063A','\uFECE','\uFECF','\uFED0'),
    0x0641:('\u0641','\uFED2','\uFED3','\uFED4'),
    0x0642:('\u0642','\uFED6','\uFED7','\uFED8'),
    0x0643:('\u0643','\uFEDA','\uFEDB','\uFEDC'),
    0x0644:('\u0644','\uFEDE','\uFEDF','\uFEE0'),
    0x0645:('\u0645','\uFEE2','\uFEE3','\uFEE4'),
    0x0646:('\u0646','\uFEE6','\uFEE7','\uFEE8'),
    0x0647:('\u0647','\uFEEA','\uFEEB','\uFEEC'),
    0x0648:('\u0648','\uFEEE',None,None),0x0649:('\u0649','\uFEF0',None,None),
    0x064A:('\u064A','\uFEF2','\uFEF3','\uFEF4'),
}
_NJ = {0x0621,0x0622,0x0623,0x0624,0x0625,0x0627,0x0629,0x062F,0x0630,0x0631,0x0632,0x0648,0x0649}
_LA = {(0xFEDF,0x0622):'\uFEF5',(0xFEE0,0x0622):'\uFEF6',(0xFEDF,0x0623):'\uFEF7',
       (0xFEE0,0x0623):'\uFEF8',(0xFEDF,0x0625):'\uFEF9',(0xFEE0,0x0625):'\uFEFA',
       (0xFEDF,0x0627):'\uFEFB',(0xFEE0,0x0627):'\uFEFC'}

def _is_ar(ch): cp=ord(ch); return (0x0600<=cp<=0x06FF)or(0xFB50<=cp<=0xFDFF)or(0xFE70<=cp<=0xFEFF)
def is_arabic(t): return any(_is_ar(c) for c in t)

def reshape_arabic(text):
    chars=list(text); n=len(chars); shaped=[]
    for i,ch in enumerate(chars):
        cp=ord(ch)
        if cp not in _AR: shaped.append(ch); continue
        pc=ord(chars[i-1]) if i>0 else 0; nc=ord(chars[i+1]) if i<n-1 else 0
        jr=pc in _AR and pc not in _NJ; jl=nc in _AR; f=_AR[cp]
        if jr and jl and f[3]: shaped.append(f[3])
        elif jr and f[1]:      shaped.append(f[1])
        elif jl and f[2]:      shaped.append(f[2])
        else:                  shaped.append(f[0])
    m,j=[],0
    while j<len(shaped):
        if j<len(shaped)-1:
            lig=_LA.get((ord(shaped[j]),ord(shaped[j+1])))
            if lig: m.append(lig); j+=2; continue
        m.append(shaped[j]); j+=1
    m.reverse(); return "".join(m)

def prepare_text(t):
    if not is_arabic(t):
        return t
    if _RAQM:
        return t
    return reshape_arabic(t)


# ══════════════════════════════════════════════════════════════════════════════
# 5.  NETWORK
# ══════════════════════════════════════════════════════════════════════════════
_http: httpx.AsyncClient | None = None

async def http() -> httpx.AsyncClient:
    global _http
    if _http is None or _http.is_closed:
        _http = httpx.AsyncClient(
            timeout=httpx.Timeout(Config.HTTP_TIMEOUT),
            headers={"User-Agent": "SorexBot/2.1"},
            follow_redirects=True,
            limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
        )
    return _http

async def close_http():
    global _http
    if _http and not _http.is_closed:
        await _http.aclose()


# ══════════════════════════════════════════════════════════════════════════════
# 6.  JSON HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def json_read(path: str, default: Any = None) -> Any:
    if default is None:
        default = {}
    try:
        if not os.path.exists(path):
            json_write(path, default)
            return default
        with open(path, "r", encoding="utf-8") as f:
            return _json.load(f)
    except Exception as e:
        logger.error(f"[json] read {path}: {e}")
        return default


def json_write(path: str, data: Any):
    d = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp_", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            _json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception:
        with suppress(FileNotFoundError):
            os.remove(tmp)
        raise


# ══════════════════════════════════════════════════════════════════════════════
# 7.  BULLETPROOF DEDUPLICATION
# ══════════════════════════════════════════════════════════════════════════════
_seen_msg_ids: set[str] = set()
_command_cooldown: dict[str, float] = {}
COMMAND_COOLDOWN_S = 5.0

_dedup_persistent: dict[str, float] = {}


def _load_dedup():
    global _dedup_persistent
    data = json_read(Config.DEDUP_FILE, {})
    if not isinstance(data, dict):
        data = {}
    now = time.time()
    _dedup_persistent = {
        k: v for k, v in data.items()
        if isinstance(v, (int, float)) and now - v < Config.DEDUP_MAX_AGE
    }


def _save_dedup():
    try:
        json_write(Config.DEDUP_FILE, _dedup_persistent)
    except Exception as e:
        logger.warning(f"[dedup] failed to save: {e}")


def _prune_dedup():
    global _dedup_persistent
    now = time.time()
    cutoff = now - Config.DEDUP_MAX_AGE
    if len(_dedup_persistent) > Config.DEDUP_MAX_ENTRIES:
        sorted_items = sorted(_dedup_persistent.items(), key=lambda x: x[1], reverse=True)
        _dedup_persistent = dict(sorted_items[:Config.DEDUP_MAX_ENTRIES])
    else:
        _dedup_persistent = {k: v for k, v in _dedup_persistent.items() if v > cutoff}


async def _dedup_message(message: ChatMessage) -> bool:
    mid = message.messageId
    now = time.time()
    if mid in _seen_msg_ids:
        return True
    if mid in _dedup_persistent:
        return True
    _seen_msg_ids.add(mid)
    _dedup_persistent[mid] = now
    if len(_seen_msg_ids) > 5000 or len(_dedup_persistent) > Config.DEDUP_MAX_ENTRIES:
        _seen_msg_ids.clear()
        _prune_dedup()
        _save_dedup()
    return False


async def _dedup_command(uid: str, command: str) -> bool:
    key = f"{uid}|{command}"
    now = time.time()
    last = _command_cooldown.get(key, 0)
    if now - last < COMMAND_COOLDOWN_S:
        return True
    _command_cooldown[key] = now
    if len(_command_cooldown) > 5000:
        cutoff = now - COMMAND_COOLDOWN_S * 10
        dead = [k for k, v in _command_cooldown.items() if v < cutoff]
        for k in dead:
            del _command_cooldown[k]
    return False


# ══════════════════════════════════════════════════════════════════════════════
# 8.  DATA STORES  (accounts + banned only)
# ══════════════════════════════════════════════════════════════════════════════
accounts: dict[str, dict] = {}
banned:   dict[str, str]  = {}

_accounts_lock = asyncio.Lock()
_banned_lock = asyncio.Lock()

_pending_accounts_save = False
_pending_banned_save = False

# In-memory nickname → uid index for fast /view lookups
_nickname_index: dict[str, str] = {}


def _default_account(name: str, bio: str, nickname: str = "", avatar_url: str = "") -> dict:
    """
    Create a new account. `nickname` is the normalized Kyodo name,
    captured once at signup and stored permanently for /view lookups.
    """
    return {
        "name": name,
        "nickname": nickname,
        "bio": bio,
        "bio_removed": False,
        "games_played": 0,
        "wins": 0,
        "wealth": 0,
        "owned_assets": ["bgdefault", "framedefault", "bubbledefault"],
        "active_bg": "bgdefault",
        "active_frame": "framedefault",
        "active_bubble": "bubbledefault",
        "active_colour": "black",
        "game_stats": {},
        "avatar_url": avatar_url,
    }


def _rebuild_nickname_index():
    """Rebuild the nickname → uid lookup from accounts."""
    global _nickname_index
    _nickname_index = {}
    for uid, data in accounts.items():
        nick = data.get("nickname", "")
        if nick:
            _nickname_index[normalize_text(nick)] = uid
        name = data.get("name", "")
        if name:
            _nickname_index[normalize_text(name)] = uid


def resolve_user(query: str) -> str | None:
    """
    Find a user ID by profile name OR Kyodo nickname.
    Handles fancy fonts, Arabic, case differences via normalize_text().
    """
    if not query:
        return None

    q = query.strip()
    q_lower = q.lower()
    q_norm = normalize_text(q)

    # 1. Exact case-insensitive name match
    for uid, data in accounts.items():
        if data.get("name", "").lower() == q_lower:
            return uid

    # 2. Exact nickname match (case-insensitive)
    for uid, data in accounts.items():
        if data.get("nickname", "").lower() == q_lower:
            return uid

    # 3. Normalized match (handles fancy fonts, Unicode variations)
    for uid, data in accounts.items():
        if names_match(data.get("name", ""), q):
            return uid
        if names_match(data.get("nickname", ""), q):
            return uid

    # 4. In-memory nickname index (fast O(1))
    if q_norm in _nickname_index:
        return _nickname_index[q_norm]

    # 5. Partial match on name (last resort)
    for uid, data in accounts.items():
        name = data.get("name", "")
        if q_lower in name.lower() or normalize_text(name) == q_norm:
            return uid
        nick = data.get("nickname", "")
        if q_lower in nick.lower() or normalize_text(nick) == q_norm:
            return uid

    return None


# ── Load/Save ────────────────────────────────────────────────────────────────

async def load_all():
    global accounts, banned

    github_data = await pull_all()

    acc = github_data.get("accounts.json")
    if acc is not None and isinstance(acc, dict):
        accounts = acc
        json_write(Config.ACCOUNTS_FILE, accounts)
    else:
        accounts = json_read(Config.ACCOUNTS_FILE, {})

    ban = github_data.get("banned.json")
    if ban is not None and isinstance(ban, dict):
        banned = ban
        json_write(Config.BANNED_FILE, banned)
    else:
        banned = json_read(Config.BANNED_FILE, {})

    _rebuild_nickname_index()
    _load_dedup()

    logger.info(f"Data loaded — accounts:{len(accounts)} banned:{len(banned)}")


# ── Save functions ──────────────────────────────────────────────────────────

async def save_accounts():
    global _pending_accounts_save
    async with _accounts_lock:
        try:
            json_write(Config.ACCOUNTS_FILE, accounts)
            if not _pending_accounts_save:
                _pending_accounts_save = True
                schedule_push("accounts.json", accounts)
                _pending_accounts_save = False
        except Exception as e:
            logger.exception(f"[accounts] save error: {e}")


async def save_banned():
    global _pending_banned_save
    async with _banned_lock:
        try:
            json_write(Config.BANNED_FILE, banned)
            if not _pending_banned_save:
                _pending_banned_save = True
                schedule_push("banned.json", banned)
                _pending_banned_save = False
        except Exception as e:
            logger.exception(f"[banned] save error: {e}")


def save_accounts_sync():
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(save_accounts())
    except RuntimeError:
        pass


def save_banned_sync():
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(save_banned())
    except RuntimeError:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# 9.  ECONOMY HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def has_account(uid): return uid in accounts
def is_banned(uid):   return uid in banned
def get_balance(uid): return accounts.get(uid, {}).get("wealth", 0)


def add_sorex(uid, amount):
    if uid in accounts:
        accounts[uid]["wealth"] = accounts[uid].get("wealth", 0) + amount
        save_accounts_sync()


def deduct_sorex(uid, amount):
    if uid in accounts and accounts[uid].get("wealth", 0) >= amount:
        accounts[uid]["wealth"] -= amount
        save_accounts_sync()
        return True
    return False


def owns_asset(uid, key): return key in accounts.get(uid, {}).get("owned_assets", [])


def grant_asset(uid, key):
    if uid in accounts and key not in accounts[uid]["owned_assets"]:
        accounts[uid]["owned_assets"].append(key)
        save_accounts_sync()


# ══════════════════════════════════════════════════════════════════════════════
# 10.  RANK SYSTEM
# ══════════════════════════════════════════════════════════════════════════════
def get_rank(wins):
    if wins == 0:  return ""
    if wins <= 5:  return "Beginner"
    if wins <= 10: return "Apprentice"
    if wins <= 20: return "Advanced"
    if wins <= 30: return "Expert"
    if wins <= 40: return "Elite"
    if wins <= 50: return "Master"
    if wins <= 60: return "Grandmaster"
    if wins <= 79: return "Legend"
    return "Mythic"


def get_primary_game(uid):
    stats = accounts.get(uid, {}).get("game_stats", {})
    if not stats:
        return None, None
    primary = max(stats, key=lambda g: stats[g].get("plays", 0))
    if stats[primary].get("plays", 0) == 0:
        return None, None
    icon = next((ik for gn, ik in GAME_MAP.items() if gn.lower() == primary.lower()), None)
    return primary, icon


def update_game_stats(uid, game_name, won):
    if uid not in accounts:
        return
    gs = accounts[uid].setdefault("game_stats", {})
    e = gs.setdefault(game_name, {"plays": 0, "wins": 0})
    e["plays"] += 1
    e["wins"] += (1 if won else 0)
    accounts[uid]["games_played"] = accounts[uid].get("games_played", 0) + 1
    if won:
        accounts[uid]["wins"] = accounts[uid].get("wins", 0) + 1
    save_accounts_sync()


# ══════════════════════════════════════════════════════════════════════════════
# 11.  PROFILE CARD LAYOUT & RENDERING
# ══════════════════════════════════════════════════════════════════════════════
LAYOUT = {
    "bg":       {"x": -15,  "y": -15,  "size": 1040},
    "pfp":      {"x": 377,  "y": 250,  "size": 268},
    "frame":    {"x": 357,  "y": 230,  "size": 312},
    "bubble":   {"x": 160,  "y": 600,  "size": 700},
    "gameicon": {"x": 280,  "y": 1130, "size": 120},
    "name":     {"cx": 511, "y": 540,  "size": 50,  "color": (255, 255, 255, 255)},
    "bio":      {"cx": 510, "cy": 720, "size": 34,  "color": (30, 30, 30, 255),
                 "max_width": 480, "max_lines": 2, "line_gap": 10},
    "games":    {"cx": 183, "y": 935,  "size": 50,  "color": (255, 255, 255, 255)},
    "wins":     {"cx": 511, "y": 935,  "size": 50,  "color": (255, 255, 255, 255)},
    "wealth":   {"cx": 770, "y": 935,  "size": 50,  "color": (255, 255, 255, 255)},
    "game":     {"x": 430,  "y": 1150, "size": 46,  "color": (255, 255, 255, 255)},
    "rank":     {"x": 430,  "y": 1208, "size": 38,  "color": (200, 200, 200, 255)},
    "nogame":   {"cx": 511, "y": 1180, "size": 40,  "color": (160, 160, 160, 255)},
}


def _font(text, size):
    p = Config.FONT_ARABIC if is_arabic(text) else Config.FONT_LATIN
    for q in (p, Config.FONT_LATIN):
        try:
            return ImageFont.truetype(q, size)
        except Exception:
            pass
    return ImageFont.load_default()


def _cx(draw, text, cx, y, font, color):
    t = prepare_text(text)
    bb = font.getbbox(t)
    w = bb[2] - bb[0]
    sh = bb[1] if bb[1] > 3 else 0
    draw.text((cx - w // 2, y - sh), t, font=font, fill=color)


def _lx(draw, text, x, y, font, color):
    t = prepare_text(text)
    bb = font.getbbox(t)
    sh = bb[1] if bb[1] > 3 else 0
    draw.text((x, y - sh), t, font=font, fill=color)


def _wrap(text, font, mw, ml):
    words, lines, cur = text.split(), [], ""
    for w in words:
        test = (cur + " " + w).strip()
        test_width = font.getbbox(prepare_text(test))[2] - font.getbbox(prepare_text(test))[0]
        if test_width <= mw:
            cur = test
        else:
            if cur:
                lines.append(cur)
            cur = w
            if len(lines) >= ml:
                cur = ""
                break
    if cur and len(lines) < ml:
        lines.append(cur)
    lines = lines[:ml]
    if lines:
        last = lines[-1]
        last_width = font.getbbox(prepare_text(last))[2]
        if last_width > mw:
            while last:
                ellipsis_width = font.getbbox(prepare_text(last + "..."))[2]
                if ellipsis_width <= mw:
                    lines[-1] = last + "..."
                    break
                last = last[:-1]
    return lines


def _hex_rgba(h):
    h = h.lstrip("#")
    return (int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16), 255)


def _circular(img, size):
    w, h = img.size
    m = min(w, h)
    img = img.crop(((w - m) // 2, (h - m) // 2, (w + m) // 2, (h + m) // 2)).resize((size, size), Image.LANCZOS)
    mask = Image.new("L", (size * 3, size * 3), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, size * 3 - 1, size * 3 - 1), fill=255)
    mask = mask.resize((size, size), Image.LANCZOS)
    img.putalpha(mask)
    return img


def _paste(base, key, x, y, size, circ=False):
    fn = ASSETS.get(key)
    if not fn:
        return base
    fn = os.path.join(BASE_DIR, fn)
    if not os.path.exists(fn):
        return base
    img = Image.open(fn).convert("RGBA")
    if circ:
        img = _circular(img, size)
        tmp = Image.new("RGBA", base.size, (0, 0, 0, 0))
        tmp.paste(img, (x, y), img)
    else:
        ow, oh = img.size
        img = img.resize((size, int(size * oh / ow)), Image.LANCZOS)
        tmp = Image.new("RGBA", base.size, (0, 0, 0, 0))
        tmp.paste(img, (x, y), img)
    return Image.alpha_composite(base, tmp)


def _paste_raw(base, raw_bytes, x, y, size):
    img = _circular(Image.open(BytesIO(raw_bytes)).convert("RGBA"), size)
    tmp = Image.new("RGBA", base.size, (0, 0, 0, 0))
    tmp.paste(img, (x, y), img)
    return Image.alpha_composite(base, tmp)


async def _dl(url):
    try:
        c = await http()
        r = await c.get(url, timeout=Config.HTTP_TIMEOUT)
        if r.status_code == 200:
            return r.content
    except Exception as e:
        logger.warning(f"[dl] {e}")
    return None


# ── Frame layout helper: returns (x, y, size) for a given frame key ──────────
def _get_frame_layout(frame_key: str):
    """
    Look up per-frame layout overrides.  If *frame_key* exists in
    FRAME_LAYOUTS return its custom (x, y, size).  Otherwise fall
    back to the default LAYOUT['frame'] values.
    """
    custom = FRAME_LAYOUTS.get(frame_key)
    if custom:
        return custom["x"], custom["y"], custom["size"]
    d = LAYOUT["frame"]
    return d["x"], d["y"], d["size"]


async def render_profile_card(uid: str) -> BytesIO | None:
    user = accounts.get(uid)
    if not user or not os.path.exists(Config.TEMPLATE_PATH):
        return None
    try:
        base = Image.open(Config.TEMPLATE_PATH).convert("RGBA")
        L = LAYOUT
        base = _paste(base, user.get("active_bg", "bgdefault"), L["bg"]["x"], L["bg"]["y"], L["bg"]["size"])
        av = user.get("avatar_url", "")
        if av:
            raw = await _dl(av)
            if raw:
                base = _paste_raw(base, raw, L["pfp"]["x"], L["pfp"]["y"], L["pfp"]["size"])

        # --- PER-FAME LAYOUT (flexible) ---
        active_frame = user.get("active_frame", "framedefault")
        fx, fy, fsize = _get_frame_layout(active_frame)
        base = _paste(base, active_frame, fx, fy, fsize)
        # -----------------------------------

        base = _paste(base, user.get("active_bubble", "bubbledefault"), L["bubble"]["x"], L["bubble"]["y"], L["bubble"]["size"])
        pg, ik = get_primary_game(uid)
        hg = pg is not None and ik is not None
        if hg:
            base = _paste(base, ik, L["gameicon"]["x"], L["gameicon"]["y"], L["gameicon"]["size"], circ=True)
        draw = ImageDraw.Draw(base)

        cfg = L["name"]
        _cx(draw, normalize_display_name(user["name"]), cfg["cx"], cfg["y"], _font(normalize_display_name(user["name"]), cfg["size"]), cfg["color"])

        cfg = L["bio"]
        bt = "Empty" if user.get("bio_removed") else (user.get("bio") or "")
        bc = _hex_rgba(COLOR_MAP.get(user.get("active_colour", "black"), "#1E1E1E"))
        font = _font(bt, cfg["size"])
        lines = _wrap(bt, font, cfg["max_width"], cfg["max_lines"])
        lh = cfg["size"] + cfg["line_gap"]
        total = len(lines) * cfg["size"] + max(0, len(lines) - 1) * cfg["line_gap"]
        yc = cfg["cy"] - total // 2
        for line in lines:
            t = prepare_text(line)
            bb = font.getbbox(t)
            w = bb[2] - bb[0]
            sh = bb[1] if bb[1] > 3 else 0
            draw.text((cfg["cx"] - w // 2, yc - sh), t, font=font, fill=bc)
            yc += lh

        for fld, val in (("games", str(user.get("games_played", 0))),
                         ("wins", str(user.get("wins", 0))),
                         ("wealth", str(user.get("wealth", 0)))):
            cfg = L[fld]
            _cx(draw, val, cfg["cx"], cfg["y"], _font(val, cfg["size"]), cfg["color"])

        if hg:
            rank = get_rank(user.get("wins", 0))
            cfg = L["game"]
            _lx(draw, pg, cfg["x"], cfg["y"], _font(pg, cfg["size"]), cfg["color"])
            if rank:
                cfg = L["rank"]
                _lx(draw, rank, cfg["x"], cfg["y"], _font(rank, cfg["size"]), cfg["color"])
        else:
            cfg = L["nogame"]
            _cx(draw, "No games", cfg["cx"], cfg["y"], _font("No games", cfg["size"]), cfg["color"])

        out = BytesIO()
        base.convert("RGB").save(out, "JPEG", quality=Config.OUTPUT_QUALITY)
        out.seek(0)
        return out
    except Exception as e:
        logger.exception(f"[render] {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
# 12.  IMAGE SENDING
# ══════════════════════════════════════════════════════════════════════════════
async def send_image_bytes(chat_id, circle_id, img_bytes):
    with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as f:
        f.write(img_bytes.read())
        tmp = f.name
    try:
        img = Image.open(tmp)
        w, h = img.size
        with open(tmp, "rb") as f:
            media = await client.upload_media(f, MediaTarget.ChatImageMessage)
        await client.send_chat_entity(
            chat_id,
            circle_id,
            ChatMessageTypes.IMAGE,
            media_id=media.media_id,
            width=w,
            height=h,
        )
    except Exception as e:
        logger.warning(f"[send_image] {e}")
    finally:
        with suppress(FileNotFoundError):
            os.remove(tmp)


async def send_profile_card(chat_id, circle_id, uid):
    img = await render_profile_card(uid)
    if img:
        await send_image_bytes(chat_id, circle_id, img)


# ══════════════════════════════════════════════════════════════════════════════
# 13.  WELCOME IMAGE
# ══════════════════════════════════════════════════════════════════════════════
async def render_welcome(name, avatar_url):
    base = Image.open(Config.WELCOME_BACKGROUND).convert("RGBA")
    raw = await _dl(avatar_url)
    if raw:
        pfp = _circular(Image.open(BytesIO(raw)).convert("RGBA"), Config.WELCOME_PROFILE_SIZE)
        tmp = Image.new("RGBA", base.size, (0, 0, 0, 0))
        tmp.paste(pfp, Config.WELCOME_PROFILE_POSITION, pfp)
        base = Image.alpha_composite(base, tmp)
    draw = ImageDraw.Draw(base)
    font = ImageFont.truetype(Config.FONT_LATIN, Config.WELCOME_NAME_SIZE)
    text = f"Welcome {name}"
    x = Config.WELCOME_NAME_X
    y = Config.WELCOME_NAME_Y
    sx = Config.WELCOME_NAME_SHADOW_OFF
    sy = Config.WELCOME_NAME_SHADOW_OFF
    draw.text((x + sx, y + sy), text, font=font, fill=Config.WELCOME_NAME_SHADOW)
    draw.text((x, y), text, font=font, fill=Config.WELCOME_NAME_FILL)
    out = BytesIO()
    base.convert("RGB").save(out, "JPEG", quality=95)
    out.seek(0)
    return out


# ══════════════════════════════════════════════════════════════════════════════
# 14.  COMMANDS
# ══════════════════════════════════════════════════════════════════════════════
HELP_TEXT = (
    "━━━━━━━━━━━━━━━━━━━━\n"
    "      Sorex Profile Bot\n"
    "━━━━━━━━━━━━━━━━━━━━\n\n"
    "/signup\n"
    "  Create your profile (step-by-step)\n\n"
    "/profile\n"
    "  View your profile card\n\n"
    "/view <name>\n"
    "  View someone else's profile\n\n"
    "/buy <type> <n>\n"
    "  Purchase an asset\n"
    "  type = frame | bubble | bg | colour\n\n"
    "/use <type> <n>\n"
    "  Equip a purchased asset\n\n"
    "/set bio <text>\n"
    "  Set your bio\n\n"
    "/remove bio\n"
    "  Clear your bio\n\n"
    "/set name <name>\n"
    "  Change profile name\n\n"
    "/balance\n"
    "  Check sorex balance\n\n"
    "/shop\n"
    "  List purchasable assets\n\n"
    "/help\n"
    "  Show this message\n\n"
    "━━━━━━━━━━━━━━━━━━━━"
)


async def cmd_help(msg):
    await client.send_chat_message(msg.chatId, msg.circleId, HELP_TEXT)


async def cmd_signup(msg, args):
    uid = msg.createdBy
    if is_banned(uid):
        return
    if has_account(uid):
        await client.send_chat_message(msg.chatId, msg.circleId, "You already have a profile. Use /profile to view it.")
        return

    await client.send_chat_message(msg.chatId, msg.circleId, "Enter your profile name:")
    name_msg = await client.wait_for(EventType.CHAT_MESSAGE, timeout=120)
    if not name_msg or name_msg.createdBy != uid:
        await client.send_chat_message(msg.chatId, msg.circleId, "Signup timed out. Please try again.")
        return
    name = name_msg.body.strip()
    if not name:
        await client.send_chat_message(msg.chatId, msg.circleId, "Name cannot be empty. Signup cancelled.")
        return

    await client.send_chat_message(msg.chatId, msg.circleId, "Enter your bio:")
    bio_msg = await client.wait_for(EventType.CHAT_MESSAGE, timeout=120)
    if not bio_msg or bio_msg.createdBy != uid:
        await client.send_chat_message(msg.chatId, msg.circleId, "Signup timed out. Please try again.")
        return
    bio = bio_msg.body.strip()

    nickname = ""
    if hasattr(msg, 'sender') and msg.sender:
        nickname = getattr(msg.sender, 'nickname', '') or ''

    avatar_url = ""
    if hasattr(msg, 'sender') and msg.sender:
        avatar_url = getattr(msg.sender, 'icon', '') or ''

    accounts[uid] = _default_account(name, bio, nickname, avatar_url)
    await save_accounts()
    _rebuild_nickname_index()

    await client.send_chat_message(msg.chatId, msg.circleId, f"Welcome, {name}! Your profile has been created.")
    await send_profile_card(msg.chatId, msg.circleId, uid)


async def cmd_profile(msg, args):
    uid = msg.createdBy
    if is_banned(uid):
        return
    if not has_account(uid):
        await client.send_chat_message(msg.chatId, msg.circleId, "You don't have a profile yet. Use /signup to create one.")
        return
    await send_profile_card(msg.chatId, msg.circleId, uid)


async def cmd_view(msg, args):
    if is_banned(msg.createdBy):
        return
    if not args:
        await client.send_chat_message(msg.chatId, msg.circleId, "Usage: /view <profile name>")
        return
    target = resolve_user(args)
    if not target:
        await client.send_chat_message(msg.chatId, msg.circleId, f"No profile found for '{args}'.")
        return
    await send_profile_card(msg.chatId, msg.circleId, target)


async def cmd_shop(msg, args):
    lines = ["Shop — Available Assets", "=" * 25]
    for key, path in ASSETS.items():
        if "default" in key:
            continue
        price = get_price(key)
        asset_type = key.replace("default", "").rstrip("0123456789")
        lines.append(f"  {asset_type} {key[-1] if key[-1].isdigit() else ''} — {price} sorex")
    lines.append("=" * 25)
    lines.append("Use /buy <type> <n> to purchase")
    await client.send_chat_message(msg.chatId, msg.circleId, "\n".join(lines))


async def cmd_buy(msg, args):
    uid = msg.createdBy
    if is_banned(uid):
        return
    if not has_account(uid):
        await client.send_chat_message(msg.chatId, msg.circleId, "You need to /signup first.")
        return
    parts = args.split()
    if len(parts) < 2:
        await client.send_chat_message(msg.chatId, msg.circleId, "Usage: /buy <type> <n>")
        return
    asset_type = parts[0].lower()
    number = parts[1]
    key = f"{asset_type}{number}"
    if key not in ASSETS:
        await client.send_chat_message(msg.chatId, msg.circleId, f"'{key}' is not available in the shop.")
        return
    if owns_asset(uid, key):
        await client.send_chat_message(msg.chatId, msg.circleId, "You already own this asset.")
        return
    price = get_price(key)
    if not deduct_sorex(uid, price):
        await client.send_chat_message(msg.chatId, msg.circleId, f"You need {price} sorex to buy this asset.")
        return
    grant_asset(uid, key)
    await client.send_chat_message(msg.chatId, msg.circleId, f"You purchased {asset_type} {number} for {price} sorex!")


async def cmd_use(msg, args):
    uid = msg.createdBy
    if is_banned(uid):
        return
    if not has_account(uid):
        await client.send_chat_message(msg.chatId, msg.circleId, "You need to /signup first.")
        return
    parts = args.split()
    if len(parts) < 2:
        await client.send_chat_message(msg.chatId, msg.circleId, "Usage: /use <type> <n>")
        return
    asset_type = parts[0].lower()
    number = parts[1]
    key = f"{asset_type}{number}"
    if key not in ASSETS and asset_type != "colour":
        await client.send_chat_message(msg.chatId, msg.circleId, f"'{key}' is not a valid asset.")
        return
    if asset_type == "colour":
        color_name = number.lower()
        if color_name not in COLOR_MAP:
            await client.send_chat_message(msg.chatId, msg.circleId, f"Available colours: {', '.join(COLOR_MAP.keys())}")
            return
        if color_name != "black" and not owns_asset(uid, color_name):
            await client.send_chat_message(msg.chatId, msg.circleId, f"You don't own the colour '{color_name}'. Buy it first with /buy colour {color_name}")
            return
        accounts[uid]["active_colour"] = color_name
        await save_accounts()
        await client.send_chat_message(msg.chatId, msg.circleId, f"Colour set to {color_name}.")
        await send_profile_card(msg.chatId, msg.circleId, uid)
        return
    if not owns_asset(uid, key):
        await client.send_chat_message(msg.chatId, msg.circleId, f"You don't own {asset_type} {number}. Buy it first with /buy {asset_type} {number}")
        return
    if asset_type == "bg":
        accounts[uid]["active_bg"] = key
    elif asset_type == "frame":
        accounts[uid]["active_frame"] = key
    elif asset_type == "bubble":
        accounts[uid]["active_bubble"] = key
    else:
        await client.send_chat_message(msg.chatId, msg.circleId, f"Unknown asset type '{asset_type}'.")
        return
    await save_accounts()
    await client.send_chat_message(msg.chatId, msg.circleId, f"Equipped {asset_type} {number}!")
    await send_profile_card(msg.chatId, msg.circleId, uid)


async def cmd_balance(msg, args):
    uid = msg.createdBy
    if is_banned(uid):
        return
    if not has_account(uid):
        await client.send_chat_message(msg.chatId, msg.circleId, "You need to /signup first.")
        return
    bal = get_balance(uid)
    await client.send_chat_message(msg.chatId, msg.circleId, f"Your sorex balance: {bal}")


async def cmd_set_bio(msg, args):
    uid = msg.createdBy
    if is_banned(uid):
        return
    if not has_account(uid):
        await client.send_chat_message(msg.chatId, msg.circleId, "You need to /signup first.")
        return
    if not args.strip():
        await client.send_chat_message(msg.chatId, msg.circleId, "Usage: /set bio <text>")
        return
    accounts[uid]["bio"] = args.strip()
    accounts[uid]["bio_removed"] = False
    await save_accounts()
    await client.send_chat_message(msg.chatId, msg.circleId, "Bio updated!")
    await send_profile_card(msg.chatId, msg.circleId, uid)


async def cmd_remove_bio(msg, args):
    uid = msg.createdBy
    if is_banned(uid):
        return
    if not has_account(uid):
        await client.send_chat_message(msg.chatId, msg.circleId, "You need to /signup first.")
        return
    accounts[uid]["bio"] = ""
    accounts[uid]["bio_removed"] = True
    await save_accounts()
    await client.send_chat_message(msg.chatId, msg.circleId, "Bio cleared.")
    await send_profile_card(msg.chatId, msg.circleId, uid)


async def cmd_set_name(msg, args):
    uid = msg.createdBy
    if is_banned(uid):
        return
    if not has_account(uid):
        await client.send_chat_message(msg.chatId, msg.circleId, "You need to /signup first.")
        return
    if not args.strip():
        await client.send_chat_message(msg.chatId, msg.circleId, "Usage: /set name <new name>")
        return
    accounts[uid]["name"] = args.strip()
    await save_accounts()
    _rebuild_nickname_index()
    await client.send_chat_message(msg.chatId, msg.circleId, f"Profile name updated to: {args.strip()}")
    await send_profile_card(msg.chatId, msg.circleId, uid)


# ── Owner commands ──────────────────────────────────────────────────────────

async def cmd_give(msg, args):
    if msg.createdBy != Config.SOR_ID:
        return
    parts = args.split()
    if len(parts) < 2 or parts[1].lower() != "sorex":
        await client.send_chat_message(msg.chatId, msg.circleId, "Usage: /give <amount> sorex (reply to target user)")
        return
    try:
        amount = int(parts[0])
    except ValueError:
        await client.send_chat_message(msg.chatId, msg.circleId, "Invalid amount.")
        return
    if not hasattr(msg, 'replyTo') or not msg.replyTo:
        await client.send_chat_message(msg.chatId, msg.circleId, "Reply to a user's message to give them sorex.")
        return
    target_id = msg.replyTo.createdBy if hasattr(msg.replyTo, 'createdBy') else str(msg.replyTo)
    if not has_account(target_id):
        await client.send_chat_message(msg.chatId, msg.circleId, "Target user does not have a profile.")
        return
    add_sorex(target_id, amount)
    await client.send_chat_message(msg.chatId, msg.circleId, f"Gave {amount} sorex to user.")


async def cmd_ban(msg, args):
    if msg.createdBy != Config.SOR_ID:
        return
    if not hasattr(msg, 'replyTo') or not msg.replyTo:
        await client.send_chat_message(msg.chatId, msg.circleId, "Reply to a user's message to ban them.")
        return
    target_id = msg.replyTo.createdBy if hasattr(msg.replyTo, 'createdBy') else str(msg.replyTo)
    reason = args.strip() or "No reason given"
    banned[target_id] = reason
    await save_banned()
    await client.send_chat_message(msg.chatId, msg.circleId, "User has been banned.")


async def cmd_unban(msg, args):
    if msg.createdBy != Config.SOR_ID:
        return
    if not hasattr(msg, 'replyTo') or not msg.replyTo:
        await client.send_chat_message(msg.chatId, msg.circleId, "Reply to a user's message to unban them.")
        return
    target_id = msg.replyTo.createdBy if hasattr(msg.replyTo, 'createdBy') else str(msg.replyTo)
    if target_id in banned:
        del banned[target_id]
        await save_banned()
        await client.send_chat_message(msg.chatId, msg.circleId, "User has been unbanned.")
    else:
        await client.send_chat_message(msg.chatId, msg.circleId, "User is not banned.")


# ══════════════════════════════════════════════════════════════════════════════
# 15.  MESSAGE ROUTER
# ══════════════════════════════════════════════════════════════════════════════
COMMAND_MAP = {
    "help":       cmd_help,
    "signup":     cmd_signup,
    "profile":    cmd_profile,
    "view":       cmd_view,
    "shop":       cmd_shop,
    "buy":        cmd_buy,
    "use":        cmd_use,
    "balance":    cmd_balance,
    "set":        None,
    "remove":     None,
    "give":       cmd_give,
    "ban":        cmd_ban,
    "unban":      cmd_unban,
}


async def _route_command(msg, body: str):
    """Parse and dispatch a command message."""
    body = body.strip()
    if not body.startswith("/"):
        return False
    tokens = body[1:].split(None, 1)
    cmd = tokens[0].lower() if tokens else ""
    args = tokens[1] if len(tokens) > 1 else ""

    # Sub-commands for /set and /remove
    if cmd == "set":
        sub_tokens = args.split(None, 1)
        sub = sub_tokens[0].lower() if sub_tokens else ""
        sub_args = sub_tokens[1] if len(sub_tokens) > 1 else ""
        if sub == "bio":
            await cmd_set_bio(msg, sub_args)
        elif sub == "name":
            await cmd_set_name(msg, sub_args)
        else:
            await client.send_chat_message(msg.chatId, msg.circleId, "Usage: /set bio <text>  or  /set name <name>")
        return True

    if cmd == "remove":
        sub = args.strip().lower()
        if sub == "bio":
            await cmd_remove_bio(msg, "")
        else:
            await client.send_chat_message(msg.chatId, msg.circleId, "Usage: /remove bio")
        return True

    handler = COMMAND_MAP.get(cmd)
    if handler is None:
        return False
    try:
        await handler(msg, args)
    except Exception as e:
        logger.exception(f"[cmd] /{cmd} failed: {e}")
        await client.send_chat_message(msg.chatId, msg.circleId, "Something went wrong. Please try again.")
    return True


# ══════════════════════════════════════════════════════════════════════════════
# 16.  GAME ROUTER
# ══════════════════════════════════════════════════════════════════════════════
_active_games: dict[str, Any] = {}

GAME_TRIGGERS = {
    "4 in a row":   "gameicon1",
    "bara alsalfa":  "gameicon2",
    "chess":        "gameicon3",
}


def _game_key(chat_id, uid1, uid2):
    players = sorted([uid1, uid2])
    return f"{chat_id}:{players[0]}:{players[1]}"


async def _route_game(msg, body: str):
    """Check if the message triggers a game challenge."""
    body_lower = body.lower().strip()
    game_name = None
    for trigger in GAME_TRIGGERS:
        if trigger in body_lower:
            game_name = trigger
            break
    if not game_name:
        return False

    uid = msg.createdBy
    if not has_account(uid):
        await client.send_chat_message(msg.chatId, msg.circleId, "You need to /signup before playing games.")
        return True

    if not hasattr(msg, 'replyTo') or not msg.replyTo:
        return True

    target_id = msg.replyTo.createdBy if hasattr(msg.replyTo, 'createdBy') else str(msg.replyTo)
    if target_id == uid:
        await client.send_chat_message(msg.chatId, msg.circleId, "You can't challenge yourself!")
        return True
    if not has_account(target_id):
        await client.send_chat_message(msg.chatId, msg.circleId, "Your opponent needs to /signup first.")
        return True

    key = _game_key(msg.chatId, uid, target_id)
    if key in _active_games:
        await client.send_chat_message(msg.chatId, msg.circleId, "A game between you two is already in progress!")
        return True

    GameClass = GAME_CLASSES.get(game_name)
    if not GameClass:
        await client.send_chat_message(msg.chatId, msg.circleId, f"'{game_name}' is not available yet.")
        return True

    game = GameClass(client, msg.chatId, msg.circleId, uid, target_id)
    _active_games[key] = game

    try:
        winner = await game.run()
        if winner:
            reward = get_reward(game_name)
            add_sorex(winner, reward)
            winner_name = accounts.get(winner, {}).get("name", "Unknown")
            await client.send_chat_message(msg.chatId, msg.circleId, f"{winner_name} wins! +{reward} sorex")
    except Exception as e:
        logger.exception(f"[game] {game_name} error: {e}")
    finally:
        _active_games.pop(key, None)

    return True


async def _check_game_cancel(msg, body: str):
    """Check if the message is a game cancellation request."""
    body_lower = body.lower().strip()
    if body_lower not in ("انهاء اللعبة", "end game"):
        return False
    uid = msg.createdBy
    for key, game in list(_active_games.items()):
        if hasattr(game, 'host_id') and game.host_id == uid:
            if hasattr(game, 'cancel'):
                game.cancel()
            del _active_games[key]
            await client.send_chat_message(msg.chatId, msg.circleId, "Game cancelled by host.")
            return True
        if hasattr(game, 'player1') and hasattr(game, 'player2'):
            if uid in (game.player1, game.player2):
                if hasattr(game, 'cancel'):
                    game.cancel()
                del _active_games[key]
                await client.send_chat_message(msg.chatId, msg.circleId, "Game cancelled.")
                return True
    await client.send_chat_message(msg.chatId, msg.circleId, "You don't have an active game to cancel.")
    return True


# ══════════════════════════════════════════════════════════════════════════════
# 17.  MAIN EVENT HANDLER
# ══════════════════════════════════════════════════════════════════════════════
client: Client | None = None


async def on_message(msg: ChatMessage):
    if not hasattr(msg, 'body') or not msg.body:
        return
    if not msg.chatId or not msg.circleId:
        return

    body = msg.body.strip()
    if not body:
        return

    # Deduplicate
    if await _dedup_message(msg):
        return

    # Update avatar URL if available
    uid = msg.createdBy
    if has_account(uid) and hasattr(msg, 'sender') and msg.sender:
        avatar_url = getattr(msg.sender, 'icon', '') or ''
        if avatar_url and accounts[uid].get("avatar_url") != avatar_url:
            accounts[uid]["avatar_url"] = avatar_url
            save_accounts_sync()

    # Route commands
    if body.startswith("/"):
        if await _dedup_command(uid, body.split()[0]):
            return
        await _route_command(msg, body)
        return

    # Route game triggers
    if await _route_game(msg, body):
        return

    # Check game cancellation
    if await _check_game_cancel(msg, body):
        return


# ══════════════════════════════════════════════════════════════════════════════
# 18.  MAIN ENTRYPOINT
# ══════════════════════════════════════════════════════════════════════════════
async def main():
    global client

    keep_alive()

    client = Client()
    await client.login(Config.EMAIL, Config.PASSWORD, Config.DEVICE_ID)
    client.on(EventType.CHAT_MESSAGE, on_message)

    await load_all()

    logger.info("Sorex Profile Bot started!")

    try:
        await client.connect()
    except Exception as e:
        logger.exception(f"[main] connection error: {e}")
    finally:
        await close_http()
        close_github()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")

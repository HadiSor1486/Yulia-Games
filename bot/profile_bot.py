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

# Greek-ish “lookalike” letters often used in fancy names
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
        base = _paste(base, user.get("active_frame", "framedefault"), L["frame"]["x"], L["frame"]["y"], L["frame"]["size"])
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
            {"content": f"{media.url}?wh={w}x{h}"},
            ChatMessageTypes.Photo,
            circle_id,
        )
    finally:
        with suppress(FileNotFoundError):
            os.remove(tmp)


async def send_profile_card(uid, chat_id, circle_id):
    card = await render_profile_card(uid)
    if card:
        await send_image_bytes(chat_id, circle_id, card)
    else:
        await client.send_message(
            chat_id,
            "⚠️ Could not render the profile card. Check that template.png and fonts are present.",
            circle_id,
        )


# ══════════════════════════════════════════════════════════════════════════════
# WELCOME CARD

async def create_welcome_image(profile_img: Image.Image, nickname: str):
    try:
        base = Image.open(Config.WELCOME_BACKGROUND).convert("RGBA")

        # ── Paste profile picture ──
        pfp = _circular(profile_img.copy(), Config.WELCOME_PROFILE_SIZE)
        x, y = Config.WELCOME_PROFILE_POSITION
        tmp = Image.new("RGBA", base.size, (0, 0, 0, 0))
        tmp.paste(pfp, (x, y), pfp)
        base = Image.alpha_composite(base, tmp)

        # ── Draw nickname (gothic gray style, perfectly centered) ──
        prepared = prepare_text(normalize_display_name(nickname))
        if prepared:
            font = _font(nickname, Config.WELCOME_NAME_SIZE)
            draw = ImageDraw.Draw(base)

            # Measure actual ink bounding box
            bb = font.getbbox(prepared)
            ink_left, ink_top, ink_right, ink_bottom = bb
            text_width = ink_right - ink_left
            text_height = ink_bottom - ink_top

            # Center the INK at the configured point
            draw_x = Config.WELCOME_NAME_X - text_width // 2 - ink_left
            draw_y = Config.WELCOME_NAME_Y - text_height // 2 - ink_top

            # Gothic gray: subtle dark drop shadow then silver-gray text
            off = Config.WELCOME_NAME_SHADOW_OFF
            draw.text(
                (draw_x + off, draw_y + off),
                prepared, font=font, fill=Config.WELCOME_NAME_SHADOW,
            )
            draw.text(
                (draw_x, draw_y),
                prepared, font=font, fill=Config.WELCOME_NAME_FILL,
            )

        out = BytesIO()
        base.convert("RGB").save(out, "JPEG", quality=Config.OUTPUT_QUALITY)
        out.seek(0)
        return out
    except Exception as e:
        logger.exception(f"[welcome_img] {e}")
        return None


async def _send_welcome_bytes(chat_id, circle_id, card) -> bool:
    """Send welcome image. Returns True on success, False on failure."""
    try:
        await send_image_bytes(chat_id, circle_id, card)
        return True
    except Exception as e:
        logger.warning(f"[welcome] failed to send image: {e}")
        return False


# 13.  SIGNUP STATE MACHINE
# ══════════════════════════════════════════════════════════════════════════════
signup_states: dict[str, dict] = {}


async def handle_signup(message):
    uid = message.author.userId
    cid = message.chatId
    circ = message.circleId
    if has_account(uid):
        await client.send_message(
            cid,
            f"✅ You already have a profile, {accounts[uid]['name']}!\nUse /profile to view it.",
            circ,
        )
        return
    if uid in signup_states:
        step = signup_states[uid]["step"]
        await client.send_message(
            cid,
            f"⏳ Signup in progress! Reply with:  {'Name: YourName' if step == 'name' else 'Bio: your bio'}",
            circ,
        )
        return
    signup_states[uid] = {"step": "name"}
    await client.send_message(
        cid,
        "👋 Welcome! Let's create your profile.\n\n❓ *What is your name?*\nReply with:   Name: YourName",
        circ,
    )


async def handle_signup_step(message, content) -> bool:
    uid = message.author.userId
    if uid not in signup_states:
        return False
    state = signup_states[uid]
    cid = message.chatId
    circ = message.circleId
    if state["step"] == "name":
        if not content.lower().startswith("name:"):
            return False
        name = content[5:].strip()
        if not name:
            await client.send_message(cid, "⚠️ Name can't be empty.", circ)
            return True
        if len(name) > 30:
            await client.send_message(cid, f"⚠️ Name too long ({len(name)}). Max 30.", circ)
            return True
        state["temp_name"] = name
        state["step"] = "bio"
        await client.send_message(
            cid,
            f"✅ Name: *{name}*\n\n📝 *Put a bio that describes you.*\nReply with:   Bio: your bio here",
            circ,
        )
        return True
    if state["step"] == "bio":
        if not content.lower().startswith("bio:"):
            return False
        bio = content[4:].strip()
        if len(bio) > 100:
            await client.send_message(cid, f"⚠️ Bio too long ({len(bio)}). Max 100.", circ)
            return True

        # Capture the user's actual Kyodo nickname (normalized) at signup time
        kyodo_nick = normalize_text(getattr(message.author, "nickname", ""))
        av = getattr(message.author, "avatar_url", None) or ""

        accounts[uid] = _default_account(state["temp_name"], bio, kyodo_nick, av)
        del signup_states[uid]
        _rebuild_nickname_index()
        await save_accounts()
        await client.send_message(
            cid,
            f"🎉 Profile created!\n━━━━━━━━━━━━━━━━━━\n"
            f"👤 {accounts[uid]['name']}\n📝 {bio}\n💰 0 sorex\n━━━━━━━━━━━━━━━━━━\nUse /profile to view your card!",
            circ,
        )
        return True
    return False


# ══════════════════════════════════════════════════════════════════════════════
# 14.  COMMAND HANDLERS
# ══════════════════════════════════════════════════════════════════════════════
async def cmd_profile(msg):
    try:
        uid = msg.author.userId
        cid = msg.chatId
        circ = msg.circleId
        if not has_account(uid):
            await client.send_message(cid, "❌ No profile yet. Use /signup!", circ)
            return
        await send_profile_card(uid, cid, circ)
    except Exception:
        logger.exception("[cmd_profile]")


async def cmd_view(msg, target_name):
    """View a profile by name OR Kyodo nickname. Handles Arabic & fancy fonts."""
    try:
        cid = msg.chatId
        circ = msg.circleId

        uid = resolve_user(target_name)
        if not uid:
            await client.send_message(cid, f"❌ No profile found for *{target_name}*.", circ)
            return
        await send_profile_card(uid, cid, circ)
    except Exception:
        logger.exception("[cmd_view]")


async def cmd_buy(msg, args):
    try:
        uid = msg.author.userId
        cid = msg.chatId
        circ = msg.circleId
        if not has_account(uid):
            await client.send_message(cid, "❌ Use /signup first!", circ)
            return
        parts = args.split()
        if len(parts) < 2:
            await client.send_message(
                cid,
                "Usage:\n  /buy frame 1\n  /buy bubble 2\n  /buy bg 3\n  /buy colour white",
                circ,
            )
            return
        cat = parts[0].lower()
        item = parts[1].lower()
        if cat not in ("frame", "bubble", "bg", "colour"):
            await client.send_message(cid, "❌ Categories: frame / bubble / bg / colour", circ)
            return
        key = item if cat == "colour" else cat + item
        if key not in ASSETS:
            await client.send_message(cid, f"❌ Asset *{key}* doesn't exist.", circ)
            return
        price = get_price(key)
        if price == 0:
            await client.send_message(cid, f"✅ *{key}* is free! Use /use to equip it.", circ)
            return
        if owns_asset(uid, key):
            await client.send_message(cid, f"✅ You already own *{key}*. Use /use.", circ)
            return
        bal = get_balance(uid)
        if bal < price:
            await client.send_message(cid, f"❌ Not enough sorex!\n   Cost: {price}  |  Balance: {bal}", circ)
            return
        deduct_sorex(uid, price)
        grant_asset(uid, key)
        await client.send_message(
            cid,
            f"✅ Purchased *{key}* for {price} sorex!\n💰 Balance: {get_balance(uid)} sorex\nUse /use {cat} {item} to equip it.",
            circ,
        )
    except Exception:
        logger.exception("[cmd_buy]")


async def cmd_use(msg, args):
    try:
        uid = msg.author.userId
        cid = msg.chatId
        circ = msg.circleId
        if not has_account(uid):
            await client.send_message(cid, "❌ Use /signup first!", circ)
            return
        parts = args.split()
        if len(parts) < 2:
            await client.send_message(
                cid,
                "Usage:\n  /use frame 1\n  /use bubble 2\n  /use bg 3\n  /use colour white",
                circ,
            )
            return
        cat = parts[0].lower()
        item = parts[1].lower()
        if cat not in ("frame", "bubble", "bg", "colour"):
            await client.send_message(cid, "❌ Categories: frame / bubble / bg / colour", circ)
            return
        key = item if cat == "colour" else cat + item

        if cat == "colour":
            if key not in COLOR_MAP:
                await client.send_message(
                    cid,
                    f"❌ Unknown colour. Options: {', '.join(COLOR_MAP.keys())}",
                    circ,
                )
                return
            if key != "black" and not owns_asset(uid, key):
                await client.send_message(
                    cid,
                    f"❌ You don't own colour *{key}*.\nBuy it:  /buy colour {key}  ({get_price(key)} sorex)",
                    circ,
                )
                return
            accounts[uid]["active_colour"] = key
            await save_accounts()
            await client.send_message(cid, f"🎨 Bio colour changed to *{key}*!", circ)
            await send_profile_card(uid, cid, circ)
            return

        if key not in ASSETS:
            await client.send_message(cid, f"❌ Asset *{key}* doesn't exist.", circ)
            return
        if get_price(key) == 0 or owns_asset(uid, key):
            accounts[uid][f"active_{cat}"] = key
            await save_accounts()
            await client.send_message(cid, f"✅ Equipped *{key}*!", circ)
            await send_profile_card(uid, cid, circ)
        else:
            await client.send_message(
                cid,
                f"❌ You don't own *{key}*.\nBuy it:  /buy {cat} {item}  ({get_price(key)} sorex)",
                circ,
            )
    except Exception:
        logger.exception("[cmd_use]")


async def cmd_remove_bio(msg):
    try:
        uid = msg.author.userId
        cid = msg.chatId
        circ = msg.circleId
        if not has_account(uid):
            await client.send_message(cid, "❌ Use /signup first!", circ)
            return
        accounts[uid]["bio_removed"] = True
        accounts[uid]["bio"] = ""
        await save_accounts()
        await client.send_message(
            cid,
            "🗑️ Bio removed. Bubble will show *Empty*.\nAdd one:  /set bio your text",
            circ,
        )
    except Exception:
        logger.exception("[cmd_remove_bio]")


async def cmd_set_bio(msg, bio):
    try:
        uid = msg.author.userId
        cid = msg.chatId
        circ = msg.circleId
        if not has_account(uid):
            await client.send_message(cid, "❌ Use /signup first!", circ)
            return
        if len(bio) > 100:
            await client.send_message(cid, f"⚠️ Too long ({len(bio)}). Max 100.", circ)
            return
        accounts[uid]["bio"] = bio
        accounts[uid]["bio_removed"] = False
        await save_accounts()
        await client.send_message(cid, f"✅ Bio updated: *{bio}*", circ)
    except Exception:
        logger.exception("[cmd_set_bio]")


async def cmd_set_name(msg, new_name):
    """
    Change the profile name. Also re-captures the user's current
    Kyodo nickname (normalized) and stores it alongside.
    """
    try:
        uid = msg.author.userId
        cid = msg.chatId
        circ = msg.circleId
        if not has_account(uid):
            await client.send_message(cid, "❌ Use /signup first!", circ)
            return
        new_name = new_name.strip()
        if not new_name:
            await client.send_message(cid, "⚠️ Name can't be empty.", circ)
            return
        if len(new_name) > 30:
            await client.send_message(cid, f"⚠️ Name too long ({len(new_name)}). Max 30.", circ)
            return

        old_name = accounts[uid]["name"]
        accounts[uid]["name"] = new_name

        # Re-capture the current Kyodo nickname (normalized)
        current_kyodo_nick = normalize_text(getattr(msg.author, "nickname", ""))
        if current_kyodo_nick:
            accounts[uid]["nickname"] = current_kyodo_nick

        _rebuild_nickname_index()
        await save_accounts()
        await client.send_message(cid, f"✅ Name changed: *{old_name}* → *{new_name}*", circ)
    except Exception:
        logger.exception("[cmd_set_name]")


async def cmd_balance(msg):
    try:
        uid = msg.author.userId
        cid = msg.chatId
        circ = msg.circleId
        if not has_account(uid):
            await client.send_message(cid, "❌ Use /signup first!", circ)
            return
        u = accounts[uid]
        rank = get_rank(u.get("wins", 0))
        rs = f"  |  Rank: {rank}" if rank else ""
        await client.send_message(
            cid,
            f"💰 *{u['name']}'s Balance*\n━━━━━━━━━━━━━━━━━━\n"
            f"Sorex:  {u.get('wealth', 0)}\nWins:   {u.get('wins', 0)}{rs}\nGames:  {u.get('games_played', 0)}",
            circ,
        )
    except Exception:
        logger.exception("[cmd_balance]")


async def cmd_shop(msg):
    try:
        cid = msg.chatId
        circ = msg.circleId
        await client.send_message(cid, "https://silent-hill-shop.netlify.app", circ)
    except Exception:
        logger.exception("[cmd_shop]")


async def cmd_give(msg, args):
    try:
        uid = msg.author.userId
        cid = msg.chatId
        circ = msg.circleId
        if uid != Config.SOR_ID:
            return
        parts = args.lower().split()
        if not parts or not parts[0].isdigit():
            await client.send_message(cid, "Usage: reply to someone and say  /give <amount> sorex", circ)
            return
        amount = int(parts[0])
        ro = (getattr(msg, "replyMessage", None) or getattr(msg, "replyTo", None) or getattr(msg, "reply", None))
        if not ro:
            await client.send_message(cid, "⚠️ Reply to someone's message first.", circ)
            return
        auth = getattr(ro, "author", None)
        tuid = getattr(auth, "userId", None) if auth else None
        tnick = getattr(auth, "nickname", None) if auth else None
        if not tuid:
            tuid = getattr(ro, "userId", None) or getattr(ro, "uid", None)
        if not tuid:
            await client.send_message(cid, "⚠️ Could not identify the user.", circ)
            return
        if not has_account(tuid):
            await client.send_message(cid, "❌ That user has no profile.", circ)
            return
        add_sorex(tuid, amount)
        name = accounts[tuid]["name"]
        await client.send_message(
            cid,
            f"💰 Transferred {amount} sorex to *{name}*!\nNew balance: {get_balance(tuid)} sorex",
            circ,
        )
    except Exception:
        logger.exception("[cmd_give]")


async def _reply_uid_nick(msg):
    ro = getattr(msg, "replyMessage", None) or getattr(msg, "replyTo", None) or getattr(msg, "reply", None)
    if not ro:
        return None, None
    auth = getattr(ro, "author", None)
    uid = getattr(auth, "userId", None) if auth else None
    nick = getattr(auth, "nickname", None) if auth else None
    if not uid:
        uid = getattr(ro, "userId", None) or getattr(ro, "uid", None)
    return uid, nick


async def cmd_ban(msg):
    try:
        uid = msg.author.userId
        cid = msg.chatId
        circ = msg.circleId
        if uid != Config.SOR_ID:
            return
        tuid, tnick = await _reply_uid_nick(msg)
        if not tuid:
            await client.send_message(cid, "⚠️ Reply to someone's message first.", circ)
            return
        if tuid == Config.SOR_ID:
            await client.send_message(cid, "😅 Can't ban the owner!", circ)
            return
        banned[tuid] = tnick or "Unknown"
        await save_banned()
        await client.send_message(cid, f"🔨 *{tnick or tuid}* has been banned.", circ)
    except Exception:
        logger.exception("[cmd_ban]")


async def cmd_unban(msg):
    try:
        uid = msg.author.userId
        cid = msg.chatId
        circ = msg.circleId
        if uid != Config.SOR_ID:
            return
        tuid, tnick = await _reply_uid_nick(msg)
        if not tuid:
            await client.send_message(cid, "⚠️ Reply to someone's message first.", circ)
            return
        if tuid in banned:
            nick = banned.pop(tuid)
            await save_banned()
            await client.send_message(cid, f"✅ *{nick}* has been unbanned.", circ)
        else:
            await client.send_message(cid, f"⚠️ {tnick or tuid} is not banned.", circ)
    except Exception:
        logger.exception("[cmd_unban]")


async def cmd_help(msg):
    try:
        cid = msg.chatId
        circ = msg.circleId
        await client.send_message(
            cid,
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "👤 Profile\n"
            "/signup  /profile  /view <name>  /balance  /shop\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "🛒 Economy\n"
            "/buy frame 1  /buy bubble 2  /buy bg 3  /buy colour white\n"
            "/use frame 1  /use colour white  ← shows card automatically\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "📝 Bio\n"
            "/set bio <text>   /set name <new name>   /remove bio\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "🎮 Games\n"
            "Reply to someone:  4 in a row\n"
            "Or just type:      لعبة برا السالفة   ← open lobby (no reply needed)\n"
            "انهاء اللعبة  — end game (host only)\n"
            "━━━━━━━━━━━━━━━━━━━━━",
            circ,
        )
    except Exception:
        logger.exception("[cmd_help]")


# ══════════════════════════════════════════════════════════════════════════════
# 15.  KYODO CLIENT + GAME PLUGIN SETUP
# ══════════════════════════════════════════════════════════════════════════════
client = Client(deviceId=Config.DEVICE_ID)

_economy = {
    "has_account":       has_account,
    "add_sorex":         add_sorex,
    "update_game_stats": update_game_stats,
    "get_rank":          get_rank,
    "get_reward":        get_reward,
    "get_account_wins":  lambda uid: accounts.get(uid, {}).get("wins", 0),
}

game_instances = [G(client, Config, _economy) for G in GAME_CLASSES]


def _active_game():
    return next((g for g in game_instances if g.is_active), None)


def _triggered_game(content):
    return next((g for g in game_instances if g.is_trigger(content)), None)


# ══════════════════════════════════════════════════════════════════════════════
# 16.  EVENT HANDLERS
# ══════════════════════════════════════════════════════════════════════════════
@client.event(EventType.ChatMemberJoin)
async def on_join(message: ChatMessage):
    try:
        nickname = normalize_display_name(message.author.nickname)
        avatar_url = getattr(message.author, "avatar_url", None) or ""
        sent = False

        if avatar_url:
            raw = await _dl(avatar_url)
            if raw:
                try:
                    profile_img = Image.open(BytesIO(raw)).convert("RGBA")
                    card = await create_welcome_image(profile_img, nickname)
                    if card:
                        sent = await _send_welcome_bytes(message.chatId, message.circleId, card)
                        if sent:
                            logger.info(f"[welcome] sent welcome card for {nickname}")
                except Exception as e:
                    logger.warning(f"[welcome] image creation failed: {e}")

        if not sent:
            await client.send_message(
                message.chatId,
                f"🌫️ Welcome To Silent Hill, {nickname}!",
                message.circleId,
            )
            logger.info(f"[welcome] sent text fallback for {nickname}")
    except Exception as e:
        logger.exception(f"[on_join] {e}")


@client.middleware(EventType.ChatMessage)
async def _self_filter(message: ChatMessage):
    if message.author.userId == client.userId:
        return False


@client.event(EventType.ChatMessage)
async def on_message(message: ChatMessage):
    try:
        # ── Layer 1: Message dedup ────────────────────────────────────────
        if await _dedup_message(message):
            return

        content = (message.content or "").strip()
        uid = message.author.userId
        nickname = message.author.nickname
        av = getattr(message.author, "avatar_url", None) or ""
        chat_id = message.chatId
        circle_id = message.circleId

        if not content or chat_id != Config.CHAT_ID:
            return

        # Silently sync avatar
        if av and uid in accounts and accounts[uid].get("avatar_url") != av:
            accounts[uid]["avatar_url"] = av

        if is_banned(uid):
            return

        cl = content.lower()

        # ── SIGNUP ─────────────────────────────────────────────────────────
        if uid in signup_states:
            if await handle_signup_step(message, content):
                return

        # ── Layer 2: Command cooldown (ONE check, guards all commands) ────
        cmd_key = cl.split()[0] if cl.startswith("/") else cl
        on_cooldown = await _dedup_command(uid, cmd_key)

        # ── USER COMMANDS ──────────────────────────────────────────────────
        if cl == "/signup":
            await handle_signup(message)
            return

        if cl in ("/profile", "/card"):
            if on_cooldown:
                return
            await cmd_profile(message)
            return

        if cl.startswith("/view "):
            if on_cooldown:
                return
            await cmd_view(message, content[6:].strip())
            return

        if cl.startswith("/buy "):
            if on_cooldown:
                return
            await cmd_buy(message, content[5:].strip())
            return

        if cl.startswith("/use "):
            if on_cooldown:
                return
            await cmd_use(message, content[5:].strip())
            return

        if cl == "/remove bio":
            if on_cooldown:
                return
            await cmd_remove_bio(message)
            return

        if cl.startswith("/set bio "):
            if on_cooldown:
                return
            await cmd_set_bio(message, content[9:].strip())
            return

        if cl.startswith("/set name "):
            if on_cooldown:
                return
            await cmd_set_name(message, content[10:].strip())
            return

        if cl in ("/balance", "/sorex", "/coins"):
            if on_cooldown:
                return
            await cmd_balance(message)
            return

        if cl in ("/shop", "/store", "/assets"):
            if on_cooldown:
                return
            await cmd_shop(message)
            return

        if cl in ("/help", "/commands"):
            if on_cooldown:
                return
            await cmd_help(message)
            return

        # ── OWNER COMMANDS ─────────────────────────────────────────────────
        if uid == Config.SOR_ID:
            if cl.startswith("/give "):
                if on_cooldown:
                    return
                await cmd_give(message, content[6:].strip())
                return
            if cl == "/ban":
                if on_cooldown:
                    return
                await cmd_ban(message)
                return
            if cl == "/unban":
                if on_cooldown:
                    return
                await cmd_unban(message)
                return

        # ══ GAME ROUTING ════════════════════════════════════════════════════
        active = _active_game()

        if content in ("انهاء اللعبة",) or cl == "end game":
            if active:
                await active.force_end(uid, nickname, chat_id, circle_id)
            return

        if active:
            if await active.handle_message(uid, nickname, content, chat_id, circle_id):
                return

        triggered = _triggered_game(content)
        if triggered:
            if active:
                await client.send_message(
                    chat_id,
                    "⚠️ هناك لعبة شغالة بالفعل.\nالمضيف يكتب *انهاء اللعبة* أولاً.",
                    circle_id,
                    reply_message_id=message.messageId,
                )
                return

            if not getattr(triggered, 'NEEDS_REPLY', True):
                await triggered.start_challenge(uid, nickname, None, None, chat_id, circle_id)
                return

            ro = (getattr(message, "replyMessage", None)
                  or getattr(message, "replyTo", None)
                  or getattr(message, "reply", None))
            if not ro:
                return

            auth = getattr(ro, "author", None)
            opp_id = getattr(auth, "userId", None) if auth else None
            opp_nick = getattr(auth, "nickname", None) if auth else None
            if not opp_id:
                opp_id = getattr(ro, "userId", None) or getattr(ro, "uid", None)
                opp_nick = getattr(ro, "nickname", None)

            if not opp_id or opp_id == uid:
                return

            await triggered.start_challenge(uid, nickname, opp_id, opp_nick, chat_id, circle_id)

    except Exception:
        logger.exception("[on_message] unhandled")


# ══════════════════════════════════════════════════════════════════════════════
# 17.  MAIN
# ══════════════════════════════════════════════════════════════════════════════
_stop = False


def _shutdown(sig, frame):
    global _stop
    logger.info(f"Signal {sig}")
    _stop = True


signal.signal(signal.SIGTERM, _shutdown)
signal.signal(signal.SIGINT, _shutdown)


async def _cleanup_client():
    for attr in ('disconnect', 'close', 'stop'):
        try:
            fn = getattr(client, attr, None)
            if fn and callable(fn):
                await fn()
        except Exception:
            pass
    await asyncio.sleep(1)


def _jittered_backoff(base, attempt):
    delay = min(base * (2 ** attempt), Config.RESTART_BACKOFF_MAX)
    jitter = delay * 0.3 * (2 * __import__('random').random() - 1)
    return max(Config.RESTART_BACKOFF_MIN, delay + jitter)


async def main():
    global _stop
    await load_all()

    backoff = Config.RESTART_BACKOFF_MIN
    try:
        while not _stop:
            start = time.time()
            try:
                logger.info("Logging in…")
                await client.login(Config.EMAIL, Config.PASSWORD)
                logger.success("✅ Logged in — Profile Bot is alive!")
                await client.socket_wait()
                logger.warning("[main] session ended")
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as e:
                logger.exception(f"[main] {e}")
            if _stop:
                break
            await _cleanup_client()
            session_duration = time.time() - start
            if session_duration > 300:
                backoff = Config.RESTART_BACKOFF_MIN
            else:
                backoff = _jittered_backoff(Config.RESTART_BACKOFF_MIN, 0 if backoff <= Config.RESTART_BACKOFF_MIN else 1)
            logger.info(f"[main] restart in {backoff:.1f}s…")
            slept = 0.0
            while slept < backoff and not _stop:
                await asyncio.sleep(1.0)
                slept += 1.0
    finally:
        _save_dedup()
        await close_http()
        await close_github()


if __name__ == "__main__":
    while True:
        keep_alive()
        try:
            asyncio.run(main())
        except (KeyboardInterrupt, SystemExit):
            break
        except Exception as e:
            logger.error(f"[top] {e}")
            time.sleep(5)


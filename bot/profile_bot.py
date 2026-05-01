"""
╔══════════════════════════════════════════════════════════════════════════╗
║                     SOREX PROFILE BOT  — Kyodo                          ║
║                                                                          ║
║  USER COMMANDS                                                           ║
║   /signup          — create your profile (step-by-step)                 ║
║   /profile         — view your own profile card                         ║
║   /view <name>     — view another player's profile card                 ║
║   /buy <type> <n>  — buy an asset  (frame 1 / bubble 2 / bg 3 /         ║
║                                     colour white)                       ║
║   /use <type> <n>  — equip a purchased asset (card shown automatically) ║
║   /remove bio      — clear your bio (shows "Empty")                     ║
║   /set bio <text>  — set or update your bio                             ║
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
║   yulia scan       — refresh member list                                ║
║   yulia welcome <name> — manual welcome card                            ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import asyncio
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(file)))
import signal
import tempfile
import time
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
    ASSETS, COLOR_MAP,
    get_price, get_reward,
)
from .games import GAME_CLASSES   # all registered game plugins

with suppress(ImportError):
    import uvloop
    uvloop.install()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def _p(*parts):
    """Resolve a path relative to this script's folder."""
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

    # Profile card
    TEMPLATE_PATH    = _p("template.png")
    FONT_LATIN       = _p("font1.ttf")
    FONT_ARABIC      = _p("fontar.ttf")
    OUTPUT_QUALITY   = 95
    MAX_OUTPUT_WIDTH = 1100

    # Welcome card
    WELCOME_BG               = _p("shbg.jpg")
    WELCOME_PROFILE_SIZE     = (303, 303)
    WELCOME_PROFILE_POSITION = (389, 291)

    # Storage
    ACCOUNTS_FILE = _p("accounts.json")
    BANNED_FILE   = _p("banned.json")
    MEMBERS_FILE  = _p("members.json")
    LOG_FILE      = _p("profile_bot.log")

    # Connect Four timeouts (read by the FourInARow plugin)
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
# 3.  ARABIC TEXT RESHAPING  (no external deps)
# ══════════════════════════════════════════════════════════════════════════════
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

def prepare_text(t): return reshape_arabic(t) if is_arabic(t) else t


# ══════════════════════════════════════════════════════════════════════════════
# 4.  NETWORK
# ══════════════════════════════════════════════════════════════════════════════
_http: httpx.AsyncClient | None = None

async def http() -> httpx.AsyncClient:
    global _http
    if _http is None or _http.is_closed:
        _http = httpx.AsyncClient(
            timeout=httpx.Timeout(Config.HTTP_TIMEOUT),
            headers={"User-Agent": "SorexBot/2.0"},
            follow_redirects=True,
            limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
        )
    return _http

async def close_http():
    global _http
    if _http and not _http.is_closed: await _http.aclose()


# ══════════════════════════════════════════════════════════════════════════════
# 5.  JSON HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def json_read(path: str, default: Any = None) -> Any:
    if default is None: default = {}
    try:
        if not os.path.exists(path): json_write(path, default); return default
        with open(path,"r",encoding="utf-8") as f: return _json.load(f)
    except Exception as e: logger.error(f"[json] read {path}: {e}"); return default

def json_write(path: str, data: Any):
    d=os.path.dirname(os.path.abspath(path)) or "."; os.makedirs(d, exist_ok=True)
    fd,tmp=tempfile.mkstemp(prefix=".tmp_",dir=d)
    try:
        with os.fdopen(fd,"w",encoding="utf-8") as f: _json.dump(data,f,indent=2,ensure_ascii=False)
        os.replace(tmp,path)
    except Exception:
        with suppress(FileNotFoundError): os.remove(tmp)
        raise


# ══════════════════════════════════════════════════════════════════════════════
# 6.  DATA STORES
# ══════════════════════════════════════════════════════════════════════════════
accounts: dict[str, dict] = {}
banned:   dict[str, str]  = {}
members:  dict[str, dict] = {}

def _default_account(name, bio, avatar_url=""):
    return {
        "name": name, "bio": bio, "bio_removed": False,
        "games_played": 0, "wins": 0, "wealth": 0,
        "owned_assets": ["bgdefault","framedefault","bubbledefault"],
        "active_bg": "bgdefault", "active_frame": "framedefault",
        "active_bubble": "bubbledefault", "active_colour": "black",
        "game_stats": {}, "avatar_url": avatar_url,
    }

def load_all():
    global accounts, banned, members
    accounts = json_read(Config.ACCOUNTS_FILE, {})
    banned   = json_read(Config.BANNED_FILE,   {})
    members  = json_read(Config.MEMBERS_FILE,  {})

def save_accounts():
    try: json_write(Config.ACCOUNTS_FILE, accounts)
    except Exception as e: logger.exception(f"[accounts] {e}")

def save_banned():
    try: json_write(Config.BANNED_FILE, banned)
    except Exception as e: logger.exception(f"[banned] {e}")

def save_members():
    try: json_write(Config.MEMBERS_FILE, members)
    except Exception as e: logger.exception(f"[members] {e}")

async def scan_members():
    found={}; pt=None
    AV=["icon","avatarUrl","avatar_url","avatar","profileImage","profilePicture",
        "profileImg","photo","image","iconUrl","picture","thumbnail"]
    try:
        while True:
            res=await client.get_chat_users(Config.CHAT_ID,Config.CIRCLE_ID,pageToken=pt)
            for entry in res.data.get("chatMemberList",[]):
                user=entry.get("user",{}); uid=entry.get("uid") or user.get("uid"); nick=user.get("nickname")
                av=""
                for f in AV:
                    v=user.get(f) or entry.get(f)
                    if v and isinstance(v,str) and v.startswith("http"): av=v; break
                if uid and nick:
                    key=nick.lower()
                    found[key]={"nickname":nick,"userId":uid,"avatar_url":av or members.get(key,{}).get("avatar_url","")}
                    if uid in accounts and av: accounts[uid]["avatar_url"]=av; save_accounts()
            pt=res.pagination.get("fwd") if res.pagination else None
            if not pt: break
        for k in [k for k in members if k not in found]: del members[k]
        members.update(found); save_members(); save_accounts()
        logger.info(f"[members] {len(members)} members")
    except Exception as e: logger.exception(f"[scan] {e}")


# ══════════════════════════════════════════════════════════════════════════════
# 7.  ECONOMY HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def has_account(uid): return uid in accounts
def is_banned(uid):   return uid in banned
def get_balance(uid): return accounts.get(uid,{}).get("wealth",0)

def add_sorex(uid, amount):
    if uid in accounts: accounts[uid]["wealth"]=accounts[uid].get("wealth",0)+amount; save_accounts()

def deduct_sorex(uid, amount):
    if uid in accounts and accounts[uid].get("wealth",0)>=amount:
        accounts[uid]["wealth"]-=amount; save_accounts(); return True
    return False

def owns_asset(uid, key): return key in accounts.get(uid,{}).get("owned_assets",[])

def grant_asset(uid, key):
    if uid in accounts and key not in accounts[uid]["owned_assets"]:
        accounts[uid]["owned_assets"].append(key); save_accounts()


# ══════════════════════════════════════════════════════════════════════════════
# 8.  RANK SYSTEM
# ══════════════════════════════════════════════════════════════════════════════
def get_rank(wins):
    if wins==0:  return ""
    if wins<=5:  return "Beginner"
    if wins<=10: return "Apprentice"
    if wins<=20: return "Advanced"
    if wins<=30: return "Expert"
    if wins<=40: return "Elite"
    if wins<=50: return "Master"
    if wins<=60: return "Grandmaster"
    if wins<=79: return "Legend"
    return "Mythic"

def get_primary_game(uid):
    from assets import GAME_MAP
    stats=accounts.get(uid,{}).get("game_stats",{})
    if not stats: return None,None
    primary=max(stats,key=lambda g:stats[g].get("plays",0))
    if stats[primary].get("plays",0)==0: return None,None
    icon=next((ik for gn,ik in GAME_MAP.items() if gn.lower()==primary.lower()),None)
    return primary,icon

def update_game_stats(uid, game_name, won):
    if uid not in accounts: return
    gs=accounts[uid].setdefault("game_stats",{})
    e=gs.setdefault(game_name,{"plays":0,"wins":0})
    e["plays"]+=1; e["wins"]+=(1 if won else 0)
    accounts[uid]["games_played"]=accounts[uid].get("games_played",0)+1
    if won: accounts[uid]["wins"]=accounts[uid].get("wins",0)+1
    save_accounts()


# ══════════════════════════════════════════════════════════════════════════════
# 9.  PROFILE CARD LAYOUT  (pixel positions — matches template.png)
# ══════════════════════════════════════════════════════════════════════════════
LAYOUT = {
    "bg":       {"x":-15,  "y":-15,  "size":1040},
    "pfp":      {"x":377,  "y":250,  "size":268 },
    "frame":    {"x":357,  "y":230,  "size":312 },
    "bubble":   {"x":160,  "y":600,  "size":700 },
    "gameicon": {"x":280,  "y":1130, "size":120 },
    "name":     {"cx":511, "y":540,  "size":50,  "color":(255,255,255,255)},
    "bio":      {"cx":510, "cy":720, "size":34,  "color":(30,30,30,255),
                 "max_width":480,"max_lines":2,"line_gap":10},
    "games":    {"cx":183, "y":935,  "size":50,  "color":(255,255,255,255)},
    "wins":     {"cx":511, "y":935,  "size":50,  "color":(255,255,255,255)},
    "wealth":   {"cx":770, "y":935,  "size":50,  "color":(255,255,255,255)},
    "game":     {"x":430,  "y":1150, "size":46,  "color":(255,255,255,255)},
    "rank":     {"x":430,  "y":1208, "size":38,  "color":(200,200,200,255)},
    "nogame":   {"cx":511, "y":1180, "size":40,  "color":(160,160,160,255)},
}


# ══════════════════════════════════════════════════════════════════════════════
# 10.  PROFILE CARD RENDERING
# ══════════════════════════════════════════════════════════════════════════════
def _font(text, size):
    p=Config.FONT_ARABIC if is_arabic(text) else Config.FONT_LATIN
    for q in (p,Config.FONT_LATIN):
        try: return ImageFont.truetype(q,size)
        except: pass
    return ImageFont.load_default()

def _cx(draw, text, cx, y, font, color):
    t=prepare_text(text); bb=font.getbbox(t); w=bb[2]-bb[0]; sh=bb[1] if bb[1]>3 else 0
    draw.text((cx-w//2,y-sh),t,font=font,fill=color)

def _lx(draw, text, x, y, font, color):
    t=prepare_text(text); bb=font.getbbox(t); sh=bb[1] if bb[1]>3 else 0
    draw.text((x,y-sh),t,font=font,fill=color)

def _wrap(text, font, mw, ml):
    words,lines,cur=text.split(),[],""
    for w in words:
        test=(cur+" "+w).strip()
        if font.getbbox(prepare_text(test))[2]-font.getbbox(prepare_text(test))[0]<=mw: cur=test
        else:
            if cur: lines.append(cur)
            cur=w
            if len(lines)>=ml: cur=""; break
    if cur and len(lines)<ml: lines.append(cur)
    lines=lines[:ml]
    if lines:
        last=lines[-1]
        if font.getbbox(prepare_text(last))[2]>mw:
            while last:
                if font.getbbox(prepare_text(last+"..."))[2]<=mw: lines[-1]=last+"..."; break
                last=last[:-1]
    return lines

def _hex_rgba(h):
    h=h.lstrip("#"); return(int(h[0:2],16),int(h[2:4],16),int(h[4:6],16),255)

def _circular(img, size):
    w,h=img.size; m=min(w,h)
    img=img.crop(((w-m)//2,(h-m)//2,(w+m)//2,(h+m)//2)).resize((size,size),Image.LANCZOS)
    mask=Image.new("L",(size*3,size*3),0)
    ImageDraw.Draw(mask).ellipse((0,0,size*3-1,size*3-1),fill=255)
    mask=mask.resize((size,size),Image.LANCZOS); img.putalpha(mask); return img

def _paste(base, key, x, y, size, circ=False):
    fn=ASSETS.get(key)
    fn = os.path.join(BASE_DIR, fn)
    if not fn or not os.path.exists(fn): return base
    img=Image.open(fn).convert("RGBA")
    if circ:
        img=_circular(img,size); tmp=Image.new("RGBA",base.size,(0,0,0,0)); tmp.paste(img,(x,y),img)
    else:
        ow,oh=img.size; img=img.resize((size,int(size*oh/ow)),Image.LANCZOS)
        tmp=Image.new("RGBA",base.size,(0,0,0,0)); tmp.paste(img,(x,y),img)
    return Image.alpha_composite(base,tmp)

def _paste_raw(base, raw_bytes, x, y, size):
    img=_circular(Image.open(BytesIO(raw_bytes)).convert("RGBA"),size)
    tmp=Image.new("RGBA",base.size,(0,0,0,0)); tmp.paste(img,(x,y),img)
    return Image.alpha_composite(base,tmp)

async def _dl(url):
    try:
        c=await http(); r=await c.get(url,timeout=Config.HTTP_TIMEOUT)
        if r.status_code==200: return r.content
    except Exception as e: logger.warning(f"[dl] {e}")
    return None

async def render_profile_card(uid: str) -> BytesIO | None:
    user=accounts.get(uid)
    if not user or not os.path.exists(Config.TEMPLATE_PATH): return None
    try:
        base=Image.open(Config.TEMPLATE_PATH).convert("RGBA"); L=LAYOUT
        base=_paste(base,user.get("active_bg","bgdefault"),L["bg"]["x"],L["bg"]["y"],L["bg"]["size"])
        av=user.get("avatar_url","")
        if av:
            raw=await _dl(av)
            if raw: base=_paste_raw(base,raw,L["pfp"]["x"],L["pfp"]["y"],L["pfp"]["size"])
        base=_paste(base,user.get("active_frame","framedefault"),L["frame"]["x"],L["frame"]["y"],L["frame"]["size"])
        base=_paste(base,user.get("active_bubble","bubbledefault"),L["bubble"]["x"],L["bubble"]["y"],L["bubble"]["size"])
        pg,ik=get_primary_game(uid); hg=pg is not None and ik is not None
        if hg: base=_paste(base,ik,L["gameicon"]["x"],L["gameicon"]["y"],L["gameicon"]["size"],circ=True)
        draw=ImageDraw.Draw(base)
        # Name
        cfg=L["name"]; _cx(draw,user["name"],cfg["cx"],cfg["y"],_font(user["name"],cfg["size"]),cfg["color"])
        # Bio
        cfg=L["bio"]
        bt="Empty" if user.get("bio_removed") else (user.get("bio") or "")
        bc=_hex_rgba(COLOR_MAP.get(user.get("active_colour","black"),"#1E1E1E"))
        font=_font(bt,cfg["size"]); lines=_wrap(bt,font,cfg["max_width"],cfg["max_lines"])
        lh=cfg["size"]+cfg["line_gap"]; total=len(lines)*cfg["size"]+max(0,len(lines)-1)*cfg["line_gap"]
        yc=cfg["cy"]-total//2
        for line in lines:
            t=prepare_text(line); bb=font.getbbox(t); w=bb[2]-bb[0]; sh=bb[1] if bb[1]>3 else 0
            draw.text((cfg["cx"]-w//2,yc-sh),t,font=font,fill=bc); yc+=lh
        # Stats
        for fld,val in (("games",str(user.get("games_played",0))),
                         ("wins",str(user.get("wins",0))),
                         ("wealth",str(user.get("wealth",0)))):
            cfg=L[fld]; _cx(draw,val,cfg["cx"],cfg["y"],_font(val,cfg["size"]),cfg["color"])
        # Game section
        if hg:
            rank=get_rank(user.get("wins",0))
            cfg=L["game"]; _lx(draw,pg,cfg["x"],cfg["y"],_font(pg,cfg["size"]),cfg["color"])
            if rank:
                cfg=L["rank"]; _lx(draw,rank,cfg["x"],cfg["y"],_font(rank,cfg["size"]),cfg["color"])
        else:
            cfg=L["nogame"]; _cx(draw,"No games",cfg["cx"],cfg["y"],_font("No games",cfg["size"]),cfg["color"])
        out=BytesIO(); base.convert("RGB").save(out,"JPEG",quality=Config.OUTPUT_QUALITY); out.seek(0); return out
    except Exception as e: logger.exception(f"[render] {e}"); return None


# ══════════════════════════════════════════════════════════════════════════════
# 11.  IMAGE SENDING
# ══════════════════════════════════════════════════════════════════════════════
async def send_image_bytes(chat_id, circle_id, img_bytes):
    with tempfile.NamedTemporaryFile(suffix=".jpg",delete=False) as f:
        f.write(img_bytes.read()); tmp=f.name
    try:
        img=Image.open(tmp); w,h=img.size
        with open(tmp,"rb") as f: media=await client.upload_media(f,MediaTarget.ChatImageMessage)
        await client.send_chat_entity(chat_id,{"content":f"{media.url}?wh={w}x{h}"},ChatMessageTypes.Photo,circle_id)
    finally:
        with suppress(FileNotFoundError): os.remove(tmp)

async def send_profile_card(uid, chat_id, circle_id):
    card=await render_profile_card(uid)
    if card: await send_image_bytes(chat_id,circle_id,card)
    else: await client.send_message(chat_id,
        "⚠️ Could not render the profile card. Check that template.png and fonts are present.",circle_id)

async def send_welcome_card(chat_id, circle_id, avatar_url, nickname):
    try:
        c=await http(); r=await c.get(avatar_url,timeout=Config.HTTP_TIMEOUT)
        if r.status_code!=200: raise ValueError("bad status")
        pfp=Image.open(BytesIO(r.content)).convert("RGBA")
        w,h=pfp.size; m=min(w,h)
        pfp=pfp.crop(((w-m)//2,(h-m)//2,(w+m)//2,(h+m)//2)).resize(Config.WELCOME_PROFILE_SIZE,Image.LANCZOS)
        bs=(pfp.size[0]*3,pfp.size[1]*3); mask=Image.new("L",bs,0)
        ImageDraw.Draw(mask).ellipse((0,0)+bs,fill=255)
        mask=mask.resize(pfp.size,Image.LANCZOS); pfp.putalpha(mask)
        base=Image.open(Config.WELCOME_BG).convert("RGBA")
        base.paste(pfp,Config.WELCOME_PROFILE_POSITION,pfp)
        if base.width>Config.MAX_OUTPUT_WIDTH:
            ratio=Config.MAX_OUTPUT_WIDTH/base.width
            base=base.resize((Config.MAX_OUTPUT_WIDTH,int(base.height*ratio)),Image.LANCZOS)
        buf=BytesIO(); base.convert("RGB").save(buf,"JPEG",quality=75,optimize=True); buf.seek(0)
        await send_image_bytes(chat_id,circle_id,buf)
    except Exception as e:
        logger.warning(f"[welcome] {e}")
        await client.send_message(chat_id,f"🌫️ أهلاً بك في المملكة, {nickname}",circle_id)


# ══════════════════════════════════════════════════════════════════════════════
# 12.  SIGNUP STATE MACHINE
# ══════════════════════════════════════════════════════════════════════════════
signup_states: dict[str, dict] = {}

async def handle_signup(message):
    uid=message.author.userId; cid=message.chatId; circ=message.circleId
    if has_account(uid):
        await client.send_message(cid,
            f"✅ You already have a profile, {accounts[uid]['name']}!\nUse /profile to view it.",circ); return
    if uid in signup_states:
        step=signup_states[uid]["step"]
        await client.send_message(cid,
            f"⏳ Signup in progress! Reply with:  {'Name: YourName' if step=='name' else 'Bio: your bio'}",circ); return
    signup_states[uid]={"step":"name"}
    await client.send_message(cid,
        "👋 Welcome! Let's create your profile.\n\n❓ *What is your name?*\nReply with:   Name: YourName",circ)

async def handle_signup_step(message, content) -> bool:
    uid=message.author.userId
    if uid not in signup_states: return False
    state=signup_states[uid]; cid=message.chatId; circ=message.circleId
    if state["step"]=="name":
        if not content.lower().startswith("name:"): return False
        name=content[5:].strip()
        if not name: await client.send_message(cid,"⚠️ Name can't be empty.",circ); return True
        if len(name)>30: await client.send_message(cid,f"⚠️ Name too long ({len(name)}). Max 30.",circ); return True
        state["temp_name"]=name; state["step"]="bio"
        await client.send_message(cid,
            f"✅ Name: *{name}*\n\n📝 *Put a bio that describes you.*\nReply with:   Bio: your bio here",circ)
        return True
    if state["step"]=="bio":
        if not content.lower().startswith("bio:"): return False
        bio=content[4:].strip()
        if len(bio)>100: await client.send_message(cid,f"⚠️ Bio too long ({len(bio)}). Max 100.",circ); return True
        av=next((m.get("avatar_url","") for m in members.values() if m["userId"]==uid),"")
        accounts[uid]=_default_account(state["temp_name"],bio,av)
        del signup_states[uid]; save_accounts()
        await client.send_message(cid,
            f"🎉 Profile created!\n━━━━━━━━━━━━━━━━━━\n"
            f"👤 {accounts[uid]['name']}\n📝 {bio}\n💰 0 sorex\n━━━━━━━━━━━━━━━━━━\nUse /profile to view your card!",circ)
        return True
    return False


# ══════════════════════════════════════════════════════════════════════════════
# 13.  COMMAND HANDLERS
# ══════════════════════════════════════════════════════════════════════════════
async def cmd_profile(msg):
    uid=msg.author.userId; cid=msg.chatId; circ=msg.circleId
    if not has_account(uid):
        await client.send_message(cid,"❌ No profile yet. Use /signup!",circ); return
    await send_profile_card(uid,cid,circ)

async def cmd_view(msg, target_name):
    cid=msg.chatId; circ=msg.circleId
    uid=next((u for u,d in accounts.items() if d.get("name","").lower()==target_name.lower()),None)
    if not uid:
        m=members.get(target_name.lower())
        if m and m["userId"] in accounts: uid=m["userId"]
    if not uid: await client.send_message(cid,f"❌ No profile found for *{target_name}*.",circ); return
    await send_profile_card(uid,cid,circ)

async def cmd_buy(msg, args):
    uid=msg.author.userId; cid=msg.chatId; circ=msg.circleId
    if not has_account(uid): await client.send_message(cid,"❌ Use /signup first!",circ); return
    parts=args.split()
    if len(parts)<2:
        await client.send_message(cid,"Usage:\n  /buy frame 1\n  /buy bubble 2\n  /buy bg 3\n  /buy colour white",circ); return
    cat=parts[0].lower(); item=parts[1].lower()
    if cat not in ("frame","bubble","bg","colour"):
        await client.send_message(cid,"❌ Categories: frame / bubble / bg / colour",circ); return
    key=item if cat=="colour" else cat+item
    if key not in ASSETS: await client.send_message(cid,f"❌ Asset *{key}* doesn't exist.",circ); return
    price=get_price(key)
    if price==0: await client.send_message(cid,f"✅ *{key}* is free! Use /use to equip it.",circ); return
    if owns_asset(uid,key): await client.send_message(cid,f"✅ You already own *{key}*. Use /use.",circ); return
    bal=get_balance(uid)
    if bal<price: await client.send_message(cid,f"❌ Not enough sorex!\n   Cost: {price}  |  Balance: {bal}",circ); return
    deduct_sorex(uid,price); grant_asset(uid,key)
    await client.send_message(cid,
        f"✅ Purchased *{key}* for {price} sorex!\n💰 Balance: {get_balance(uid)} sorex\nUse /use {cat} {item} to equip it.",circ)

async def cmd_use(msg, args):
    uid=msg.author.userId; cid=msg.chatId; circ=msg.circleId
    if not has_account(uid): await client.send_message(cid,"❌ Use /signup first!",circ); return
    parts=args.split()
    if len(parts)<2:
        await client.send_message(cid,"Usage:\n  /use frame 1\n  /use bubble 2\n  /use bg 3\n  /use colour white",circ); return
    cat=parts[0].lower(); item=parts[1].lower()
    if cat not in ("frame","bubble","bg","colour"):
        await client.send_message(cid,"❌ Categories: frame / bubble / bg / colour",circ); return
    key=item if cat=="colour" else cat+item

    if cat=="colour":
        if key not in COLOR_MAP:
            await client.send_message(cid,f"❌ Unknown colour. Options: {', '.join(COLOR_MAP.keys())}",circ); return
        if key!="black" and not owns_asset(uid,key):
            await client.send_message(cid,
                f"❌ You don't own colour *{key}*.\nBuy it:  /buy colour {key}  ({get_price(key)} sorex)",circ); return
        accounts[uid]["active_colour"]=key; save_accounts()
        await client.send_message(cid,f"🎨 Bio colour changed to *{key}*!",circ)
        await send_profile_card(uid,cid,circ)   # ← auto-show updated card
        return

    if key not in ASSETS: await client.send_message(cid,f"❌ Asset *{key}* doesn't exist.",circ); return
    if get_price(key)==0 or owns_asset(uid,key):
        accounts[uid][f"active_{cat}"]=key; save_accounts()
        await client.send_message(cid,f"✅ Equipped *{key}*!",circ)
        await send_profile_card(uid,cid,circ)   # ← auto-show updated card
    else:
        await client.send_message(cid,
            f"❌ You don't own *{key}*.\nBuy it:  /buy {cat} {item}  ({get_price(key)} sorex)",circ)

async def cmd_remove_bio(msg):
    uid=msg.author.userId; cid=msg.chatId; circ=msg.circleId
    if not has_account(uid): await client.send_message(cid,"❌ Use /signup first!",circ); return
    accounts[uid]["bio_removed"]=True; accounts[uid]["bio"]=""; save_accounts()
    await client.send_message(cid,"🗑️ Bio removed. Bubble will show *Empty*.\nAdd one:  /set bio your text",circ)

async def cmd_set_bio(msg, bio):
    uid=msg.author.userId; cid=msg.chatId; circ=msg.circleId
    if not has_account(uid): await client.send_message(cid,"❌ Use /signup first!",circ); return
    if len(bio)>100: await client.send_message(cid,f"⚠️ Too long ({len(bio)}). Max 100.",circ); return
    accounts[uid]["bio"]=bio; accounts[uid]["bio_removed"]=False; save_accounts()
    await client.send_message(cid,f"✅ Bio updated: *{bio}*",circ)

async def cmd_set_name(msg, new_name):
    uid=msg.author.userId; cid=msg.chatId; circ=msg.circleId
    if not has_account(uid): await client.send_message(cid,"❌ Use /signup first!",circ); return
    new_name=new_name.strip()
    if not new_name: await client.send_message(cid,"⚠️ Name can't be empty.",circ); return
    if len(new_name)>30: await client.send_message(cid,f"⚠️ Name too long ({len(new_name)}). Max 30.",circ); return
    old_name=accounts[uid]["name"]
    accounts[uid]["name"]=new_name; save_accounts()
    await client.send_message(cid,f"✅ Name changed: *{old_name}* → *{new_name}*",circ)

async def cmd_balance(msg):
    uid=msg.author.userId; cid=msg.chatId; circ=msg.circleId
    if not has_account(uid): await client.send_message(cid,"❌ Use /signup first!",circ); return
    u=accounts[uid]; rank=get_rank(u.get("wins",0)); rs=f"  |  Rank: {rank}" if rank else ""
    await client.send_message(cid,
        f"💰 *{u['name']}'s Balance*\n━━━━━━━━━━━━━━━━━━\n"
        f"Sorex:  {u.get('wealth',0)}\nWins:   {u.get('wins',0)}{rs}\nGames:  {u.get('games_played',0)}",circ)

async def cmd_shop(msg):
    cid=msg.chatId; circ=msg.circleId; uid=msg.author.userId
    lines=["🛒 *Asset Shop*","━━━━━━━━━━━━━━━━━━"]
    cats={"Backgrounds":[k for k in ASSETS if k.startswith("bg") and "default" not in k],
          "Frames":      [k for k in ASSETS if k.startswith("frame") and "default" not in k],
          "Bubbles":     [k for k in ASSETS if k.startswith("bubble") and "default" not in k],
          "Colours":     [k for k in ASSETS if ASSETS[k] is None]}
    for cat,keys in cats.items():
        if not keys: continue
        lines.append(f"\n{cat}:")
        for k in sorted(keys):
            owned="✅" if (has_account(uid) and owns_asset(uid,k)) else "  "
            lines.append(f"  {owned} {k:<14} {get_price(k)} sorex")
    lines+=["","━━━━━━━━━━━━━━━━━━","/buy <type> <n>  — purchase","/use <type> <n>  — equip (shows card auto)"]
    await client.send_message(cid,"\n".join(lines),circ)

async def cmd_give(msg, args):
    uid=msg.author.userId; cid=msg.chatId; circ=msg.circleId
    if uid!=Config.SOR_ID: return
    parts=args.lower().split()
    if not parts or not parts[0].isdigit():
        await client.send_message(cid,"Usage: reply to someone and say  /give <amount> sorex",circ); return
    amount=int(parts[0])
    ro=(getattr(msg,"replyMessage",None) or getattr(msg,"replyTo",None) or getattr(msg,"reply",None))
    if not ro: await client.send_message(cid,"⚠️ Reply to someone's message first.",circ); return
    auth=getattr(ro,"author",None)
    tuid=getattr(auth,"userId",None) if auth else None
    tnick=getattr(auth,"nickname",None) if auth else None
    if not tuid: tuid=(getattr(ro,"userId",None) or getattr(ro,"uid",None))
    if not tuid: await client.send_message(cid,"⚠️ Could not identify the user.",circ); return
    if not has_account(tuid): await client.send_message(cid,"❌ That user has no profile.",circ); return
    add_sorex(tuid,amount); name=accounts[tuid]["name"]
    await client.send_message(cid,
        f"💰 Transferred {amount} sorex to *{name}*!\nNew balance: {get_balance(tuid)} sorex",circ)

async def _reply_uid_nick(msg):
    ro=(getattr(msg,"replyMessage",None) or getattr(msg,"replyTo",None) or getattr(msg,"reply",None))
    if not ro: return None,None
    auth=getattr(ro,"author",None)
    uid=getattr(auth,"userId",None) if auth else None
    nick=getattr(auth,"nickname",None) if auth else None
    if not uid: uid=(getattr(ro,"userId",None) or getattr(ro,"uid",None))
    return uid,nick

async def cmd_ban(msg):
    uid=msg.author.userId; cid=msg.chatId; circ=msg.circleId
    if uid!=Config.SOR_ID: return
    tuid,tnick=await _reply_uid_nick(msg)
    if not tuid: await client.send_message(cid,"⚠️ Reply to someone's message first.",circ); return
    if tuid==Config.SOR_ID: await client.send_message(cid,"😅 Can't ban the owner!",circ); return
    banned[tuid]=tnick or "Unknown"; save_banned()
    await client.send_message(cid,f"🔨 *{tnick or tuid}* has been banned.",circ)

async def cmd_unban(msg):
    uid=msg.author.userId; cid=msg.chatId; circ=msg.circleId
    if uid!=Config.SOR_ID: return
    tuid,tnick=await _reply_uid_nick(msg)
    if not tuid: await client.send_message(cid,"⚠️ Reply to someone's message first.",circ); return
    if tuid in banned:
        nick=banned.pop(tuid); save_banned()
        await client.send_message(cid,f"✅ *{nick}* has been unbanned.",circ)
    else: await client.send_message(cid,f"⚠️ {tnick or tuid} is not banned.",circ)

async def cmd_help(msg):
    cid=msg.chatId; circ=msg.circleId
    await client.send_message(cid,
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
        "━━━━━━━━━━━━━━━━━━━━━",circ)


# ══════════════════════════════════════════════════════════════════════════════
# 14.  KYODO CLIENT + GAME PLUGIN SETUP
# ══════════════════════════════════════════════════════════════════════════════
client = Client(deviceId=Config.DEVICE_ID)

# Economy interface passed into every game plugin
_economy = {
    "has_account":       has_account,
    "add_sorex":         add_sorex,
    "update_game_stats": update_game_stats,
    "get_rank":          get_rank,
    "get_reward":        get_reward,
    "get_account_wins":  lambda uid: accounts.get(uid, {}).get("wins", 0),
}

# Instantiate all registered game plugins with their shared dependencies
game_instances = [G(client, Config, _economy) for G in GAME_CLASSES]

def _active_game():
    """Return the first game with an active session, or None."""
    return next((g for g in game_instances if g.is_active), None)

def _triggered_game(content):
    """Return the game whose trigger matches this content, or None."""
    return next((g for g in game_instances if g.is_trigger(content)), None)


# ══════════════════════════════════════════════════════════════════════════════
# 15.  EVENT HANDLERS
# ══════════════════════════════════════════════════════════════════════════════
@client.middleware(EventType.ChatMessage)
async def _self_filter(message: ChatMessage):
    if message.author.userId == client.userId: return False

@client.event(EventType.ChatMemberJoin)
async def on_join(message: ChatMessage):
    try:
        nick=message.author.nickname; av=message.author.avatar_url or ""; uid=message.author.userId
        if uid and nick:
            members[nick.lower()]={"nickname":nick,"userId":uid,"avatar_url":av}; save_members()
            if uid in accounts and av: accounts[uid]["avatar_url"]=av; save_accounts()
        if av and os.path.exists(Config.WELCOME_BG): await send_welcome_card(message.chatId,message.circleId,av,nick)
        else: await client.send_message(message.chatId,f"🌫️ أهلاً بك في المملكة, {nick}",message.circleId)
    except Exception as e: logger.exception(f"[on_join] {e}")


@client.event(EventType.ChatMessage)
async def on_message(message: ChatMessage):
    try:
        content=( message.content or "").strip()
        uid=message.author.userId; nickname=message.author.nickname
        av=message.author.avatar_url or ""
        chat_id=message.chatId; circle_id=message.circleId; msg_id=message.messageId

        if not content or chat_id!=Config.CHAT_ID: return

        # Silently sync avatar
        if av and uid in accounts and accounts[uid].get("avatar_url")!=av:
            accounts[uid]["avatar_url"]=av; save_accounts()

        if is_banned(uid): return  # banned users are completely ignored

        cl=content.lower()

        # ── SIGNUP (highest priority) ──────────────────────────────────────
        if uid in signup_states:
            if await handle_signup_step(message,content): return

        # ── USER COMMANDS ──────────────────────────────────────────────────
        if cl=="/signup":              await handle_signup(message);          return
        if cl in ("/profile","/card"): await cmd_profile(message);            return
        if cl.startswith("/view "):    await cmd_view(message,content[6:].strip()); return
        if cl.startswith("/buy "):     await cmd_buy(message,content[5:].strip()); return
        if cl.startswith("/use "):     await cmd_use(message,content[5:].strip()); return
        if cl=="/remove bio":          await cmd_remove_bio(message);         return
        if cl.startswith("/set bio "): await cmd_set_bio(message,content[9:].strip()); return
        if cl.startswith("/set name "): await cmd_set_name(message,content[10:].strip()); return
        if cl in ("/balance","/sorex","/coins"): await cmd_balance(message);  return
        if cl in ("/shop","/store","/assets"):   await cmd_shop(message);     return
        if cl in ("/help","/commands"):          await cmd_help(message);     return

        # ── OWNER COMMANDS ─────────────────────────────────────────────────
        if uid==Config.SOR_ID:
            if cl.startswith("/give "): await cmd_give(message,content[6:].strip()); return
            if cl=="/ban":              await cmd_ban(message);   return
            if cl=="/unban":            await cmd_unban(message); return
            if cl=="yulia scan":
                await scan_members()
                await client.send_message(chat_id,f"✅ Scan done — {len(members)} members.",circle_id); return
            for pfx in ("yulia welcome ","y welcome "):
                if cl.startswith(pfx):
                    tname=content[len(pfx):].strip(); m=members.get(tname.lower())
                    if not m: await client.send_message(chat_id,f"❌ '{tname}' not found.",circle_id); return
                    if m.get("avatar_url") and os.path.exists(Config.WELCOME_BG):
                        await send_welcome_card(chat_id,circle_id,m["avatar_url"],m["nickname"])
                    else: await client.send_message(chat_id,f"🌫️ أهلاً بك في المملكة, {m['nickname']}",circle_id)
                    return

        # ══ GAME ROUTING ════════════════════════════════════════════════════
        active=_active_game()

        # End-game command
        if content in ("انهاء اللعبة",) or cl=="end game":
            if active: await active.force_end(uid,nickname,chat_id,circle_id)
            return

        # Route to active game first
        if active:
            if await active.handle_message(uid,nickname,content,chat_id,circle_id): return

        # Check for new game trigger
        triggered=_triggered_game(content)
        if triggered:
            if active:
                await client.send_message(chat_id,
                    "⚠️ هناك لعبة شغالة بالفعل.\nالمضيف يكتب *انهاء اللعبة* أولاً.",
                    circle_id,reply_message_id=msg_id); return

            # ── Open-lobby games (no reply needed) ─────────────────────────
            if not getattr(triggered, 'NEEDS_REPLY', True):
                await triggered.start_challenge(uid, nickname, None, None, chat_id, circle_id)
                return

            # ── 1v1 games (reply required) ─────────────────────────────────
            ro=(getattr(message,"replyMessage",None) or getattr(message,"replyTo",None) or getattr(message,"reply",None))
            if not ro:
                await client.send_message(chat_id,"⚠️ رد على رسالة الشخص اللي تبي تتحداه.",
                    circle_id,reply_message_id=msg_id); return
            auth=getattr(ro,"author",None)
            ouid=getattr(auth,"userId",None) if auth else None
            onick=getattr(auth,"nickname",None) if auth else None
            if not ouid: ouid=(getattr(ro,"userId",None) or getattr(ro,"uid",None))
            if not ouid:
                await client.send_message(chat_id,"⚠️ ما قدرت أعرف اللاعب الثاني.",
                    circle_id,reply_message_id=msg_id); return
            if ouid==uid:
                await client.send_message(chat_id,"⚠️ ما تقدر تتحدى نفسك! 😅",
                    circle_id,reply_message_id=msg_id); return
            if ouid==client.userId:
                await client.send_message(chat_id,"⚠️ تحدى لاعب حقيقي مو أنا.",
                    circle_id,reply_message_id=msg_id); return
            if not onick:
                onick=next((v["nickname"] for v in members.values() if v["userId"]==ouid),None) or "اللاعب 2"
            await triggered.start_challenge(uid,nickname,ouid,onick,chat_id,circle_id)

    except Exception as e: logger.exception(f"[on_message] {e}")


# ══════════════════════════════════════════════════════════════════════════════
# 16.  MAIN
# ══════════════════════════════════════════════════════════════════════════════
_stop=False
def _shutdown(sig,frame): global _stop; logger.info(f"Signal {sig}"); _stop=True
signal.signal(signal.SIGTERM,_shutdown); signal.signal(signal.SIGINT,_shutdown)

async def _run_session():
    logger.info("Logging in…")
    await client.login(Config.EMAIL,Config.PASSWORD)
    logger.success("✅ Logged in — Profile Bot is alive!")
    asyncio.create_task(scan_members())
    await client.socket_wait()

async def main():
    global _stop; load_all(); backoff=Config.RESTART_BACKOFF_MIN
    try:
        while not _stop:
            start=time.time()
            try: await _run_session(); logger.warning("[main] session ended")
            except (KeyboardInterrupt,SystemExit): raise
            except Exception as e: logger.exception(f"[main] {e}")
            if _stop: break
            backoff=Config.RESTART_BACKOFF_MIN if time.time()-start>300 else min(backoff*2,Config.RESTART_BACKOFF_MAX)
            logger.info(f"[main] restart in {backoff}s…")
            slept=0.0
            while slept<backoff and not _stop: await asyncio.sleep(1.0); slept+=1.0
    finally: await close_http()

if __name__=="__main__":
    while True:
        keep_alive()
        try: asyncio.run(main())
        except (KeyboardInterrupt,SystemExit): break
        except Exception as e: logger.error(f"[top] {e}"); time.sleep(5)

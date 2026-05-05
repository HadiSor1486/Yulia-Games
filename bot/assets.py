# ═══════════════════════════════════════════════════════════════════════════
#  assets.py  —  All assets, prices, colors & game mappings
#
#  ┌─ HOW TO REPRICE AN ASSET ─────────────────────────────────────────────┐
#  │  Find the key in ASSET_PRICES → change the number → restart bot.      │
#  │  Default price (for anything not listed) = DEFAULT_ASSET_PRICE.       │
#  │  Assets whose key contains "default" are always FREE (price = 0).   │
#  └───────────────────────────────────────────────────────────────────────┘
#
#  ┌─ HOW TO ADD A NEW ASSET ──────────────────────────────────────────────┐
#  │  1. Drop the image file into the correct sub-folder:                  │
#  │     • Backgrounds  →  bg/                                               │
#  │     • Frames       →  frames/                                           │
#  │     • Bubbles      →  bubbles/                                          │
#  │     • Game icons   →  icons/                                            │
#  │  2. Add an entry in ASSETS:   "myasset": "folder/filename.png"        │
#  │  3. Optionally add a price in ASSET_PRICES (omit = DEFAULT_PRICE).    │
#  │  4. Restart — done.                                                     │
#  └───────────────────────────────────────────────────────────────────────┘
#
#  ┌─ HOW TO ADD A NEW FRAME WITH CUSTOM SIZE/POSITION ────────────────────┐
#  │  1. Drop frame image in frames/  (e.g. frames/frame4.png)             │
#  │  2. Add it to ASSETS:     "frame4": "frames/frame4.png"               │
#  │  3. Add its custom layout to FRAME_LAYOUTS:                           │
#  │        "frame4": {"x": 350, "y": 220, "size": 330}                    │
#  │     x, y  = top-left position on the card                             │
#  │     size  = width in pixels (height is auto-scaled to keep ratio)     │
#  │  4. If you DON'T add a FRAME_LAYOUTS entry, the frame uses the        │
#  │     default layout (same as framedefault / frame1 / frame2).          │
#  │  5. Optionally add a price in ASSET_PRICES.                           │
#  │  6. Restart — done.                                                     │
#  └───────────────────────────────────────────────────────────────────────┘
#
#  ┌─ HOW TO ADD A NEW GAME ───────────────────────────────────────────────┐
#  │  1. Add its icon file + entry in ASSETS  (e.g. "gameicon4": "icons/g4.png") │
#  │  2. Add its sorex reward in GAME_REWARDS (omit = DEFAULT_GAME_REWARD) │
#  │  3. Add its display name → icon key in GAME_MAP.                      │
#  │  4. Restart — done.                                                     │
#  └───────────────────────────────────────────────────────────────────────┘
#
#  ┌─ NOTE ON COLOURS ─────────────────────────────────────────────────────┐
#  │  Colours are "virtual" assets — no image file is needed.                │
#  │  They are purchased with  /buy colour <name>                            │
#  │  and equipped with        /use colour <name>                            │
#  │  "black" is the built-in default colour and is FREE for everyone.       │
#  └───────────────────────────────────────────────────────────────────────┘
# ═══════════════════════════════════════════════════════════════════════════


# ─── Global defaults ────────────────────────────────────────────────────────
# Change these two numbers to reprice everything at once.
DEFAULT_ASSET_PRICE  = 20   # sorex — fallback for any asset not in ASSET_PRICES
DEFAULT_GAME_REWARD  = 40   # sorex — fallback for any game not in GAME_REWARDS


# ─── Asset registry: key → relative path on disk ────────────────────────────
# Virtual assets (colours) use None — no file needed.
ASSETS = {

    # ── Backgrounds ────────────────────────────────────────────────────────
    "bgdefault":  "bg/default-bg.jpg",   # FREE — always owned by everyone
    "bg1":        "bg/bg1.jpg",
    "bg2":        "bg/bg2.jpg",
    "bg3":        "bg/bg3.jpg",
    "bg4":        "bg/bg4.jpg",
    "bg5":        "bg/bg5.jpg",
    "bg6":        "bg/bg6.jpg",
    "bg7":        "bg/bg7.jpg",
    "bg8":        "bg/bg8.jpg",
    "bg9":        "bg/bg9.jpg",
    "bg10":        "bg/bg10.jpg",
    # ── Frames ─────────────────────────────────────────────────────────────
    "framedefault":  "frames/default-frame.png",   # FREE
    "frame1":        "frames/frame1.png",
    "frame2":        "frames/frame2.png",
    "frame3":        "frames/frame3.png",
    "frame4":        "frames/frame4.png",

    # ── Chat Bubbles ────────────────────────────────────────────────────────
    "bubbledefault":  "bubbles/default-bubble.png",  # FREE
    "bubble1":        "bubbles/bubble1.png",
    "bubble2":        "bubbles/bubble2.png",
    "bubble3":        "bubbles/bubble3.png",

    # ── Game Icons (shown on profile card) ──────────────────────────────────
    "gameicon1":  "icons/game-icon1.png",   # 4 in a row
    "gameicon2":  "icons/game-icon2.png",   # bara lsalfa
    "gameicon3":  "icons/game-icon3.png",   # chess

    # ── Bio / Name Colours (virtual — no file on disk) ──────────────────────
    # "black" is the built-in default; it is never listed here as purchasable.
    "white":   None,
    "pink":    None,
    "purple":  None,
    "gray":    None,
}


# ─── Per-frame layout overrides ─────────────────────────────────────────────
# If a frame key exists here, its custom (x, y, size) is used instead of the
# default layout.  Frames NOT listed here automatically use the default.
#   x, y   = top-left corner position on the profile card
#   size   = width in pixels (height auto-scales to preserve image ratio)
#
# The 3 existing frames (framedefault, frame1, frame2) use the default layout
# and do NOT need entries here.
FRAME_LAYOUTS = {
    # EXAMPLE — add frame4 with its own custom position & size:
 "frame3": {"x": 305, "y": 175, "size": 410},
 "frame4": {"x": 305, "y": 175, "size": 410},
}


# ─── Asset prices in sorex ──────────────────────────────────────────────────
# • Keys containing "default" are automatically FREE (price = 0).
# • Any asset NOT listed here costs DEFAULT_ASSET_PRICE (20 sorex).
# • Override individual prices below.
ASSET_PRICES = {

    # Backgrounds  ─────────────────────────────────────────────────────────
    "bg1": 1000,
    "bg2": 30,
    "bg3": 30,
    "bg4": 30,
    "bg5": 35,
    "bg6": 35,
    "bg7": 35,
    "bg8": 35,
    "bg9": 35,
    "bg10": 35,

    # Frames  ──────────────────────────────────────────────────────────────
    "frame1": 20,
    "frame2": 20,
    "frame3": 25,
    "frame4": 25,   # ← EXAMPLE: add price when you add frame4

    # Bubbles  ─────────────────────────────────────────────────────────────
    "bubble1": 30,
    "bubble2": 30,
    "bubble3": 30,

    # Colours  ─────────────────────────────────────────────────────────────
    "white":  25,
    "pink":   25,
    "purple": 25,
    "gray":   25,
}


# ─── Colour hex map ─────────────────────────────────────────────────────────
# Used by the renderer to colour bio / name text.
COLOR_MAP = {
    "black":   "#1E1E1E",   # default — always free, no purchase needed
    "white":   "#FFFFFF",
    "pink":    "#FF69B4",
    "purple":  "#9370DB",
    "gray":    "#9E9E9E",
}


# ─── Game rewards (sorex given to the winner) ────────────────────────────────
# Any game NOT listed here rewards DEFAULT_GAME_REWARD (40 sorex).
GAME_REWARDS = {
    "4 in a row":   5,
    "bara alsalfa":  7,
    "chess":        10,
}


# ─── Game name → icon asset key ─────────────────────────────────────────────
# Matched case-insensitively by the bot.
GAME_MAP = {
    "4 in a row":   "gameicon1",
    "bara alsalfa":  "gameicon2",
    "chess":        "gameicon3",
}


# ─── Helper: resolve the sorex price for any asset key ───────────────────────
def get_price(key: str) -> int:
    """Return the sorex price for *key*.

    • Keys containing 'default' are always 0 (free).
    • Keys listed in ASSET_PRICES use that price.
    • Everything else costs DEFAULT_ASSET_PRICE.
    """
    if "default" in key:
        return 0
    return ASSET_PRICES.get(key, DEFAULT_ASSET_PRICE)


# ─── Helper: resolve the sorex reward for a game ────────────────────────────
def get_reward(game_name: str) -> int:
    """Return the sorex reward for *game_name* (case-insensitive lookup)."""
    lower = game_name.lower()
    for name, reward in GAME_REWARDS.items():
        if name.lower() == lower:
            return reward
    return DEFAULT_GAME_REWARD

# ═══════════════════════════════════════════════════════════════════════════
#  games/__init__.py  —  Game registry
#
#  THIS IS THE ONLY FILE YOU EDIT WHEN ADDING A NEW GAME.
#
#  Steps to add a new game
#  ─────────────────────────────────────────────────────────────────────────
#  1. Create  games/my_game.py  with a class inheriting BaseGame
#     (copy four_in_a_row.py as a starting template)
#
#  2. Update  assets.py:
#       • Add the game icon file entry in ASSETS
#         e.g.  "gameicon4": "game-icon4.png"
#       • Add the reward in GAME_REWARDS
#         e.g.  "my game name": 50
#       • Add name → icon in GAME_MAP
#         e.g.  "my game name": "gameicon4"
#
#  3. HERE in this file:
#       a. Import your class  (line below the existing imports)
#       b. Add the class to GAME_CLASSES  (the list at the bottom)
#
#  4. Restart the bot — done.  No other file needs changing.
# ═══════════════════════════════════════════════════════════════════════════

from .four_in_a_row import FourInARow
from .bara_alsalfa import BaraAlsalfeh


# ── Step 3a: import new game classes here ─────────────────────────────────
# from .chess       import ChessGame
# from .bara_lsalfa import BaraLSalfa


# ── Step 3b: add them to this list ────────────────────────────────────────
GAME_CLASSES = [
    FourInARow,
    BaraAlsalfeh,
]


# ═══════════════════════════════════════════════════════════════════════════
#  games/base_game.py  —  Abstract contract every game plugin must follow
#
#  To create a new game:
#    1. Make a new file in this folder  (e.g. games/chess.py)
#    2. Create a class that inherits BaseGame
#    3. Fill in every @abstractmethod
#    4. Register the class in games/__init__.py
# ═══════════════════════════════════════════════════════════════════════════

from __future__ import annotations
from abc import ABC, abstractmethod


class BaseGame(ABC):
    """
    Every game plugin must inherit this class and implement all abstract methods.

    Constructor receives three arguments — the main bot passes them automatically:

        client  : the Kyodo Client instance  (for send_message, etc.)
        config  : the Config class           (for CHAT_ID, timeouts, etc.)
        economy : dict of economy helpers    (see keys listed below)

    economy keys
    ─────────────────────────────────────────────────────────────────────────
    "has_account"       (uid) → bool
    "add_sorex"         (uid, amount) → None
    "update_game_stats" (uid, game_name, won) → None
    "get_rank"          (total_wins) → str
    "get_reward"        (game_name) → int
    "get_account_wins"  (uid) → int
    ─────────────────────────────────────────────────────────────────────────
    """

    # ── Identity ───────────────────────────────────────────────────────────
    # Must match the key used in assets.py GAME_MAP / GAME_REWARDS exactly.
    GAME_NAME: str = ""

    # ── Reply requirement ──────────────────────────────────────────────────
    # Set to False for open-lobby / party games that do NOT require replying
    # to a specific opponent to start (e.g. برا السالفة).
    NEEDS_REPLY: bool = True

    def __init__(self, client, config, economy: dict):
        self.client  = client
        self.config  = config
        self.eco     = economy

    # ── Economy shortcut helpers (use these inside your game) ──────────────
    def has_account(self, uid: str) -> bool:
        return self.eco["has_account"](uid)

    def add_sorex(self, uid: str, amount: int) -> None:
        self.eco["add_sorex"](uid, amount)

    def update_stats(self, uid: str, *, won: bool) -> None:
        """Record a play (and optionally a win) for GAME_NAME."""
        self.eco["update_game_stats"](uid, self.GAME_NAME, won)

    def get_rank(self, wins: int) -> str:
        return self.eco["get_rank"](wins)

    def get_reward(self) -> int:
        """Return the sorex reward for winning GAME_NAME."""
        return self.eco["get_reward"](self.GAME_NAME)

    def get_account_wins(self, uid: str) -> int:
        return self.eco["get_account_wins"](uid)

    # ── Abstract interface — every game MUST implement these ───────────────

    @property
    @abstractmethod
    def is_active(self) -> bool:
        """Return True while a game session is in progress."""
        ...

    @property
    @abstractmethod
    def host_id(self) -> str | None:
        """Return the user ID of the current game host, or None."""
        ...

    @abstractmethod
    def is_trigger(self, content: str) -> bool:
        """
        Return True if *content* should start this game.
        Called on every incoming message when no game is active.
        """
        ...

    @abstractmethod
    async def start_challenge(
        self,
        host_id: str, host_name: str,
        opp_id:  str | None, opp_name: str | None,
        chat_id: str, circle_id: str,
    ) -> bool:
        """
        Initiate a challenge between host and opponent.
        Return True if the game started successfully.
        """
        ...

    @abstractmethod
    async def handle_message(
        self,
        user_id:   str,
        nickname:  str,
        content:   str,
        chat_id:   str,
        circle_id: str,
    ) -> bool:
        """
        Process an incoming message during an active game.
        Return True if the message was consumed (prevents other handlers
        from seeing it).  Return False to let the message fall through.
        """
        ...

    @abstractmethod
    async def force_end(
        self,
        requester_id:   str,
        requester_name: str,
        chat_id:        str,
        circle_id:      str,
    ) -> bool:
        """
        Force-end the game (called when host types 'انهاء اللعبة').
        Return True if a game was actually running.
        """
        ...

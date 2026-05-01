# ═══════════════════════════════════════════════════════════════════════════
#  games/four_in_a_row.py  —  Connect Four (4 in a row) game plugin
# ═══════════════════════════════════════════════════════════════════════════

from __future__ import annotations

import asyncio
import re
import time

from .base_game import BaseGame


# ── Pre-compiled regex covering all standard Unicode emojis & components ────
_EMOJI_RE = re.compile(
    r"["
    r"\U0001F600-\U0001F64F"   # emoticons
    r"\U0001F300-\U0001F5FF"   # misc symbols & pictographs
    r"\U0001F680-\U0001F6FF"   # transport & map
    r"\U0001F700-\U0001F77F"   # alchemical
    r"\U0001F780-\U0001F7FF"   # geometric shapes extended
    r"\U0001F800-\U0001F8FF"   # supplemental arrows-c
    r"\U0001F900-\U0001F9FF"   # supplemental symbols & pictographs
    r"\U0001FA00-\U0001FA6F"   # chess symbols
    r"\U0001FA70-\U0001FAFF"   # symbols & pictographs extended-a
    r"\U0001F1E6-\U0001F1FF"   # regional indicators (flags)
    r"\U00002600-\U000026FF"   # miscellaneous symbols
    r"\U00002700-\U000027BF"   # dingbats
    r"\U00002300-\U000023FF"   # miscellaneous technical
    r"\U00002194-\U00002199"   # arrows
    r"\U000021A9-\U000021AA"   # arrows
    r"\U00002328"              # keyboard
    r"\U000023CF"              # eject symbol
    r"\U000024C2"              # circled M
    r"\U0000203C"              # double exclamation mark
    r"\U00002049"              # exclamation question mark
    r"\U00002139"              # information source
    r"\U000026B0-\U000026B1"   # funeral / coffin
    r"\U000026C8"              # thunder cloud & rain
    r"\U000026CF"              # pick
    r"\U000026D1"              # helmet with white cross
    r"\U000026D3"              # chains
    r"\U000026E9"              # shinto shrine
    r"\U000026F0-\U000026F6"   # mountain, canoe, etc.
    r"\U00002B50-\U00002B55"   # stars
    r"\U00002B06"              # up arrow
    r"\U00002B07"              # down arrow
    r"\U00002B05"              # left arrow
    r"\U00002B1C"              # white large square
    r"\U00002B1B"              # black large square
    r"\U000025B6"              # play button
    r"\U000025C0"              # reverse button
    r"\U000025FB-\U000025FE"   # medium squares
    r"\U000025AA-\U000025AB"   # small squares
    r"\U0000231A-\U0000231B"   # watch, hourglass
    r"\U000023E9-\U000023EC"   # fast-forward / rewind triangles
    r"\U000023ED-\U000023EF"   # AV buttons
    r"\U000023F1-\U000023F2"   # stopwatch, timer clock
    r"\U000023F8-\U000023FA"   # play, pause, record
    r"\U000027A1"              # arrow right
    r"\U000027B0-\U000027BF"   # loop, double loop
    r"\U00002934-\U00002935"   # arrow upper-right / lower-right
    r"\U00002763-\U00002767"   # hearts
    r"\U00002660-\U00002668"   # card suits, hot springs
    r"\U0000267B"              # recycle symbol
    r"\U0000267E-\U0000267F"   # permanent paper sign, wheelchair
    r"\U00002640-\U00002642"   # female, male sign
    r"\U00002695"              # medical symbol
    r"\U000026A0-\U000026A1"   # warning, high voltage
    r"\U000026AA-\U000026AB"   # circles
    r"\U000026BD-\U000026BE"   # football, baseball
    r"\U000026C4-\U000026C5"   # snowman, cloudy
    r"\U000026CE"              # Ophiuchus
    r"\U000026D4"              # no entry
    r"\U000026EA"              # church
    r"\U000026F2-\U000026F3"   # fountain, tent
    r"\U000026F5"              # sailboat
    r"\U000026FA-\U000026FD"   # tent, fuel pump
    r"\U00002614-\U00002615"   # umbrella with rain, hot beverage
    r"\U000026A7"              # transgender symbol
    r"\U0000270A-\U0000270D"   # raised fists, victory, writing hand
    r"\U0000274C"              # cross mark
    r"\U00002795-\U00002797"   # heavy plus, minus, divide
    r"\U000000A9"              # copyright
    r"\U000000AE"              # registered
    r"\U00002122"              # trademark
    r"\U0000303D"              # part alternation mark
    r"\U00003297"              # Japanese "congratulations"
    r"\U00003299"              # Japanese "secret"
    r"\U0001F3FB-\U0001F3FF"   # skin-tone modifiers
    r"\U0000200D"              # zero-width joiner (ZWJ)
    r"\U0000FE0F"              # variation selector-16
    r"\U000020E3"              # combining enclosing keycap
    r"\U00000023"              # number sign (for keycaps)
    r"\U0000002A"              # asterisk (for keycaps)
    r"\U00000030-\U00000039"   # digits 0-9 (for keycaps)
    r"]+"
)

# Codepoints that are allowed inside an emoji sequence but should not count as a
# standalone emoji by themselves (digits, #, *, ZWJ, skin tones, VS16, keycap).
_COMPONENT_CPS = frozenset({
    0x200D, 0xFE0F, 0x20E3, 0x0023, 0x002A,
    *range(0x0030, 0x003A),
    *range(0x1F3FB, 0x1F3FF + 1),
})


class FourInARow(BaseGame):
    """Full Connect-Four implementation.  Awards sorex to the winner."""

    GAME_NAME = "4 in a row"   # must match assets.py GAME_MAP key exactly

    # ── Board constants ────────────────────────────────────────────────────
    ROWS  = 6
    COLS  = 7
    WIN   = 4
    EMPTY = "⚪"
    COL_ROW = "1️⃣2️⃣3️⃣4️⃣5️⃣6️⃣7️⃣"

    # ── Trigger phrases ────────────────────────────────────────────────────
    _TRIGGERS_EN = frozenset({
        "4 in a row", "4 in row", "four in a row",
        "connect 4", "connect four", "4inarow", "4inrow",
    })
    _TRIGGERS_AR = frozenset({
        "اربعة على التوالي", "أربعة على التوالي",
        "اربعه على التوالي", "أربعه على التوالي",
        "4 على التوالي", "4 في صف",
        "أربعة في صف",   "اربعة في صف",
    })

    # ── Internal states ────────────────────────────────────────────────────
    _IDLE  = "idle"
    _PICK  = "emoji_select"
    _PLAY  = "playing"

    # ══════════════════════════════════════════════════════════════════════
    def __init__(self, client, config, economy: dict):
        super().__init__(client, config, economy)
        self._lock         = asyncio.Lock()
        self._timeout_task: asyncio.Task | None = None

        # Game session state
        self._state        = self._IDLE
        self._host_id:   str | None = None
        self._host_name: str | None = None
        self._opp_id:    str | None = None
        self._opp_name:  str | None = None
        self._host_emoji:  str | None = None
        self._opp_emoji:   str | None = None
        self._current_turn: str | None = None
        self._board: list[list[str | None]] = self._blank_board()

    # ── Properties ────────────────────────────────────────────────────────
    @property
    def is_active(self) -> bool:
        return self._state != self._IDLE

    @property
    def host_id(self) -> str | None:
        return self._host_id

    # ── Trigger detection ──────────────────────────────────────────────────
    def is_trigger(self, content: str) -> bool:
        s = content.strip()
        return s.lower() in self._TRIGGERS_EN or s in self._TRIGGERS_AR

    # ══════════════════════════════════════════════════════════════════════
    # Public game interface
    # ══════════════════════════════════════════════════════════════════════

    async def start_challenge(
        self,
        host_id: str, host_name: str,
        opp_id:  str, opp_name:  str,
        chat_id: str, circle_id: str,
    ) -> bool:
        async with self._lock:
            if self._state != self._IDLE:
                return False
            self._hard_reset()
            self._state     = self._PICK
            self._host_id   = host_id;   self._host_name = host_name
            self._opp_id    = opp_id;    self._opp_name  = opp_name

        await self.client.send_message(chat_id,
            f"🎮 *4 في صف* — تحدي جديد!\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"⚔️ {host_name} ضد {opp_name}\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
            f"📌 كل لاعب يختار إيموجي مختلف ليلعب به.\n"
            f"أرسل الإيموجي الذي تريده (يجب أن يكون مختلفًا).\n\n"
            f"المضيف يكتب *انهاء اللعبة* للإلغاء.\n"
            f"⏱️ عندكم {self.config.C4_EMOJI_TIMEOUT_S // 60} دقائق.",
            circle_id)

        self._arm_timeout(
            self.config.C4_EMOJI_TIMEOUT_S, self._PICK,
            "⏱️ انتهى وقت اختيار الإيموجي. تم إلغاء لعبة 4 في صف.\n"
            "للعبة جديدة، رد على شخص بـ *4 in a row*",
            chat_id, circle_id,
        )
        return True

    async def force_end(
        self,
        requester_id: str, requester_name: str,
        chat_id: str, circle_id: str,
    ) -> bool:
        if not self.is_active:
            return False
        if requester_id != self._host_id:
            await self.client.send_message(chat_id,
                "⚠️ فقط مضيف اللعبة يقدر ينهيها.", circle_id)
            return True   # consumed (we replied), even if we didn't end it
        async with self._lock:
            host = self._host_name or requester_name
            self._hard_reset()
        await self.client.send_message(chat_id,
            f"🛑 تم إنهاء لعبة 4 في صف بواسطة {host}.\n\n"
            f"للعبة جديدة، رد على شخص بـ *4 in a row*",
            circle_id)
        return True

    async def handle_message(
        self,
        user_id: str, nickname: str, content: str,
        chat_id: str, circle_id: str,
    ) -> bool:
        # ═════════════════════════════════════════════════════════════════
        #  EMOJI SELECTION PHASE
        #  ── Everyone can chat freely. Only emoji messages from the two
        #     players are consumed; everything else falls through silently.
        # ═════════════════════════════════════════════════════════════════
        if self._state == self._PICK:
            return await self._handle_emoji(user_id, nickname, content, chat_id, circle_id)

        # ═════════════════════════════════════════════════════════════════
        #  PLAYING PHASE
        #  ── Outsiders and players can chat freely.
        #     Only digit messages from the two players are treated as moves.
        #     Sending a digit out of turn triggers the "not your turn" warning.
        # ═════════════════════════════════════════════════════════════════
        if self._state == self._PLAY:
            # Outsiders talk freely → bot stays silent
            if user_id not in (self._host_id, self._opp_id):
                return False

            stripped = content.strip()
            if stripped.isdigit():
                return await self._handle_move(
                    user_id, nickname, int(stripped), chat_id, circle_id
                )

            # Players can chat / use commands freely during play
            return False

        return False

    # ══════════════════════════════════════════════════════════════════════
    # Internal — emoji selection phase
    # ══════════════════════════════════════════════════════════════════════

    async def _handle_emoji(
        self,
        user_id: str, nickname: str, content: str,
        chat_id: str, circle_id: str,
    ) -> bool:
        # Only the two players can pick emojis; outsiders talk freely
        if user_id not in (self._host_id, self._opp_id):
            return False

        cand    = content.strip()
        is_host = user_id == self._host_id

        # Non-emoji messages from players are ignored so they can keep chatting
        if not self._is_emoji(cand):
            return False

        already = dup = both_picked = False
        host_e = opp_e = host_n = opp_n = cur_n = cur_e = None

        async with self._lock:
            if self._state != self._PICK:
                return True
            my_emoji  = self._host_emoji if is_host else self._opp_emoji
            oth_emoji = self._opp_emoji  if is_host else self._host_emoji

            if my_emoji is not None:
                already = True
            elif oth_emoji == cand:
                dup = True
            else:
                if is_host:
                    self._host_emoji = cand
                else:
                    self._opp_emoji = cand

            host_e = self._host_emoji
            opp_e  = self._opp_emoji
            host_n = self._host_name
            opp_n  = self._opp_name

            if host_e and opp_e and not (already or dup):
                self._state         = self._PLAY
                self._current_turn  = self._host_id
                both_picked         = True
                cur_n = host_n; cur_e = host_e
                self._cancel_timeout()

        if already:
            await self.client.send_message(chat_id,
                f"⚠️ {nickname}، اخترت إيموجي بالفعل. انتظر اللاعب الآخر.", circle_id)
            return True
        if dup:
            await self.client.send_message(chat_id,
                f"⚠️ {nickname}، الخصم اختار هذا الإيموجي. اختر إيموجي مختلف.", circle_id)
            return True

        if both_picked:
            self._arm_timeout(
                self.config.C4_GAME_TIMEOUT_S, self._PLAY,
                "⏱️ انتهى وقت اللعبة. تم إنهاء لعبة 4 في صف.\n"
                "للعبة جديدة، رد على شخص بـ *4 in a row*",
                chat_id, circle_id,
            )
            async with self._lock:
                board = self._render()
            await self.client.send_message(chat_id,
                f"✅ تم اختيار الإيموجيات!\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"{host_n}: {host_e}  |  {opp_n}: {opp_e}\n"
                f"━━━━━━━━━━━━━━━━━━\n\n"
                f"{board}\n\n"
                f"🎯 الدور لـ *{cur_n}* {cur_e}\n"
                f"اكتب رقم العمود (1–{self.COLS}).",
                circle_id)
        else:
            await self.client.send_message(chat_id,
                f"✅ {nickname} اختار {cand}\nبانتظار اللاعب الآخر...",
                circle_id)
        return True

    # ══════════════════════════════════════════════════════════════════════
    # Internal — move phase
    # ══════════════════════════════════════════════════════════════════════

    async def _handle_move(
        self,
        user_id: str, nickname: str, col_num: int,
        chat_id: str, circle_id: str,
    ) -> bool:
        # Only the two players can make moves
        if user_id not in (self._host_id, self._opp_id):
            return False

        not_turn = bad_col = col_full = won = drawn = False
        board_view = placed_emoji = next_n = next_e = ""
        winner_id = winner_name = loser_id = loser_name = None
        draw_host_id = draw_opp_id = None

        async with self._lock:
            if self._state != self._PLAY:
                return True

            if user_id != self._current_turn:
                not_turn = True
            elif not (1 <= col_num <= self.COLS):
                bad_col = True
            else:
                is_host      = user_id == self._host_id
                placed_emoji = self._host_emoji if is_host else self._opp_emoji
                row          = self._drop(col_num - 1, placed_emoji)
                if row is None:
                    col_full = True
                else:
                    won   = self._check_win(row, col_num - 1, placed_emoji)
                    drawn = not won and self._is_board_full()
                    board_view = self._render()

                    if won:
                        winner_id   = user_id
                        winner_name = nickname
                        loser_id    = self._opp_id   if is_host else self._host_id
                        loser_name  = self._opp_name if is_host else self._host_name
                        self._hard_reset()
                    elif drawn:
                        draw_host_id = self._host_id
                        draw_opp_id  = self._opp_id
                        self._hard_reset()
                    else:
                        self._current_turn = (
                            self._opp_id if is_host else self._host_id
                        )
                        next_n = self._opp_name  if is_host else self._host_name
                        next_e = self._opp_emoji if is_host else self._host_emoji
                        # Refresh timeout so the game doesn't die mid-play
                        self._arm_timeout(
                            self.config.C4_GAME_TIMEOUT_S, self._PLAY,
                            "⏱️ انتهى وقت اللعبة. تم إنهاء لعبة 4 في صف.\n"
                            "للعبة جديدة، رد على شخص بـ *4 in a row*",
                            chat_id, circle_id,
                        )

        # ── Reply messages (outside lock) ──────────────────────────────────
        if not_turn:
            await self.client.send_message(chat_id,
                f"⚠️ مو دورك يا {nickname}، انتظر دورك.", circle_id)
            return True
        if bad_col:
            await self.client.send_message(chat_id,
                f"⚠️ اختر عمود من 1 إلى {self.COLS}.", circle_id)
            return True
        if col_full:
            await self.client.send_message(chat_id,
                f"⚠️ العمود {col_num} ممتلئ، اختر عمود ثاني.", circle_id)
            return True

        if won:
            reward    = self.get_reward()
            sorex_msg = ""
            if self.has_account(winner_id):
                self.add_sorex(winner_id, reward)
                self.update_stats(winner_id, won=True)
                sorex_msg = f"\n💰 +{reward} sorex → {winner_name}"
            if self.has_account(loser_id):
                self.update_stats(loser_id, won=False)

            rank_str = ""
            if self.has_account(winner_id):
                rank = self.get_rank(self.get_account_wins(winner_id))
                if rank:
                    rank_str = f"  |  🏅 {rank}"

            await self.client.send_message(chat_id,
                f"{board_view}\n\n"
                f"🎉 *{winner_name}* {placed_emoji} فاز!\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"4 في صف! 🏆{sorex_msg}{rank_str}\n\n"
                f"للعبة جديدة، رد على شخص بـ *4 in a row*",
                circle_id)
            return True

        if drawn:
            if draw_host_id: self.update_stats(draw_host_id, won=False)
            if draw_opp_id:  self.update_stats(draw_opp_id,  won=False)
            await self.client.send_message(chat_id,
                f"{board_view}\n\n"
                f"🤝 *تعادل!* اللوحة ممتلئة ومحد فاز.\n\n"
                f"للعبة جديدة، رد على شخص بـ *4 in a row*",
                circle_id)
            return True

        await self.client.send_message(chat_id,
            f"{board_view}\n\n"
            f"🎯 الدور لـ *{next_n}* {next_e}\n"
            f"اكتب رقم العمود (1–{self.COLS}).",
            circle_id)
        return True

    # ══════════════════════════════════════════════════════════════════════
    # Board helpers
    # ══════════════════════════════════════════════════════════════════════

    @staticmethod
    def _blank_board() -> list[list[str | None]]:
        return [[None] * FourInARow.COLS for _ in range(FourInARow.ROWS)]

    def _render(self) -> str:
        rows = []
        for r in range(self.ROWS - 1, -1, -1):
            rows.append("".join(self._board[r][c] or self.EMPTY for c in range(self.COLS)))
        rows.append(self.COL_ROW)
        return "\n".join(rows)

    def _drop(self, col: int, emoji: str) -> int | None:
        for r in range(self.ROWS):
            if self._board[r][col] is None:
                self._board[r][col] = emoji
                return r
        return None

    def _check_win(self, row: int, col: int, emoji: str) -> bool:
        b = self._board
        for dr, dc in ((0, 1), (1, 0), (1, 1), (1, -1)):
            cnt = 1
            for sign in (1, -1):
                r, c = row + sign * dr, col + sign * dc
                while 0 <= r < self.ROWS and 0 <= c < self.COLS and b[r][c] == emoji:
                    cnt += 1
                    r += sign * dr
                    c += sign * dc
            if cnt >= self.WIN:
                return True
        return False

    def _is_board_full(self) -> bool:
        return all(self._board[self.ROWS - 1][c] is not None for c in range(self.COLS))

    # ══════════════════════════════════════════════════════════════════════
    # Timeout helpers
    # ══════════════════════════════════════════════════════════════════════

    def _cancel_timeout(self):
        if self._timeout_task and not self._timeout_task.done():
            self._timeout_task.cancel()
        self._timeout_task = None

    def _arm_timeout(self, seconds: float, state: str, msg: str,
                     chat_id: str, circle_id: str):
        self._cancel_timeout()
        self._timeout_task = asyncio.create_task(
            self._run_timeout(seconds, state, msg, chat_id, circle_id)
        )

    async def _run_timeout(self, seconds: float, state: str, msg: str,
                           chat_id: str, circle_id: str):
        try:
            await asyncio.sleep(seconds)
            async with self._lock:
                if self._state != state:
                    return
                try:
                    await self.client.send_message(chat_id, msg, circle_id)
                except Exception:
                    pass
                self._hard_reset()
        except asyncio.CancelledError:
            pass

    def _hard_reset(self):
        """Reset all state. Call inside the lock when possible."""
        self._cancel_timeout()
        self._state         = self._IDLE
        self._host_id       = self._host_name = None
        self._opp_id        = self._opp_name  = None
        self._host_emoji    = self._opp_emoji  = None
        self._current_turn  = None
        self._board         = self._blank_board()

    # ══════════════════════════════════════════════════════════════════════
    # Emoji validation — accepts ALL standard Unicode emojis
    # ══════════════════════════════════════════════════════════════════════

    @staticmethod
    def _is_emoji(text: str) -> bool:
        s = text.strip()
        if not s or s.isdigit() or len(s) > 30:
            return False
        if any(c.isalpha() for c in s):
            return False
        if not _EMOJI_RE.fullmatch(s):
            return False

        # Keycap sequences (e.g. #️⃣, 1️⃣) are valid even though every codepoint
        # is technically a "component".
        is_keycap = any(c == "\u20e3" for c in s)

        # Must contain at least one real emoji base (not just a component).
        has_base = is_keycap or any(ord(c) not in _COMPONENT_CPS for c in s)
        return has_base

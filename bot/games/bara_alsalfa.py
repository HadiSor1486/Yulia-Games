# ═══════════════════════════════════════════════════════════════════════════
#  games/bara_alsalfa.py  —  برا السالفة game plugin
# ═══════════════════════════════════════════════════════════════════════════

from __future__ import annotations

import asyncio
import random
import time

from .base_game import BaseGame


class BaraAlsalfeh(BaseGame):
    """برا السالفة — party game.  Everyone knows the topic except one impostor.
    Fair round-robin: every player must ask AND be asked before voting.
    Awards sorex to the winning side."""

    GAME_NAME   = "bara alsalfa"
    NEEDS_REPLY = False          # ← open lobby — anyone can start without replying

    # ── Topic pool ─────────────────────────────────────────────────────────
    _TOPICS = [
        "كلب", "قطة", "أسد", "فيل", "قرد", "ثعلب", "ذئب", "نمر", "زرافة", "دلفين",
        "ببغاء", "نسر", "سلحفاة", "أخطبوط", "حصان",
        "الشاطئ", "المستشفى", "المدرسة", "المطار", "السوق", "الملعب", "السينما",
        "الحفلة", "المكتبة", "المسرح", "الصالة الرياضية", "المتحف", "الفندق",
        "المصنع", "القصر", "الحديقة", "الغابة", "الصحراء", "الميناء", "المحطة",
        "كرة القدم", "السباحة", "التنس", "الملاكمة", "كرة السلة", "الغوص",
        "ركوب الأمواج", "الجودو", "الجمباز", "الفروسية",
        "الهاتف الذكي", "اللابتوب", "التلفزيون", "الكاميرا", "الطابعة",
        "سماعات الرأس", "المايكروفون", "جهاز التحكم", "الراديو", "الساعة الذكية",
        "الدرون", "الروبوت", "شاشة العرض",
        "البيانو", "الغيتار", "الطبلة", "الكمان", "الناي", "حفلة موسيقية",
        "فيلم رعب", "مسرحية", "معرض فني",
        "الطائرة", "القطار", "السفينة", "الدراجة النارية", "الغواصة", "المروحية",
        "سيارة الإسعاف", "القارب", "الدراجة الهوائية",
        "الطبيب", "المعلم", "الممثل", "المغني", "الطيار", "رجل الإطفاء",
        "الشرطي", "المحامي", "المهندس", "الرياضي", "الطاهي", "الصحفي",
        "الصيف", "الشتاء", "الربيع", "الخريف", "البركان", "الزلزال",
        "قوس قزح", "الثلج", "العاصفة", "الشفق القطبي",
        "الإجازة", "يوم الميلاد", "حفل الزفاف", "التخرج", "الكرنفال",
        "الألعاب النارية", "التسوق", "المباراة النهائية",
    ]

    # ── Trigger phrases ────────────────────────────────────────────────────
    _TRIGGERS = frozenset({
        "لعبة برا السالفة", "برا السالفة",
    })

    # ── Internal states ────────────────────────────────────────────────────
    _IDLE   = "idle"
    _LOBBY  = "lobby"
    _ACTIVE = "active"
    _VOTING = "voting"

    # ══════════════════════════════════════════════════════════════════════
    def __init__(self, client, config, economy: dict):
        super().__init__(client, config, economy)
        self._lock         = asyncio.Lock()
        self._timeout_task: asyncio.Task | None = None

        # Game session state
        self._state      = self._IDLE
        self._host_id:   str | None = None
        self._host_name: str | None = None
        self._players:   list[dict] = []       # [{"userId": str, "nickname": str}]
        self._impostor_id: str | None = None
        self._topic:     str | None = None
        self._turn_index = 0
        self._votes:     dict[str, int] = {}    # userId -> vote count
        self._voted_ids: set[str] = set()       # userIds who already voted
        self._started_at = 0.0

    # ── Safe config helper (supports BARRA_* or plain names) ─────────────
    def _cfg(self, key: str, default):
        """Read config value; fall back to plain name (no BARRA_ prefix)."""
        if hasattr(self.config, key):
            return getattr(self.config, key)
        plain = key.replace("BARRA_", "", 1)   # e.g. BARRA_MIN_PLAYERS → MIN_PLAYERS
        if hasattr(self.config, plain):
            return getattr(self.config, plain)
        return default

    # ── Properties ────────────────────────────────────────────────────────
    @property
    def is_active(self) -> bool:
        return self._state != self._IDLE

    @property
    def host_id(self) -> str | None:
        return self._host_id

    # ── Trigger detection ──────────────────────────────────────────────────
    def is_trigger(self, content: str) -> bool:
        return content.strip() in self._TRIGGERS

    # ══════════════════════════════════════════════════════════════════════
    # Public game interface
    # ══════════════════════════════════════════════════════════════════════

    async def start_challenge(
        self,
        host_id: str, host_name: str,
        opp_id:  str | None, opp_name: str | None,
        chat_id: str, circle_id: str,
    ) -> bool:
        """Open the lobby.  opp_id / opp_name are ignored (multiplayer lobby)."""
        async with self._lock:
            if self._state != self._IDLE:
                return False
            self._hard_reset()
            self._state       = self._LOBBY
            self._host_id     = host_id
            self._host_name   = host_name
            self._started_at  = time.time()

        await self.client.send_message(chat_id,
            f"🎮 لعبة *برا السالفة* بدأت!\n"
            f"المضيف: {host_name}\n\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📋 طريقة اللعب:\n"
            f"• كل اللاعبين يعرفون موضوع مشترك\n"
            f"• شخص واحد فقط *برا السالفة* — ما يعرف الموضوع\n"
            f"• يسألون بعض ويحاولون يكتشفون مين برا السالفة\n"
            f"• كل لاعب لازم يسأل وينسأل قبل التصويت — اللعبة عادلة\n"
            f"• في النهاية يصوتون على اللاعب المشبوه\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
            f"✏️ للمشاركة اكتبوا: *مشاركة*\n"
            f"(نحتاج {self._cfg('BARRA_MIN_PLAYERS', 3)} لاعبين على الأقل)\n\n"
            f"لما يكتمل العدد، المضيف يكتب: *اكتمل العدد*",
            circle_id)

        self._arm_timeout(
            self._cfg('BARRA_LOBBY_TIMEOUT_S', 300), self._LOBBY,
            "⏱️ انتهى وقت الانتظار في اللوبي. تم إلغاء اللعبة.\n"
            "ابدأ من جديد بكتابة: *لعبة برا السالفة*",
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
                "⚠️ فقط المضيف يقدر ينهي اللعبة.", circle_id)
            return True   # consumed (we replied)
        async with self._lock:
            self._hard_reset()
        await self.client.send_message(chat_id,
            "🛑 تم إنهاء لعبة برا السالفة.\n\n"
            "لتبدأ لعبة جديدة اكتب: *لعبة برا السالفة*",
            circle_id)
        return True

    async def handle_message(
        self,
        user_id: str, nickname: str, content: str,
        chat_id: str, circle_id: str,
    ) -> bool:
        # ═════════════════════════════════════════════════════════════════
        #  LOBBY PHASE — only join commands are consumed
        # ═════════════════════════════════════════════════════════════════
        if self._state == self._LOBBY:
            return await self._handle_lobby(user_id, nickname, content, chat_id, circle_id)

        # ═════════════════════════════════════════════════════════════════
        #  ACTIVE PHASE — host controls, players chat freely
        # ═════════════════════════════════════════════════════════════════
        if self._state == self._ACTIVE:
            return await self._handle_active(user_id, nickname, content, chat_id, circle_id)

        # ═════════════════════════════════════════════════════════════════
        #  VOTING PHASE — players send numbers to vote
        # ═════════════════════════════════════════════════════════════════
        if self._state == self._VOTING:
            return await self._handle_voting(user_id, nickname, content, chat_id, circle_id)

        return False

    # ══════════════════════════════════════════════════════════════════════
    # Internal — lobby phase
    # ══════════════════════════════════════════════════════════════════════

    async def _handle_lobby(
        self,
        user_id: str, nickname: str, content: str,
        chat_id: str, circle_id: str,
    ) -> bool:
        text = content.strip()

        # ── Join ──────────────────────────────────────────────────────
        if text == "مشاركة":
            async with self._lock:
                if self._find_player(user_id):
                    await self.client.send_message(chat_id,
                        f"⚠️ {nickname}، أنت مسجل بالفعل! 😄", circle_id)
                    return True
                self._players.append({"userId": user_id, "nickname": nickname})
                count = len(self._players)
            await self.client.send_message(chat_id,
                f"✅ {nickname} انضم للعبة! إجمالي اللاعبين: {count}", circle_id)
            return True

        # ── Close lobby (host only) ──────────────────────────────────
        if text == "اكتمل العدد":
            if user_id != self._host_id:
                return False   # let other handlers deal with it
            async with self._lock:
                if len(self._players) < self._cfg('BARRA_MIN_PLAYERS', 3):
                    await self.client.send_message(chat_id,
                        f"⚠️ ما يكفي لاعبين! عندنا {len(self._players)} فقط، "
                        f"نحتاج {self._cfg('BARRA_MIN_PLAYERS', 3)} على الأقل.", circle_id)
                    return True
            await self._start_reveal(chat_id, circle_id)
            return True

        return False   # allow normal chat during lobby

    # ══════════════════════════════════════════════════════════════════════
    # DM helper (private messages to players)
    # ══════════════════════════════════════════════════════════════════════

    async def _send_dm(self, user_id: str, message: str) -> bool:
        """Send a private message to a user via Kyodo direct chat."""
        try:
            result = await self.client.start_direct_chat(
                user_id, circleId=self.config.CIRCLE_ID)
            chat = result[0]
            dm_chat_id = chat.chatId

            if not dm_chat_id:
                return False

            # Rejoin if the bot left the chat
            member_status = None
            try:
                member_status = chat.member.status
            except Exception:
                pass

            if member_status != 0:
                try:
                    await self.client.join_chat(dm_chat_id)
                except Exception:
                    try:
                        await self.client.join_chat(
                            dm_chat_id, circleId=self.config.CIRCLE_ID)
                    except Exception:
                        return False

            await self.client.send_message(dm_chat_id, message, None)
            return True

        except Exception:
            return False

    # ══════════════════════════════════════════════════════════════════════
    # Internal — reveal (lobby → active transition)
    # ══════════════════════════════════════════════════════════════════════

    async def _start_reveal(self, chat_id: str, circle_id: str):
        async with self._lock:
            random.shuffle(self._players)
            impostor           = random.choice(self._players)
            self._impostor_id  = impostor["userId"]
            self._topic        = random.choice(self._TOPICS)
            self._state        = self._ACTIVE
            self._turn_index   = 0
            self._cancel_timeout()
            # Snapshot for DM loop (released lock before async DM calls)
            impostor_id_snap = self._impostor_id
            topic_snap       = self._topic
            players_snap     = list(self._players)

        # DM each player their role
        for p in players_snap:
            uid = p["userId"]
            is_impostor = (uid == impostor_id_snap)
            if is_impostor:
                dm_text = (
                    "🕵️ أنت *برا السالفة* !\n\n"
                    "الجميع عندهم موضوع مشترك — أنت ما تعرفه.\n"
                    "استمع للأسئلة وجاوب بذكاء ولا تنكشف! 🤫"
                )
            else:
                dm_text = (
                    f"🎯 الموضوع هو: *{topic_snap}*\n\n"
                    f"في شخص واحد ما يعرف الموضوع — هو *برا السالفة*.\n"
                    f"اسأل واجاوب بذكاء ولا تقول الموضوع صراحة! 🤐"
                )
            try:
                await self._send_dm(uid, dm_text)
            except Exception:
                pass

        await self.client.send_message(chat_id,
            f"✅ اكتمل العدد! {len(self._players)} لاعبين جاهزين.\n\n"
            f"👥 اللاعبون (الترتيب):\n{self._player_list()}\n\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🎉 نبدأ اللعبة الآن!\n"
            f"كل لاعب استلم دوره عبر الرسالة الخاصة.\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
            + self._turn_msg(),
            circle_id)

    # ══════════════════════════════════════════════════════════════════════
    # Internal — active phase
    # ══════════════════════════════════════════════════════════════════════

    async def _handle_active(
        self,
        user_id: str, nickname: str, content: str,
        chat_id: str, circle_id: str,
    ) -> bool:
        text = content.strip()

        # ── Next turn (host only) ────────────────────────────────────
        if text == "التالي":
            if user_id != self._host_id:
                return False
            await self._next_turn(chat_id, circle_id)
            return True

        # ── Start voting (host only) ─────────────────────────────────
        if text == "تصويت":
            if user_id != self._host_id:
                await self.client.send_message(chat_id,
                    "⚠️ فقط المضيف يقدر يبدأ التصويت.", circle_id)
                return True
            await self._start_voting(chat_id, circle_id)
            return True

        return False   # normal chat freely during active phase

    async def _next_turn(self, chat_id: str, circle_id: str):
        wrapped_round = 0
        async with self._lock:
            if self._state != self._ACTIVE:
                return
            self._turn_index += 1
            n = len(self._players)
            if n > 0 and self._turn_index % n == 0:
                wrapped_round = self._turn_index // n

        if wrapped_round:
            await self.client.send_message(chat_id,
                f"🏁 انتهت الجولة {wrapped_round} — كل اللاعبين سألوا وانسألوا.\n"
                f"المضيف يقدر يكتب *تصويت* الآن\n"
                f"أو *التالي* لجولة جديدة.",
                circle_id)
            return
        await self.client.send_message(chat_id, self._turn_msg(), circle_id)

    async def _start_voting(self, chat_id: str, circle_id: str):
        async with self._lock:
            if self._state != self._ACTIVE:
                return
            if not self._round_complete():
                rnd, pos, total = self._round_progress()
                await self.client.send_message(chat_id,
                    f"⏳ ما خلصت الجولة بعد.\n"
                    f"الحالة: السؤال {pos}/{total} في الجولة {rnd}.\n"
                    f"كل لاعب لازم يسأل وينسأل قبل التصويت — هذي اللعبة العادلة.\n"
                    f"اكتب *التالي* لإكمالها.",
                    circle_id)
                return
            self._state     = self._VOTING
            self._votes     = {}
            self._voted_ids = set()
            self._cancel_timeout()

        await self.client.send_message(chat_id,
            f"🗳️ وقت التصويت!\n\n"
            f"من هو *برا السالفة*؟\n\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"{self._player_list()}\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
            f"📌 كل لاعب يرسل رقم الشخص اللي يشك فيه.\n"
            f"(ما تقدر تصوت على نفسك)\n"
            f"⏱️ عندكم {self._cfg('BARRA_VOTING_TIMEOUT_S', 180) // 60} دقائق",
            circle_id)

        self._arm_timeout(
            self._cfg('BARRA_VOTING_TIMEOUT_S', 180), self._VOTING,
            "⏱️ انتهى وقت التصويت. سيتم إعلان النتائج بناءً على الأصوات الموجودة.",
            chat_id, circle_id,
        )
        asyncio.create_task(self._auto_tally(chat_id, circle_id))

    # ══════════════════════════════════════════════════════════════════════
    # Internal — voting phase
    # ══════════════════════════════════════════════════════════════════════

    async def _handle_voting(
        self,
        user_id: str, nickname: str, content: str,
        chat_id: str, circle_id: str,
    ) -> bool:
        text = content.strip()
        if not text.isdigit():
            return False   # non-digits are normal chat during voting

        # Only registered players can vote
        if not self._find_player(user_id):
            return False

        vote_num = int(text)
        await self._cast_vote(user_id, nickname, vote_num, chat_id, circle_id)
        return True

    async def _cast_vote(self, user_id: str, nickname: str, vote_num: int,
                         chat_id: str, circle_id: str):
        should_announce = False
        remaining = 0

        async with self._lock:
            if self._state != self._VOTING:
                return
            n = len(self._players)
            if user_id in self._voted_ids:
                await self.client.send_message(chat_id,
                    f"⚠️ {nickname}، صوتك مسجل بالفعل. 🗳️", circle_id)
                return
            if not (1 <= vote_num <= n):
                await self.client.send_message(chat_id,
                    f"⚠️ رقم غير صحيح. اختار من 1 إلى {n}.", circle_id)
                return
            target = self._players[vote_num - 1]
            if target["userId"] == user_id:
                await self.client.send_message(chat_id,
                    "⚠️ ما تقدر تصوت على نفسك! 😅", circle_id)
                return
            target_id = target["userId"]
            self._votes[target_id] = self._votes.get(target_id, 0) + 1
            self._voted_ids.add(user_id)
            remaining = n - len(self._voted_ids)
            if remaining == 0:
                should_announce = True

        if should_announce:
            await self._announce_result(chat_id, circle_id)
        else:
            await self.client.send_message(chat_id,
                f"🗳️ {nickname} صوّت. متبقي {remaining} "
                f"{'صوت' if remaining == 1 else 'أصوات'}.",
                circle_id)

    async def _auto_tally(self, chat_id: str, circle_id: str):
        await asyncio.sleep(self._cfg('BARRA_VOTING_TIMEOUT_S', 180))
        if self._state == self._VOTING:
            await self._announce_result(chat_id, circle_id)

    # ══════════════════════════════════════════════════════════════════════
    # Results & economy
    # ══════════════════════════════════════════════════════════════════════

    async def _announce_result(self, chat_id: str, circle_id: str):
        async with self._lock:
            if self._state not in (self._VOTING, self._ACTIVE):
                return
            votes_local       = dict(self._votes)
            players_local     = list(self._players)
            impostor_id_local = self._impostor_id
            topic_local       = self._topic
            self._hard_reset()

        impostor_name = next(
            (p["nickname"] for p in players_local if p["userId"] == impostor_id_local), "؟")

        if not votes_local:
            await self.client.send_message(chat_id,
                f"⚠️ ما في أصوات.\n"
                f"🕵️ برا السالفة كان: *{impostor_name}*\n"
                f"🎯 الموضوع كان: *{topic_local}*\n\n"
                f"لعبة جديدة؟ اكتب: *لعبة برا السالفة*",
                circle_id)
            return

        max_votes  = max(votes_local.values())
        candidates = [uid for uid, v in votes_local.items() if v == max_votes]
        summary    = "\n".join(
            f"  {p['nickname']} ← {votes_local.get(p['userId'], 0)} صوت"
            for p in players_local
        )

        # ── Tie ──────────────────────────────────────────────────────
        if len(candidates) > 1:
            tied_names = [p["nickname"] for p in players_local if p["userId"] in candidates]
            msg = (
                f"📊 نتائج التصويت:\n{summary}\n\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"⚖️ تعادل! الأصوات تساوت بين:\n"
                + "\n".join(f"  • {n}" for n in tied_names) +
                f"\n\n🕵️ برا السالفة كان: *{impostor_name}*\n"
                f"🎯 الموضوع كان: *{topic_local}*\n\n"
                f"🏆 لا فائز — انتهت اللعبة!\n\n"
                f"لعبة جديدة؟ اكتب: *لعبة برا السالفة*"
            )
            await self.client.send_message(chat_id, msg, circle_id)
            return

        # ── Winner/loser ─────────────────────────────────────────────
        eliminated_id   = candidates[0]
        eliminated_name = next(
            (p["nickname"] for p in players_local if p["userId"] == eliminated_id), "؟")
        impostor_wins = (eliminated_id != impostor_id_local)

        # Build sorex / stats messages
        reward    = self.get_reward()
        sorex_msg = ""

        if impostor_wins:
            # Impostor escaped — impostor wins
            if self.has_account(impostor_id_local):
                self.add_sorex(impostor_id_local, reward)
                self.update_stats(impostor_id_local, won=True)
                sorex_msg = f"\n💰 +{reward} sorex → {impostor_name}"
            # Players who voted wrong get loss stat
            for p in players_local:
                uid = p["userId"]
                if uid != impostor_id_local and self.has_account(uid):
                    self.update_stats(uid, won=False)

            rank_str = ""
            if self.has_account(impostor_id_local):
                rank = self.get_rank(self.get_account_wins(impostor_id_local))
                if rank:
                    rank_str = f"  |  🏅 {rank}"

            msg = (
                f"📊 نتائج التصويت:\n{summary}\n\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"😈 غلطتم! طردتم *{eliminated_name}* وهو بريء!\n\n"
                f"🕵️ برا السالفة الحقيقي كان: *{impostor_name}*\n"
                f"🎯 الموضوع كان: *{topic_local}*\n\n"
                f"🏆 فاز برا السالفة!{sorex_msg}{rank_str}\n\n"
                f"لعبة جديدة؟ اكتب: *لعبة برا السالفة*"
            )
        else:
            # Players caught the impostor — every non-impostor gets reward
            winners = [p for p in players_local if p["userId"] != impostor_id_local]
            sorex_lines = []
            for p in winners:
                uid = p["userId"]
                if self.has_account(uid):
                    self.add_sorex(uid, reward)
                    self.update_stats(uid, won=True)
                    sorex_lines.append(f"💰 +{reward} sorex → {p['nickname']}")
            if sorex_lines:
                sorex_msg = "\n" + "\n".join(sorex_lines)
            # Impostor gets loss stat
            if self.has_account(impostor_id_local):
                self.update_stats(impostor_id_local, won=False)

            # Show rank for first winner (representative)
            rank_str = ""
            if winners and self.has_account(winners[0]["userId"]):
                rank = self.get_rank(self.get_account_wins(winners[0]["userId"]))
                if rank:
                    rank_str = f"  |  🏅 {rank}"

            msg = (
                f"📊 نتائج التصويت:\n{summary}\n\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🎉 أحسنتم! طردتم *{eliminated_name}*\n"
                f"وهو فعلاً كان *برا السالفة*! 🕵️\n\n"
                f"🎯 الموضوع كان: *{topic_local}*\n\n"
                f"🏆 فاز اللاعبون!{sorex_msg}{rank_str}\n\n"
                f"لعبة جديدة؟ اكتب: *لعبة برا السالفة*"
            )

        await self.client.send_message(chat_id, msg, circle_id)

    # ══════════════════════════════════════════════════════════════════════
    # Helpers
    # ══════════════════════════════════════════════════════════════════════

    def _find_player(self, user_id: str) -> dict | None:
        for p in self._players:
            if p["userId"] == user_id:
                return p
        return None

    def _player_list(self) -> str:
        return "\n".join(f"{i}. {p['nickname']}" for i, p in enumerate(self._players, 1))

    def _round_progress(self) -> tuple[int, int, int]:
        n = len(self._players)
        if n == 0:
            return (1, 0, 0)
        return ((self._turn_index // n) + 1,
                (self._turn_index %  n) + 1,
                n)

    def _round_complete(self) -> bool:
        n = len(self._players)
        return n > 0 and self._turn_index >= n

    def _turn_msg(self) -> str:
        players = self._players
        n = len(players)
        if n == 0:
            return "⚠️ لا لاعبين"
        asker  = players[self._turn_index % n]
        target = players[(self._turn_index + 1) % n]
        rnd, pos, total = self._round_progress()
        voting_ready = (
            "✅ يقدر يكتب *تصويت* الآن"
            if self._round_complete() else
            "🔒 *تصويت* مقفل لحد ما تخلص الجولة"
        )
        return (
            f"🎙️ الجولة {rnd} — السؤال {pos}/{total}\n\n"
            f"👤 {asker['nickname']}\n"
            f"يسأل\n"
            f"👤 {target['nickname']}\n\n"
            f"المضيف يكتب *التالي* للدور التالي\n"
            f"{voting_ready}"
        )

    # ══════════════════════════════════════════════════════════════════════
    # Timeout helpers  (same pattern as FourInARow)
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
        """Reset all state.  Must be called inside the lock when possible."""
        self._cancel_timeout()
        self._state        = self._IDLE
        self._host_id      = self._host_name = None
        self._players      = []
        self._impostor_id  = None
        self._topic        = None
        self._turn_index   = 0
        self._votes        = {}
        self._voted_ids    = set()
        self._started_at   = 0.0

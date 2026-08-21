import os
import json
import asyncio
import random
import re
import time
from collections import deque
import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv
from datetime import datetime

from codes import CODES_TABLE
from crates import CRATES_TABLE
from extra import BANK_CAPS_BY_LEVEL, BET_CAPS_BY_LEVEL, RARITY_ICONS
from roles import ROLES_TABLE
from store import STORE_ITEMS_BY_GUILD
from upd_log import UPD_LOG

PUBLIC_COMMANDS = {
    "help",
    "updatelog",
    "balance",
    "profile",
    "store",
    "work",
    "crime",
    "open",
    "crate",
    "leaderboard",
    "roles",
    "pay",
    "deposit",
    "withdraw",
    "coinflip",
    "roulette",
    "slots",
    "blackjack",
}

# 1. Load configuration environment variables
load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')
if not TOKEN:
    raise RuntimeError(
        "Missing DISCORD_TOKEN environment variable. Add DISCORD_TOKEN to your .env file or environment."
    )

# 2. Setup the bot
intents = discord.Intents.default()
intents.members = True          
intents.message_content = True  
bot = commands.Bot(command_prefix="!", intents=intents)

# Global rate-limit handling: wrap discord HTTP client's request method
# to serialize requests and retry on global 429 Too Many Requests responses.
import asyncio

try:
    _orig_http_request = discord.http.HTTPClient.request
    _http_semaphore = asyncio.Semaphore(1)

    async def _request_with_global_backoff(self, *args, **kwargs):
        """Wrapper that forwards all args/kwargs to the original
        HTTPClient.request and implements a semaphore + exponential
        backoff on 429 responses.
        """
        route = args[0] if args else kwargs.get('route')
        route_path = getattr(route, 'path', None) if route is not None else None
        route_str = str(route) if route is not None else ''
        is_interaction_route = bool(
            (route_path and 'interactions' in route_path) or 'interactions' in route_str
        )

        last_exc = None
        backoff = 1.0
        # Try a few attempts with exponential backoff when hitting 429s
        for attempt in range(6):
            async with _http_semaphore:
                try:
                    return await _orig_http_request(self, *args, **kwargs)
                except discord.errors.HTTPException as e:
                    last_exc = e
                    status = getattr(e, 'status', None)
                    text = str(e)
                    http_code = getattr(e, 'code', None)
                    if (
                        status == 429
                        or http_code == 0
                        or 'Too Many Requests' in text
                        or 'cloudflare' in text.lower()
                        or '1015' in text
                    ):
                        retry_after = 5.0
                        try:
                            if getattr(e, 'retry_after', None) is not None:
                                retry_after = float(e.retry_after)
                        except Exception:
                            pass

                        try:
                            resp = getattr(e, 'response', None)
                            if resp is not None:
                                headers = getattr(resp, 'headers', {}) or {}
                                if 'retry-after' in headers:
                                    retry_after = float(headers['retry-after'])
                                elif 'Retry-After' in headers:
                                    retry_after = float(headers['Retry-After'])
                        except Exception:
                            pass

                        sleep_time = max(0.5, retry_after * backoff)
                        if is_interaction_route:
                            sleep_time = min(sleep_time, 1.5)
                        await asyncio.sleep(sleep_time)
                        backoff *= 2
                        continue
                    raise
                except Exception as e:
                    last_exc = e
                    raise
        # If we exhausted retries, raise the last exception
        if last_exc:
            raise last_exc

    discord.http.HTTPClient.request = _request_with_global_backoff
except Exception:
    # If something goes wrong patching (e.g. running in an environment
    # without discord internals available yet), continue without crashing.
    pass

# Centralized rate-limited sender to smooth API bursts
class RateLimitedSender:
    def __init__(self, delay: float = 0.5):
        self._queue: asyncio.Queue = asyncio.Queue()
        self._delay = delay
        self._worker_task: asyncio.Task | None = None

    async def _worker(self):
        while True:
            item = await self._queue.get()
            coro_func, args, kwargs, fut = item
            try:
                # Try to execute the coroutine, retrying on HTTP 429 errors
                attempts = 0
                backoff = 1.0
                while True:
                    try:
                        result = await coro_func(*args, **kwargs)
                        if fut and not fut.done():
                            fut.set_result(result)
                        break
                    except discord.errors.HTTPException as he:
                        attempts += 1
                        status = getattr(he, 'status', None)
                        http_code = getattr(he, 'code', None)
                        text = str(he)
                        if (
                            status == 429
                            or http_code == 0
                            or 'Too Many Requests' in text
                            or 'cloudflare' in text.lower()
                            or '1015' in text
                        ):
                            # Determine retry delay
                            retry_after = None
                            try:
                                if hasattr(he, 'retry_after') and he.retry_after:
                                    retry_after = float(he.retry_after)
                            except Exception:
                                retry_after = None
                            # Try to inspect response headers if available
                            try:
                                resp = getattr(he, 'response', None)
                                if resp is not None:
                                    headers = getattr(resp, 'headers', {}) or {}
                                    # header keys may be lower/upper
                                    for h in ('retry-after', 'Retry-After'):
                                        if h in headers:
                                            try:
                                                retry_after = float(headers[h])
                                                break
                                            except Exception:
                                                pass
                            except Exception:
                                pass

                            # Fallback default
                            delay = max(1.0, (retry_after or 1.0) * backoff)
                            # Cap delay to a reasonable amount
                            delay = min(delay, 60.0)
                            await asyncio.sleep(delay)
                            backoff *= 2
                            # Give up after a handful of attempts
                            if attempts >= 6:
                                raise
                            continue
                        # Non-rate-limit HTTP exception -> re-raise
                        raise
                    except Exception as e:
                        # Other exceptions: set on future and break
                        if fut and not fut.done():
                            fut.set_exception(e)
                        break
            finally:
                await asyncio.sleep(self._delay)
                self._queue.task_done()

    def start(self):
        if self._worker_task is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                # No event loop is running yet; worker will be started later.
                return
            self._worker_task = loop.create_task(self._worker())

    async def schedule_coroutine(self, coro_func, *args, **kwargs):
        """Schedule an async callable (like channel.send or message.edit) and
        wait for it to complete under rate-limiting."""
        if self._worker_task is None:
            self.start()
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        await self._queue.put((coro_func, args, kwargs, fut))
        return await fut


# create a global sender with a conservative default (2 req/sec)
rate_limited_sender = RateLimitedSender(delay=0.5)
rate_limited_sender.start()

# --- ECONOMY ---


def parse_update_version(version: str) -> tuple[int, ...]:
    match = re.match(r"v(\d+)(?:\.(\d+))?", version)
    if not match:
        return (0,)
    major = int(match.group(1))
    minor = int(match.group(2) or 0)
    return (major, minor)


def get_sorted_update_versions(update_log: dict) -> list[str]:
    return sorted(update_log.keys(), key=parse_update_version, reverse=True)


def build_update_log_embed(version: str, update_log: dict) -> discord.Embed:
    entry = update_log.get(version, {})
    additions = entry.get("additions", [])
    if isinstance(additions, str):
        additions = [additions]
    elif not isinstance(additions, list):
        additions = list(additions or [])

    lines = [f"• {item}" for item in additions] if additions else ["• No details listed."]
    embed = discord.Embed(
        title=f"📝 Update Log — {version}",
        description="\n".join(lines),
        color=discord.Color.purple()
    )
    embed.add_field(name="Date", value=entry.get("date", "Unknown"), inline=True)
    if entry.get("footer"):
        embed.set_footer(text=entry["footer"])
    return embed


class UpdateLogView(discord.ui.View):
    def __init__(self, versions: list[str], update_log: dict):
        super().__init__(timeout=120)
        self.update_log = update_log
        if versions:
            options = [
                discord.SelectOption(label=version, description=f"View {version}", value=version)
                for version in versions
            ]
            self.select = discord.ui.Select(placeholder="Choose a previous update", min_values=1, max_values=1, options=options)
            self.select.callback = self.on_select
            self.add_item(self.select)

    async def on_select(self, interaction: discord.Interaction):
        selected_version = self.select.values[0]
        embed = build_update_log_embed(selected_version, self.update_log)
        await interaction.response.edit_message(embed=embed, view=self)

JACKPOT_CHANNEL_ID = 1522563511715237939

CASINO_DATA_FILE = "casino_data.json"
CASINO_CATEGORY_ID = 1521966391626961056
CASINO_WON_CURRENCY_EMOJI = "<:won:1521968473394253834>"
CASINO_STARDUST_CURRENCY_EMOJI = "<:stardust:1521968507225505822>"
WORK_COOLDOWN_SECONDS = 10
CHAT_MONEY_COOLDOWN_SECONDS = 10
CRIME_COOLDOWN_SECONDS = 30
HORSE_RACE_TIMEOUT_SECONDS = 120
ROB_COOLDOWN_SECONDS = 3600
ROULETTE_JOIN_TIME_SECONDS = 20
WORK_STATION_CHANNEL_ID = 1522305863530975423
WORK_REWARD_MIN = 75
WORK_REWARD_MAX = 150
WORK_STARDUST_REWARD_MIN = 3
WORK_STARDUST_REWARD_MAX = 4
CHAT_MONEY_REWARD_MIN = 50
CHAT_MONEY_REWARD_MAX = 70
MINIGAME_WIN_STARDUST_REWARD_MINIMUM = 5
MINIGAME_WIN_STARDUST_REWARD_MAXIMUM = 7
WORK_MESSAGES = [
    "You worked hard as a waiter and earned {amount} and {stardust}!",
    "Your shift at the cafe was a success and you pocketed {amount} and {stardust}!",
    "You hustled through the day and earned {amount} and {stardust} for your efforts!",
    "A long day of work paid off — you earned {amount} and {stardust}!"
]
LEVEL_START_XP = 10
LEVEL_XP_STEP = 8
LEVEL_BAR_LENGTH = 12


def get_store_items_for_guild(guild: discord.Guild | None = None) -> list[dict]:
    if guild and guild.id in STORE_ITEMS_BY_GUILD:
        return STORE_ITEMS_BY_GUILD[guild.id]
    return []


def get_purchasable_store_crates() -> list[dict]:
    purchasable = []
    for crate_id, crate in CRATES_TABLE.items():
        store_cfg = (crate.get("obtained_through", {}) or {}).get("store", {})
        if store_cfg.get("purchasable"):
            purchasable.append({
                "id": f"crate:{crate_id}",
                "name": crate.get("display_name", crate_id.replace("_", " ").title()),
                "crate_id": crate_id,
                "cost": int(store_cfg.get("cost", 0)),
                "currency_type": store_cfg.get("currency_type", "won_currency"),
                "display_emoji": crate.get("icon", "📦"),
                "type": "crate",
            })
    return purchasable

# --- CONFIGURATION CONSTANTS ---
LEADERBOARD_CHANNEL_ID = 1516775989051658342
SEASON_2_LB_CHANNEL_ID = 1517036103108923542
TOP_3_POWER_ROLE_ID = 1516775595693052026
POINTS_TOP_3_ROLE_ID = 1517138093596213308
MEMBERLIST_CHANNEL_ID = 1516171338174431252
PS_REQUEST_CHANNEL_ID = 1517134701629014048
PS_ADMIN_LOG_CHANNEL_ID = 1517191653759389696
ECONOMY_LOG_CHANNEL_ID = 1516160495411921068
DATA_FILE = "leaderboard.json"
REQUIRED_CLAN_ROLE_ID = 1516170905317806160
DEATHS_CHANNEL_ID = 1516855436123312179
DEATHS_INSPECT_ROLE_ID = 1516160494141046793
BOT_LOGS_CHANNEL_ID = 1517484734614081644
SERVER_BOOST_PING_ROLE_ID = 1516160494082330711
# Economy commands allowed channels per guild
ECONOMY_CHANNEL_IDS_BY_GUILD = {
    1521967226918277161: {1521967227925037240},  # test server
    1516160494082330705: {1520753085146730666}   # main server
}

def is_economy_channel(channel, user_id: int = None) -> bool:
    # Developers can use economy commands everywhere
    if user_id is not None and user_id in DEV_USER_IDS:
        return True
    if not channel:
        return False
    guild = getattr(channel, "guild", None)
    if guild is None:
        return False
    return getattr(channel, "id", None) in ECONOMY_CHANNEL_IDS_BY_GUILD.get(guild.id, set())

def economy_channels_mention(guild: discord.Guild | None = None) -> str:
    channel_ids = set()
    if guild and guild.id in ECONOMY_CHANNEL_IDS_BY_GUILD:
        channel_ids = ECONOMY_CHANNEL_IDS_BY_GUILD[guild.id]
    else:
        for ids in ECONOMY_CHANNEL_IDS_BY_GUILD.values():
            channel_ids |= ids
    return " ".join(f"<#{c}>" for c in channel_ids)

async def log_economy_action(user: discord.User | discord.Member, command_name: str, outcome: str, amount: int, entry: dict, details: str = "", guild: discord.Guild | None = None):
    channel = bot.get_channel(ECONOMY_LOG_CHANNEL_ID)
    if channel is None:
        try:
            channel = await bot.fetch_channel(ECONOMY_LOG_CHANNEL_ID)
        except Exception:
            return

    cash = entry.get("won", 0)
    bank = entry.get("bank", 0)
    total = cash + bank
    embed = discord.Embed(
        title="Economy Log",
        color=discord.Color.blurple(),
        timestamp=datetime.utcnow()
    )
    embed.add_field(name="User", value=f"{user} ({user.id})", inline=False)
    embed.add_field(name="Command", value=command_name, inline=True)
    embed.add_field(name="Result", value=outcome, inline=True)
    embed.add_field(name="Amount", value=f"{amount:,}", inline=True)
    embed.add_field(name="Cash", value=f"{cash:,}", inline=True)
    embed.add_field(name="Bank", value=f"{bank:,}", inline=True)
    embed.add_field(name="Total", value=f"{total:,}", inline=True)
    if details:
        embed.add_field(name="Details", value=details, inline=False)
    if guild:
        embed.set_footer(text=f"Guild: {guild.name} ({guild.id})")
    try:
        # Use the centralized rate-limited sender to avoid bursting requests
        await rate_limited_sender.schedule_coroutine(channel.send, embed=embed)
    except Exception:
        pass

SEASON_DURATIONS = {
    "1": "<t:1781377200:D> - <t:1781740800:D>",
    "2": "<t:1781740800:D> - <t:1782950400:D>"
}

# Fallback Developer IDs (always allowed to manage whitelist/permissions)
DEV_USER_IDS = {391670571454300195, 508376346280460289}

# URL generation tracking (prevents duplicate processing)
url_generation_active = False

# Slash command paths that should be restricted to the designated channel.
RESTRICTED_COMMAND_PATHS = {
    "help",
    "work",
    "profile",
    "store",
    "balance",
    "deposit",
    "withdraw",
    "coinflip",
    "pay",
    "blackjack",
    "rob",
    "lookup",
    "list",
    "check",
    "leaderboard",
    "race",
    "member",
    "member lookup",
    "member list",
    "member check",
    "member refresh",
    "member setname",
    "member remove",
    "member deaths",
    "member strike",
    "member kick",
    "horse race",
    "economy leaderboard",
}
RESTRICTED_COMMAND_CHANNEL_ID = 1516160495072051222
RESTRICTED_COMMAND_CHANNEL_URL = "https://discord.com/channels/1516160494082330705/1516160495072051222"
RESTRICTED_COMMAND_WARNING = f"<:warnicon:1522036105573171201> To avoid spam, you can only execute commands in {RESTRICTED_COMMAND_CHANNEL_URL}."
active_roulette_game = None  # Global roulette game state
active_crash_game = None  # Global crash game state

MINIGAME_CHANNEL_ID = 1520753085146730666
MINIGAME_ACTIVITY_WINDOW_SECONDS = 60
MINIGAME_COOLDOWN_SECONDS = 300
MINIGAME_TIMEOUT_SECONDS = 60
MINIGAME_WORDS = ["apple", "banana", "orange", "grape", "piano", "dragon", "planet", "rocket", "ghost", "flower"]
MINIGAME_COMMAND_BLOCK_MESSAGE = "You can't use commands in this channel until the minigame is over! Use /guess to participate."


def _get_interaction_command_names(interaction: discord.Interaction) -> set[str]:
    names: set[str] = set()

    command = getattr(interaction, "command", None)
    if command is not None:
        qualified_name = getattr(command, "qualified_name", "") or getattr(command, "name", "") or ""
        if qualified_name:
            names.add(qualified_name.lower().strip())
        name = getattr(command, "name", "") or ""
        if name:
            names.add(name.lower().strip())

    data = getattr(interaction, "data", None) or {}
    if isinstance(data, dict):
        command_name = data.get("name")
        if isinstance(command_name, str) and command_name:
            names.add(command_name.lower().strip())

        def walk_options(options, path_parts=None):
            if not isinstance(options, list):
                return

            current_parts = list(path_parts or [])
            for option in options:
                if not isinstance(option, dict):
                    continue

                option_name = option.get("name")
                if isinstance(option_name, str) and option_name:
                    next_parts = current_parts + [option_name.lower().strip()]
                    names.add(" ".join(next_parts))
                    walk_options(option.get("options"), next_parts)
                else:
                    walk_options(option.get("options"), current_parts)

        walk_options(data.get("options"), [command_name.lower().strip()] if isinstance(command_name, str) and command_name else None)

    return names


def should_restrict_command_channel(interaction: discord.Interaction) -> bool:
    if not interaction.guild:
        return False
    if interaction.user.id in DEV_USER_IDS:
        return False
    if interaction.channel_id == RESTRICTED_COMMAND_CHANNEL_ID:
        return False
    if is_economy_channel(interaction.channel, interaction.user.id):
        return False

    command_names = _get_interaction_command_names(interaction)
    if not command_names:
        return False

    normalized_names = {name.strip().lower() for name in command_names if isinstance(name, str)}
    return any(name in RESTRICTED_COMMAND_PATHS for name in normalized_names)


def should_block_minigame_commands(interaction: discord.Interaction) -> bool:
    return False


async def global_interaction_check(interaction: discord.Interaction) -> bool:
    if should_restrict_command_channel(interaction):
        await interaction.response.send_message(RESTRICTED_COMMAND_WARNING, ephemeral=True)
        return False
    return True

# Directly overwrite the tree's method with your function
bot.tree.interaction_check = global_interaction_check

# --- POWER MULTIPLIERS ---
POWER_SUFFIXES = {
    'K': 10**3, 'M': 10**6, 'B': 10**9, 'T': 10**12, 'QD': 10**15, 
    'QN': 10**18, 'SX': 10**21, 'SP': 10**24, 'OC': 10**27, 'N': 10**30, 'DE': 10**33,
    'UD': 10**36, 'DD': 10**39, 'TDD': 10**42, 'QDD': 10**45, 'QND': 10**48,
    'SXD': 10**51, 'SPD': 10**54, 'OCD': 10**57, 'NVD': 10**60, 'V': 10**63
}

def load_data():
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r") as f:
                data = json.load(f)
                
                if "players" in data and "power" not in data:
                    data = {
                        "message_id": data.get("message_id"),
                        "power": data["players"],
                        "points": {}
                    }
                if "power" not in data: data["power"] = {}
                if "points" not in data: data["points"] = {}
                if "season_dates" not in data: data["season_dates"] = {}
                if "whitelist" not in data: data["whitelist"] = []
                if "whitelisted_roles" not in data: data["whitelisted_roles"] = []
                if "command_roles" not in data: data["command_roles"] = {}
                if "usernames" not in data: data["usernames"] = {}
                if "join_dates" not in data: data["join_dates"] = {}
                if "current_season" not in data: data["current_season"] = "1"
                if "pending_ps_requests" not in data: data["pending_ps_requests"] = []
                if "deaths" not in data: data["deaths"] = {}
                if "private_server_access" not in data: data["private_server_access"] = []
                if "strikes" not in data: data["strikes"] = {}
                if "economy" not in data: data["economy"] = {}
                if "work_cooldowns" not in data: data["work_cooldowns"] = {}
                if "rob_cooldowns" not in data: data["rob_cooldowns"] = {}
                if "work_station_message_id" not in data: data["work_station_message_id"] = None
                if "work_station_channel_id" not in data: data["work_station_channel_id"] = None
                if "active_roulette_game" not in data: data["active_roulette_game"] = None
                if "guess_minigame_state" not in data: data["guess_minigame_state"] = {}
                if "guess_minigame_activity" not in data: data["guess_minigame_activity"] = {}
                
                bad_keys = [k for k in data["usernames"] if len(k) < 15]
                for k in bad_keys:
                    del data["usernames"][k]
                
                if data["points"] and not any(isinstance(v, dict) for v in data["points"].values()):
                    data["points"] = {"1": data["points"]}
                    
                return data
        except json.JSONDecodeError:
            pass
    return {
        "message_id": None,
        "power": {},
        "points": {"1": {}},
        "season_dates": {},
        "whitelist": [],
        "whitelisted_roles": [],
        "command_roles": {},
        "usernames": {},
        "join_dates": {},
        "current_season": "1",
        "pending_ps_requests": [],
        "private_server_access": [],
        "deaths": {},
        "strikes": {},
        "economy": {},
        "work_cooldowns": {},
        "rob_cooldowns": {},
        "work_station_message_id": None,
        "work_station_channel_id": None,
        "chat_cooldowns": {},
        "store_purchases": {},
        "active_roulette_game": None,
        "guess_minigame_state": {},
        "guess_minigame_activity": {}
    }


def save_data(data: dict) -> None:
    try:
        with open(DATA_FILE, "w") as f:
            json.dump(data, f, indent=4)
    except Exception:
        pass

    try:
        casino_subset = {
            "economy": data.get("economy", {}),
            "work_cooldowns": data.get("work_cooldowns", {}),
            "rob_cooldowns": data.get("rob_cooldowns", {}),
            "chat_cooldowns": data.get("chat_cooldowns", {}),
            "store_purchases": data.get("store_purchases", {}),
            "active_roulette_game": data.get("active_roulette_game"),
            "guess_minigame_state": data.get("guess_minigame_state", {}),
            "guess_minigame_activity": data.get("guess_minigame_activity", {})
        }
        with open(CASINO_DATA_FILE, "w") as f2:
            json.dump(casino_subset, f2, indent=4)
    except Exception:
        pass


def get_store_purchase_data(data: dict) -> dict:
    return data.setdefault("store_purchases", {})


def has_user_purchased_item(data: dict, item_id: str, user_id: int) -> bool:
    purchases = get_store_purchase_data(data)
    uid_str = str(user_id)
    return uid_str in purchases.setdefault(item_id, [])


def mark_user_purchased_item(data: dict, item_id: str, user_id: int) -> None:
    purchases = get_store_purchase_data(data)
    uid_str = str(user_id)
    item_purchases = purchases.setdefault(item_id, [])
    if uid_str not in item_purchases:
        item_purchases.append(uid_str)


def get_level_info(stardust: int) -> tuple[int, int, int]:
    level = 1
    remaining_xp = stardust
    required_xp = LEVEL_START_XP

    while remaining_xp >= required_xp:
        remaining_xp -= required_xp
        level += 1
        required_xp += LEVEL_XP_STEP

    return level, remaining_xp, required_xp


def get_bank_cap(level: int) -> int:
    cap = 0
    for required_level, value in sorted(BANK_CAPS_BY_LEVEL.items()):
        if level >= required_level:
            cap = value
        else:
            break
    return cap if cap > 0 else BANK_CAPS_BY_LEVEL.get(0, 10000)


def get_bet_cap(level: int) -> int:
    cap = 0
    for required_level, value in sorted(BET_CAPS_BY_LEVEL.items()):
        if level >= required_level:
            cap = value
        else:
            break
    return cap if cap > 0 else BET_CAPS_BY_LEVEL.get(0, 2000)


class BetCapConfirmationView(discord.ui.View):
    def __init__(self, bet_cap: int, on_confirm, on_cancel=None):
        super().__init__(timeout=60)
        self.bet_cap = bet_cap
        self.on_confirm = on_confirm
        self.on_cancel = on_cancel

    @discord.ui.button(label="Yes", style=discord.ButtonStyle.success)
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        if self.on_confirm:
            await self.on_confirm(interaction, self.bet_cap)

    @discord.ui.button(label="No", style=discord.ButtonStyle.danger)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        if self.on_cancel:
            await self.on_cancel(interaction)
        else:
            try:
                await interaction.message.edit(content="Bet cancelled.", view=None)
            except Exception:
                await interaction.followup.send("Bet cancelled.", ephemeral=True)


async def prompt_bet_cap_confirmation(interaction: discord.Interaction, bet_cap: int, on_confirm, on_cancel=None):
    view = BetCapConfirmationView(bet_cap, on_confirm, on_cancel)
    await interaction.response.send_message(
        f"Your bet exceeds your current bet cap ({bet_cap:,}) do you wish to bet that?",
        view=view,
        ephemeral=True,
    )


async def maybe_notify_level_milestone(interaction: discord.Interaction, user: discord.abc.User, prev_level: int, new_level: int):
    # Determine if any milestone thresholds were crossed (e.g., 5, 10)
    milestones = sorted(set(list(BANK_CAPS_BY_LEVEL.keys()) + list(BET_CAPS_BY_LEVEL.keys())))
    crossed = [m for m in milestones if prev_level < m <= new_level]
    if not crossed:
        return

    prev_bank = get_bank_cap(prev_level)
    new_bank = get_bank_cap(new_level)
    prev_bet = get_bet_cap(prev_level)
    new_bet = get_bet_cap(new_level)

    embed = discord.Embed(title="Level Milestone Unlocked!", color=discord.Color.green())
    embed.description = f"{user.mention}\n\n**Previous → New**"
    embed.add_field(name="Bank Cap", value=f"{prev_bank:,} <:d_arrow:1522026441984704672> {new_bank:,}", inline=False)
    embed.add_field(name="Bet Cap", value=f"{prev_bet:,} <:d_arrow:1522026441984704672> {new_bet:,}", inline=False)

    try:
        await interaction.followup.send(embed=embed)
    except Exception:
        try:
            await interaction.channel.send(embed=embed)
        except Exception:
            pass


def enforce_balance_cap(entry: dict, level: int) -> None:
    cap = get_bank_cap(level)
    if entry.get("bank", 0) > cap:
        entry["bank"] = cap


def format_level_bar(progress: float) -> str:
    filled = int(round(progress * LEVEL_BAR_LENGTH))
    filled = max(0, min(LEVEL_BAR_LENGTH, filled))
    empty = LEVEL_BAR_LENGTH - filled

    parts = []
    if filled > 0:
        parts.append("<:full_left:1521975405005897769>")
    else:
        parts.append("<:empty_left:1521975374278561802>")

    for i in range(1, LEVEL_BAR_LENGTH - 1):
        if i < filled:
            parts.append("<:full_middle:1521975443304222960>")
        else:
            parts.append("<:empty_middle:1521975347065917641>")

    if filled >= LEVEL_BAR_LENGTH:
        parts.append("<:full_right:1521975479085568000>")
    else:
        parts.append("<:empty_right:1521975292539699220>")

    return "".join(parts)


def is_authorized(interaction: discord.Interaction, command_name: str, data: dict) -> bool:
    if interaction.user.id in DEV_USER_IDS:
        return True
    
    # Old user ID whitelist fallback
    if interaction.user.id in data.get("whitelist", []):
        return True

    if isinstance(interaction.user, discord.Member):
        user_role_ids = {r.id for r in interaction.user.roles}
        
        # Check global whitelisted roles
        if any(r_id in data.get("whitelisted_roles", []) for r_id in user_role_ids):
            return True
            
        # Check command-specific roles
        allowed_roles = data.get("command_roles", {}).get(command_name.lower(), [])
        if any(r_id in allowed_roles for r_id in user_role_ids):
            return True

    return False

def parse_power(power_str: str) -> float:
    power_str = power_str.strip().upper()
    match = re.match(r"^([0-9.]+)\s*([A-Z]*)$", power_str)
    if not match:
        raise ValueError("Invalid format.")
    
    value = float(match.group(1))
    suffix = match.group(2)
    
    if suffix:
        if suffix in POWER_SUFFIXES:
            value *= POWER_SUFFIXES[suffix]
        else:
            raise ValueError(f"Unknown shorthand suffix: '{suffix}'")
    return value

def format_power(value: float) -> str:
    if value < 1000:
        return f"{value:.3f}".rstrip('0').rstrip('.')
    
    for suffix, multiplier in sorted(POWER_SUFFIXES.items(), key=lambda x: x[1], reverse=True):
        if value >= multiplier:
            scaled = value / multiplier
            formatted = f"{scaled:.3f}".rstrip('0').rstrip('.')
            return f"{formatted}{suffix}"
    return str(value)

def get_user_power_rank(user_id: int, power_dict: dict) -> str:
    if str(user_id) not in power_dict:
        return "Unranked"
    sorted_players = sorted(power_dict.items(), key=lambda x: x[1], reverse=True)
    for index, (uid, _) in enumerate(sorted_players):
        if int(uid) == user_id:
            return f"#{index + 1}"
    return "Unranked"

def extract_leaderboard_stats(message_content: str) -> dict:
    stats = {}
    clean_prefix_pattern = r"^(?:#\s*\d+\s*[-|]?\s*)?"
    pattern = r"^(?P<username>[^\(—\-]+?)(?:\s*\((?P<uid>\d+)\))?\s*[—\-]\s*(?P<val>[0-9\.KMBTQDNSXPO]+)"
    
    for line in message_content.split('\n'):
        cleaned_line = line.replace('`', '').replace('*', '').strip()
        cleaned_line = re.sub(clean_prefix_pattern, "", cleaned_line)
        
        match = re.search(pattern, cleaned_line)
        if match:
            username = match.group("username").strip()
            uid = match.group("uid")
            try:
                val = parse_power(match.group("val"))
                stats[username] = {
                    "username": username,
                    "value": val,
                    "uid": uid
                }
            except ValueError:
                pass
    return stats

def generate_power_page_embed(guild: discord.Guild, page: int) -> discord.Embed:
    data = load_data()
    players_dict = data.get("power", {})
    usernames_data = data.get("usernames", {})
    
    # Filter to only show users who are still in the server
    active_players = {}
    for uid, val in players_dict.items():
        if uid.isdigit() and guild.get_member(int(uid)) is not None:
            active_players[uid] = val
            
    sorted_players = sorted(active_players.items(), key=lambda x: x[1], reverse=True)
    
    title = "Kimetsu's Strongest Members"
    desc = ""
    
    if page == 0:
        desc += f"**Top 3** - <@&{TOP_3_POWER_ROLE_ID}>\n\n"
        
    start_idx = page * 10
    end_idx = start_idx + 10
    
    lines = []
    for i in range(start_idx, end_idx):
        pos = i + 1
        if i < len(sorted_players):
            uid, val = sorted_players[i]
            mention = f"<@{uid}>"
            ingame_name = usernames_data.get(str(uid), "Unknown")
            display_value = format_power(val)
        else:
            mention = "<@>"
            ingame_name = "Unknown"
            display_value = "TBD"
            
        if pos == 1: emoji_str = "🥇"
        elif pos == 2: emoji_str = "🥈"
        elif pos == 3: emoji_str = "🥉"
        else: emoji_str = f"#{pos} 🏅"
            
        lines.append(f"{emoji_str} {mention}: `{ingame_name}` | {display_value}")
        
        if pos == 3: lines.append("")
            
    lines.append("")
    total_entries = len(sorted_players)
    max_pages = max(1, (total_entries + 9) // 10)
    lines.append(f"*Page {page + 1}/{max_pages}*")
    
    if page == 0:
        lines.append("\nSend your total power in https://discord.com/channels/1516160494082330705/1516776477918494792 to get ranked.")
        
    desc += "\n".join(lines)
    embed = discord.Embed(title=title, description=desc, color=discord.Color.blue())
    return embed

def calculate_avg_daily_points(points: float, season_str: str) -> float:
    """Calculates the average points earned per day based on elapsed season time."""
    if season_str not in SEASON_DURATIONS:
        return 0.0
    
    # Extract the start and end Unix timestamps from the Discord timestamp string
    timestamps = re.findall(r'<t:(\d+):', SEASON_DURATIONS[season_str])
    if len(timestamps) < 2:
        return 0.0
        
    start_ts = int(timestamps[0])
    end_ts = int(timestamps[1])
    
    current_ts = datetime.now().timestamp()
    
    # If the season hasn't started yet, rate is 0
    if current_ts < start_ts:
        return 0.0
        
    # Cap the elapsed time calculation at the end of the season
    end_calculation_ts = min(current_ts, end_ts)
    
    days_elapsed = (end_calculation_ts - start_ts) / 86400.0
    
    # Prevent division by zero or extreme spikes at the very beginning minute of a season
    if days_elapsed < 0.1:
        days_elapsed = 0.1
        
    return points / days_elapsed

def generate_points_leaderboard_embed(guild: discord.Guild, season_str: str = "1", page: int = 0, active_keys: list = None) -> discord.Embed:
    data = load_data()
    
    title = f"Kimetsu's Season {season_str} Top Point Earners"
    desc = ""
    
    # NEW: Fetch duration from hardcoded constants
    if season_str in SEASON_DURATIONS:
        desc += f"**Season duration:**\n{SEASON_DURATIONS[season_str]}\n\n"
        
    desc += f"**Top 3** - <@&{POINTS_TOP_3_ROLE_ID}>\n\n"
        
    points_data = data.get("points", {})
    usernames_data = data.get("usernames", {})
    players_dict = points_data.get(season_str, {})
    
    # Filter for Season 2 using the live active names from the latest channel message
    if season_str == "2" and active_keys is not None:
        active_set = {k.lower() for k in active_keys}
        filtered_players = {}
        for k, v in players_dict.items():
            if k.isdigit():  # Stored by Discord ID
                uname = usernames_data.get(k, "").lower()
                if uname in active_set:
                    filtered_players[k] = v
            else:  # Stored by raw string name
                if k.lower() in active_set:
                    filtered_players[k] = v
        players_dict = filtered_players
    
    # Reverse lookup mapping for cases where names are stored as keys instead of IDs
    discord_by_name = {name.lower(): uid for uid, name in usernames_data.items()}
    
    # --- FIX: Consolidate duplicate entries (Name vs Discord ID) ---
    consolidated_players = {}
    for key, val in players_dict.items():
        if key.isdigit():
            # Already an ID, keep or update with highest value
            consolidated_players[key] = max(consolidated_players.get(key, 0), val)
        elif key.lower() in discord_by_name:
            # It's a name string, but we now have an ID for it -> merge into the ID key
            uid = discord_by_name[key.lower()]
            consolidated_players[uid] = max(consolidated_players.get(uid, 0), val)
        else:
            # Unlinked name string, keep as is
            consolidated_players[key] = max(consolidated_players.get(key, 0), val)

    # Grab all tracked players for the season after deduplication
    sorted_players = sorted(consolidated_players.items(), key=lambda x: x[1], reverse=True)
    # ---------------------------------------------------------------
    
    lines = []
    start_idx = page * 10
    end_idx = start_idx + 10
    
    for i in range(start_idx, end_idx):
        pos = i + 1
        if i < len(sorted_players):
            key, val = sorted_players[i]
            ingame_name = key
            mention = "@Unknown"

            if key.isdigit():
                ingame_name = usernames_data.get(key, key)
                member = guild.get_member(int(key))
                if member is not None:
                    mention = f"<@{key}>"
            else:
                matched_uid = discord_by_name.get(key.lower())
                if matched_uid is not None:
                    ingame_name = key
                    member = guild.get_member(int(matched_uid))
                    if member is not None:
                        mention = f"<@{matched_uid}>"
                else:
                    ingame_name = key

            # Calculate and append daily average points
            avg_daily = calculate_avg_daily_points(val, season_str)
            avg_str = f" (*{int(avg_daily):,}/day*)" if avg_daily > 0 else ""
            display_value = f"{int(val):,}{avg_str}"
        else:
            mention = "<@>"
            ingame_name = "Unknown"
            display_value = "0"
            
        if pos == 1: emoji_str = "🥇"
        elif pos == 2: emoji_str = "🥈"
        elif pos == 3: emoji_str = "🥉"
        else: emoji_str = f"#{pos} 🏅"
            
        lines.append(f"{emoji_str} {mention}: `{ingame_name}` | {display_value}")
        if pos == 3: lines.append("")
            
    lines.append("")
    total_entries = len(sorted_players)
    max_pages = max(1, (total_entries + 9) // 10)
    lines.append(f"*Page {page + 1}/{max_pages}*")
    lines.append(f"\nShowing ranks finalized for Season {season_str}. *Note: You must have been in the Discord server during the season.*")
    
    desc += "\n".join(lines)
    embed = discord.Embed(title=title, description=desc, color=discord.Color.gold())
    return embed

async def update_top_3_power_role(guild: discord.Guild):
    if not guild: return
    data = load_data()
    power_players = data.get("power", {})
    sorted_players = sorted(power_players.items(), key=lambda x: x[1], reverse=True)

    role = guild.get_role(TOP_3_POWER_ROLE_ID)
    if role and sorted_players:
        top_3_ids = [int(p[0]) for p in sorted_players[:3]]
        
        for member in role.members:
            if member.id not in top_3_ids:
                try: await member.remove_roles(role, reason="No longer Top 3 Power")
                except discord.DiscordException: pass
                
        for top_id in top_3_ids:
            try:
                top_member = await guild.fetch_member(top_id)
                if role not in top_member.roles:
                    await top_member.add_roles(role, reason="Top 3 Power")
            except discord.DiscordException: pass

async def update_points_top_3_role(guild: discord.Guild):
    if not guild: return
    data = load_data()
    current_season = data.get("current_season", "1")
    points_players = data.get("points", {}).get(current_season, {})
    
    valid_players = {k: v for k, v in points_players.items() if k.isdigit()}
    sorted_players = sorted(valid_players.items(), key=lambda x: x[1], reverse=True)

    role = guild.get_role(POINTS_TOP_3_ROLE_ID)
    if role and sorted_players:
        top_3_ids = [int(p[0]) for p in sorted_players[:3]]
        
        for member in role.members:
            if member.id not in top_3_ids:
                try: await member.remove_roles(role, reason="No longer Top 3 Points")
                except discord.DiscordException: pass
                
        for top_id in top_3_ids:
            try:
                top_member = await guild.fetch_member(top_id)
                if role not in top_member.roles:
                    await top_member.add_roles(role, reason="Top 3 Points")
            except discord.DiscordException: pass

# --- LEADERBOARD PAGINATION & VIEWS ---
class PowerPaginationView(discord.ui.View):
    def __init__(self, guild: discord.Guild, current_page: int = 0):
        super().__init__(timeout=60)
        self.guild = guild
        self.current_page = current_page
        self.update_buttons()

    def update_buttons(self):
        data = load_data()
        players_dict = data.get("power", {})
        total_players = sum(1 for uid in players_dict if uid.isdigit() and self.guild.get_member(int(uid)) is not None)
        self.prev_page.disabled = (self.current_page == 0)
        self.next_page.disabled = ((self.current_page + 1) * 10 >= total_players)

    @discord.ui.button(label="◀ Previous", style=discord.ButtonStyle.secondary)
    async def prev_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page -= 1
        self.update_buttons()
        embed = generate_power_page_embed(interaction.guild, self.current_page)
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.primary)
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page += 1
        self.update_buttons()
        embed = generate_power_page_embed(interaction.guild, self.current_page)
        await interaction.response.edit_message(embed=embed, view=self)


class PointsPaginationView(discord.ui.View):
    def __init__(self, guild: discord.Guild, season_str: str, current_page: int = 0, active_keys: list = None):
        super().__init__(timeout=60)
        self.guild = guild
        self.season_str = season_str
        self.current_page = current_page
        self.active_keys = active_keys
        self.update_buttons()

    def update_buttons(self):
        data = load_data()
        players_dict = data.get("points", {}).get(self.season_str, {})
        usernames_data = data.get("usernames", {})
        
        # Filter for Season 2 calculation inside the pagination buttons
        if self.season_str == "2" and self.active_keys is not None:
            active_set = {k.lower() for k in self.active_keys}
            filtered_players = {}
            for k, v in players_dict.items():
                if k.isdigit():
                    uname = usernames_data.get(k, "").lower()
                    if uname in active_set:
                        filtered_players[k] = v
                else:
                    if k.lower() in active_set:
                        filtered_players[k] = v
            players_dict = filtered_players
        
        total_players = len(players_dict)
                    
        self.prev_page.disabled = (self.current_page == 0)
        self.next_page.disabled = ((self.current_page + 1) * 10 >= total_players)

    @discord.ui.button(label="◀ Previous", style=discord.ButtonStyle.secondary)
    async def prev_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page -= 1
        self.update_buttons()
        embed = generate_points_leaderboard_embed(interaction.guild, self.season_str, self.current_page, active_keys=self.active_keys)
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.primary)
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page += 1
        self.update_buttons()
        embed = generate_points_leaderboard_embed(interaction.guild, self.season_str, self.current_page, active_keys=self.active_keys)
        await interaction.response.edit_message(embed=embed, view=self)

def generate_gain_page_embed(gains: list, time_diff_mins: float, page: int) -> discord.Embed:
    embed = discord.Embed(
        title="🏆 Clan Point Gain Velocity Leaderboard",
        description=f"Calculated over a **{time_diff_mins:.1f}** minute window.",
        color=discord.Color.purple()
    )
    start_idx = page * 10
    end_idx = start_idx + 10
    
    lines = []
    for idx in range(start_idx, min(end_idx, len(gains))):
        pos = idx + 1
        mention, username, current_pts, rate = gains[idx]
        
        formatted_rate = format_power(rate)
        formatted_pts = format_power(current_pts)
        
        lines.append(f"**#{pos}** {mention}: `{username}` | {formatted_pts} | ({formatted_rate}/min)")
        
    embed.description += "\n\n" + "\n".join(lines)
    max_pages = max(1, (len(gains) + 9) // 10)
    embed.set_footer(text=f"Page {page + 1}/{max_pages}")
    return embed

class CalculateGainPaginationView(discord.ui.View):
    def __init__(self, gains: list, time_diff_mins: float, current_page: int = 0):
        super().__init__(timeout=120)
        self.gains = gains
        self.time_diff_mins = time_diff_mins
        self.current_page = current_page
        self.update_buttons()

    def update_buttons(self):
        total_players = len(self.gains)
        self.prev_page.disabled = (self.current_page == 0)
        self.next_page.disabled = ((self.current_page + 1) * 10 >= total_players)

    @discord.ui.button(label="◀ Previous", style=discord.ButtonStyle.secondary)
    async def prev_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page -= 1
        self.update_buttons()
        embed = generate_gain_page_embed(self.gains, self.time_diff_mins, self.current_page)
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.primary)
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page += 1
        self.update_buttons()
        embed = generate_gain_page_embed(self.gains, self.time_diff_mins, self.current_page)
        await interaction.response.edit_message(embed=embed, view=self)

class SeasonSelectionView(discord.ui.View):
    def __init__(self, seasons_list: list):
        super().__init__(timeout=60)
        for season in seasons_list:
            button = discord.ui.Button(label=f"Season {season}", style=discord.ButtonStyle.secondary, custom_id=f"season_select:{season}")
            button.callback = self.create_callback(season)
            self.add_item(button)

    def create_callback(self, season: str):
        async def callback(interaction: discord.Interaction):
            active_keys = None
            if str(season) == "2":
                channel = interaction.guild.get_channel(SEASON_2_LB_CHANNEL_ID)
                if channel:
                    try:
                        # Grab the absolute newest history entry from the channel to live-filter
                        async for msg in channel.history(limit=1):
                            content = msg.content or (msg.embeds[0].description if msg.embeds else "")
                            parsed_data = extract_leaderboard_stats(content)
                            if parsed_data:
                                active_keys = list(parsed_data.keys())
                    except Exception as e:
                        print(f"Error fetching latest season 2 message: {e}")
                        
            embed = generate_points_leaderboard_embed(interaction.guild, season_str=season, page=0, active_keys=active_keys)
            view = PointsPaginationView(interaction.guild, season_str=season, current_page=0, active_keys=active_keys)
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        return callback


class LeaderboardHubView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Total Power", style=discord.ButtonStyle.primary, custom_id="hub_view:total_power")
    async def total_power(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = generate_power_page_embed(interaction.guild, 0)
        view = PowerPaginationView(interaction.guild, current_page=0)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @discord.ui.button(label="Clan Points", style=discord.ButtonStyle.primary, custom_id="hub_view:clan_points")
    async def clan_points(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = load_data()
        points_data = data.get("points", {})
        seasons = sorted(list(points_data.keys()), key=lambda x: int(x) if x.isdigit() else 0)
        if not seasons: seasons = ["1"]
        view = SeasonSelectionView(seasons)
        await interaction.response.send_message(content="Select a season leaderboard to review:", view=view, ephemeral=True)

def generate_deaths_page_embed(ingame_name: str, deaths: list, page: int) -> discord.Embed:
    title = f"💀 Death History for `{ingame_name}`"
    
    start_idx = page * 10
    end_idx = start_idx + 10
    
    lines = []
    for i in range(start_idx, min(end_idx, len(deaths))):
        d = deaths[i]
        try:
            dt = datetime.fromisoformat(d["timestamp"])
            time_str = f"<t:{int(dt.timestamp())}:f>"  # Live Discord short date + time format
        except Exception:
            time_str = "Unknown Time"
            
        lines.append(f"**{i+1}.** {time_str} | `{d.get('last_damage', 'Unknown')}`")
        
    if not lines:
        lines.append("No records found on this page.")
        
    lines.append("")
    total_entries = len(deaths)
    max_pages = max(1, (total_entries + 9) // 10)
    lines.append(f"*Page {page + 1}/{max_pages}*")
    
    embed = discord.Embed(description="\n".join(lines), color=discord.Color.dark_theme())
    embed.title = title
    return embed


class DeathsPaginationView(discord.ui.View):
    def __init__(self, ingame_name: str, deaths: list, current_page: int = 0):
        super().__init__(timeout=60)
        self.ingame_name = ingame_name
        self.deaths = deaths
        self.current_page = current_page
        self.update_buttons()

    def update_buttons(self):
        total_deaths = len(self.deaths)
        self.prev_page.disabled = (self.current_page == 0)
        self.next_page.disabled = ((self.current_page + 1) * 10 >= total_deaths)

    @discord.ui.button(label="◀ Previous", style=discord.ButtonStyle.secondary)
    async def prev_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page -= 1
        self.update_buttons()
        embed = generate_deaths_page_embed(self.ingame_name, self.deaths, self.current_page)
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.primary)
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page += 1
        self.update_buttons()
        embed = generate_deaths_page_embed(self.ingame_name, self.deaths, self.current_page)
        await interaction.response.edit_message(embed=embed, view=self)

# --- PRIVATE SERVER VIEWS ---
class AdminRequestDecisionView(discord.ui.View):
    def __init__(self, target_user_id: int):
        super().__init__(timeout=None)
        self.target_user_id = target_user_id

    def remove_pending(self):
        data = load_data()
        uid_str = str(self.target_user_id)
        if uid_str in data.get("pending_ps_requests", []):
            data["pending_ps_requests"].remove(uid_str)
            save_data(data)

    @discord.ui.button(label="Accept", style=discord.ButtonStyle.success)
    async def accept_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.remove_pending()
        # Grant persistent private server access
        data = load_data()
        uid_str = str(self.target_user_id)
        if uid_str not in data.get("private_server_access", []):
            data.setdefault("private_server_access", []).append(uid_str)
            save_data(data)

        user = await interaction.client.fetch_user(self.target_user_id)
        if user:
            try:
                await user.send("You are now in the private server, check your server list!")
            except discord.Forbidden:
                pass
        
        for item in self.children:
            item.disabled = True
            
        await interaction.response.edit_message(
            content=f"{interaction.message.content}\n\n*✅ Accepted by {interaction.user.mention}*", 
            view=self
        )

    @discord.ui.button(label="Deny", style=discord.ButtonStyle.danger)
    async def deny_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.remove_pending()
        # Ensure the user does not retain access if denied
        data = load_data()
        uid_str = str(self.target_user_id)
        if uid_str in data.get("private_server_access", []):
            try:
                data["private_server_access"].remove(uid_str)
                save_data(data)
            except ValueError:
                pass

        user = await interaction.client.fetch_user(self.target_user_id)
        if user:
            try:
                await user.send("You couldn't be added to the private server due to your privacy settings. Please turn Private Servers -> EVERYONE in your settings and try again.")
            except discord.Forbidden:
                pass
        
        for item in self.children:
            item.disabled = True
            
        await interaction.response.edit_message(
            content=f"{interaction.message.content}\n\n*❌ Denied by {interaction.user.mention}*", 
            view=self
        )


class PrivateServerRequestView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Request", style=discord.ButtonStyle.success, custom_id="ps_request_btn")
    async def request_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if isinstance(interaction.user, discord.Member):
            has_role = any(role.id == REQUIRED_CLAN_ROLE_ID for role in interaction.user.roles)
            if not has_role:
                await interaction.response.send_message(
                    "You're not a Clan Member. To be able to request the private server you must go through https://discord.com/channels/1516160494082330705/1516176749237370950 first.", 
                    ephemeral=True
                )
                return

        data = load_data()
        uid_str = str(interaction.user.id)
        
        if uid_str in data.get("pending_ps_requests", []):
            await interaction.response.send_message(
                "You already have a pending private server request. Please wait until it is accepted or denied.", 
                ephemeral=True
            )
            return

        ingame_name = data.get("usernames", {}).get(uid_str)

        if not ingame_name:
            await interaction.response.send_message(
                "You must set your username in https://discord.com/channels/1516160494082330705/1516171338174431252 before doing this!", 
                ephemeral=True
            )
            return

        data.setdefault("pending_ps_requests", []).append(uid_str)
        save_data(data)

        await interaction.response.send_message("Request sent! You'll be DM'd shortly about your request.", ephemeral=True)
        
        admin_channel = interaction.guild.get_channel(PS_ADMIN_LOG_CHANNEL_ID)
        if admin_channel:
            view = AdminRequestDecisionView(interaction.user.id)
            await admin_channel.send(
                f"🔔 {interaction.user.mention} requested to join the private server (`{ingame_name}`).", 
                view=view
            )


# --- EVENT LISTENERS ---
async def compensate_roulette_restart(data: dict, game_data: dict, bot_client: commands.Bot):
    players = game_data.get("players", {}) or {}
    for player_id, player_data in players.items():
        try:
            entry = get_or_create_economy_entry(data, int(player_id))
        except Exception:
            continue

        refund_amount = player_data.get("bet", 0)
        if refund_amount <= 0:
            continue

        entry["won"] = entry.get("won", 0) + refund_amount
        enforce_balance_cap(entry, get_level_info(entry.get("stardust", 0))[0])

        try:
            user = await bot_client.fetch_user(int(player_id))
            embed = discord.Embed(
                title="🎡 Roulette Cancelled",
                description=(
                    f"Your unfinished roulette bet of **{refund_amount:,} {CASINO_WON_CURRENCY_EMOJI}** "
                    "was refunded after a bot restart."
                ),
                color=discord.Color.orange()
            )
            try:
                await rate_limited_sender.schedule_coroutine(user.send, embed=embed)
            except Exception:
                pass
        except Exception:
            pass

    data["active_roulette_game"] = None
    save_data(data)


async def run_crash_runner(game_data: dict, bot_client: commands.Bot):
    """Runs the crash multiplier loop: increases multiplier until crash and handles payouts."""
    global active_crash_game
    data = load_data()
    channel = None
    try:
        channel = await bot_client.fetch_channel(game_data.get("channel_id"))
    except Exception:
        pass

    # Mark as running
    game_data["state"] = "running"
    crash_point = float(game_data.get("crash_point", 0))
    multiplier = 1.0
    game_data["current_multiplier"] = multiplier
    active_crash_game = game_data
    save_data(data)

    # fetch message to edit
    message = None
    try:
        if game_data.get("message_id") and channel:
            message = await channel.fetch_message(game_data.get("message_id"))
    except Exception:
        message = None

    start_ts = datetime.now().timestamp()
    # Growth loop
    try:
        while multiplier < crash_point:
            # slower exponential-ish growth for a longer game
            multiplier = round(multiplier * 1.008 + 0.002, 2)
            game_data["current_multiplier"] = multiplier
            # update embed message
            try:
                if message:
                    embed = discord.Embed(title="📈 Crash — Running", description=f"Multiplier: **{multiplier:.2f}x**\nCrash at unknown point...", color=discord.Color.gold())
                    await rate_limited_sender.schedule_coroutine(message.edit, embed=embed)
            except Exception:
                pass

            await asyncio.sleep(1)

            # small safety to avoid infinite loops
            if datetime.now().timestamp() - start_ts > 60:
                break

        # Crash occurred
        game_data["state"] = "crashed"
        game_data["current_multiplier"] = 0.0

        # handle payouts for those who cashed out
        data = load_data()
        economy = data.setdefault("economy", {})
        results_lines = []
        for uid_str, p in game_data.get("players", {}).items():
            try:
                uid = int(uid_str)
            except Exception:
                continue
            entry = get_or_create_economy_entry(data, uid)
            if p.get("cashed_out", False):
                cm = float(p.get("cashout_multiplier", 0.0))
                base_payout = int(p.get("bet", 0) * cm)
                payout, _ = get_reward_with_role_bonus(base_payout, "crash", data, uid, channel.guild if channel else None)
                entry["won"] = entry.get("won", 0) + payout
                results_lines.append(f"<@{uid}> cashed out at {cm:.2f}x and received {payout:,} {CASINO_WON_CURRENCY_EMOJI}")
                await log_economy_action(
                    discord.Object(id=uid),
                    "crash",
                    "Cashed out",
                    payout,
                    entry,
                    details=f"Cashed out at {cm:.2f}x",
                    guild=channel.guild if channel else None
                )
            else:
                # lost already (bet was deducted when joining)
                results_lines.append(f"<@{uid}> lost their bet of {p.get('bet',0):,} {CASINO_WON_CURRENCY_EMOJI}")

        save_data(data)

        # Edit final message
        try:
            if message:
                embed = discord.Embed(title="💥 Crash — Game Over", description="\n".join(results_lines) or "No players.", color=discord.Color.red())
                await message.edit(embed=embed, view=None)
        except Exception:
            pass

    finally:
        active_crash_game = None
        # clear persisted active crash if stored
        try:
            data = load_data()
            data["active_crash_game"] = None
            save_data(data)
        except Exception:
            pass


@bot.event
async def on_ready():
    print(f'--- Leaderboard Bot Online ---')
    bot.add_view(LeaderboardHubView())
    bot.add_view(PrivateServerRequestView())
    bot.add_view(WorkStationButton())
    
    data = load_data()
    active_roulette_data = data.get("active_roulette_game")
    if isinstance(active_roulette_data, dict):
        now_ts = datetime.now().timestamp()
        expires_at = active_roulette_data.get("expires_at")
        if expires_at and now_ts < expires_at:
            remaining = max(0, int(expires_at - now_ts))
            active_roulette_game = active_roulette_data
            view = RouletteJoinView(active_roulette_game, bot, timeout=remaining)
            try:
                channel = bot.get_channel(active_roulette_data.get("channel_id"))
                if channel:
                    message = await channel.fetch_message(int(active_roulette_data.get("message_id")))
                    view.message = message
                    bot.add_view(view)
            except Exception:
                pass
        else:
            # End any expired roulette game on bot restart
            active_roulette_game = active_roulette_data
            view = RouletteJoinView(active_roulette_game, bot, timeout=0)
            try:
                channel = bot.get_channel(active_roulette_data.get("channel_id"))
                if channel and active_roulette_data.get("message_id"):
                    message = await channel.fetch_message(int(active_roulette_data.get("message_id")))
                    view.message = message
                    await view.on_timeout()
                else:
                    await compensate_roulette_restart(data, active_roulette_data, bot)
            except Exception:
                await compensate_roulette_restart(data, active_roulette_data, bot)

    try:
        synced = await bot.tree.sync()
        print(f"Synced {len(synced)} slash command(s).")
    except Exception as e:
        print(f"Failed to sync slash commands: {e}")

    # Check for uncompensated active blackjack games
    try:
        data = load_data()
        active_games = data.get("active_blackjack_games", {})
        if active_games:
            print(f"Found {len(active_games)} uncompensated blackjack game(s). Compensating players...")
            for message_id_str, game_data in list(active_games.items()):
                try:
                    user_id = game_data.get("user_id")
                    channel_id = game_data.get("channel_id")
                    guild_id = game_data.get("guild_id")
                    bet = game_data.get("bet", 0)
                    
                    # Try to fetch and edit the message
                    try:
                        channel = bot.get_channel(channel_id)
                        if channel:
                            message = await channel.fetch_message(int(message_id_str))
                            cancel_embed = discord.Embed(
                                title="🃏 Blackjack — Match Cancelled",
                                description="The bot was restarted before this game could complete. You have been compensated.",
                                color=discord.Color.red()
                            )
                            await message.edit(embed=cancel_embed, view=None)
                    except Exception:
                        pass  # Message may not exist anymore
                    
                    # Compensate the player
                    entry = get_or_create_economy_entry(data, user_id)
                    entry["won"] = entry.get("won", 0) + bet
                    enforce_balance_cap(entry, get_level_info(entry.get("stardust", 0))[0])
                    
                    # DM the player
                    try:
                        user = await bot.fetch_user(user_id)
                        compensation_embed = discord.Embed(
                            title="💸 Blackjack Compensation",
                            description=f"Your unfinished blackjack game was cancelled due to a bot restart. You have been compensated **{bet:,}** {CASINO_WON_CURRENCY_EMOJI}.",
                            color=discord.Color.gold()
                        )
                        await user.send(embed=compensation_embed)
                    except Exception:
                        pass  # User may have DMs disabled
                    
                    # Remove from active games
                    del active_games[message_id_str]
                except Exception as e:
                    print(f"Error compensating player for game {message_id_str}: {e}")
            
            save_data(data)
            print("Blackjack compensation completed.")
    except Exception as e:
        print(f"Error processing blackjack compensation: {e}")

    asyncio.create_task(update_work_station_message())
    asyncio.create_task(update_guess_minigame_state())


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: Exception):
    # Unwrap CommandInvokeError to the original exception when possible
    orig = getattr(error, 'original', error)
    try:
        if isinstance(orig, discord.errors.HTTPException):
            status = getattr(orig, 'status', None)
            http_code = getattr(orig, 'code', None)
            text = str(orig)
            if (
                status == 429
                or http_code == 0
                or 'Too Many Requests' in text
                or 'cloudflare' in text.lower()
                or '1015' in text
            ):
                # Inform the user without raising further
                try:
                    # Use response if not yet responded, otherwise followup
                    if not interaction.response.is_done():
                        await interaction.response.send_message("⚠️ The bot is being rate limited. Please try again shortly.", ephemeral=True)
                    else:
                        await interaction.followup.send("⚠️ The bot is being rate limited. Please try again shortly.", ephemeral=True)
                except Exception:
                    pass
                return
    except Exception:
        pass
    # Fallback: log unexpected errors to console for visibility
    try:
        print(f"App command error: {error}")
    except Exception:
        pass

async def build_work_station_embed() -> discord.Embed:
    embed = discord.Embed(
        title="💼 Work Station",
        description="Ready to earn some **Won**? Click the button below to work!",
        color=discord.Color.gold()
    )
    rewards_value = f"• **{WORK_REWARD_MIN}-{WORK_REWARD_MAX}** Won\n• **{WORK_STARDUST_REWARD_MIN}-{WORK_STARDUST_REWARD_MAX}** Stardust"
    crate_lines = []
    for crate_id, crate in CRATES_TABLE.items():
        wt = (crate.get("obtained_through", {}) or {}).get("work", {})
        if wt and wt.get("obtainable", False):
            try:
                chance = float(wt.get("chance", 0)) * 100
            except Exception:
                chance = 0.0
            display_name = crate.get("display_name", crate_id.replace("_", " ").title())
            crate_lines.append(f"• **{int(chance) if float(chance).is_integer() else round(chance,1)}%** Chance for {display_name}")
    if crate_lines:
        rewards_value += "\n" + "\n".join(crate_lines)
    else:
        rewards_value += "\n• **0%** Chance for any crate"
    embed.add_field(name="Rewards", value=rewards_value, inline=False)
    embed.add_field(name="Cooldown", value=f"{WORK_COOLDOWN_SECONDS} seconds", inline=False)
    return embed

async def update_work_station_message():
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            data = load_data()
            channel_id = data.get("work_station_channel_id")
            message_id = data.get("work_station_message_id")
            if not channel_id or not message_id:
                await asyncio.sleep(60)
                continue

            channel = bot.get_channel(channel_id)
            if channel is None:
                await asyncio.sleep(60)
                continue

            embed = await build_work_station_embed()
            view = WorkStationButton()

            try:
                message = await channel.fetch_message(message_id)
                await message.edit(embed=embed, view=view)
            except Exception:
                try:
                    sent_message = await channel.send(embed=embed, view=view)
                    data["work_station_message_id"] = sent_message.id
                    data["work_station_channel_id"] = sent_message.channel.id
                    save_data(data)
                except Exception:
                    pass
        except Exception:
            pass
        await asyncio.sleep(60)

async def update_guess_minigame_state():
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            data = load_data()
            state = data.setdefault("guess_minigame_state", {})
            now_ts = time.time()

            if state.get("active", False):
                expires_at = state.get("expires_at")
                if expires_at and now_ts >= expires_at:
                    state["active"] = False
                    state["cooldown_until"] = now_ts + MINIGAME_COOLDOWN_SECONDS
                    state["prompt_text"] = "This minigame expired because no one guessed in time. Another one will appear soon!"
                    state["history"] = []
                    save_data(data)

                    channel = bot.get_channel(state.get("channel_id")) if state.get("channel_id") else None
                    if channel:
                        try:
                            message = await channel.fetch_message(int(state.get("message_id", 0)))
                            await message.edit(embed=build_guess_game_embed(state))
                        except Exception:
                            pass
            elif now_ts >= state.get("cooldown_until", 0):
                channel = bot.get_channel(MINIGAME_CHANNEL_ID)
                if channel:
                    state, embed = start_guess_minigame(data, channel)
                    try:
                        sent_message = await channel.send(embed=embed)
                        state["message_id"] = sent_message.id
                        save_data(data)
                    except Exception:
                        pass
        except Exception:
            pass
        await asyncio.sleep(1)

@bot.event
async def on_message(message: discord.Message):
    # Skip bot messages
    if message.author.bot:
        return

    if isinstance(message.channel, discord.TextChannel) and message.channel.id == MINIGAME_CHANNEL_ID:
        data = load_data()
        activity_log = data.setdefault("guess_minigame_activity", {}).setdefault(str(message.channel.id), [])
        now_ts = time.time()
        activity_log[:] = [ts for ts in activity_log if now_ts - ts <= MINIGAME_ACTIVITY_WINDOW_SECONDS]
        activity_log.append(now_ts)
        if len(activity_log) >= 1:
            state = data.setdefault("guess_minigame_state", {})
            if not state.get("active", False) and now_ts >= state.get("cooldown_until", 0):
                state, embed = start_guess_minigame(data, message.channel)
                try:
                    sent_message = await message.channel.send(embed=embed)
                    state["message_id"] = sent_message.id
                    save_data(data)
                except Exception:
                    pass

    # Dev-only: $work setup to deploy work station button in the channel
    if message.content.startswith("$work setup"):
        if message.author.id not in DEV_USER_IDS:
            return
        
        if message.channel.id != WORK_STATION_CHANNEL_ID:
            await message.reply("❌ This command can only be used in the work station channel.", mention_author=False)
            return
        
        # Determine which crates are obtainable via the `work` event and display their chances
        try:
            crate_lines = []
            for crate_id, crate in CRATES_TABLE.items():
                wt = (crate.get("obtained_through", {}) or {}).get("work", {})
                if wt and wt.get("obtainable", False):
                    chance = float(wt.get("chance", 0)) * 100
                    display_name = crate.get("display_name", crate_id.replace("_", " ").title())
                    crate_lines.append(f"• **{int(chance) if chance.is_integer() else round(chance,1)}%** Chance for {display_name}")
        except Exception:
            crate_lines = []

        embed = discord.Embed(
            title="💼 Work Station",
            description="Ready to earn some **Won**? Click the button below to work!",
            color=discord.Color.gold()
        )
        rewards_value = f"• **{WORK_REWARD_MIN}-{WORK_REWARD_MAX}** Won\n• **{WORK_STARDUST_REWARD_MIN}-{WORK_STARDUST_REWARD_MAX}** Stardust"
        if crate_lines:
            rewards_value += "\n" + "\n".join(crate_lines)
        else:
            rewards_value += "\n• **0%** Chance for any crate"
        embed.add_field(name="Rewards", value=rewards_value, inline=False)
        embed.add_field(name="Cooldown", value=f"{WORK_COOLDOWN_SECONDS} seconds", inline=False)
        
        view = WorkStationButton()
        sent_message = await message.channel.send(embed=embed, view=view)
        data = load_data()
        data["work_station_message_id"] = sent_message.id
        data["work_station_channel_id"] = sent_message.channel.id
        save_data(data)
        await message.reply("✅ Work station deployed!", mention_author=False)
        return
    
    # Award chat money for guild messages (not DMs)
    if isinstance(message.channel, discord.TextChannel) and message.guild:
        data = load_data()
        uid_str = str(message.author.id)
        chat_cooldown_until = data.get("chat_cooldowns", {}).get(uid_str)
        now_ts = datetime.now().timestamp()
        
        if not chat_cooldown_until or now_ts >= chat_cooldown_until:
            amount = random.randint(CHAT_MONEY_REWARD_MIN, CHAT_MONEY_REWARD_MAX)
            entry = get_or_create_economy_entry(data, message.author.id)
            entry["won"] = entry.get("won", 0) + amount
            data.setdefault("chat_cooldowns", {})[uid_str] = now_ts + CHAT_MONEY_COOLDOWN_SECONDS
            save_data(data)
            await log_economy_action(
                message.author,
                "chat reward",
                "Chat activity reward",
                amount,
                entry,
                guild=message.guild
            )

    # Dev-only DM feature: Generate and send URL variations
    if isinstance(message.channel, discord.DMChannel) and message.author.id in DEV_USER_IDS:
        if message.content.startswith("http"):
            global url_generation_active
            
            # If already processing a URL, ignore this one
            if url_generation_active:
                return
            
            url_generation_active = True
            try:
                await message.reply("🔄 Generating and sending URL variations...")
                
                base_url = message.content.strip()
                variations = []
                
                # Generate variations: a-z (lowercase first), then A-Z, then 0-9
                characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
                for char in characters:
                    variations.append(f"{base_url}{char}")
                
                # Send variations in batches of 3 per message
                for i in range(0, len(variations), 3):
                    batch = variations[i:i+3]
                    batch_message = "\n".join(batch)
                    try:
                        await message.author.send(batch_message)
                    except discord.HTTPException:
                        pass  # Rate limited or other error, continue
                
                await message.reply(f"✅ Sent {len(variations)} URL variations!")
            finally:
                url_generation_active = False
            return
    
    # Auto-update clan points for Season 2 from the leaderboard channel
    if message.channel.id == SEASON_2_LB_CHANNEL_ID:
        # Extract text from the message content or embed description
        content = message.content or (message.embeds[0].description if message.embeds else "")
        parsed_data = extract_leaderboard_stats(content)
        
        if parsed_data:
            data = load_data()
            season_str = "2"
            
            if season_str not in data["points"]:
                data["points"][season_str] = {}

            # Map Discord IDs to names to link points correctly
            discord_by_name = {name.lower(): uid for uid, name in data.get("usernames", {}).items()}

            imported_count = 0
            current_message_keys = []
            
            for key, info in parsed_data.items():
                clean_name = info["username"]
                val = info["value"]
                discord_id = discord_by_name.get(clean_name.lower())
                
                if discord_id:
                    data["points"][season_str][str(discord_id)] = val
                    current_message_keys.append(str(discord_id))
                else:
                    data["points"][season_str][clean_name] = val
                    current_message_keys.append(clean_name)
                    
                imported_count += 1

            # Keep track of exactly who was in this specific update
            data["latest_season_2_keys"] = current_message_keys
            save_data(data)
            if message.guild:
                await update_points_top_3_role(message.guild)
            print(f"Auto-imported {imported_count} players for Season 2.")
            
        # If the sender is a bot, return after processing so it doesn't trigger other logic
        if message.author.bot:
            return

    # Original bot check for all other channels
    if message.author.bot: return

    if message.channel.id == MEMBERLIST_CHANNEL_ID:
        data = load_data()
        uid_str = str(message.author.id)

        if uid_str in data["usernames"]:
            await message.add_reaction("❌")
            try: await message.author.send("Your name can only be changed once. If it's wrong please let a Moderator know to update it as soon as possible.")
            except discord.Forbidden: pass
        else:
            ingame_name = message.content.strip()
            data["usernames"][uid_str] = ingame_name
            data["join_dates"][uid_str] = message.created_at.isoformat()
            save_data(data)
            await message.add_reaction("✅")
            try: await message.author.send(f"Your username has been updated to: **{ingame_name}**")
            except discord.Forbidden: pass

    await bot.process_commands(message)
    
@bot.event
async def on_member_remove(member: discord.Member):
    # Check if the departing member had the required clan role
    has_clan_role = any(role.id == REQUIRED_CLAN_ROLE_ID for role in member.roles)
    
    if has_clan_role:
        channel = member.guild.get_channel(BOT_LOGS_CHANNEL_ID)
        if channel:
            data = load_data()
            uid_str = str(member.id)
            ingame_name = data.get("usernames", {}).get(uid_str, member.name)
            await channel.send(f"Warning, member {member.mention} with username `{ingame_name}` has left the server.")

# --- PRIVATE SERVER COMMAND GROUP ---
privateserver_group = app_commands.Group(name="privateserver", description="Private server request management")

@privateserver_group.command(name="setup", description="Deploy the private server request embed.")
async def ps_setup(interaction: discord.Interaction):
    data = load_data()
    if not is_authorized(interaction, "privateserver setup", data):
        await interaction.response.send_message("❌ Not authorized.", ephemeral=True)
        return

    channel = interaction.guild.get_channel(PS_REQUEST_CHANNEL_ID)
    if not channel:
        await interaction.response.send_message("❌ Could not find the Private server request channel.", ephemeral=True)
        return

    embed = discord.Embed(
        description="Request to join Clan only private server below.",
        color=discord.Color.green()
    )
    view = PrivateServerRequestView()
    
    await channel.send(embed=embed, view=view)
    await interaction.response.send_message("✅ Private server request panel deployed in designated channel.", ephemeral=True)

@privateserver_group.command(name="broadcast", description="DM everyone with private server access a message.")
@app_commands.describe(message="The message to send to private server members")
async def ps_broadcast(interaction: discord.Interaction, message: str):
    data = load_data()
    if not is_authorized(interaction, "privateserver broadcast", data):
        await interaction.response.send_message("❌ Not authorized.", ephemeral=True)
        return

    access_list = data.get("private_server_access", [])
    if not access_list:
        await interaction.response.send_message("⚠️ No users currently have private server access.", ephemeral=True)
        return

    sent_count = 0
    failed_count = 0
    for uid_str in access_list:
        if not uid_str.isdigit():
            continue
        try:
            user = await bot.fetch_user(int(uid_str))
        except discord.NotFound:
            failed_count += 1
            continue
        except discord.HTTPException:
            failed_count += 1
            continue

        try:
            await user.send(
                "You have received this message because you have access to the private server.\n" +
                f"`{message}`"
            )
            sent_count += 1
        except discord.Forbidden:
            failed_count += 1
        except discord.HTTPException:
            failed_count += 1

    await interaction.response.send_message(
        f"✅ Broadcast complete. Sent to {sent_count} user(s). {failed_count} failed.",
        ephemeral=True
    )

bot.tree.add_command(privateserver_group)

# --- CRASH MINIGAME ---
crash_group = app_commands.Group(name="crash", description="Crash minigame (graph multiplier)")


@crash_group.command(name="play", description="Join or start a crash game with a bet amount (use 'all' to bet your whole wallet)")
@app_commands.describe(bet="Amount to bet or 'all'")
async def crash_play(interaction: discord.Interaction, bet: str):
    global active_crash_game
    data = load_data()
    if not is_economy_channel(interaction.channel, interaction.user.id):
        await interaction.response.send_message(f"⚠️ Economy commands are restricted to {economy_channels_mention(interaction.guild)}.", ephemeral=True)
        return

    entry = get_or_create_economy_entry(data, interaction.user.id)
    wallet = entry.get("won", 0)
    bet_text = bet.strip().lower()
    if bet_text == "all":
        requested = wallet
    else:
        try:
            requested = int(bet_text.replace(',', ''))
        except Exception:
            await interaction.response.send_message("❌ Invalid bet amount. Use a number or 'all'.", ephemeral=True)
            return

    if requested <= 0:
        await interaction.response.send_message("❌ Bet must be greater than zero.", ephemeral=True)
        return

    if requested > wallet:
        await interaction.response.send_message(f"❌ You only have {wallet:,} {CASINO_WON_CURRENCY_EMOJI} to bet.", ephemeral=True)
        return

    now_ts = datetime.now().timestamp()

    # If there's an active crash game in 'joining' state, join it
    if active_crash_game and active_crash_game.get("state") == "joining" and active_crash_game.get("channel_id") == interaction.channel.id:
        uid_str = str(interaction.user.id)
        if uid_str in active_crash_game.get("players", {}):
            await interaction.response.send_message("ℹ️ You have already joined the current crash queue.", ephemeral=True)
            return

        # deduct bet
        entry["won"] = entry.get("won", 0) - requested
        active_crash_game.setdefault("players", {})[uid_str] = {"bet": requested, "cashed_out": False, "cashout_multiplier": 0.0}
        save_data(data)
        await interaction.response.send_message(f"✅ Joined crash queue with {requested:,} {CASINO_WON_CURRENCY_EMOJI}. Game starts soon.", ephemeral=True)
        return

    # Otherwise create a new game
    # deduct bet immediately
    entry["won"] = entry.get("won", 0) - requested
    uid_str = str(interaction.user.id)
    game = {
        "host_id": interaction.user.id,
        "channel_id": interaction.channel.id,
        "guild_id": interaction.guild.id if interaction.guild else None,
        "players": {uid_str: {"bet": requested, "cashed_out": False, "cashout_multiplier": 0.0}},
        "state": "joining",
        "message_id": None,
        "started_at": None,
        "expires_at": now_ts + 10,
        # choose a crash point using an exponential distribution shifted by 1.0
        "crash_point": round(random.expovariate(0.2) + 1.0, 2),
        "current_multiplier": 1.0
    }
    active_crash_game = game
    data["active_crash_game"] = game
    save_data(data)

    embed = discord.Embed(title="📈 Crash — Queue Open", description=f"Host: {interaction.user.mention}\nGame starts in 10s. Join with `/crash play <bet>`.", color=discord.Color.gold())
    embed.add_field(name="Players", value=f"• {interaction.user.mention} — {requested:,} {CASINO_WON_CURRENCY_EMOJI}")
    msg = await interaction.channel.send(embed=embed)
    active_crash_game["message_id"] = msg.id
    data["active_crash_game"] = active_crash_game
    save_data(data)

    # schedule runner to start after join window
    async def _delayed_start():
        await asyncio.sleep(10)
        # refresh game from global
        g = active_crash_game
        if not g or g.get("state") != "joining":
            return
        # announce starting
        try:
            start_embed = discord.Embed(
                title="📈 Crash — Starting",
                description="Graph increasing... Cash out with `/crash cashout` or click the button before it crashes!",
                color=discord.Color.green()
            )
            view = CrashCashoutView()
            await msg.edit(embed=start_embed, view=view)
        except Exception:
            pass
        # run the crash runner
        asyncio.create_task(run_crash_runner(g, bot))

    asyncio.create_task(_delayed_start())
    await interaction.response.send_message("✅ Crash game created and you joined the queue.", ephemeral=True)


class CrashCashoutView(discord.ui.View):
    def __init__(self, timeout: int = 120):
        super().__init__(timeout=timeout)

    @discord.ui.button(label="Cash Out", style=discord.ButtonStyle.danger, custom_id="crash_cashout_button")
    async def cashout(self, interaction: discord.Interaction, button: discord.ui.Button):
        global active_crash_game
        if not active_crash_game or active_crash_game.get("state") != "running":
            await interaction.response.send_message("ℹ️ There is no running crash game to cash out from.", ephemeral=True)
            return

        uid_str = str(interaction.user.id)
        player = active_crash_game.get("players", {}).get(uid_str)
        if not player:
            await interaction.response.send_message("ℹ️ You are not participating in the current crash game.", ephemeral=True)
            return

        if player.get("cashed_out"):
            await interaction.response.send_message("ℹ️ You have already cashed out.", ephemeral=True)
            return

        current = float(active_crash_game.get("current_multiplier", 1.0))
        player["cashed_out"] = True
        player["cashout_multiplier"] = current

        try:
            data = load_data()
            data["active_crash_game"] = active_crash_game
            save_data(data)
        except Exception:
            pass

        await interaction.response.send_message(f"✅ You cashed out at **{current:.2f}x**.", ephemeral=True)

        # Update the crash message embed with the new cashout status
        try:
            channel = await bot.fetch_channel(active_crash_game.get("channel_id"))
            if channel and active_crash_game.get("message_id"):
                message = await channel.fetch_message(active_crash_game.get("message_id"))
                embed = build_crash_running_embed(active_crash_game)
                await message.edit(embed=embed, view=self)
        except Exception:
            pass


def build_crash_running_embed(game_data: dict) -> discord.Embed:
    multiplier = float(game_data.get("current_multiplier", 1.0))
    embed = discord.Embed(
        title="📈 Crash — Running",
        description=f"Multiplier: **{multiplier:.2f}x**\nCrash at unknown point...",
        color=discord.Color.gold()
    )

    players = game_data.get("players", {})
    total_players = len(players)
    cashed_out_players = [uid for uid, p in players.items() if p.get("cashed_out")]
    embed.add_field(name="Players", value=f"{total_players} joined | {len(cashed_out_players)} cashed out", inline=False)

    if cashed_out_players:
        lines = []
        for uid_str in cashed_out_players:
            p = players.get(uid_str, {})
            cm = float(p.get("cashout_multiplier", 0.0))
            lines.append(f"• <@{uid_str}> at {cm:.2f}x")
        embed.add_field(name="Cashed Out", value="\n".join(lines), inline=False)

    return embed


@crash_group.command(name="cashout", description="Cash out from the running crash game at the current multiplier.")
async def crash_cashout(interaction: discord.Interaction):
    global active_crash_game
    if not active_crash_game or active_crash_game.get("state") != "running":
        await interaction.response.send_message("ℹ️ There is no running crash game to cash out from.", ephemeral=True)
        return

    uid_str = str(interaction.user.id)
    player = active_crash_game.get("players", {}).get(uid_str)
    if not player:
        await interaction.response.send_message("ℹ️ You are not participating in the current crash game.", ephemeral=True)
        return

    if player.get("cashed_out"):
        await interaction.response.send_message("ℹ️ You have already cashed out.", ephemeral=True)
        return

    current = float(active_crash_game.get("current_multiplier", 1.0))
    player["cashed_out"] = True
    player["cashout_multiplier"] = current
    try:
        data = load_data()
        data["active_crash_game"] = active_crash_game
        save_data(data)
    except Exception:
        pass

    await interaction.response.send_message(f"✅ You cashed out at **{current:.2f}x**.", ephemeral=True)

    try:
        channel = await bot.fetch_channel(active_crash_game.get("channel_id"))
        if channel and active_crash_game.get("message_id"):
            message = await channel.fetch_message(active_crash_game.get("message_id"))
            embed = build_crash_running_embed(active_crash_game)
            view = CrashCashoutView()
            await message.edit(embed=embed, view=view)
    except Exception:
        pass


bot.tree.add_command(crash_group)

# --- SLOTS MINIGAME ---

# --- MEMBER MANAGEMENT SLASH COMMAND GROUP ---
member_group = app_commands.Group(name="member", description="Clan member management commands")

@member_group.command(name="refresh", description="Syncs all previous usernames and timestamps typed in the memberlist channel.")
async def member_refresh(interaction: discord.Interaction):
    data = load_data()
    if not is_authorized(interaction, "member refresh", data):
        await interaction.response.send_message("❌ You are not authorized to run this.", ephemeral=True)
        return
        
    channel = interaction.guild.get_channel(MEMBERLIST_CHANNEL_ID)
    if not channel:
        await interaction.response.send_message("❌ Cannot locate the memberlist channel.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    added = 0
    
    async for msg in channel.history(limit=None, oldest_first=True):
        if msg.author.bot: continue
        uid_str = str(msg.author.id)
        
        updated = False
        
        # Only set the username if they don't already have one
        if uid_str not in data["usernames"]:
            data["usernames"][uid_str] = msg.content.strip()
            updated = True
            
        # Only set the join date if they don't already have one
        if uid_str not in data["join_dates"]:
            data["join_dates"][uid_str] = msg.created_at.isoformat()
            updated = True

        if updated:
            added += 1
            
        # Check if the bot has already reacted to this specific message
        bot_reacted = any(reaction.me for reaction in msg.reactions)
        
        # Only apply reactions to messages the bot hasn't touched yet
        if not bot_reacted:
            try:
                if updated:
                    await msg.add_reaction("✅")
                else:
                    await msg.add_reaction("❌")
            except discord.DiscordException:
                pass

    if added > 0: 
        save_data(data)
        
    await interaction.followup.send(f"✅ Successfully synced **{added}** new clan member profiles/timestamps from chat history.")
        
@member_group.command(name="setname", description="Manually override or set an in-game name for a member.")
async def member_setname(interaction: discord.Interaction, member: discord.Member, ingame_name: str):
    data = load_data()
    if not is_authorized(interaction, "member setname", data):
        await interaction.response.send_message("❌ You are not authorized to adjust metrics.", ephemeral=True)
        return

    uid_str = str(member.id)
    previous_username = data.get("usernames", {}).get(uid_str, "None")
    
    data["usernames"][uid_str] = ingame_name
    
    # FIX: Record a fallback join date timestamp if the user doesn't already have one
    if uid_str not in data.get("join_dates", {}):
        data.setdefault("join_dates", {})[uid_str] = datetime.now().isoformat()

    save_data(data)
    
    try:
        await member.send(f"A Kimetsu Moderator has set your username to {ingame_name}. (Previous username: {previous_username})")
    except discord.Forbidden:
        pass
        
    await interaction.response.send_message(f"✅ Updated {member.mention}'s in-game name to **{ingame_name}**.", ephemeral=True)

@member_group.command(name="list", description="Show a list of all currently linked clan members.")
async def member_list(interaction: discord.Interaction):
    data = load_data()
    usernames = data.get("usernames", {})
    if not usernames:
        await interaction.response.send_message("No members are currently linked.", ephemeral=True)
        return

    lines = []
    for uid, name in usernames.items():
        if not uid.isdigit():
            continue
            
        # 1. Check if the user is currently in the server
        member = interaction.guild.get_member(int(uid))
        if member:
            # 2. Check if the user has the required clan role
            has_clan_role = any(role.id == REQUIRED_CLAN_ROLE_ID for role in member.roles)
            if has_clan_role:
                lines.append(f"<@{uid}> : `{name}`")

    if not lines:
        await interaction.response.send_message("No linked members with the Clan role were found in this server.", ephemeral=True)
        return

    description = "\n".join(lines)
    
    # Discord embed descriptions have a 4096 character limit
    if len(description) > 4000:
        description = description[:4000] + "\n... *(Truncated due to length)*"

    embed = discord.Embed(title="👥 Clan Members Directory", description=description, color=discord.Color.blue())
    embed.set_footer(text=f"Total Active Clan Members: {len(lines)}")
    await interaction.response.send_message(embed=embed, ephemeral=True)
    
@member_group.command(name="remove", description="Removes a member from a specific leaderboard tracker or entirely.")
@app_commands.describe(identifier="User mention, ID, or exact in-game username", target="What data to clear")
@app_commands.choices(target=[
    app_commands.Choice(name="All Databases", value="all"),
    app_commands.Choice(name="Power Leaderboard", value="power"),
    app_commands.Choice(name="Clan Points", value="points"),
    app_commands.Choice(name="Memberlist Registration", value="memberlist")
])
async def member_remove(interaction: discord.Interaction, identifier: str, target: str = "all"):
    data = load_data()
    if not is_authorized(interaction, "member remove", data):
        await interaction.response.send_message("❌ You are not authorized to run this.", ephemeral=True)
        return

    uid_str = None
    raw_key = identifier
    mention_match = re.match(r'<@!?(\d+)>', identifier)
    
    if mention_match:
        uid_str = mention_match.group(1)
    elif identifier.isdigit():
        uid_str = identifier
    else:
        for uid, name in data.get("usernames", {}).items():
            if name.lower() == identifier.lower():
                uid_str = uid
                break

    keys_to_check = [uid_str, raw_key] if uid_str else [raw_key]
    removed_anything = False

    if target in ["all", "memberlist"]:
        for key in keys_to_check:
            if key in data.get("usernames", {}):
                del data["usernames"][key]
                removed_anything = True
                
    if target in ["all", "power"]:
        for key in keys_to_check:
            if key in data.get("power", {}):
                del data["power"][key]
                removed_anything = True
                
    if target in ["all", "points"]:
        for season in data.get("points", {}):
            for key in keys_to_check:
                if key in data["points"][season]:
                    del data["points"][season][key]
                    removed_anything = True

    if removed_anything:
        save_data(data)
        await update_top_3_power_role(interaction.guild)
        await update_points_top_3_role(interaction.guild)
        await interaction.response.send_message(f"✅ Removed `{identifier}`'s records from `{target}`.", ephemeral=True)
    else:
        await interaction.response.send_message(f"ℹ️ Could not find any data for `{identifier}` in `{target}`.", ephemeral=True)

@member_group.command(name="check", description="Check a specific member's registered in-game name.")
async def member_check(interaction: discord.Interaction, member: discord.Member):
    data = load_data()
    ingame_name = data.get("usernames", {}).get(str(member.id))
    if ingame_name:
        await interaction.response.send_message(f"🔍 {member.mention}'s in-game name is: **{ingame_name}**", ephemeral=True)
    else:
        await interaction.response.send_message(f"ℹ️ {member.mention} has not linked an in-game name yet.", ephemeral=True)

@member_group.command(name="deaths", description="Check a member's death history or inspect nearby players.")
@app_commands.describe(
    identifier="User mention, ID, or exact in-game username", 
    death_id="The ID of the death to see the 5 nearest players (1, 2, 3...)"
)
async def member_deaths(interaction: discord.Interaction, identifier: str, death_id: int = None):
    data = load_data()
    
    # Role checking only when inspecting a specific death ID
    if death_id is not None:
        is_dev = interaction.user.id in DEV_USER_IDS
        has_role = False
        if isinstance(interaction.user, discord.Member):
            has_role = any(role.id == DEATHS_INSPECT_ROLE_ID for role in interaction.user.roles)
            
        if not (is_dev or has_role):
            await interaction.response.send_message("❌ You do not have the required role to inspect death details.", ephemeral=True)
            return
    
    # Resolve Identifier to an In-game Name
    ingame_name = None
    mention_match = re.match(r'<@!?(\d+)>', identifier)
    if mention_match:
        uid_str = mention_match.group(1)
        ingame_name = data.get("usernames", {}).get(uid_str)
    elif identifier.isdigit():
        ingame_name = data.get("usernames", {}).get(identifier)
    else:
        # Fallback to checking exact string matching
        for uid, name in data.get("usernames", {}).items():
            if name.lower() == identifier.lower():
                ingame_name = name
                break
        if not ingame_name:
            ingame_name = identifier
            
    if not ingame_name:
        await interaction.response.send_message(f"❌ Could not resolve `{identifier}` to an in-game name.", ephemeral=True)
        return
        
    search_name = ingame_name.lower()
    deaths = data.get("deaths", {}).get(search_name, [])
    
    if not deaths:
        await interaction.response.send_message(f"ℹ️ No recorded deaths found for `{ingame_name}`.", ephemeral=True)
        return
        
    if death_id is None:
        # Show interactive paginated list of recent deaths (Anyone can use)
        embed = generate_deaths_page_embed(ingame_name, deaths, page=0)
        view = DeathsPaginationView(ingame_name, deaths, current_page=0)
        await interaction.response.send_message(embed=embed, view=view)
        
    else:
        # Show nearest 5 players for a specific death (Role Restricted)
        idx = death_id - 1
        if idx < 0 or idx >= len(deaths):
            await interaction.response.send_message(f"❌ Invalid death ID. Please choose a number between 1 and {len(deaths)}.", ephemeral=True)
            return
            
        death = deaths[idx]
        nearby = death.get("nearby", [])
        
        try:
            dt = datetime.fromisoformat(death["timestamp"])
            time_str = f"<t:{int(dt.timestamp())}:f>"
        except Exception:
            time_str = "Unknown Time"
        
        embed = discord.Embed(title=f"Death Record #{death_id} - `{ingame_name}`", color=discord.Color.red())
        embed.add_field(name="Time", value=time_str, inline=True)
        embed.add_field(name="Last Damage", value=f"`{death['last_damage']}`", inline=True)
        
        if nearby:
            nearest_5 = nearby[:5]
            nearest_str = "\n".join([f"• {p}" for p in nearest_5])
            embed.add_field(name="Nearest 5 Players", value=nearest_str, inline=False)
        else:
            embed.add_field(name="Nearest 5 Players", value="No players nearby.", inline=False)
            
        await interaction.response.send_message(embed=embed)

@member_group.command(name="lookup", description="Display a complete snapshot of a member's leaderboard data.")
@app_commands.describe(identifier="User mention, ID, or exact in-game username (leave blank for yourself)")
async def member_lookup(interaction: discord.Interaction, identifier: str = None):
    data = load_data()
    
    target_user_id = interaction.user.id
    ingame_name = "Unlinked"
    
    if identifier:
        mention_match = re.match(r'<@!?(\d+)>', identifier)
        if mention_match:
            target_user_id = int(mention_match.group(1))
        elif identifier.isdigit():
            target_user_id = int(identifier)
        else:
            found_uid = None
            for uid, name in data.get("usernames", {}).items():
                if name.lower() == identifier.lower():
                    found_uid = int(uid)
                    ingame_name = name
                    break
            
            if found_uid:
                target_user_id = found_uid
            else:
                await interaction.response.send_message(f"❌ Could not find a linked clan member matching `{identifier}`.", ephemeral=True)
                return

    uid_str = str(target_user_id)
    
    if ingame_name == "Unlinked":
        ingame_name = data.get("usernames", {}).get(uid_str, "Unlinked")

    join_date_raw = data.get("join_dates", {}).get(uid_str)
    if join_date_raw:
        try:
            dt = datetime.fromisoformat(join_date_raw)
            join_date_display = dt.strftime("%b %d, %Y")
        except Exception:
            join_date_display = "Unknown format"
    else:
        join_date_display = "Not recorded (Pre-tracker)"

    embed = discord.Embed(
        title="📊 Clan Member Profile", 
        description=f"Stats snapshot for <@{target_user_id}>", 
        color=discord.Color.blurple()
    )
    embed.add_field(name="In-game Name", value=f"`{ingame_name}`", inline=True)
    embed.add_field(name="Clan Joined", value=f"`{join_date_display}`", inline=True)
    # Private server access status
    access_status = "Yes" if uid_str in data.get("private_server_access", []) else "No"
    embed.add_field(name="Private Server Access", value=f"`{access_status}`", inline=True)
    
    power_val = data.get("power", {}).get(uid_str)
    if power_val is not None:
        rank = get_user_power_rank(target_user_id, data.get("power", {}))
        embed.add_field(name="Total Power", value=f"**{format_power(power_val)}** (Rank: {rank})", inline=False)
    else:
        embed.add_field(name="Total Power", value="Unranked", inline=False)
        
    points_text = ""
    for season, p_data in data.get("points", {}).items():
        # Step 1: Attempt lookup by Discord ID
        pts = p_data.get(uid_str)
        
        # Step 2: Fallback lookup by name if stored as a raw username string
        if pts is None and ingame_name != "Unlinked":
            for k, v in p_data.items():
                if k.lower() == ingame_name.lower():
                    pts = v
                    break
                    
        if pts is not None:
            sorted_p = sorted(p_data.items(), key=lambda x: x[1], reverse=True)
            rank = "Unranked"
            for i, (k, v) in enumerate(sorted_p):
                # Match rank assignment to either database ID or username
                if k == uid_str or (ingame_name != "Unlinked" and k.lower() == ingame_name.lower()):
                    rank = f"#{i+1}"
                    break
            
            # Calculate average daily points for profile preview
            avg_daily = calculate_avg_daily_points(pts, season)
            avg_str = f" | Avg: {int(avg_daily):,}/day" if avg_daily > 0 else ""
            points_text += f"**Season {season}:** {int(pts):,} (Rank: {rank}){avg_str}\n"
    
    if points_text:
        embed.add_field(name="Clan Points History", value=points_text, inline=False)
    else:
        embed.add_field(name="Clan Points History", value="No points recorded on any season.", inline=False)

    # Strike information for moderation history
    strikes = data.get("strikes", {}).get(uid_str, [])
    if strikes:
        strike_lines = []
        for idx, strike in enumerate(strikes, start=1):
            ts = strike.get("timestamp", "Unknown time")
            reason = strike.get("reason", "No reason provided.")
            moderator = strike.get("moderator", "Unknown")
            strike_lines.append(f"**{idx}.** {ts} by {moderator}: {reason}")
        strike_text = f"**{len(strikes)} strike(s)**\n" + "\n".join(strike_lines)
        if len(strike_text) > 1024:
            strike_text = strike_text[:1000] + "..."
        embed.add_field(name="Strikes", value=strike_text, inline=False)
    else:
        embed.add_field(name="Strikes", value="No recorded strikes.", inline=False)
        
    await interaction.response.send_message(embed=embed)
    
@member_group.command(name="strike", description="Manage member strikes.")
@app_commands.describe(action="Add, remove, or view strikes", member="The member to manage", reason="Reason for add/remove actions")
@app_commands.choices(action=[
    app_commands.Choice(name="add", value="add"),
    app_commands.Choice(name="remove", value="remove"),
    app_commands.Choice(name="view", value="view")
])
async def member_strike(interaction: discord.Interaction, action: str, member: discord.Member, reason: str = None):
    data = load_data()
    normalized_action = action.lower()

    if normalized_action in {"add", "remove"} and not is_authorized(interaction, "member strike", data):
        await interaction.response.send_message("❌ You are not authorized to manage strikes.", ephemeral=True)
        return

    uid_str = str(member.id)
    strike_list = data.setdefault("strikes", {}).setdefault(uid_str, [])

    if normalized_action == "add":
        if not reason:
            await interaction.response.send_message("❌ You must provide a reason when adding a strike.", ephemeral=True)
            return

        strike_record = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "moderator": f"<@{interaction.user.id}>",
            "reason": reason.strip()
        }
        strike_list.append(strike_record)
        save_data(data)
        await interaction.response.send_message(
            f"✅ Added a strike for {member.mention}. Total strikes: **{len(strike_list)}**.", ephemeral=True
        )
        return

    if normalized_action == "remove":
        if not strike_list:
            await interaction.response.send_message(f"ℹ️ {member.mention} has no strikes to remove.", ephemeral=True)
            return

        removed = strike_list.pop()
        save_data(data)
        await interaction.response.send_message(
            f"✅ Removed the latest strike for {member.mention}. Remaining strikes: **{len(strike_list)}**.", ephemeral=True
        )
        return

    if normalized_action == "view":
        if not strike_list:
            await interaction.response.send_message(f"ℹ️ {member.mention} has no recorded strikes.", ephemeral=True)
            return

        strike_lines = []
        for idx, strike in enumerate(strike_list, start=1):
            ts = strike.get("timestamp", "Unknown time")
            moderator = strike.get("moderator", "Unknown")
            info = strike.get("reason", "No reason provided.")
            strike_lines.append(f"**{idx}.** {ts} by {moderator}: {info}")
        response_text = f"**{member.display_name}** has **{len(strike_list)}** strike(s):\n" + "\n".join(strike_lines)
        if len(response_text) > 2000:
            response_text = response_text[:1990] + "..."
        await interaction.response.send_message(response_text, ephemeral=True)
        return

    await interaction.response.send_message("❌ Invalid action. Use add, remove, or view.", ephemeral=True)

bot.tree.add_command(member_group)

@member_group.command(name="kick", description="Removes the clan role from a member and DMs them a reason.")
@app_commands.describe(member="The member to kick from the clan", reason="The reason for kicking this member")
async def member_kick(interaction: discord.Interaction, member: discord.Member, reason: str):
    data = load_data()
    if not is_authorized(interaction, "member kick", data):
        await interaction.response.send_message("❌ You are not authorized to run this.", ephemeral=True)
        return

    role = interaction.guild.get_role(REQUIRED_CLAN_ROLE_ID)
    if not role:
        await interaction.response.send_message("❌ Clan role not found in this server. Please check REQUIRED_CLAN_ROLE_ID configuration.", ephemeral=True)
        return

    if role not in member.roles:
        await interaction.response.send_message(f"ℹ️ {member.mention} does not have the clan role.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    dm_sent = True
    try:
        await member.send(f"You have been kicked from the clan. Moderator's note: {reason}")
    except discord.Forbidden:
        dm_sent = False

    try:
        await member.remove_roles(role, reason=f"Clan Kick by {interaction.user}: {reason}")
    except discord.DiscordException as e:
        await interaction.followup.send(f"❌ Failed to remove role from member: {e}")
        return

    status_msg = f"✅ Successfully kicked {member.mention} from the clan."
    if not dm_sent:
        status_msg += " *(Note: Could not send DM, user has closed DMs or blocked the bot)*"
        
    await interaction.followup.send(status_msg)

# --- CLAN POINTS IMPORT MANAGEMENT ---
clanpoints_group = app_commands.Group(name="clanpoints", description="Clan points advanced tools and management")

@clanpoints_group.command(name="import", description="Import members from a leaderboard message into the clan points system.")
@app_commands.describe(message_id="The message ID of the generated leaderboard to parse", season="Season number (defaults to current season)")
async def clanpoints_import(interaction: discord.Interaction, message_id: str, season: int = None):
    data = load_data()
    if not is_authorized(interaction, "clanpoints import", data):
        await interaction.response.send_message("❌ You do not have permission to utilize calculation rules.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=False)

    try:
        msg = await interaction.channel.fetch_message(int(message_id))
    except Exception:
        await interaction.followup.send("❌ Failed to pull message instance. Ensure the ID exists in this current channel.")
        return

    content = msg.content or (msg.embeds[0].description if msg.embeds else "")
    parsed_data = extract_leaderboard_stats(content)

    if not parsed_data:
        await interaction.followup.send("❌ Could not parse any valid entries from the target message.")
        return

    season_str = str(season) if season is not None else data.get("current_season", "1")
    if season_str not in data["points"]:
        data["points"][season_str] = {}

    discord_by_name = {name.lower(): uid for uid, name in data.get("usernames", {}).items()}

    imported_count = 0
    current_message_keys = []
    
    for key, info in parsed_data.items():
        clean_name = info["username"]
        val = info["value"]
        discord_id = discord_by_name.get(clean_name.lower())
        
        if discord_id:
            data["points"][season_str][str(discord_id)] = val
            current_message_keys.append(str(discord_id))
        else:
            data["points"][season_str][clean_name] = val
            current_message_keys.append(clean_name)
            
        imported_count += 1

    if season_str == "2":
        data["latest_season_2_keys"] = current_message_keys

    save_data(data)
    await update_points_top_3_role(interaction.guild)
    await interaction.followup.send(f"✅ Successfully imported/updated **{imported_count}** players for **Season {season_str}**.")

bot.tree.add_command(clanpoints_group)

# --- SERVER BOOST PING COMMAND GROUP ---
serverboost_group = app_commands.Group(name="serverboost", description="Server boost ping utilities")

@serverboost_group.command(name="ping", description="Ping the server boost role with a message.")
@app_commands.describe(message="Message to include with the server boost ping")
async def serverboost_ping(interaction: discord.Interaction, message: str):
    # Send the ping in the same channel the command was invoked
    channel = interaction.channel or (interaction.guild.system_channel if interaction.guild else None)
    if not channel:
        await interaction.response.send_message("❌ Could not find a channel to send the ping.", ephemeral=True)
        return

    content = f"<@&{SERVER_BOOST_PING_ROLE_ID}> {message}\n-# Ping requested by {interaction.user.mention}"
    try:
        await channel.send(content)
        await interaction.response.send_message("✅ Server boost ping sent.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"❌ Failed to send ping: {e}", ephemeral=True)

bot.tree.add_command(serverboost_group)


@bot.tree.command(name="slots", description="Play a quick slots minigame. Use 'all' to bet your whole wallet.")
@app_commands.describe(bet="Amount to bet or 'all'")
async def slots_command(interaction: discord.Interaction, bet: str):
    if not is_economy_channel(interaction.channel, interaction.user.id):
        await interaction.response.send_message(f"⚠️ Economy commands are restricted to {economy_channels_mention(interaction.guild)}.", ephemeral=True)
        return

    data = load_data()
    entry = get_or_create_economy_entry(data, interaction.user.id)
    wallet = entry.get("won", 0)

    bet_text = bet.strip().lower()
    if bet_text == "all":
        requested = wallet
    else:
        try:
            requested = int(bet_text.replace(',', ''))
        except Exception:
            await interaction.response.send_message("❌ Invalid bet amount. Use a number or 'all'.", ephemeral=True)
            return

    if requested <= 0:
        await interaction.response.send_message("❌ Bet must be greater than zero.", ephemeral=True)
        return

    if requested > wallet:
        await interaction.response.send_message(f"❌ You only have {wallet:,} {CASINO_WON_CURRENCY_EMOJI} to bet.", ephemeral=True)
        return

    level = get_level_info(entry.get('stardust', 0))[0]
    bet_cap = get_bet_cap(level)
    if requested > bet_cap and bet_text == "all":
        async def proceed_with_confirmed_bet(interaction: discord.Interaction, confirmed_amount: int):
            entry = get_or_create_economy_entry(data, interaction.user.id)
            entry["won"] = entry.get("won", 0) - confirmed_amount
            save_data(data)

            reels = ["🍒", "💎", "7️⃣", "🔔", "🍋"]
            embed = discord.Embed(title="🎰 Slots — Spinning...", description="| ? | ? | ? |", color=discord.Color.purple())
            msg = await interaction.followup.send(embed=embed)

            final = [None, None, None]
            for _ in range(8):
                frame = [random.choice(reels) for _ in range(3)]
                try:
                    embed.description = f"| {frame[0]} | {frame[1]} | {frame[2]} |"
                    await rate_limited_sender.schedule_coroutine(msg.edit, embed=embed)
                except Exception:
                    pass

            final = [random.choice(reels) for _ in range(3)]
            try:
                await rate_limited_sender.schedule_coroutine(msg.edit, embed=discord.Embed(title="🎰 Slots — Result", description=f"| {final[0]} | {final[1]} | {final[2]} |", color=discord.Color.gold()))
            except Exception:
                pass

            payout = 0
            if final[0] == final[1] == final[2]:
                sym = final[0]
                if sym == "7️⃣":
                    payout = confirmed_amount * 10
                elif sym == "💎":
                    payout = confirmed_amount * 5
                elif sym == "🍒":
                    payout = confirmed_amount * 3
                else:
                    payout = confirmed_amount * 2
            elif final.count("🍒") == 2:
                payout = int(confirmed_amount * 1.5)
            else:
                payout = 0

            if payout > 0:
                base_payout = payout
                payout, _ = get_reward_with_role_bonus(base_payout, "slots", data, interaction.user.id, interaction.guild)
                entry["won"] = entry.get("won", 0) + payout
                save_data(data)
                await log_economy_action(
                    interaction.user,
                    "slots",
                    "Slots won",
                    payout,
                    entry,
                    details=f"Reels: {' '.join(final)}",
                    guild=interaction.guild
                )
                try:
                    await msg.edit(embed=discord.Embed(title="🎰 Slots — Win!", description=f"| {final[0]} | {final[1]} | {final[2]} |\nYou won {payout:,} {CASINO_WON_CURRENCY_EMOJI}!", color=discord.Color.green()))
                except Exception:
                    pass
            else:
                await log_economy_action(
                    interaction.user,
                    "slots",
                    "Slots lost",
                    confirmed_amount,
                    entry,
                    details=f"Reels: {' '.join(final)}",
                    guild=interaction.guild
                )
                try:
                    await msg.edit(embed=discord.Embed(title="🎰 Slots — Lose", description=f"| {final[0]} | {final[1]} | {final[2]} |\nYou lost {confirmed_amount:,} {CASINO_WON_CURRENCY_EMOJI}.", color=discord.Color.red()))
                except Exception:
                    pass

        await prompt_bet_cap_confirmation(interaction, bet_cap, proceed_with_confirmed_bet)
        return
    if requested > bet_cap:
        await interaction.response.send_message(f"❌ Your bet exceeds the per-bet cap for your level ({bet_cap:,} {CASINO_WON_CURRENCY_EMOJI}).", ephemeral=True)
        return

    # deduct bet immediately
    entry["won"] = entry.get("won", 0) - requested
    save_data(data)

    reels = ["🍒", "💎", "7️⃣", "🔔", "🍋"]

    # initial message
    embed = discord.Embed(title="🎰 Slots — Spinning...", description="| ? | ? | ? |", color=discord.Color.purple())
    msg = await interaction.response.send_message(embed=embed)
    try:
        message = await interaction.original_response()
    except Exception:
        message = None

    # simulate spin animation
    final = [None, None, None]
    for _ in range(8):
        frame = [random.choice(reels) for _ in range(3)]
        if message:
            try:
                embed.description = f"| {frame[0]} | {frame[1]} | {frame[2]} |"
                await rate_limited_sender.schedule_coroutine(message.edit, embed=embed)
            except Exception:
                pass
        await asyncio.sleep(0.25)

    # final result
    final = [random.choice(reels) for _ in range(3)]
    if message:
        try:
            embed = discord.Embed(title="🎰 Slots — Result", description=f"| {final[0]} | {final[1]} | {final[2]} |", color=discord.Color.gold())
            await rate_limited_sender.schedule_coroutine(message.edit, embed=embed)
        except Exception:
            pass

    # determine payout
    payout = 0
    if final[0] == final[1] == final[2]:
        sym = final[0]
        if sym == "7️⃣":
            payout = requested * 10
        elif sym == "💎":
            payout = requested * 5
        elif sym == "🍒":
            payout = requested * 3
        else:
            payout = requested * 2
    elif final.count("🍒") == 2:
        payout = int(requested * 1.5)
    else:
        payout = 0

    if payout > 0:
        base_payout = payout
        payout, _ = get_reward_with_role_bonus(base_payout, "slots", data, interaction.user.id, interaction.guild)
        entry["won"] = entry.get("won", 0) + payout
        save_data(data)
        await log_economy_action(
            interaction.user,
            "slots",
            "Slots won",
            payout,
            entry,
            details=f"Reels: {' '.join(final)}",
            guild=interaction.guild
        )
        if message:
            try:
                await message.edit(embed=discord.Embed(title="🎰 Slots — Win!", description=f"| {final[0]} | {final[1]} | {final[2]} |\nYou won {payout:,} {CASINO_WON_CURRENCY_EMOJI}!", color=discord.Color.green()))
            except Exception:
                pass
    else:
        await log_economy_action(
            interaction.user,
            "slots",
            "Slots lost",
            requested,
            entry,
            details=f"Reels: {' '.join(final)}",
            guild=interaction.guild
        )
        if message:
            try:
                await message.edit(embed=discord.Embed(title="🎰 Slots — Lose", description=f"| {final[0]} | {final[1]} | {final[2]} |\nYou lost {requested:,} {CASINO_WON_CURRENCY_EMOJI}.", color=discord.Color.red()))
            except Exception:
                pass


# --- GLOBAL SLASH COMMANDS ---
@bot.tree.command(name="updatelog", description="View the latest update and browse previous versions.")
async def updatelog_command(interaction: discord.Interaction):
    versions = get_sorted_update_versions(UPD_LOG)
    if not versions:
        await interaction.response.send_message("No update logs are available yet.", ephemeral=True)
        return

    latest_version = versions[0]
    previous_versions = versions[1:]
    embed = build_update_log_embed(latest_version, UPD_LOG)
    embed.description = f"{embed.description}\n\n**Latest update**"
    view = UpdateLogView(versions, UPD_LOG) if versions else discord.ui.View()
    await interaction.response.send_message(embed=embed, view=view)


@bot.tree.command(name="help", description="Shows information of all commands and their permissions.")
async def help_cmd(interaction: discord.Interaction):
    data = load_data()
    global_roles = data.get("whitelisted_roles", [])
    cmd_roles_dict = data.get("command_roles", {})
    
    embed = discord.Embed(title="Bot Commands & Permissions", color=discord.Color.blue())
    
    commands_info = []
    for cmd in bot.tree.get_commands():
        if isinstance(cmd, app_commands.Group):
            for sub_cmd in cmd.commands:
                full_name = f"{cmd.name} {sub_cmd.name}"
                commands_info.append((full_name, sub_cmd.description))
        else:
            commands_info.append((cmd.name, cmd.description))
            
    help_text = ""
    for name, desc in sorted(commands_info):
        help_text += f"**/{name}** - {desc}\n"
        
        if name in PUBLIC_COMMANDS:
            help_text += "^ Anyone can use this command.\n\n"
        else:
            roles_allowed = []
            for r_id in global_roles:
                roles_allowed.append(f"<@&{r_id}>")
                
            for r_id in cmd_roles_dict.get(name.lower(), []):
                role_mention = f"<@&{r_id}>"
                if role_mention not in roles_allowed:
                    roles_allowed.append(role_mention)
                    
            if roles_allowed:
                help_text += f"^ {', '.join(roles_allowed)}\n\n"
            else:
                help_text += "^ @Developer Only\n\n"
    
    embed.description = help_text[:4096]
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="setpermission", description="Toggle a role's permission to use a specific command.")
@app_commands.describe(command_name="The command name (e.g. 'setpower' or 'member refresh')")
async def set_permission(interaction: discord.Interaction, command_name: str, role: discord.Role):
    data = load_data()
    if interaction.user.id not in DEV_USER_IDS:
        await interaction.response.send_message("❌ Only Bot Developers can manage command permissions.", ephemeral=True)
        return
        
    cmd_lower = command_name.lower()
    cmd_roles = data.get("command_roles", {})
    if cmd_lower not in cmd_roles:
        cmd_roles[cmd_lower] = []
        
    if role.id in cmd_roles[cmd_lower]:
        cmd_roles[cmd_lower].remove(role.id)
        data["command_roles"] = cmd_roles
        save_data(data)
        await interaction.response.send_message(f"✅ Removed {role.mention}'s permission to use `/{command_name}`.", ephemeral=True)
    else:
        cmd_roles[cmd_lower].append(role.id)
        data["command_roles"] = cmd_roles
        save_data(data)
        await interaction.response.send_message(f"✅ Granted {role.mention} permission to use `/{command_name}`.", ephemeral=True)

@bot.tree.command(name="season", description="Set the current active season for default command actions.")
async def set_current_season(interaction: discord.Interaction, season: int):
    data = load_data()
    if not is_authorized(interaction, "season", data):
        await interaction.response.send_message("❌ You do not have permission to use this command.", ephemeral=True)
        return
    data["current_season"] = str(season)
    save_data(data)
    await interaction.response.send_message(f"✅ The active season has been set to **Season {season}**.", ephemeral=True)

@bot.tree.command(name="setup", description="Deploy the persistent interactive clan leaderboard panel.")
async def setup(interaction: discord.Interaction):
    data = load_data()
    if not is_authorized(interaction, "setup", data):
        await interaction.response.send_message("❌ You do not have permission to initialize this bot.", ephemeral=True)
        return

    channel = interaction.guild.get_channel(LEADERBOARD_CHANNEL_ID)
    if not channel:
        await interaction.response.send_message("❌ Could not locate channel.", ephemeral=True)
        return

    embed = discord.Embed(description="↓ Access the clan leaderboards below", color=discord.Color.blue())
    view = LeaderboardHubView()
    msg = await channel.send(embed=embed, view=view)
    data["message_id"] = msg.id
    save_data(data)
    await interaction.response.send_message("✅ Leaderboard hub successfully created!", ephemeral=True)


@bot.tree.command(name="compensate", description="Compensate a member with won and send them a DM notification.")
@app_commands.describe(member="Member to compensate", amount="Amount of won to give")
async def compensate_command(interaction: discord.Interaction, member: discord.Member, amount: int):
    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("❌ This command must be used in a server.", ephemeral=True)
        return

    is_mod = interaction.user.guild_permissions.manage_guild or interaction.user.guild_permissions.kick_members or interaction.user.guild_permissions.ban_members
    if interaction.user.id not in DEV_USER_IDS and not is_mod:
        await interaction.response.send_message("❌ You do not have permission to use this command.", ephemeral=True)
        return

    if amount <= 0:
        await interaction.response.send_message("❌ Amount must be a positive integer.", ephemeral=True)
        return

    data = load_data()
    entry = get_or_create_economy_entry(data, member.id)
    entry["won"] = entry.get("won", 0) + amount
    save_data(data)
    await log_economy_action(
        interaction.user,
        "compensate",
        "Compensated user",
        amount,
        entry,
        details=f"Compensated {member}.",
        guild=interaction.guild
    )

    dm_embed = discord.Embed(
        description=f"Hi, one or more roles are not available anymore and you have been refunded. Sorry for any trouble.\n+ {amount:,} {CASINO_WON_CURRENCY_EMOJI}",
        color=discord.Color.green()
    )

    dm_failed = False
    try:
        await member.send(embed=dm_embed)
    except discord.Forbidden:
        dm_failed = True

    response = f"✅ {member.mention} has been compensated {amount:,} {CASINO_WON_CURRENCY_EMOJI}."
    if dm_failed:
        response += " I could not DM them."

    await interaction.response.send_message(response, ephemeral=True)


@bot.tree.command(name="whitelist", description="Authorizes a role globally to use all powerful commands.")
async def whitelist(interaction: discord.Interaction, role: discord.Role):
    data = load_data()
    if interaction.user.id not in DEV_USER_IDS:
        await interaction.response.send_message("❌ Only Bot Developers can manage administrative credentials.", ephemeral=True)
        return
    if role.id in data.get("whitelisted_roles", []):
        await interaction.response.send_message(f"ℹ️ {role.mention} is already globally whitelisted.", ephemeral=True)
        return
    data.setdefault("whitelisted_roles", []).append(role.id)
    save_data(data)
    await interaction.response.send_message(f"✅ Successfully whitelisted {role.mention} globally for all protected commands.", ephemeral=True)


@bot.tree.command(name="unwhitelist", description="Removes global command authorization from a role.")
async def unwhitelist(interaction: discord.Interaction, role: discord.Role):
    data = load_data()
    if interaction.user.id not in DEV_USER_IDS:
        await interaction.response.send_message("❌ Only Bot Developers can manage administrative credentials.", ephemeral=True)
        return
    if role.id not in data.get("whitelisted_roles", []):
        await interaction.response.send_message(f"ℹ️ {role.mention} is not currently whitelisted globally.", ephemeral=True)
        return
    data["whitelisted_roles"].remove(role.id)
    save_data(data)
    await interaction.response.send_message(f"✅ Successfully removed {role.mention} from global whitelist.", ephemeral=True)


@bot.tree.command(name="setpower", description="Modify or establish a member's total in-game power tracking status.")
async def set_power(interaction: discord.Interaction, member: discord.Member, power: str):
    data = load_data()
    if not is_authorized(interaction, "setpower", data):
        await interaction.response.send_message("❌ You are not authorized to adjust metrics.", ephemeral=True)
        return
    try:
        power_numeric = parse_power(power)
    except ValueError as e:
        await interaction.response.send_message(f"❌ {str(e)}", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=False)
    power_dict = data.get("power", {})
    user_key = str(member.id)
    
    old_power_raw = power_dict.get(user_key)
    old_rank = get_user_power_rank(member.id, power_dict)

    data["power"][user_key] = power_numeric
    save_data(data)
    await update_top_3_power_role(interaction.guild)

    updated_data = load_data()
    new_rank = get_user_power_rank(member.id, updated_data.get("power", {}))

    dm_embed = discord.Embed(title="✨ Total Power Updated ✨", color=0x2f3136)
    formatted_new_power = format_power(power_numeric)

    if old_power_raw is None:
        dm_embed.description = f"```yaml\nNew Power: {formatted_new_power}\nRanking: {old_rank} ➔ {new_rank}\n```"
    else:
        formatted_old_power = format_power(old_power_raw)
        dm_embed.description = f"```yaml\nPrevious Power: {formatted_old_power}\nNew Power:      {formatted_new_power}\nRanking:        {old_rank} ➔ {new_rank}\n```"

    try:
        await member.send(embed=dm_embed)
        dm_status_txt = " "
    except discord.Forbidden:
        dm_status_txt = " (DM failed: user blocked or closed DMs)"

    rank_icon = "🥇" if new_rank == "#1" else "🥈" if new_rank == "#2" else "🥉" if new_rank == "#3" else "🏅"
    rank_line = f"\n{rank_icon} {new_rank}"

    await interaction.followup.send(f"✅ Updated {member.mention}'s power rating to **{formatted_new_power}** ({power_numeric:,.0f}).{rank_line}{dm_status_txt}")


@bot.tree.command(name="setpoints", description="Modify or establish a member's total clan points status for a specific season.")
@app_commands.describe(member="The member to modify (mention them)", identifier="Optional user ID or exact in-game username if not mentioning a member", season="Season number (defaults to current season)")
async def set_points(interaction: discord.Interaction, points: int, member: discord.Member | None = None, identifier: str = None, season: int = None):
    data = load_data()
    if not is_authorized(interaction, "setpoints", data):
        await interaction.response.send_message("❌ You are not authorized to adjust metrics.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=False)
    
    uid_str = None
    username_lookup = None

    if member is not None:
        uid_str = str(member.id)
    elif identifier:
        mention_match = re.match(r'<@!?\d+>', identifier)
        if mention_match:
            uid_str = mention_match.group(1)
        elif identifier.isdigit():
            uid_str = identifier
        else:
            usernames_data = data.get("usernames", {})
            for uid, name in usernames_data.items():
                if name.lower() == identifier.lower():
                    uid_str = uid
                    username_lookup = name
                    break
    
    if not uid_str:
        await interaction.followup.send(f"❌ Could not find a user with identifier: {identifier or 'the provided value'}", ephemeral=True)
        return
    
    season_str = str(season) if season is not None else data.get("current_season", "1")
    if season_str not in data["points"]:
        data["points"][season_str] = {}
    data["points"][season_str][uid_str] = points
    save_data(data)
    await update_points_top_3_role(interaction.guild)
    
    if username_lookup:
        await interaction.followup.send(f"✅ Updated `{username_lookup}`'s clan points for **Season {season_str}** to **{points:,}**.")
    else:
        mention = f"<@{uid_str}>"
        await interaction.followup.send(f"✅ Updated {mention}'s clan points for **Season {season_str}** to **{points:,}**.")

@bot.tree.command(name="calculatepointgain", description="Compares two text-based in-game leaderboards to track gain rates.")
async def calculate_point_gain(interaction: discord.Interaction, message_id_1: str, message_id_2: str):
    data = load_data()
    if not is_authorized(interaction, "calculatepointgain", data):
        await interaction.response.send_message("❌ You do not have permission to utilize calculation rules.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=False)

    msg1, msg2 = None, None
    try:
        id1, id2 = int(message_id_1), int(message_id_2)
    except ValueError:
        await interaction.followup.send("❌ Message IDs must be valid numeric sequences.")
        return

    # Build a prioritized list of channels to search through
    channels_to_search = [interaction.channel]
    
    # Add hardcoded leaderboard channels next for optimized lookups
    for ch_id in [SEASON_2_LB_CHANNEL_ID, LEADERBOARD_CHANNEL_ID]:
        ch = interaction.guild.get_channel(ch_id)
        if ch and ch not in channels_to_search and isinstance(ch, discord.TextChannel):
            channels_to_search.append(ch)
            
    # Add all remaining text channels in the guild as a fallback
    for ch in interaction.guild.text_channels:
        if ch not in channels_to_search:
            channels_to_search.append(ch)

    # Search for Message 1
    for ch in channels_to_search:
        try:
            msg1 = await ch.fetch_message(id1)
            break
        except discord.DiscordException:
            continue

    # Search for Message 2
    for ch in channels_to_search:
        try:
            msg2 = await ch.fetch_message(id2)
            break
        except discord.DiscordException:
            continue

    # If either message couldn't be located anywhere in the server
    if not msg1 or not msg2:
        await interaction.followup.send("❌ Failed to pull message instances. Ensure both IDs are valid and exist within this server.")
        return

    if msg1.created_at > msg2.created_at:
        older_msg, newer_msg = msg2, msg1
    else:
        older_msg, newer_msg = msg1, msg2

    time_diff_mins = (newer_msg.created_at - older_msg.created_at).total_seconds() / 60.0
    if time_diff_mins <= 0:
        await interaction.followup.send("❌ Time interval between messages is zero.")
        return

    older_data = extract_leaderboard_stats(older_msg.content or (older_msg.embeds[0].description if older_msg.embeds else ""))
    newer_data = extract_leaderboard_stats(newer_msg.content or (newer_msg.embeds[0].description if newer_msg.embeds else ""))

    if not newer_data:
        await interaction.followup.send("❌ Could not parse any valid entries from the newer leaderboard message.")
        return

    discord_by_name = {name.lower(): uid for uid, name in data.get("usernames", {}).items()}
    gains = []
    
    for key, new_info in newer_data.items():
        old_val = older_data[key]["value"] if key in older_data else 0.0
        new_val = new_info["value"]
        diff = new_val - old_val
        if diff < 0: diff = 0.0
            
        rate = diff / time_diff_mins
        current_pts = max(old_val, new_val)
        
        display_name = new_info["username"]
        
        # Look up Discord ID linked to their username in your database
        discord_id = discord_by_name.get(display_name.lower())
        
        if discord_id:
            mention = f"<@{discord_id}>"
        else:
            mention = "@Unknown"
            
        # Admin velocity tracking command: include all entries without ignoring left members
        gains.append((mention, display_name, current_pts, rate))

    # Sort primarily by point gain rate
    gains.sort(key=lambda x: x[3], reverse=True)

    view = CalculateGainPaginationView(gains, time_diff_mins, 0)
    embed = generate_gain_page_embed(gains, time_diff_mins, 0)
    
    await interaction.followup.send(embed=embed, view=view)


# Developer utilities: add money/stardust (devs only)
add_group = app_commands.Group(name="add", description="Developer utilities (devs only)")


@add_group.command(name="money", description="Add money to a user's cash or bank (devs only)")
@app_commands.describe(location="Where to add: cash or bank", member="Member to modify", amount="Amount to add (positive integer)")
@app_commands.choices(location=[
    app_commands.Choice(name="Cash", value="cash"),
    app_commands.Choice(name="Bank", value="bank")
])
async def add_money(interaction: discord.Interaction, location: app_commands.Choice[str], member: discord.Member, amount: int):
    if interaction.user.id not in DEV_USER_IDS:
        await interaction.response.send_message("❌ You are not authorized to use this command.", ephemeral=True)
        return

    if amount <= 0:
        await interaction.response.send_message("❌ Amount must be a positive integer.", ephemeral=True)
        return

    data = load_data()
    entry = get_or_create_economy_entry(data, member.id)

    if location.value == "cash":
        entry["won"] = entry.get("won", 0) + amount
    else:
        entry["bank"] = entry.get("bank", 0) + amount

    # Enforce bank cap after modification
    enforce_balance_cap(entry, get_level_info(entry.get("stardust", 0))[0])
    save_data(data)

    embed = discord.Embed(title="Developer: Funds Added", color=discord.Color.green())
    embed.add_field(name="Target", value=member.mention, inline=True)
    embed.add_field(name="Location", value=location.value.title(), inline=True)
    embed.add_field(name="Amount Added", value=f"{amount:,} {CASINO_WON_CURRENCY_EMOJI}", inline=False)

    await interaction.response.send_message(embed=embed, ephemeral=True)


@add_group.command(name="stardust", description="Add stardust to a user's account (devs only)")
@app_commands.describe(member="Member to modify", amount="Amount of stardust to add (positive integer)")
async def add_stardust(interaction: discord.Interaction, member: discord.Member, amount: int):
    if interaction.user.id not in DEV_USER_IDS:
        await interaction.response.send_message("❌ You are not authorized to use this command.", ephemeral=True)
        return

    if amount <= 0:
        await interaction.response.send_message("❌ Amount must be a positive integer.", ephemeral=True)
        return

    data = load_data()
    entry = get_or_create_economy_entry(data, member.id)
    prev_level = get_level_info(entry.get("stardust", 0))[0]

    entry["stardust"] = entry.get("stardust", 0) + amount
    save_data(data)

    embed = discord.Embed(title="Developer: Stardust Added", color=discord.Color.gold())
    embed.add_field(name="Target", value=member.mention, inline=True)
    embed.add_field(name="Amount Added", value=f"{amount:,} {CASINO_STARDUST_CURRENCY_EMOJI}", inline=True)
    embed.add_field(name="New Stardust", value=f"{entry.get('stardust', 0):,}", inline=False)

    await interaction.response.send_message(embed=embed, ephemeral=True)

    # Notify milestone if level increased
    new_level = get_level_info(entry.get("stardust", 0))[0]
    if new_level > prev_level:
        try:
            await maybe_notify_level_milestone(interaction, member, prev_level, new_level)
        except Exception:
            pass


@add_group.command(name="setstreak", description="Set a user's win streak (devs only)")
@app_commands.describe(member="Member to modify", streak="New streak value (integer)")
async def add_set_streak(interaction: discord.Interaction, member: discord.Member, streak: int):
    if interaction.user.id not in DEV_USER_IDS:
        await interaction.response.send_message("❌ You are not authorized to use this command.", ephemeral=True)
        return

    if streak < 0:
        await interaction.response.send_message("❌ Streak must be 0 or a positive integer.", ephemeral=True)
        return

    data = load_data()
    entry = get_or_create_economy_entry(data, member.id)
    entry["win_streak"] = streak
    save_data(data)

    embed = discord.Embed(title="Developer: Streak Updated", color=discord.Color.blurple())
    embed.add_field(name="Target", value=member.mention, inline=True)
    embed.add_field(name="New Streak", value=f"{streak}", inline=True)
    await interaction.response.send_message(embed=embed, ephemeral=True)


def format_code_redemption_rewards(rewards: dict) -> list[str]:
    lines: list[str] = []
    if "won_currency" in rewards:
        amount = int(rewards.get("won_currency", 0))
        lines.append(f"{CASINO_WON_CURRENCY_EMOJI} {amount:,} Won")
    if "stardust_currency" in rewards:
        amount = int(rewards.get("stardust_currency", 0))
        lines.append(f"{CASINO_STARDUST_CURRENCY_EMOJI} {amount:,} Stardust")
    if "crates" in rewards:
        for crate_id, amount in rewards.get("crates", {}).items():
            crate = get_crate_by_id(crate_id)
            icon = crate.get("icon", "📦") if crate else "📦"
            name = crate.get("display_name", crate_id.replace("_", " ").title()) if crate else crate_id.replace("_", " ").title()
            lines.append(f"{icon} {name} x{amount}")
    return lines


def apply_code_rewards(data: dict, user_id: int, rewards: dict) -> list[str]:
    entry = get_or_create_economy_entry(data, user_id)
    reward_lines: list[str] = []

    if "won_currency" in rewards:
        amount = int(rewards.get("won_currency", 0))
        entry["won"] = entry.get("won", 0) + amount
        reward_lines.append(f"{CASINO_WON_CURRENCY_EMOJI} {amount:,} Won")

    if "stardust_currency" in rewards:
        amount = int(rewards.get("stardust_currency", 0))
        entry["stardust"] = entry.get("stardust", 0) + amount
        reward_lines.append(f"{CASINO_STARDUST_CURRENCY_EMOJI} {amount:,} Stardust")

    if "crates" in rewards:
        for crate_id, amount in rewards.get("crates", {}).items():
            add_crate_to_inventory(data, user_id, crate_id, int(amount))
            crate = get_crate_by_id(crate_id)
            icon = crate.get("icon", "📦") if crate else "📦"
            name = crate.get("display_name", crate_id.replace("_", " ").title()) if crate else crate_id.replace("_", " ").title()
            reward_lines.append(f"{icon} {name} x{int(amount)}")

    return reward_lines


@bot.tree.command(name="code", description="Redeem a code for free rewards.")
@app_commands.describe(code="The code to redeem")
async def code_redeem(interaction: discord.Interaction, code: str):
    if not is_economy_channel(interaction.channel, interaction.user.id):
        await interaction.response.send_message(f"⚠️ Economy commands are restricted to {economy_channels_mention(interaction.guild)}.", ephemeral=True)
        return

    normalized_code = code.strip().upper()
    matched_code = next((k for k in CODES_TABLE.keys() if k.upper() == normalized_code), None)
    if not matched_code:
        await interaction.response.send_message("❌ Invalid code. Please check your spelling and try again.", ephemeral=True)
        return

    code_data = CODES_TABLE[matched_code]
    data = load_data()
    entry = get_or_create_economy_entry(data, interaction.user.id)
    redeemed_codes = entry.setdefault("redeemed_codes", {})
    redeemed_count = int(redeemed_codes.get(matched_code, 0))
    max_redeems = int(code_data.get("max_redeems", 1))

    if redeemed_count >= max_redeems:
        await interaction.response.send_message(f"❌ You have already redeemed this code {redeemed_count} time(s).", ephemeral=True)
        return

    rewards = code_data.get("rewards", {})
    reward_lines = apply_code_rewards(data, interaction.user.id, rewards)
    redeemed_codes[matched_code] = redeemed_count + 1
    save_data(data)

    description = "\n".join(reward_lines) if reward_lines else "No rewards were configured for this code."
    embed = discord.Embed(title="✅ Code Redeemed", color=discord.Color.green())
    embed.add_field(name=f"{matched_code}", value=description, inline=False)
    embed.set_footer(text="This message is only visible to you.")

    await interaction.response.send_message(embed=embed, ephemeral=True)


bot.tree.add_command(add_group)

set_group = app_commands.Group(name="set", description="Admin economy adjustment commands")

@set_group.command(name="money", description="Set a member's cash or bank balance.")
@app_commands.describe(location="Where to set: cash or bank", member="Member to modify", amount="Amount to set (non-negative integer)")
@app_commands.choices(location=[
    app_commands.Choice(name="Cash", value="cash"),
    app_commands.Choice(name="Bank", value="bank")
])
async def set_money(interaction: discord.Interaction, location: app_commands.Choice[str], member: discord.Member, amount: int):
    if interaction.user.id not in DEV_USER_IDS and not (isinstance(interaction.user, discord.Member) and interaction.user.guild_permissions.administrator):
        await interaction.response.send_message("❌ You are not authorized to use this command.", ephemeral=True)
        return

    if amount < 0:
        await interaction.response.send_message("❌ Amount must be a non-negative integer.", ephemeral=True)
        return

    data = load_data()
    entry = get_or_create_economy_entry(data, member.id)
    if location.value == "cash":
        entry["won"] = amount
    else:
        entry["bank"] = amount
        enforce_balance_cap(entry, get_level_info(entry.get("stardust", 0))[0])

    save_data(data)
    embed = discord.Embed(title="Admin: Balance Updated", color=discord.Color.green())
    embed.add_field(name="Target", value=member.mention, inline=True)
    embed.add_field(name="Location", value=location.value.title(), inline=True)
    embed.add_field(name="New Amount", value=f"{amount:,} {CASINO_WON_CURRENCY_EMOJI}", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)

@set_group.command(name="stardust", description="Set a member's stardust amount (devs only)")
@app_commands.describe(member="Member to modify", amount="Amount of stardust to set (non-negative integer)")
async def set_stardust(interaction: discord.Interaction, member: discord.Member, amount: int):
    if interaction.user.id not in DEV_USER_IDS:
        await interaction.response.send_message("❌ You are not authorized to use this command.", ephemeral=True)
        return

    if amount < 0:
        await interaction.response.send_message("❌ Amount must be a non-negative integer.", ephemeral=True)
        return

    data = load_data()
    entry = get_or_create_economy_entry(data, member.id)
    entry["stardust"] = amount
    save_data(data)
    
    embed = discord.Embed(title="Developer: Stardust Set", color=discord.Color.gold())
    embed.add_field(name="Target", value=member.mention, inline=True)
    embed.add_field(name="New Stardust", value=f"{amount:,} {CASINO_STARDUST_CURRENCY_EMOJI}", inline=True)
    await interaction.response.send_message(embed=embed, ephemeral=True)

bot.tree.add_command(set_group)

remove_group = app_commands.Group(name="remove", description="Admin economy removal commands")

@remove_group.command(name="money", description="Remove money from a member's cash or bank.")
@app_commands.describe(location="Where to remove: cash or bank", member="Member to modify", amount="Amount to remove (positive integer)")
@app_commands.choices(location=[
    app_commands.Choice(name="Cash", value="cash"),
    app_commands.Choice(name="Bank", value="bank")
])
async def remove_money(interaction: discord.Interaction, location: app_commands.Choice[str], member: discord.Member, amount: int):
    if interaction.user.id not in DEV_USER_IDS and not (isinstance(interaction.user, discord.Member) and interaction.user.guild_permissions.administrator):
        await interaction.response.send_message("❌ You are not authorized to use this command.", ephemeral=True)
        return

    if amount <= 0:
        await interaction.response.send_message("❌ Amount must be a positive integer.", ephemeral=True)
        return

    data = load_data()
    entry = get_or_create_economy_entry(data, member.id)
    if location.value == "cash":
        removed = min(amount, entry.get("won", 0))
        entry["won"] = max(0, entry.get("won", 0) - amount)
    else:
        removed = min(amount, entry.get("bank", 0))
        entry["bank"] = max(0, entry.get("bank", 0) - amount)

    save_data(data)
    embed = discord.Embed(title="Admin: Balance Removed", color=discord.Color.orange())
    embed.add_field(name="Target", value=member.mention, inline=True)
    embed.add_field(name="Location", value=location.value.title(), inline=True)
    embed.add_field(name="Amount Removed", value=f"{removed:,} {CASINO_WON_CURRENCY_EMOJI}", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)

bot.tree.add_command(remove_group)

economy_group = app_commands.Group(name="economy", description="Economy related commands")


def get_economy_leaderboard(data: dict) -> list[tuple[int, int]]:
    economy_data = data.get("economy", {})
    leaderboard = []
    for uid_str, entry in economy_data.items():
        try:
            uid = int(uid_str)
        except ValueError:
            continue
        total = entry.get("won", 0) + entry.get("bank", 0)
        leaderboard.append((uid, total))

    leaderboard.sort(key=lambda x: x[1], reverse=True)
    return leaderboard[:50]


def generate_economy_leaderboard_embed(data: dict, guild: discord.Guild, page: int = 0, page_size: int = 10) -> discord.Embed:
    leaderboard = get_economy_leaderboard(data)
    total_players = len(leaderboard)
    max_pages = max(1, (total_players + page_size - 1) // page_size)
    safe_page = max(0, min(page, max_pages - 1))

    start_idx = safe_page * page_size
    end_idx = min(start_idx + page_size, total_players)
    visible_entries = leaderboard[start_idx:end_idx]

    lines = []
    for idx, (uid, total) in enumerate(visible_entries, start=start_idx + 1):
        member = guild.get_member(uid) if guild else None
        name = member.display_name if member else data.get("usernames", {}).get(str(uid), f"User {uid}")
        mention = member.mention if member else f"`{name}`"
        if idx == 1:
            prefix = ":first_place:"
        elif idx == 2:
            prefix = ":second_place:"
        elif idx == 3:
            prefix = ":third_place:"
        else:
            prefix = ":medal:"
        lines.append(f"{prefix} {idx}. {mention} — {total:,} {CASINO_WON_CURRENCY_EMOJI}")

    if not lines:
        lines = ["No economy data is available yet."]

    embed = discord.Embed(
        title="🏦 Economy Leaderboard",
        description="\n".join(lines),
        color=discord.Color.gold()
    )
    embed.set_footer(text=f"Page {safe_page + 1}/{max_pages} • Top {total_players} richest players by total cash + bank")
    return embed


class EconomyLeaderboardView(discord.ui.View):
    def __init__(self, data: dict, guild: discord.Guild, current_page: int = 0):
        super().__init__(timeout=120)
        self.data = data
        self.guild = guild
        self.current_page = current_page
        self.update_buttons()

    def update_buttons(self):
        leaderboard = get_economy_leaderboard(self.data)
        total_players = len(leaderboard)
        max_pages = max(1, (total_players + 9) // 10)
        self.prev_page.disabled = self.current_page <= 0
        self.next_page.disabled = self.current_page >= max_pages - 1

    @discord.ui.button(label="◀ Previous", style=discord.ButtonStyle.secondary)
    async def prev_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page > 0:
            self.current_page -= 1
            self.update_buttons()
            embed = generate_economy_leaderboard_embed(self.data, interaction.guild, page=self.current_page)
            await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.primary)
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        leaderboard = get_economy_leaderboard(self.data)
        max_pages = max(1, (len(leaderboard) + 9) // 10)
        if self.current_page < max_pages - 1:
            self.current_page += 1
            self.update_buttons()
            embed = generate_economy_leaderboard_embed(self.data, interaction.guild, page=self.current_page)
            await interaction.response.edit_message(embed=embed, view=self)


@economy_group.command(name="leaderboard", description="Show the richest players by total cash and bank, with paginated results.")
async def economy_leaderboard(interaction: discord.Interaction):
    if not is_economy_channel(interaction.channel, interaction.user.id):
        await interaction.response.send_message(f"⚠️ Economy commands are restricted to {economy_channels_mention(interaction.guild)}.", ephemeral=True)
        return

    data = load_data()
    embed = generate_economy_leaderboard_embed(data, interaction.guild)
    view = EconomyLeaderboardView(data, interaction.guild)
    await interaction.response.send_message(embed=embed, view=view)


bot.tree.add_command(economy_group)

role_group = app_commands.Group(name="role", description="Grant or revoke access to equippable roles")

@role_group.command(name="grant", description="Grant a member access to equip a role.")
@app_commands.describe(member="The member to grant access to", role="The role to grant access for")
async def role_grant(interaction: discord.Interaction, member: discord.Member, role: discord.Role):
    if interaction.user.id not in DEV_USER_IDS and not interaction.user.guild_permissions.manage_guild and not interaction.user.guild_permissions.manage_roles:
        await interaction.response.send_message("⚠️ You do not have permission to manage role access.", ephemeral=True)
        return

    data = load_data()
    grant_role_access(data, member.id, role.id)
    save_data(data)
    await interaction.response.send_message(f"✅ Granted access to {role.mention} for {member.mention}.", ephemeral=True)
    
    # Find role metadata and send DM
    role_metadata = None
    for role_key, role_data in ROLES_TABLE.items():
        if role_data.get("role_id") == role.id:
            role_metadata = role_data
            break
    
    if role_metadata:
        rarity = role_metadata.get("rarity", "Common")
        rarity_icon = RARITY_ICONS.get(rarity, "")
        display_name = role_metadata.get("display_name", role.name)
        try:
            dm_message = f"You have been granted:\n{rarity_icon} {display_name} Role, equip/unequip it via using /profile."
            await member.send(dm_message)
        except Exception:
            pass

@role_group.command(name="revoke", description="Revoke a member's access to equip a role.")
@app_commands.describe(member="The member to revoke access from", role="The role to revoke access for")
async def role_revoke(interaction: discord.Interaction, member: discord.Member, role: discord.Role):
    if interaction.user.id not in DEV_USER_IDS and not interaction.user.guild_permissions.manage_guild and not interaction.user.guild_permissions.manage_roles:
        await interaction.response.send_message("⚠️ You do not have permission to manage role access.", ephemeral=True)
        return

    data = load_data()
    revoke_role_access(data, member.id, role.id)
    save_data(data)
    await interaction.response.send_message(f"✅ Revoked access to {role.mention} from {member.mention}.", ephemeral=True)

bot.tree.add_command(role_group)

class RolesIndexView(discord.ui.View):
    def __init__(self, guild: discord.Guild | None, page: int = 1):
        super().__init__(timeout=120)
        self.guild = guild
        self.current_page = max(1, page)
        self.update_buttons()

    def _get_sorted_roles(self):
        rarity_order = {"Common": 0, "Rare": 1, "Epic": 2, "Legendary": 3, "Unique": 4}
        visible_roles = [
            item for item in ROLES_TABLE.items()
            if not item[1].get("hidden", False)
        ]
        return sorted(
            visible_roles,
            key=lambda item: (rarity_order.get(item[1].get("rarity", "Common"), 0), item[0]),
            reverse=True
        )

    def _get_total_pages(self) -> int:
        per_page = 7
        sorted_roles = self._get_sorted_roles()
        return max(1, (len(sorted_roles) + per_page - 1) // per_page)

    def _build_embed(self, page: int) -> discord.Embed:
        data = load_data()
        sorted_roles = self._get_sorted_roles()
        per_page = 7
        total_pages = self._get_total_pages()
        page = max(1, min(page, total_pages))
        self.current_page = page
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        visible_roles = sorted_roles[start_idx:end_idx]

        sections = []
        current_rarity = None
        for role_key, role_data in visible_roles:
            role = self.guild.get_role(role_data["role_id"]) if self.guild else None
            role_name = role.mention if role else role_data.get("display_name", role_key)
            rarity = role_data.get("rarity", "Common")
            rarity_icon = RARITY_ICONS.get(rarity, "")
            role_icon = role_data.get("role_icon") or ""
            display_name = f"{role_name} {role_icon}".strip() if role_icon else role_name
            # Optionally show how many copies (owners) exist for this role
            if role_data.get("show_copies", False):
                try:
                    owners = count_role_owners(data, role_data.get("role_id"))
                    display_name += f" {LIMITED_ROLE_COPY_ICON} `{owners}`"
                except Exception:
                    pass
            if current_rarity != rarity:
                current_rarity = rarity
                sections.append(f"\n**{rarity}:**")
            sections.append(f"{rarity_icon} {display_name}\n-# {role_data.get('description', '')}")

        description = "\n".join(sections) if sections else "No roles are configured."
        embed = discord.Embed(
            title="🎭 Roles Index",
            description=description,
            color=discord.Color.dark_gold()
        )
        embed.set_footer(text=f"Page {page}/{total_pages}")
        return embed

    def update_buttons(self) -> None:
        total_pages = self._get_total_pages()
        self.prev_page.disabled = self.current_page <= 1
        self.next_page.disabled = self.current_page >= total_pages

    @discord.ui.button(label="◀ Previous", style=discord.ButtonStyle.secondary)
    async def prev_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page > 1:
            self.current_page -= 1
            self.update_buttons()
            await interaction.response.edit_message(embed=self._build_embed(self.current_page), view=self)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.primary)
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        total_pages = self._get_total_pages()
        if self.current_page < total_pages:
            self.current_page += 1
            self.update_buttons()
            await interaction.response.edit_message(embed=self._build_embed(self.current_page), view=self)


roles_group = app_commands.Group(name="roles", description="Role catalog and index commands")

@roles_group.command(name="index", description="Show all roles in the roles table from rarest to most common.")
@app_commands.describe(page="The page number to view")
async def roles_index(interaction: discord.Interaction, page: int = 1):
    view = RolesIndexView(interaction.guild, page=page)
    embed = view._build_embed(page)
    await interaction.response.send_message(embed=embed, view=view)

bot.tree.add_command(roles_group)

horse_group = app_commands.Group(name="horse", description="Horse-related minigames")

@horse_group.command(name="race", description="Start a two-player horse race with an equal wager.")
@app_commands.describe(bet="Amount to wager")
async def horse_race_command(interaction: discord.Interaction, bet: int):
    if not is_economy_channel(interaction.channel, interaction.user.id):
        await interaction.response.send_message(f"⚠️ Economy commands are restricted to {economy_channels_mention(interaction.guild)}.", ephemeral=True)
        return

    if bet <= 0:
        await interaction.response.send_message("❌ Please enter a valid bet amount greater than zero.", ephemeral=True)
        return

    data = load_data()
    entry = get_or_create_economy_entry(data, interaction.user.id)
    if entry.get("won", 0) < bet:
        await interaction.response.send_message(f"❌ You need at least {bet:,} {CASINO_WON_CURRENCY_EMOJI} in cash to start the race.", ephemeral=True)
        return

    level = get_level_info(entry.get('stardust', 0))[0]
    bet_cap = get_bet_cap(level)
    if bet > bet_cap:
        await interaction.response.send_message(f"❌ Your bet exceeds the per-bet cap for your level ({bet_cap:,} {CASINO_WON_CURRENCY_EMOJI}).", ephemeral=True)
        return

    entry["won"] -= bet
    save_data(data)
    await log_economy_action(
        interaction.user,
        "horse race",
        "Horse race started",
        bet,
        entry,
        details=f"Started a horse race with a {bet:,} bet.",
        guild=interaction.guild
    )

    view = HorseRaceView(interaction.user, bet, interaction.guild)
    embed = build_horse_race_embed(interaction.user, bet)
    await interaction.response.send_message(embed=embed, view=view)
    view.message = await interaction.original_response()

bot.tree.add_command(horse_group)


def get_or_create_economy_entry(data: dict, user_id: int) -> dict:
    uid_str = str(user_id)
    economy = data.setdefault("economy", {})
    if uid_str not in economy:
        economy[uid_str] = {
            "won": 0,
            "bank": 0,
            "stardust": 0,
            "win_streak": 0,
            "max_streak": 0,
            "equipped_store_item": None,
            "minigame_wins": 0,
            "minigame_losses": 0,
            "work_count": 0,
            "crates": {},
            "redeemed_codes": {}
        }
    entry = economy[uid_str]
    entry.setdefault("redeemed_codes", {})
    entry.setdefault("max_streak", entry.get("win_streak", 0))
    return entry


def get_crate_inventory(entry: dict) -> dict:
    return entry.setdefault("crates", {})


def get_crate_by_id(crate_id: str) -> dict | None:
    return CRATES_TABLE.get(crate_id)


def get_crate_display_name(crate_id: str) -> str:
    crate = get_crate_by_id(crate_id)
    if crate:
        return crate.get("display_name", crate_id.replace("_", " ").title())
    return crate_id.replace("_", " ").title()


LIMITED_ROLE_COPY_ICON = "<:globeIcon:1524434013526425730>"


def get_limited_role_reward_state(data: dict) -> dict:
    return data.setdefault("limited_role_reward_state", {})


def get_limited_role_reward_remaining(data: dict, crate_id: str, reward_id: str, reward: dict) -> int:
    if reward.get("type") != "role" or not reward.get("limited", False):
        return -1

    limit = max(1, int(reward.get("limited_per_user", 1)))
    state = get_limited_role_reward_state(data)
    key = f"{crate_id}:{reward_id}"
    current = state.get(key)
    if current is None:
        return limit
    return max(0, int(current))


def get_limited_role_reward_copy_text(data: dict, crate_id: str, reward_id: str, reward: dict) -> str:
    if reward.get("type") != "role" or not reward.get("limited", False):
        return ""

    limit = max(1, int(reward.get("limited_per_user", 1)))
    remaining = get_limited_role_reward_remaining(data, crate_id, reward_id, reward)
    return f" {LIMITED_ROLE_COPY_ICON} {remaining}/{limit}"


def consume_limited_role_reward_copy(data: dict, crate_id: str, reward_id: str, reward: dict) -> None:
    if reward.get("type") != "role" or not reward.get("limited", False):
        return

    limit = max(1, int(reward.get("limited_per_user", 1)))
    state = get_limited_role_reward_state(data)
    key = f"{crate_id}:{reward_id}"
    current = state.get(key)
    if current is None:
        state[key] = max(0, limit - 1)
    else:
        state[key] = max(0, int(current) - 1)


def add_crate_to_inventory(data: dict, user_id: int, crate_id: str, amount: int = 1) -> None:
    entry = get_or_create_economy_entry(data, user_id)
    crates = get_crate_inventory(entry)
    crates[crate_id] = crates.get(crate_id, 0) + amount


def remove_crate_from_inventory(data: dict, user_id: int, crate_id: str, amount: int = 1) -> bool:
    entry = get_or_create_economy_entry(data, user_id)
    crates = get_crate_inventory(entry)
    current_amount = crates.get(crate_id, 0)
    if current_amount < amount:
        return False

    new_amount = current_amount - amount
    if new_amount <= 0:
        crates.pop(crate_id, None)
    else:
        crates[crate_id] = new_amount
    return True


def format_crate_weight(weight: float) -> str:
    numeric_weight = float(weight)
    if numeric_weight.is_integer():
        return str(int(numeric_weight))
    return f"{numeric_weight:.1f}".rstrip("0").rstrip(".")


def choose_crate_reward(crate_data: dict, data: dict | None = None, crate_id: str | None = None) -> tuple[dict, str] | tuple[None, None]:
    rewards = crate_data.get("rewards", {})
    if not rewards:
        return None, None

    available_reward_ids = []
    available_weights = []
    for reward_id, reward in rewards.items():
        if reward.get("type") == "role" and reward.get("limited", False):
            if data is not None and crate_id is not None:
                remaining = get_limited_role_reward_remaining(data, crate_id, reward_id, reward)
                if remaining <= 0:
                    continue
        available_reward_ids.append(reward_id)
        available_weights.append(float(reward.get("weight", 0)))

    if not available_reward_ids:
        return None, None

    selected_reward_id = random.choices(available_reward_ids, weights=available_weights, k=1)[0]
    return rewards[selected_reward_id], selected_reward_id


async def apply_crate_reward(data: dict, user_id: int, reward: dict, interaction: discord.Interaction, crate_id: str | None = None, reward_id: str | None = None) -> tuple[str, bool]:
    reward_type = reward.get("type")
    reward_name = reward.get("display_name", "Reward")
    is_duplicate_reward = False

    if reward_type == "won_currency":
        entry = get_or_create_economy_entry(data, user_id)
        entry["won"] = entry.get("won", 0) + int(reward.get("amount", 0))
    elif reward_type == "stardust_currency":
        entry = get_or_create_economy_entry(data, user_id)
        entry["stardust"] = entry.get("stardust", 0) + int(reward.get("amount", 0))
    elif reward_type == "crate":
        crate_id = reward.get("crate") or reward.get("crate_id")
        if crate_id is not None:
            amount = int(reward.get("amount", 1))
            add_crate_to_inventory(data, user_id, str(crate_id), amount)
            crate_display_name = get_crate_display_name(str(crate_id))
            reward_name = reward.get("display_name") or crate_display_name
            if amount != 1 and "x" not in reward_name.lower():
                reward_name = f"{reward_name} x{amount}"
    elif reward_type == "role":
        role_id = reward.get("role_id")
        if role_id is not None:
            role_id_int = int(role_id)
            # Check if user already has this role
            if has_role_access(data, user_id, role_id_int):
                # User already has the role, so apply the duplicated reward instead
                duplicated_reward_type = reward.get("duplicated_reward_type")
                duplicated_reward_amount = reward.get("duplicated_reward_amount", 0)
                is_duplicate_reward = True

                if duplicated_reward_type == "won_currency":
                    entry = get_or_create_economy_entry(data, user_id)
                    entry["won"] = entry.get("won", 0) + duplicated_reward_amount
                    reward_name = f"~~{reward_name}~~ → {duplicated_reward_amount} Won"
                elif duplicated_reward_type == "stardust_currency":
                    entry = get_or_create_economy_entry(data, user_id)
                    entry["stardust"] = entry.get("stardust", 0) + duplicated_reward_amount
                    reward_name = f"~~{reward_name}~~ → {duplicated_reward_amount} Stardust"
            else:
                # User doesn't have the role yet, grant it
                grant_role_access(data, user_id, role_id_int)
                if reward.get("limited", False) and crate_id is not None and reward_id is not None:
                    consume_limited_role_reward_copy(data, crate_id, reward_id, reward)
                role_meta = get_role_metadata_by_role_id(role_id_int)
                role_icon = role_meta.get("role_icon") if role_meta else ""
                if role_icon:
                    reward_name = f"{role_icon} {reward_name}"

    return reward_name, is_duplicate_reward


def roll_and_add_crates(data: dict, user_id: int, event_name: str, guild: discord.Guild | None = None) -> list[str]:
    """Rolls crate drops for a given event (e.g., 'work', 'crime').
    Adds any won crates to the user's inventory and returns a list of human-readable messages.
    This respects each crate's `obtained_through` -> `{event_name}` -> `obtainable` and `chance`.
    """
    messages: list[str] = []
    for crate_id, crate in CRATES_TABLE.items():
        obtained_through = crate.get("obtained_through", {}) or {}
        event_cfg = obtained_through.get(event_name) or {}
        obtainable = bool(event_cfg.get("obtainable", False))
        chance = float(event_cfg.get("chance", 0))
        if not obtainable:
            continue
        try:
            effective_chance = get_crate_drop_chance_with_role_bonus(chance, data, user_id, guild)
            if random.random() < effective_chance:
                add_crate_to_inventory(data, user_id, crate_id, 1)
                icon = crate.get("icon", "📦")
                display_name = crate.get("display_name", crate_id.replace("_", " ").title())
                messages.append(f"{icon} Lucky! You have obtained `x1 {display_name}` as a bonus. Use /open to open it!")
        except Exception:
            continue
    return messages


def get_store_item(item_id: str) -> dict | None:
    if isinstance(item_id, str) and item_id.startswith("crate:"):
        crate_id = item_id.split(":", 1)[1]
        crate = get_crate_by_id(crate_id)
        if not crate:
            return None
        store_cfg = (crate.get("obtained_through", {}) or {}).get("store", {})
        if not store_cfg.get("purchasable"):
            return None
        return {
            "id": f"crate:{crate_id}",
            "name": crate.get("display_name", crate_id.replace("_", " ").title()),
            "crate_id": crate_id,
            "cost": int(store_cfg.get("cost", 0)),
            "currency_type": store_cfg.get("currency_type", "won_currency"),
            "display_emoji": crate.get("icon", "📦"),
            "type": "crate",
        }

    for items in STORE_ITEMS_BY_GUILD.values():
        item = next((it for it in items if it["id"] == item_id), None)
        if item:
            return item
    return None


def get_store_items_owned(data: dict, user_id: int, guild: discord.Guild | None = None) -> list[dict]:
    purchases = get_store_purchase_data(data)
    owned_ids = [item_id for item_id, users in purchases.items() if str(user_id) in users]
    if guild:
        return [item for item in get_store_items_for_guild(guild) if item["id"] in owned_ids]
    return [item for items in STORE_ITEMS_BY_GUILD.values() for item in items if item["id"] in owned_ids]


def get_store_role_ids(guild: discord.Guild | None = None) -> list[int]:
    return [item["role_id"] for item in get_store_items_for_guild(guild)]


def get_role_metadata_by_role_id(role_id: int) -> dict | None:
    for role_data in ROLES_TABLE.values():
        if role_data.get("role_id") == role_id:
            return role_data
    return None


def get_role_table_metadata_for_item(item: dict | None) -> dict | None:
    if not item:
        return None

    role_id = item.get("role_id")
    if role_id is None:
        return None

    for role_key, role_data in ROLES_TABLE.items():
        if role_data.get("role_id") == role_id:
            return {
                "name": role_data.get("display_name", role_key),
                "description": role_data.get("description", ""),
                "rarity": role_data.get("rarity", "Common"),
                "display_emoji": RARITY_ICONS.get(role_data.get("rarity"), ""),
                "rarity_icon": RARITY_ICONS.get(role_data.get("rarity"), ""),
                "role_icon": role_data.get("role_icon") or item.get("display_emoji") or "",
            }

    return None


def get_equipped_store_role(entry: dict, guild: discord.Guild) -> discord.Role | None:
    equipped_id = entry.get("equipped_store_item")
    if not equipped_id:
        return None
    # Support two equip id formats:
    # - store item ids (e.g., 'candy_blossom') which map via STORE_ITEMS_BY_GUILD
    # - role_table entries (e.g., 'role_table:<role_id>') which directly reference a role id
    if isinstance(equipped_id, str) and equipped_id.startswith("role_table:"):
        try:
            role_id = int(equipped_id.split(":", 1)[1])
            return guild.get_role(role_id)
        except Exception:
            return None

    item = get_store_item(equipped_id)
    if not item:
        return None
    return guild.get_role(item["role_id"])


def set_equipped_store_item(data: dict, user_id: int, item_id: str) -> None:
    entry = get_or_create_economy_entry(data, user_id)
    entry["equipped_store_item"] = item_id


def get_role_access_data(data: dict) -> dict:
    return data.setdefault("role_access", {})


def has_role_access(data: dict, user_id: int, role_id: int) -> bool:
    uid_str = str(user_id)
    role_id_str = str(role_id)
    return uid_str in get_role_access_data(data).setdefault(role_id_str, [])


def grant_role_access(data: dict, user_id: int, role_id: int) -> None:
    uid_str = str(user_id)
    role_id_str = str(role_id)
    access_list = get_role_access_data(data).setdefault(role_id_str, [])
    if uid_str not in access_list:
        access_list.append(uid_str)


def revoke_role_access(data: dict, user_id: int, role_id: int) -> None:
    uid_str = str(user_id)
    role_id_str = str(role_id)
    access_list = get_role_access_data(data).setdefault(role_id_str, [])
    if uid_str in access_list:
        access_list.remove(uid_str)


def get_equippable_role_entries(data: dict, user_id: int, guild: discord.Guild | None = None) -> list[dict]:
    entries: list[dict] = []
    for item in get_store_items_owned(data, user_id, guild):
        merged_item = {**item, "source": "store_purchase"}
        role_meta = get_role_table_metadata_for_item(item)
        if role_meta:
            merged_item.update(role_meta)
        entries.append(merged_item)

    for role_key, role_data in ROLES_TABLE.items():
        role_id = role_data.get("role_id")
        if role_id is None:
            continue
        if has_role_access(data, user_id, role_id):
            entries.append({
                "id": f"role_table:{role_id}",
                "name": role_data.get("display_name", role_key),
                "role_id": role_id,
                "display_emoji": RARITY_ICONS.get(role_data.get("rarity"), ""),
                "description": role_data.get("description", ""),
                "rarity": role_data.get("rarity", "Common"),
                "source": "role_table"
            })

    rarity_order = {"Common": 0, "Rare": 1, "Epic": 2, "Legendary": 3, "Unique": 4}
    return sorted(entries, key=lambda item: (rarity_order.get(item.get("rarity", "Common"), 0), item.get("name", "")), reverse=True)


def get_equippable_role_entry(entry_id: str, guild: discord.Guild | None = None) -> dict | None:
    if entry_id.startswith("role_table:"):
        role_id = entry_id.split(":", 1)[1]
        try:
            role_id_int = int(role_id)
        except ValueError:
            return None
        for role_key, role_data in ROLES_TABLE.items():
            if role_data.get("role_id") == role_id_int:
                return {
                    "id": entry_id,
                    "name": role_data.get("display_name", role_key),
                    "role_id": role_id_int,
                    "display_emoji": RARITY_ICONS.get(role_data.get("rarity"), ""),
                    "description": role_data.get("description", ""),
                    "source": "role_table"
                }
        return None

    item = get_store_item(entry_id)
    if item:
        merged_item = {**item, "source": "store_purchase"}
        role_meta = get_role_table_metadata_for_item(item)
        if role_meta:
            merged_item.update(role_meta)
        return merged_item
    return None


def is_equippable_role_entry_accessible(data: dict, user_id: int, entry: dict) -> bool:
    if entry.get("source") == "role_table":
        return has_role_access(data, user_id, entry.get("role_id"))
    return has_user_purchased_item(data, entry.get("id"), user_id)


def get_all_equippable_role_ids(guild: discord.Guild | None = None) -> list[int]:
    role_ids = {item["role_id"] for item in get_store_items_for_guild(guild)}
    role_ids.update(role_data.get("role_id") for role_data in ROLES_TABLE.values() if role_data.get("role_id") is not None)
    return list(role_ids)


def count_role_owners(data: dict, role_id: int) -> int:
    """Count unique users who 'own' a role either via store purchase or role access (not whether they have it equipped).

    Ownership sources:
    - `role_access` mapping (users granted via `grant_role_access`)
    - `store_purchases` entries for store items that map to this role_id
    """
    owners = set()
    role_id_str = str(role_id)

    # role_access mapping: role_id_str -> [uid_str,...]
    role_access = data.get("role_access", {})
    if role_id_str in role_access:
        for uid in role_access.get(role_id_str, []):
            if isinstance(uid, str) and uid.isdigit():
                owners.add(uid)

    # store purchases: item_id -> [uid_str,...]
    purchases = data.get("store_purchases", {})
    for item_id, ulist in purchases.items():
        # find store item metadata and check its role_id
        for items in STORE_ITEMS_BY_GUILD.values():
            for item in items:
                if item.get("id") == item_id and item.get("role_id") == role_id:
                    for uid in ulist:
                        if isinstance(uid, str) and uid.isdigit():
                            owners.add(uid)
                    break

    return len(owners)


def get_streak(entry: dict) -> int:
    return entry.get("win_streak", 0)


def get_max_streak(entry: dict) -> int:
    return entry.get("max_streak", 0)


def get_equipped_role_boosts(data: dict, user_id: int, guild: discord.Guild | None = None) -> dict[str, float]:
    entry = get_or_create_economy_entry(data, user_id)
    equipped_id = entry.get("equipped_store_item")
    if not equipped_id or not guild:
        return {}

    if isinstance(equipped_id, str) and equipped_id.startswith("role_table:"):
        try:
            role_id = int(equipped_id.split(":", 1)[1])
        except ValueError:
            return {}
    else:
        item = get_equippable_role_entry(equipped_id, guild)
        role_id = item.get("role_id") if item else None

    if role_id is None:
        return {}

    member = guild.get_member(user_id)
    role = guild.get_role(role_id)
    if not member or not role or role not in member.roles:
        return {}

    for role_key, role_data in ROLES_TABLE.items():
        if role_data.get("role_id") == role_id:
            boosts = role_data.get("boosts", {}) or {}
            return {key: float(value) for key, value in boosts.items() if isinstance(value, (int, float))}

    return {}


def get_reward_with_role_bonus(base_amount: int, reward_type: str, data: dict, user_id: int, guild: discord.Guild | None = None) -> tuple[int, int]:
    if base_amount <= 0:
        return base_amount, 0

    boosts = get_equipped_role_boosts(data, user_id, guild)
    multiplier = boosts.get(reward_type)
    if not multiplier or multiplier <= 1:
        return base_amount, 0

    bonus_amount = int(round(base_amount * (multiplier - 1)))
    return base_amount + bonus_amount, bonus_amount


def get_crate_drop_chance_with_role_bonus(base_chance: float, data: dict, user_id: int, guild: discord.Guild | None = None) -> float:
    if base_chance <= 0:
        return 0.0

    boosts = get_equipped_role_boosts(data, user_id, guild)
    multiplier = boosts.get("crate_odds")
    if not multiplier or multiplier <= 1:
        return base_chance

    return min(1.0, float(base_chance) * float(multiplier))


def format_reward_amount(base_amount: int, bonus_amount: int, total_amount: int) -> str:
    if bonus_amount > 0:
        return f"+{base_amount} + {bonus_amount}"
    return f"+{base_amount}"


def reset_streak(entry: dict) -> None:
    entry["win_streak"] = 0


def increment_streak(entry: dict) -> None:
    entry["win_streak"] = entry.get("win_streak", 0) + 1
    entry["max_streak"] = max(entry.get("max_streak", 0), entry["win_streak"])


def get_work_message(amount: int, stardust: int) -> str:
    amount_text = f"{amount} {CASINO_WON_CURRENCY_EMOJI}"
    stardust_text = f"{stardust} {CASINO_STARDUST_CURRENCY_EMOJI}"
    template = random.choice(WORK_MESSAGES)
    return template.format(amount=amount_text, stardust=stardust_text)


def get_event_crate_odds_text(data: dict, user_id: int, event_name: str, guild: discord.Guild | None = None) -> str:
    lines = []
    for crate_id, crate in CRATES_TABLE.items():
        obtained_through = crate.get("obtained_through", {}) or {}
        event_cfg = obtained_through.get(event_name) or {}
        if not event_cfg.get("obtainable", False):
            continue
        try:
            chance = float(event_cfg.get("chance", 0))
            effective_chance = get_crate_drop_chance_with_role_bonus(chance, data, user_id, guild)
            percent = effective_chance * 100
            display_name = crate.get("display_name", crate_id.replace("_", " ").title())
            percentage_text = str(int(percent)) if float(percent).is_integer() else f"{percent:.1f}".rstrip("0").rstrip(".")
            lines.append(f"• **{percentage_text}%** for {display_name}")
        except Exception:
            continue
    if not lines:
        return "• **0%** chance for any crate"
    return "\n".join(lines)


def build_work_embed(user: discord.abc.User, base_amount: int, stardust: int, message: str, cooldown_until: float = None, bonus_amount: int = 0, crate_odds_text: str | None = None) -> discord.Embed:
    embed = discord.Embed(
        title="💼 Work Reward",
        description=message,
        color=discord.Color.gold()
    )
    embed.set_author(name=user.display_name, icon_url=user.display_avatar.url)
    embed.set_thumbnail(url=user.display_avatar.url)
    total_amount = base_amount + bonus_amount
    embed.add_field(name=f"{CASINO_WON_CURRENCY_EMOJI} Won", value=format_reward_amount(base_amount, bonus_amount, total_amount), inline=True)
    embed.add_field(name=f"{CASINO_STARDUST_CURRENCY_EMOJI} Stardust", value=f"+{stardust}", inline=True)
    if crate_odds_text:
        embed.add_field(name="🎁 Crate Odds", value=crate_odds_text, inline=False)
    return embed






def build_guess_game_embed(state: dict) -> discord.Embed:
    title = "🧩 Word Scramble" if state.get("game_type") == "word_scramble" else "🔢 Guess the Number"
    prompt = state.get("prompt_text", "")
    history = state.get("history", [])
    description = prompt
    if history:
        if state.get("game_type") == "number_guess":
            history_lines = [f"• {entry}" for entry in history]
            description = f"{description}\n\n**Guesses:**\n" + "\n".join(history_lines)
        else:
            description = f"{description}\n\n" + "\n".join(history)

    embed = discord.Embed(title=title, description=description, color=discord.Color.gold())
    embed.set_footer(text="Use /guess to play!")
    return embed


def build_number_guess_prompt(reward_amount: int, min_value: int, max_value: int) -> str:
    return f"Guess the number to gain **{reward_amount:,}** {CASINO_WON_CURRENCY_EMOJI}!\n\n**Range:** {min_value}-{max_value}"


def start_guess_minigame(data: dict, channel: discord.abc.Messageable | None = None) -> tuple[dict, discord.Embed]:
    state = data.setdefault("guess_minigame_state", {})
    next_game_type = state.get("next_game_type", "word_scramble")
    state["next_game_type"] = "number_guess" if next_game_type == "word_scramble" else "word_scramble"

    reward_amount = random.randint(10000, 12500)
    if next_game_type == "word_scramble":
        word = random.choice(MINIGAME_WORDS)
        scrambled = list(word)
        while True:
            random.shuffle(scrambled)
            if "".join(scrambled) != word:
                break
        scramble_text = "".join(scrambled).upper()
        prompt = f"Guess the word to gain **{reward_amount:,}** {CASINO_WON_CURRENCY_EMOJI}!\n\n**Scramble:** {scramble_text}"
        state["answer"] = word.lower()
        state["game_type"] = "word_scramble"
    else:
        secret_number = random.randint(1, 25)
        state["answer"] = secret_number
        state["game_type"] = "number_guess"
        state["lower_bound"] = 1
        state["upper_bound"] = 25
        prompt = build_number_guess_prompt(reward_amount, state["lower_bound"], state["upper_bound"])

    state["active"] = True
    state["channel_id"] = getattr(channel, "id", None)
    state["reward_amount"] = reward_amount
    state["prompt_text"] = prompt
    state["history"] = []
    state["expires_at"] = time.time() + MINIGAME_TIMEOUT_SECONDS
    state["cooldown_until"] = 0
    return state, build_guess_game_embed(state)


@app_commands.describe(guess="Your answer for the active minigame")
@bot.tree.command(name="guess", description="Try to solve the active word scramble or number guessing game.")
async def guess_command(interaction: discord.Interaction, guess: str):
    data = load_data()
    state = data.setdefault("guess_minigame_state", {})

    if not state.get("active", False):
        await interaction.response.send_message("❌ No active minigame right now. Keep chatting in the channel to start one!", ephemeral=False)
        return

    if state.get("channel_id") and interaction.channel_id != state.get("channel_id"):
        await interaction.response.send_message("❌ The active game is in another channel.", ephemeral=False)
        return

    game_type = state.get("game_type")
    reward_amount = state.get("reward_amount", 0)
    answer = state.get("answer")

    if game_type == "word_scramble":
        normalized_guess = re.sub(r"[^a-z0-9]", "", guess.lower())
        normalized_answer = re.sub(r"[^a-z0-9]", "", str(answer).lower())
        if normalized_guess == normalized_answer:
            entry = get_or_create_economy_entry(data, interaction.user.id)
            entry["won"] = entry.get("won", 0) + reward_amount
            state["active"] = False
            state["cooldown_until"] = time.time() + MINIGAME_COOLDOWN_SECONDS
            save_data(data)

            try:
                channel = interaction.channel
                if channel:
                    message = await channel.fetch_message(int(state.get("message_id", 0)))
                    success_embed = build_guess_game_embed(state)
                    success_embed.title = "✅ Correct!"
                    success_embed.description = f"{state.get('prompt_text', '')}\n\n{interaction.user.mention} guessed the word **{str(answer).upper()}** and won **{reward_amount:,}** {CASINO_WON_CURRENCY_EMOJI}!"
                    success_embed.set_footer(text="Use /guess to play!")
                    await message.edit(embed=success_embed)
            except Exception:
                pass

            await interaction.response.send_message(f"✅ Correct! You won **{reward_amount:,}** {CASINO_WON_CURRENCY_EMOJI}!", ephemeral=True)
            return

        state.setdefault("history", []).append(f"{guess.upper()} ❌")
    else:
        try:
            player_guess = int(guess)
        except ValueError:
            await interaction.response.send_message("❌ Please enter a number.", ephemeral=False)
            return

        lower_bound = state.get("lower_bound", 1)
        upper_bound = state.get("upper_bound", 25)
        if not (1 <= player_guess <= 25):
            await interaction.response.send_message("❌ Please enter a number between 1 and 25.", ephemeral=False)
            return

        if not (lower_bound <= player_guess <= upper_bound):
            await interaction.response.send_message(f"❌ Please enter a number between {lower_bound} and {upper_bound}.", ephemeral=False)
            return

        if player_guess == answer:
            entry = get_or_create_economy_entry(data, interaction.user.id)
            entry["won"] = entry.get("won", 0) + reward_amount
            state["active"] = False
            state["cooldown_until"] = time.time() + MINIGAME_COOLDOWN_SECONDS
            save_data(data)

            try:
                channel = interaction.channel
                if channel:
                    message = await channel.fetch_message(int(state.get("message_id", 0)))
                    success_embed = build_guess_game_embed(state)
                    success_embed.title = "✅ Correct!"
                    success_embed.description = f"{state.get('prompt_text', '')}\n\n{interaction.user.mention} guessed the number **{answer}** and won **{reward_amount:,}** {CASINO_WON_CURRENCY_EMOJI}!"
                    success_embed.set_footer(text="Use /guess to play!")
                    await message.edit(embed=success_embed)
            except Exception:
                pass

            await interaction.response.send_message(f"✅ Correct! You won **{reward_amount:,}** {CASINO_WON_CURRENCY_EMOJI}!", ephemeral=True)
            return

        if player_guess < answer:
            direction = "⬆️"
            state["lower_bound"] = max(lower_bound, player_guess)
        else:
            direction = "⬇️"
            state["upper_bound"] = min(upper_bound, player_guess)

        state["prompt_text"] = build_number_guess_prompt(reward_amount, state.get("lower_bound", 1), state.get("upper_bound", 25))
        state.setdefault("history", []).append(f"{player_guess} {direction}")

    try:
        channel = interaction.channel
        if channel:
            message = await channel.fetch_message(int(state.get("message_id", 0)))
            await message.edit(embed=build_guess_game_embed(state))
    except Exception:
        pass

    await interaction.response.send_message("❌ Wrong guess. Try again!", ephemeral=True)


@bot.tree.command(name="work", description="Work for a small random reward with a short cooldown.")
async def work_command(interaction: discord.Interaction):
    data = load_data()
    if not is_economy_channel(interaction.channel, interaction.user.id):
        await interaction.response.send_message(f"⚠️ Economy commands are restricted to {economy_channels_mention(interaction.guild)}.", ephemeral=True)
        return
    uid_str = str(interaction.user.id)
    cooldown_until = data.get("work_cooldowns", {}).get(uid_str)

    if cooldown_until and datetime.now().timestamp() < cooldown_until:
        remaining = max(1, int(cooldown_until - datetime.now().timestamp()))
        await interaction.response.send_message(f"⏳ You need to wait {remaining} second(s) before working again.", ephemeral=True)
        return

    base_amount = random.randint(WORK_REWARD_MIN, WORK_REWARD_MAX)
    stardust = random.randint(WORK_STARDUST_REWARD_MIN, WORK_STARDUST_REWARD_MAX)
    entry = get_or_create_economy_entry(data, interaction.user.id)
    prev_level = get_level_info(entry.get('stardust', 0))[0]
    reward_total, reward_bonus = get_reward_with_role_bonus(base_amount, "work", data, interaction.user.id, interaction.guild)
    entry["won"] = entry.get("won", 0) + reward_total
    entry["stardust"] = entry.get("stardust", 0) + stardust
    entry["work_count"] = entry.get("work_count", 0) + 1
    enforce_balance_cap(entry, get_level_info(entry.get("stardust", 0))[0])
    data.setdefault("work_cooldowns", {})[uid_str] = datetime.now().timestamp() + WORK_COOLDOWN_SECONDS
    crate_messages = roll_and_add_crates(data, interaction.user.id, "work", interaction.guild)
    crate_odds_text = get_event_crate_odds_text(data, interaction.user.id, "work", interaction.guild)
    save_data(data)
    await log_economy_action(
        interaction.user,
        "work",
        "Work payout",
        reward_total,
        entry,
        details=f"Received {stardust} stardust." if stardust else "",
        guild=interaction.guild
    )

    embed = build_work_embed(
        interaction.user,
        base_amount,
        stardust,
        get_work_message(reward_total, stardust),
        cooldown_until=data["work_cooldowns"][uid_str],
        bonus_amount=reward_bonus,
        crate_odds_text=crate_odds_text,
    )
    await interaction.response.send_message(embed=embed)
    if crate_messages:
        try:
            await interaction.followup.send("\n".join(crate_messages))
        except Exception:
            pass

    # Notify milestone if leveled up past configured thresholds
    new_level = get_level_info(entry.get('stardust', 0))[0]
    if new_level > prev_level:
        await maybe_notify_level_milestone(interaction, interaction.user, prev_level, new_level)


@bot.tree.command(name="crime", description="Commit a crime for double work rewards with risks and rewards.")
async def crime_command(interaction: discord.Interaction):
    data = load_data()
    if not is_economy_channel(interaction.channel, interaction.user.id):
        await interaction.response.send_message(f"⚠️ Economy commands are restricted to {economy_channels_mention(interaction.guild)}.", ephemeral=True)
        return
    uid_str = str(interaction.user.id)
    cooldown_until = data.get("crime_cooldowns", {}).get(uid_str)

    if cooldown_until and datetime.now().timestamp() < cooldown_until:
        remaining = max(1, int(cooldown_until - datetime.now().timestamp()))
        await interaction.response.send_message(f"⏳ You need to wait {remaining} second(s) before committing another crime.", ephemeral=True)
        return

    # 80% success chance
    success = random.random() < 0.80
    entry = get_or_create_economy_entry(data, interaction.user.id)
    data.setdefault("crime_cooldowns", {})[uid_str] = datetime.now().timestamp() + CRIME_COOLDOWN_SECONDS

    if success:
        # Crime succeeded: 2x work money + the same stardust reward as work
        base_amount = random.randint(WORK_REWARD_MIN * 2, WORK_REWARD_MAX * 2)
        stardust = random.randint(WORK_STARDUST_REWARD_MIN, WORK_STARDUST_REWARD_MAX)
        prev_level = get_level_info(entry.get('stardust', 0))[0]
        reward_total, reward_bonus = get_reward_with_role_bonus(base_amount, "crime", data, interaction.user.id, interaction.guild)
        entry["won"] = entry.get("won", 0) + reward_total
        entry["stardust"] = entry.get("stardust", 0) + stardust
        enforce_balance_cap(entry, get_level_info(entry.get("stardust", 0))[0])

        # Roll for any configured crate drops for the 'crime' event
        crate_messages = roll_and_add_crates(data, interaction.user.id, "crime", interaction.guild)

        save_data(data)
        await log_economy_action(
            interaction.user,
            "crime",
            "Crime succeeded",
            reward_total,
            entry,
            details=f"Received {stardust} stardust." if stardust else "",
            guild=interaction.guild
        )
        # Build a nicer embed similar to /work
        desc = f"You got away with it and earned **{reward_total:,}** {CASINO_WON_CURRENCY_EMOJI}!"
        if crate_messages:
            desc += "\n" + "\n".join(crate_messages)
        embed = discord.Embed(title="🚨 Crime Succeeded!", description=desc, color=discord.Color.green())
        embed.set_author(name=interaction.user.display_name, icon_url=interaction.user.display_avatar.url)
        embed.set_thumbnail(url=interaction.user.display_avatar.url)
        embed.add_field(name=f"{CASINO_WON_CURRENCY_EMOJI} Won", value=format_reward_amount(base_amount, reward_bonus, reward_total), inline=True)
        embed.add_field(name=f"{CASINO_STARDUST_CURRENCY_EMOJI} Stardust", value=f"+{stardust}", inline=True)

        new_level = get_level_info(entry.get('stardust', 0))[0]
        if new_level > prev_level:
            await maybe_notify_level_milestone(interaction, interaction.user, prev_level, new_level)
    else:
        # Crime failed: fined 200-300
        fine = random.randint(200, 300)
        entry["won"] = max(0, entry.get("won", 0) - fine)
        enforce_balance_cap(entry, get_level_info(entry.get("stardust", 0))[0])
        save_data(data)
        await log_economy_action(
            interaction.user,
            "crime",
            "Crime failed",
            fine,
            entry,
            details="Fine applied.",
            guild=interaction.guild
        )
        embed = discord.Embed(title="💔 Crime Failed!", description=f"You were caught and fined **{fine:,}** {CASINO_WON_CURRENCY_EMOJI}!", color=discord.Color.red())
        embed.set_author(name=interaction.user.display_name, icon_url=interaction.user.display_avatar.url)
        embed.set_thumbnail(url=interaction.user.display_avatar.url)
        embed.add_field(name="Fine", value=f"-{fine}", inline=True)

    await interaction.response.send_message(embed=embed)


def normalize_roulette_option(option: str) -> str:
    """Normalizes roulette bet choices to one of red/black/even/odd/green."""
    normalized = (option or "").strip().lower()
    aliases = {
        "red": "red",
        "black": "black",
        "green": "green",
        "even": "even",
        "odd": "odd",
        "even number": "even",
        "odd number": "odd",
    }
    return aliases.get(normalized, normalized)


def get_roulette_color(number: int) -> str:
    """Returns 'Red' or 'Black' for a roulette number (0-24)."""
    if number == 0:
        return "Green"
    red_numbers = {1, 3, 5, 7, 9, 12, 14, 16, 18, 21, 23}
    return "Red" if number in red_numbers else "Black"


def is_roulette_choice_winning(option: str, number: int) -> bool:
    """Returns whether a roulette bet option wins for the landed number."""
    normalized_option = normalize_roulette_option(option)
    if normalized_option == "green":
        return number == 0
    if normalized_option in {"red", "black"}:
        return get_roulette_color(number).lower() == normalized_option
    if normalized_option == "even":
        return number != 0 and number % 2 == 0
    if normalized_option == "odd":
        return number != 0 and number % 2 == 1
    return False


def get_roulette_display(number: int) -> str:
    """Returns formatted roulette result like 'Red 5 (Odd)' or 'Black 12 (Even)'."""
    color = get_roulette_color(number)
    if number == 0:
        return f"{color} {number}"
    parity = "Even" if number % 2 == 0 else "Odd"
    return f"{color} {number} ({parity})"


class WorkStationButton(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="Work", style=discord.ButtonStyle.success, custom_id="work_station_btn")
    async def work_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = load_data()
        uid_str = str(interaction.user.id)
        cooldown_until = data.get("work_cooldowns", {}).get(uid_str)
        
        if cooldown_until and datetime.now().timestamp() < cooldown_until:
            remaining = max(1, int(cooldown_until - datetime.now().timestamp()))
            await interaction.response.send_message(f"⏳ You need to wait {remaining} second(s) before working again.", ephemeral=True)
            return
        
        base_amount = random.randint(WORK_REWARD_MIN, WORK_REWARD_MAX)
        stardust = random.randint(WORK_STARDUST_REWARD_MIN, WORK_STARDUST_REWARD_MAX)
        entry = get_or_create_economy_entry(data, interaction.user.id)
        prev_level = get_level_info(entry.get('stardust', 0))[0]
        reward_total, reward_bonus = get_reward_with_role_bonus(base_amount, "work", data, interaction.user.id, interaction.guild)
        entry["won"] = entry.get("won", 0) + reward_total
        entry["stardust"] = entry.get("stardust", 0) + stardust
        entry["work_count"] = entry.get("work_count", 0) + 1
        enforce_balance_cap(entry, get_level_info(entry.get("stardust", 0))[0])
        data.setdefault("work_cooldowns", {})[uid_str] = datetime.now().timestamp() + WORK_COOLDOWN_SECONDS
        
        crate_messages = roll_and_add_crates(data, interaction.user.id, "work")
        crate_odds_text = get_event_crate_odds_text(data, interaction.user.id, "work", interaction.guild)
        save_data(data)

        embed = build_work_embed(
            interaction.user,
            base_amount,
            stardust,
            get_work_message(reward_total, stardust),
            bonus_amount=reward_bonus,
            crate_odds_text=crate_odds_text,
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

        # If crate(s) dropped, post a short mention in the channel and remove it shortly after
        if crate_messages:
            try:
                mention_msg = await interaction.channel.send(f"{interaction.user.mention} " + "\n".join(crate_messages))
                await asyncio.sleep(5)
                await mention_msg.delete()
            except Exception:
                pass


crate_choices = [
    app_commands.Choice(name=crate_data["display_name"], value=crate_id)
    for crate_id, crate_data in CRATES_TABLE.items()
]

crates_group = app_commands.Group(name="crates", description="Crate administration and opening")


@crates_group.command(name="add", description="Admin: add crates to a member's inventory.")
@app_commands.describe(member="The member to receive crates", crate="The crate to grant", amount="How many crates to give")
@app_commands.choices(crate=crate_choices)
async def crates_add(interaction: discord.Interaction, member: discord.Member, crate: app_commands.Choice[str], amount: int):
    if interaction.user.id not in DEV_USER_IDS and not (isinstance(interaction.user, discord.Member) and interaction.user.guild_permissions.administrator):
        await interaction.response.send_message("❌ You are not authorized to use this command.", ephemeral=True)
        return

    if amount <= 0:
        await interaction.response.send_message("❌ Amount must be a positive integer.", ephemeral=True)
        return

    data = load_data()
    add_crate_to_inventory(data, member.id, crate.value, amount)
    save_data(data)
    await log_economy_action(
        interaction.user,
        "crates add",
        "Added crates",
        amount,
        get_or_create_economy_entry(data, member.id),
        details=f"Added {amount}x {get_crate_display_name(crate.value)} to {member}.",
        guild=interaction.guild
    )

    embed = discord.Embed(title="Crates Added", color=discord.Color.green())
    embed.add_field(name="Recipient", value=member.mention, inline=True)
    embed.add_field(name="Crate", value=get_crate_display_name(crate.value), inline=True)
    embed.add_field(name="Amount", value=str(amount), inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


bot.tree.add_command(crates_group)

crate_group = app_commands.Group(name="crate", description="Crate utilities")

class CratePurchaseConfirmationView(discord.ui.View):
    def __init__(self, user_id: int, crate_id: str, amount: int, total_cost: int):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.crate_id = crate_id
        self.amount = amount
        self.total_cost = total_cost

    @discord.ui.button(label="Yes", style=discord.ButtonStyle.success)
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This purchase confirmation is not for you.", ephemeral=True)
            return

        data = load_data()
        entry = get_or_create_economy_entry(data, interaction.user.id)
        crate_data = get_crate_by_id(self.crate_id)
        if not crate_data:
            await interaction.response.edit_message(content="❌ That crate could not be found.", view=None)
            return

        if entry.get("won", 0) < self.total_cost:
            await interaction.response.edit_message(content="❌ You no longer have enough won to complete this purchase.", view=None)
            return

        entry["won"] -= self.total_cost
        add_crate_to_inventory(data, interaction.user.id, self.crate_id, self.amount)
        save_data(data)

        crate_icon = crate_data.get("icon", "📦")
        display_name = crate_data.get("display_name", self.crate_id.replace("_", " ").title())
        await log_economy_action(
            interaction.user,
            "crate buy",
            "Crate purchased",
            self.total_cost,
            entry,
            details=f"Purchased {self.amount}x {display_name}.",
            guild=interaction.guild
        )

        await interaction.response.edit_message(
            content=f"✅ Purchased {crate_icon} {display_name} x{self.amount} for {self.total_cost:,} {CASINO_WON_CURRENCY_EMOJI}.",
            view=None,
        )

    @discord.ui.button(label="No", style=discord.ButtonStyle.danger)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This purchase confirmation is not for you.", ephemeral=True)
            return
        await interaction.response.edit_message(content="Purchase cancelled.", view=None)


@crate_group.command(name="buy", description="Purchase crates from the store with won.")
@app_commands.describe(crate="The crate to purchase", amount="How many crates to buy")
@app_commands.choices(crate=crate_choices)
async def crate_buy_command(interaction: discord.Interaction, crate: app_commands.Choice[str], amount: int):
    if not is_economy_channel(interaction.channel, interaction.user.id):
        await interaction.response.send_message(f"⚠️ Economy commands are restricted to {economy_channels_mention(interaction.guild)}.", ephemeral=True)
        return

    if amount < 1:
        await interaction.response.send_message("❌ You need to buy at least 1 crate.", ephemeral=True)
        return

    crate_data = get_crate_by_id(crate.value)
    if not crate_data:
        await interaction.response.send_message("❌ That crate could not be found.", ephemeral=True)
        return

    store_cfg = (crate_data.get("obtained_through", {}) or {}).get("store", {})
    if not store_cfg.get("purchasable"):
        await interaction.response.send_message("❌ These crates cannot be bought.", ephemeral=True)
        return

    cost = int(store_cfg.get("cost", 0))
    total_cost = cost * amount
    data = load_data()
    entry = get_or_create_economy_entry(data, interaction.user.id)
    if entry.get("won", 0) < total_cost:
        await interaction.response.send_message(f"❌ You need at least {total_cost:,} {CASINO_WON_CURRENCY_EMOJI} to buy {amount} of this crate.", ephemeral=True)
        return

    crate_icon = crate_data.get("icon", "📦")
    display_name = crate_data.get("display_name", crate.value.replace("_", " ").title())
    view = CratePurchaseConfirmationView(interaction.user.id, crate.value, amount, total_cost)
    await interaction.response.send_message(
        f"You're purchasing {crate_icon} {display_name} x{amount} for {total_cost:,} {CASINO_WON_CURRENCY_EMOJI}. Would you like to continue?",
        view=view,
        ephemeral=True,
    )


@crate_group.command(name="view", description="View the rewards and odds for a crate.")
@app_commands.describe(crate="The crate to inspect")
@app_commands.choices(crate=crate_choices)
async def crate_view_command(interaction: discord.Interaction, crate: app_commands.Choice[str]):
    crate_data = get_crate_by_id(crate.value)
    if not crate_data:
        await interaction.response.send_message("❌ Could not find that crate.", ephemeral=True)
        return

    icon = crate_data.get("icon", "📦")
    display_name = crate_data.get("display_name", crate.value.replace("_", " ").title())
    embed = discord.Embed(
        title=f"{icon} {display_name} Contents",
        description=f"Here are the odds for what you can get from {display_name}.",
        color=discord.Color.gold()
    )

    reward_lines = []
    data = load_data()
    for reward_id, reward in crate_data.get("rewards", {}).items():
        chance = format_crate_weight(reward.get("weight", 0))
        name = reward.get("display_name", reward_id.replace("_", " ").title())
        if reward.get("type") == "role":
            role_meta = get_role_metadata_by_role_id(int(reward.get("role_id", 0)))
            role_icon = role_meta.get("role_icon") if role_meta else ""
            if role_icon:
                name = f"{role_icon} {name}"
        elif reward.get("type") == "crate":
            nested = get_crate_by_id(reward.get("crate") or reward.get("crate_id"))
            if nested:
                nested_icon = nested.get("icon", "📦")
                name = f"{nested_icon} {name}"
        copy_text = get_limited_role_reward_copy_text(data, crate.value, reward_id, reward)
        reward_lines.append(f"• {name}{copy_text} — {chance}%")

    if reward_lines:
        embed.add_field(name="Rewards", value="\n".join(reward_lines), inline=False)
    else:
        embed.add_field(name="Rewards", value="No rewards configured.", inline=False)

    store_cfg = (crate_data.get("obtained_through", {}) or {}).get("store", {})
    if store_cfg.get("purchasable"):
        price_text = f"{CASINO_WON_CURRENCY_EMOJI} {int(store_cfg.get('cost', 0)):,}"
        embed.add_field(name="Store Purchase", value=f"Available in /store → Crates for {price_text}", inline=False)

    await interaction.response.send_message(embed=embed, ephemeral=True)

bot.tree.add_command(crate_group)


async def maybe_send_unique_crate_jackpot(interaction: discord.Interaction, crate_id: str, reward: dict, reward_name: str, chance_text: str) -> None:
    if reward.get("type") != "role":
        return

    role_meta = get_role_metadata_by_role_id(int(reward.get("role_id", 0)))
    if not role_meta or str(role_meta.get("rarity", "")).lower() != "unique":
        return

    crate_data = get_crate_by_id(crate_id)
    crate_display_name = crate_data.get("display_name", crate_id.replace("_", " ").title()) if crate_data else crate_id.replace("_", " ").title()
    role_display_name = role_meta.get("display_name", reward_name)
    channel = bot.get_channel(JACKPOT_CHANNEL_ID)
    if channel is None:
        return

    embed = discord.Embed(
        title="🎉 Jackpot!",
        description=f"{interaction.user.mention} has just obtained a {role_display_name} from a {crate_display_name}! [{chance_text}%]",
        color=discord.Color.gold()
    )
    await channel.send(embed=embed)


class CrateOpenView(discord.ui.View):
    def __init__(self, user_id: int, crate_id: str):
        super().__init__(timeout=300)
        self.user_id = user_id
        self.crate_id = crate_id
        self.rewards_list = []
        self.open_1_button.label = "Open 1x"
        self.open_3_button.label = "Open 3x"

    async def update_crates_left(self):
        """Update button labels and availability with the current crate count."""
        data = load_data()
        entry = get_or_create_economy_entry(data, self.user_id)
        crates = get_crate_inventory(entry)
        crates_left = crates.get(self.crate_id, 0)
        self.open_1_button.label = f"Open 1x ({crates_left} left)"
        self.open_3_button.label = f"Open 3x ({crates_left} left)"
        self.open_1_button.disabled = crates_left <= 0
        self.open_3_button.disabled = crates_left < 3

    async def _open_amount(self, interaction: discord.Interaction, amount: int):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This crate opener is not for you.", ephemeral=True)
            return

        data = load_data()
        entry = get_or_create_economy_entry(data, interaction.user.id)
        crates = get_crate_inventory(entry)
        if crates.get(self.crate_id, 0) < amount:
            await interaction.response.send_message(f"❌ You need at least {amount} {get_crate_display_name(self.crate_id)}(s) to use that button.", ephemeral=True)
            return

        crate_data = get_crate_by_id(self.crate_id)
        if not crate_data:
            await interaction.response.send_message("❌ That crate could not be found.", ephemeral=True)
            return

        reward_lines = []
        total_reward_amount = 0
        for _ in range(amount):
            reward, reward_id = choose_crate_reward(crate_data, data, self.crate_id)
            if not reward:
                await interaction.response.send_message("❌ This crate has no rewards configured.", ephemeral=True)
                return

            if not remove_crate_from_inventory(data, interaction.user.id, self.crate_id, 1):
                await interaction.response.send_message("❌ You no longer have that crate to open.", ephemeral=True)
                return

            reward_name, is_duplicate_reward = await apply_crate_reward(data, interaction.user.id, reward, interaction, crate_id=self.crate_id, reward_id=reward_id)
            reward_amount = int(reward.get("amount", 0)) if reward.get("type") in {"won_currency", "stardust_currency"} else 0
            total_reward_amount += reward_amount

            chance_text = format_crate_weight(reward.get("weight", 0))
            reward_lines.append(f"+ {reward_name} [{chance_text}%]")
            if not is_duplicate_reward:
                await maybe_send_unique_crate_jackpot(interaction, self.crate_id, reward, reward_name, chance_text)

        save_data(data)
        await log_economy_action(
            interaction.user,
            "open",
            f"Opened {amount}x crate {self.crate_id}",
            total_reward_amount,
            entry,
            details=f"Received {amount} crate rewards",
            guild=interaction.guild
        )

        self.rewards_list.extend(reward_lines)
        data = load_data()
        entry = get_or_create_economy_entry(data, self.user_id)
        crates = get_crate_inventory(entry)
        crates_left = crates.get(self.crate_id, 0)
        self.open_1_button.label = f"Open 1x ({crates_left} left)"
        self.open_3_button.label = f"Open 3x ({crates_left} left)"
        self.open_1_button.disabled = crates_left <= 0
        self.open_3_button.disabled = crates_left < 3

        crate_icon = crate_data.get("icon", "📦") if crate_data else ""
        embed = interaction.message.embeds[0]
        embed.title = f"{crate_icon} {crate_data['display_name']} Opened"
        embed.description = "\n".join(self.rewards_list)

        await interaction.response.defer()
        await interaction.message.edit(embed=embed, view=self)

    @discord.ui.button(label="Open 1x")
    async def open_1_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._open_amount(interaction, 1)

    @discord.ui.button(label="Open 3x")
    async def open_3_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._open_amount(interaction, 3)


@bot.tree.command(name="open", description="Open one of your crates or a selected number of crates at once.")
@app_commands.describe(crate="The crate to open", amount="How many crates to open at once (defaults to 1)")
@app_commands.choices(crate=crate_choices)
async def open_command(interaction: discord.Interaction, crate: app_commands.Choice[str], amount: int = 1):
    if not is_economy_channel(interaction.channel, interaction.user.id):
        await interaction.response.send_message(f"⚠️ Economy commands are restricted to {economy_channels_mention(interaction.guild)}.", ephemeral=True)
        return

    if amount < 1:
        await interaction.response.send_message("❌ You need to open at least 1 crate.", ephemeral=True)
        return

    data = load_data()
    entry = get_or_create_economy_entry(data, interaction.user.id)
    crates = get_crate_inventory(entry)
    available_count = crates.get(crate.value, 0)
    if available_count <= 0:
        await interaction.response.send_message(f"❌ You do not own any {get_crate_display_name(crate.value)}.", ephemeral=True)
        return

    if amount > available_count:
        await interaction.response.send_message(f"❌ You only have {available_count} {get_crate_display_name(crate.value)}(s) available.", ephemeral=True)
        return

    crate_data = get_crate_by_id(crate.value)
    if not crate_data:
        await interaction.response.send_message("❌ That crate could not be found.", ephemeral=True)
        return

    reward, reward_id = choose_crate_reward(crate_data, data, crate.value)
    if not reward:
        await interaction.response.send_message("❌ This crate has no rewards configured.", ephemeral=True)
        return

    if amount == 1:
        if not remove_crate_from_inventory(data, interaction.user.id, crate.value, 1):
            await interaction.response.send_message("❌ You no longer have that crate to open.", ephemeral=True)
            return

        reward_name, is_duplicate_reward = await apply_crate_reward(
            data,
            interaction.user.id,
            reward,
            interaction,
            crate_id=crate.value,
            reward_id=reward_id,
        )
        reward_amount = int(reward.get("amount", 0)) if reward.get("type") in {"won_currency", "stardust_currency"} else 0
        await log_economy_action(
            interaction.user,
            "open",
            f"Opened crate {crate.value}",
            reward_amount,
            entry,
            details=f"Received {reward_name}",
            guild=interaction.guild
        )

        chance_text = format_crate_weight(reward.get("weight", 0))
        reward_text = f"+ {reward_name} [{chance_text}%]"
        if not is_duplicate_reward:
            await maybe_send_unique_crate_jackpot(interaction, crate.value, reward, reward_name, chance_text)

        crate_icon = crate_data.get("icon", "📦") if crate_data else ""
        save_data(data)

        view = CrateOpenView(interaction.user.id, crate.value)
        view.rewards_list.append(reward_text)
        await view.update_crates_left()

        embed = discord.Embed(
            title=f"{crate_icon} {crate_data['display_name']} Opened",
            description=reward_text,
            color=discord.Color.gold()
        )
        embed.set_footer(text="Click 'Open 1x' to open another crate")

        await interaction.response.send_message(embed=embed, view=view)
        return

    await interaction.response.defer(ephemeral=True)
    reward_lines = []
    total_reward_amount = 0

    for _ in range(amount):
        reward, reward_id = choose_crate_reward(crate_data, data, crate.value)
        if not reward:
            await interaction.followup.send("❌ This crate has no rewards configured.", ephemeral=True)
            return

        if not remove_crate_from_inventory(data, interaction.user.id, crate.value, 1):
            await interaction.followup.send("❌ You no longer have that crate to open.", ephemeral=True)
            return

        reward_name, is_duplicate_reward = await apply_crate_reward(
            data,
            interaction.user.id,
            reward,
            interaction,
            crate_id=crate.value,
            reward_id=reward_id,
        )
        reward_amount = int(reward.get("amount", 0)) if reward.get("type") in {"won_currency", "stardust_currency"} else 0
        total_reward_amount += reward_amount

        chance_text = format_crate_weight(reward.get("weight", 0))
        reward_lines.append(f"• {reward_name} [{chance_text}%]")
        if not is_duplicate_reward:
            await maybe_send_unique_crate_jackpot(interaction, crate.value, reward, reward_name, chance_text)

    save_data(data)
    await log_economy_action(
        interaction.user,
        "open",
        f"Opened {amount}x {crate.value}",
        total_reward_amount,
        entry,
        details=f"Received {amount} crate rewards",
        guild=interaction.guild
    )

    embed = discord.Embed(
        title=f"{get_crate_by_id(crate.value)['display_name']} Opened x{amount}",
        description="\n".join(reward_lines),
        color=discord.Color.gold()
    )
    embed.set_footer(text=f"Opened {amount} crate(s) from your inventory")
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="profile", description="Show a member's currency profile in a polished embed.")
@app_commands.describe(member="Optional member to inspect")
async def profile_command(interaction: discord.Interaction, member: discord.Member = None):
    target = member or interaction.user
    data = load_data()
    if not is_economy_channel(interaction.channel, interaction.user.id):
        await interaction.response.send_message(f"⚠️ Economy commands are restricted to {economy_channels_mention(interaction.guild)}.", ephemeral=True)
        return
    entry = get_or_create_economy_entry(data, target.id)
    stardust = entry.get("stardust", 0)
    level, xp_in_level, xp_needed = get_level_info(stardust)
    progress = xp_in_level / xp_needed if xp_needed else 1.0
    bar = format_level_bar(progress)
    total_balance = entry.get("won", 0) + entry.get("bank", 0)

    # Determine user icon based on developer status
    user_icon = "<:verified_developer:1522026472502464672>" if target.id in DEV_USER_IDS else "<:person:1522026510859505684>"
    
    embed = discord.Embed(
        title=f"{user_icon} {target.display_name}'s Profile",
        description="** **",
        color=discord.Color.from_rgb(255, 215, 0)
    )
    embed.set_thumbnail(url=target.display_avatar.url)
    bank_cap = get_bank_cap(level)
    equipped_role = get_equipped_store_role(entry, interaction.guild)
    equipped_text = equipped_role.mention if equipped_role else "None"

    embed.add_field(name=f"{CASINO_WON_CURRENCY_EMOJI} Cash", value=f"{entry.get('won', 0):,}", inline=True)
    embed.add_field(name=f"{CASINO_WON_CURRENCY_EMOJI} Bank", value=f"{entry.get('bank', 0):,}/{bank_cap:,}", inline=True)
    embed.add_field(name=f"{CASINO_WON_CURRENCY_EMOJI} Total", value=f"{total_balance:,}", inline=True)
    embed.add_field(name="Equipped", value=equipped_text, inline=True)
    
    # Display win/loss stats
    wins = entry.get("minigame_wins", 0)
    losses = entry.get("minigame_losses", 0)
    total_games = wins + losses
    win_rate = (wins / total_games * 100) if total_games > 0 else 0
    work_count = entry.get("work_count", 0)
    current_streak = get_streak(entry)
    max_streak = get_max_streak(entry)
    streak_emoji = "<:streak:1521997955572437192>" if max_streak < 5 else "<:purple_streak:1521997979941081360>"
    embed.add_field(name="Streak", value=f"🔥 {current_streak} (Best: {max_streak} {streak_emoji})", inline=True)
    embed.add_field(name="Wins", value=f"{wins}", inline=True)
    embed.add_field(name="Losses", value=f"{losses}", inline=True)
    embed.add_field(name="W/R", value=f"{win_rate:.1f}%", inline=True)
    embed.add_field(name="Work Count", value=f"{work_count:,}", inline=True)
    embed.add_field(name="", value=f"Level {level}: {CASINO_STARDUST_CURRENCY_EMOJI} {xp_in_level}/{xp_needed}\n{bar}", inline=False)
    embed.set_footer(text="Work and grow your balance with /work")

    if target.id == interaction.user.id:
        view = ProfileInventoryView(interaction.user.id)
        await interaction.response.send_message(embed=embed, view=view)
    else:
        await interaction.response.send_message(embed=embed)


@bot.tree.command(name="balance", description="Show your current cash, bank, and total balance.")
@app_commands.describe(member="(Optional) Member to check balance of. If not provided, shows your balance.")
async def balance_command(interaction: discord.Interaction, member: discord.Member | None = None):
    if not is_economy_channel(interaction.channel, interaction.user.id):
        await interaction.response.send_message(f"⚠️ Economy commands are restricted to {economy_channels_mention(interaction.guild)}.", ephemeral=True)
        return

    target_user = member or interaction.user
    data = load_data()
    entry = get_or_create_economy_entry(data, target_user.id)
    stardust = entry.get("stardust", 0)
    level = get_level_info(stardust)[0]
    bank_cap = get_bank_cap(level)
    
    cash = entry.get("won", 0)
    bank = entry.get("bank", 0)
    total = cash + bank

    embed = discord.Embed(
        title=f"💰 {target_user.display_name}'s Balance",
        color=discord.Color.gold()
    )
    embed.add_field(name=f"{CASINO_WON_CURRENCY_EMOJI} Cash", value=f"{cash:,}", inline=True)
    embed.add_field(name=f"{CASINO_WON_CURRENCY_EMOJI} Bank", value=f"{bank:,}/{bank_cap:,}", inline=True)
    embed.add_field(name=f"{CASINO_WON_CURRENCY_EMOJI} Total", value=f"{total:,}", inline=True)
    embed.set_thumbnail(url=target_user.display_avatar.url)
    
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="rob", description="Attempt to rob another member for a share of their cash. 1 hour cooldown.")
@app_commands.describe(member="Member to attempt to rob")
async def rob_command(interaction: discord.Interaction, member: discord.Member):
    if not is_economy_channel(interaction.channel, interaction.user.id):
        await interaction.response.send_message(f"⚠️ Economy commands are restricted to {economy_channels_mention(interaction.guild)}.", ephemeral=True)
        return

    if member.id == interaction.user.id:
        await interaction.response.send_message("❌ You cannot rob yourself.", ephemeral=True)
        return

    data = load_data()
    uid_str = str(interaction.user.id)
    cooldown_until = data.get("rob_cooldowns", {}).get(uid_str)
    now_ts = datetime.now().timestamp()
    if cooldown_until and now_ts < cooldown_until:
        remaining = max(0, int(cooldown_until - now_ts))
        minutes, seconds = divmod(remaining, 60)
        await interaction.response.send_message(f"⏳ You are on cooldown. Try again in {minutes}m {seconds}s.", ephemeral=True)
        return

    robber_entry = get_or_create_economy_entry(data, interaction.user.id)
    target_entry = get_or_create_economy_entry(data, member.id)
    target_cash = target_entry.get("won", 0)
    robber_total = robber_entry.get("won", 0) + robber_entry.get("bank", 0)

    penalty_amount = max(1, int(robber_total * 0.05))
    success = random.random() < 0.7
    result_title = "Robbery Attempt"
    embed = discord.Embed(title=result_title, color=discord.Color.dark_red())
    embed.set_author(name=interaction.user.display_name, icon_url=interaction.user.display_avatar.url)
    embed.add_field(name="Target", value=member.mention, inline=True)

    if target_cash <= 0:
        charged = min(penalty_amount, robber_entry.get("won", 0))
        robber_entry["won"] = robber_entry.get("won", 0) - charged
        if charged < penalty_amount:
            shortfall = penalty_amount - charged
            robber_entry["bank"] = max(0, robber_entry.get("bank", 0) - shortfall)
        embed.description = f"❌ {member.display_name} has no cash on hand. Security caught you and fined you {penalty_amount:,} {CASINO_WON_CURRENCY_EMOJI}."
    elif success:
        min_amount = max(1, int(target_cash * 0.5))
        max_amount = max(min_amount, int(target_cash * 0.7))
        stolen = random.randint(min_amount, max_amount)
        target_entry["won"] = max(0, target_cash - stolen)
        robber_entry["won"] = robber_entry.get("won", 0) + stolen
        embed.description = f"💰 Success! You robbed {member.mention} for {stolen:,} {CASINO_WON_CURRENCY_EMOJI}."
        embed.add_field(name="Robbery Chance", value="70% success", inline=True)
        try:
            await member.send(f"⚠️ You were robbed by {interaction.user.display_name} and lost {stolen:,} {CASINO_WON_CURRENCY_EMOJI}.")
        except (discord.Forbidden, discord.HTTPException):
            pass
    else:
        charged = min(penalty_amount, robber_entry.get("won", 0))
        robber_entry["won"] = robber_entry.get("won", 0) - charged
        if charged < penalty_amount:
            shortfall = penalty_amount - charged
            robber_entry["bank"] = max(0, robber_entry.get("bank", 0) - shortfall)
        embed.description = f"❌ Your attempt failed. You were caught and fined {penalty_amount:,} {CASINO_WON_CURRENCY_EMOJI}."
        embed.add_field(name="Robbery Chance", value="70% success", inline=True)

    data.setdefault("rob_cooldowns", {})[uid_str] = now_ts + ROB_COOLDOWN_SECONDS
    save_data(data)
    await log_economy_action(
        interaction.user,
        "rob",
        "Robbery success" if success else "Robbery failed",
        stolen if success else penalty_amount,
        robber_entry,
        details=(f"Target: {member}." if success else f"Caught and fined {penalty_amount:,}.") if target_cash > 0 else f"No cash target, charged {penalty_amount:,}.",
        guild=interaction.guild
    )
    embed.set_footer(text="Cooldown: 1 hour")
    await interaction.response.send_message(embed=embed)


def build_horse_race_embed(host: discord.Member, bet_amount: int, challenger: discord.Member | None = None) -> discord.Embed:
    challenger_display = challenger.mention if challenger else "Waiting for challenger..."
    embed = discord.Embed(
        title="🏁 Horse Race Challenge",
        description=f"{host.mention} has started a horse race wager for {bet_amount:,} {CASINO_WON_CURRENCY_EMOJI}. Click Join to compete!",
        color=discord.Color.blurple()
    )
    embed.add_field(name="Horse 1", value=f"🐎 {host.mention}", inline=False)
    embed.add_field(name="Horse 2", value=f"🐎 {challenger_display}", inline=False)
    embed.set_footer(text="Waiting for a second rider. Race times out after 2 minutes.")
    return embed


def build_horse_result_embed(host: discord.Member, challenger: discord.Member, bet_amount: int, winner: discord.Member) -> discord.Embed:
    embed = discord.Embed(
        title="🏆 Horse Race Result",
        description=f"The race is over! {winner.mention} wins {bet_amount * 2:,} {CASINO_WON_CURRENCY_EMOJI}.",
        color=discord.Color.gold()
    )
    embed.add_field(name="Horse 1", value=f"🐎 {host.mention}", inline=True)
    embed.add_field(name="Horse 2", value=f"🐎 {challenger.mention}", inline=True)
    embed.add_field(name="Winner", value=winner.mention, inline=False)
    embed.add_field(name="Wager", value=f"{bet_amount:,} each", inline=False)
    return embed


class HorseRaceView(discord.ui.View):
    def __init__(self, host: discord.Member, bet_amount: int, guild: discord.Guild):
        super().__init__(timeout=HORSE_RACE_TIMEOUT_SECONDS)
        self.host = host
        self.bet_amount = bet_amount
        self.guild = guild
        self.challenger: discord.Member | None = None
        self.reserved = True
        self.join_button = discord.ui.Button(label="Join", style=discord.ButtonStyle.success)
        self.join_button.callback = self.handle_join
        self.add_item(self.join_button)
        self.message: discord.Message | None = None

    async def handle_join(self, interaction: discord.Interaction):
        if interaction.user.id == self.host.id:
            await interaction.response.send_message("❌ You cannot join your own horse race.", ephemeral=True)
            return

        if self.challenger is not None:
            await interaction.response.send_message("❌ This race already has two riders.", ephemeral=True)
            return

        data = load_data()
        challenger_entry = get_or_create_economy_entry(data, interaction.user.id)
        if challenger_entry.get("won", 0) < self.bet_amount:
            await interaction.response.send_message(f"❌ You need at least {self.bet_amount:,} {CASINO_WON_CURRENCY_EMOJI} to join.", ephemeral=True)
            return

        level = get_level_info(challenger_entry.get('stardust', 0))[0]
        bet_cap = get_bet_cap(level)
        if self.bet_amount > bet_cap:
            await interaction.response.send_message(f"❌ Your bet exceeds the per-bet cap for your level ({bet_cap:,} {CASINO_WON_CURRENCY_EMOJI}).", ephemeral=True)
            return

        self.challenger = interaction.user
        challenger_entry["won"] -= self.bet_amount
        host_entry = get_or_create_economy_entry(data, self.host.id)
        if self.reserved is False:
            await interaction.response.send_message("❌ The race is no longer available.", ephemeral=True)
            return

        winner = random.choice([self.host, self.challenger])
        loser = self.challenger if winner.id == self.host.id else self.host
        winner_entry = get_or_create_economy_entry(data, winner.id)
        loser_entry = get_or_create_economy_entry(data, loser.id)
        winner_entry["won"] += self.bet_amount * 2
        winner_entry["minigame_wins"] = winner_entry.get("minigame_wins", 0) + 1
        loser_entry["minigame_losses"] = loser_entry.get("minigame_losses", 0) + 1
        stardust_reward = random.randint(MINIGAME_WIN_STARDUST_REWARD_MINIMUM, MINIGAME_WIN_STARDUST_REWARD_MAXIMUM)
        winner_entry["stardust"] = winner_entry.get("stardust", 0) + stardust_reward
        self.reserved = False
        data.setdefault("rob_cooldowns", {})  # ensure field exists for save format compatibility
        save_data(data)

        await log_economy_action(
            winner,
            "horse race",
            "Horse race won",
            self.bet_amount * 2,
            winner_entry,
            details=f"Won against {loser} and received {self.bet_amount * 2:,}.",
            guild=self.guild
        )
        await log_economy_action(
            loser,
            "horse race",
            "Horse race lost",
            self.bet_amount,
            loser_entry,
            details=f"Lost to {winner}.",
            guild=self.guild
        )

        self.join_button.disabled = True
        self.clear_items()
        embed = build_horse_result_embed(self.host, self.challenger, self.bet_amount, winner)
        await interaction.response.edit_message(embed=embed, view=self)
        self.stop()

    async def on_timeout(self):
        if self.reserved:
            data = load_data()
            host_entry = get_or_create_economy_entry(data, self.host.id)
            host_entry["won"] += self.bet_amount
            save_data(data)
            if self.message is not None:
                try:
                    timeout_embed = discord.Embed(
                        title="⌛ Horse Race Cancelled",
                        description=f"No challenger joined in time. {self.bet_amount:,} {CASINO_WON_CURRENCY_EMOJI} has been returned to {self.host.mention}.",
                        color=discord.Color.orange()
                    )
                    await self.message.edit(embed=timeout_embed, view=None)
                except Exception:
                    pass


class RouletteJoinView(discord.ui.View):
    def __init__(self, game_data: dict, bot_client, timeout: int = ROULETTE_JOIN_TIME_SECONDS):
        super().__init__(timeout=timeout)
        self.game_data = game_data
        self.message: discord.Message | None = None
        self.bot_client = bot_client

    @discord.ui.button(label="Join Roulette", style=discord.ButtonStyle.success, custom_id="roulette_join_btn")
    async def join_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RouletteModal(self.game_data))

    async def on_timeout(self):
        global active_roulette_game
        if active_roulette_game is None:
            return
        
        # End the game and calculate results
        data = load_data()
        ball_number = random.randint(0, 24)
        ball_display = get_roulette_display(ball_number)
        landing_color = get_roulette_color(ball_number)
        
        results_lines = [f"Ball landed on **{ball_display}**. Results:"]
        has_winners = False
        
        for player_id, player_data in self.game_data["players"].items():
            if is_roulette_choice_winning(player_data.get("color", "red"), ball_number):
                has_winners = True
                base_winnings = player_data["bet"] * 2
                winnings, _ = get_reward_with_role_bonus(base_winnings, "roulette", data, int(player_id), self.message.guild if self.message else None)
                user_entry = get_or_create_economy_entry(data, int(player_id))
                user_entry["won"] = user_entry.get("won", 0) + winnings
                try:
                    user = await self.bot_client.fetch_user(int(player_id))
                    results_lines.append(f"{user.mention} Won {winnings:,} {CASINO_WON_CURRENCY_EMOJI}!")
                    await log_economy_action(
                        user,
                        "roulette",
                        "Roulette won",
                        winnings,
                        user_entry,
                        details=f"Bet {player_data['bet']:,} on {player_data['color']}.",
                        guild=self.message.guild if self.message else None
                    )
                except Exception:
                    results_lines.append(f"<@{player_id}> Won {winnings:,} {CASINO_WON_CURRENCY_EMOJI}!")
                    await log_economy_action(
                        discord.Object(id=int(player_id)),
                        "roulette",
                        "Roulette won",
                        winnings,
                        user_entry,
                        details=f"Bet {player_data['bet']:,} on {player_data['color']}.",
                        guild=self.message.guild if self.message else None
                    )
        
        if not has_winners:
            results_lines.append("No winners :(")
            # Log losses for each participant if no one wins
            for player_id, player_data in self.game_data["players"].items():
                user_entry = get_or_create_economy_entry(data, int(player_id))
                try:
                    user = await self.bot_client.fetch_user(int(player_id))
                    await log_economy_action(
                        user,
                        "roulette",
                        "Roulette lost",
                        player_data["bet"],
                        user_entry,
                        details=f"Bet {player_data['bet']:,} on {player_data['color']}.",
                        guild=self.message.guild if self.message else None
                    )
                except Exception:
                    await log_economy_action(
                        discord.Object(id=int(player_id)),
                        "roulette",
                        "Roulette lost",
                        player_data["bet"],
                        user_entry,
                        details=f"Bet {player_data['bet']:,} on {player_data['color']}.",
                        guild=self.message.guild if self.message else None
                    )
        
        data["active_roulette_game"] = None
        save_data(data)
        
        results_embed = discord.Embed(
            title="🎡 Roulette Results",
            description="\n".join(results_lines),
            color=discord.Color.gold()
        )
        
        try:
            await self.message.edit(embed=results_embed, view=None)
        except Exception:
            pass
        
        self.stop()
        active_roulette_game = None


class RouletteModal(discord.ui.Modal, title="Join Roulette"):
    color_input = discord.ui.TextInput(label="Pick (Red, Black, Even, Odd)", placeholder="Red, Black, Even, or Odd", required=True)
    bet_input = discord.ui.TextInput(label="Bet amount or 'all'", placeholder="Enter bet amount or 'all'", required=True)
    
    def __init__(self, game_data: dict):
        super().__init__()
        self.game_data = game_data
    
    async def on_submit(self, interaction: discord.Interaction):
        color = normalize_roulette_option(self.color_input.value)
        bet_text = self.bet_input.value.strip().lower()
        
        if color not in ["red", "black", "even", "odd"]:
            await interaction.response.send_message("❌ Please enter 'Red', 'Black', 'Even', or 'Odd'.", ephemeral=True)
            return
        
        if str(interaction.user.id) in self.game_data["players"]:
            await interaction.response.send_message("❌ You have already joined this roulette game!", ephemeral=True)
            return
        
        data = load_data()
        entry = get_or_create_economy_entry(data, interaction.user.id)
        user_cash = entry.get("won", 0)
        
        if user_cash <= 0:
            await interaction.response.send_message("❌ You don't have enough cash to join.", ephemeral=True)
            return

        if bet_text == "all":
            bet_amount = user_cash
        else:
            try:
                bet_amount = int(bet_text.replace(",", ""))
            except Exception:
                await interaction.response.send_message("❌ Invalid bet amount. Use a number or 'all'.", ephemeral=True)
                return

        if bet_amount <= 0:
            await interaction.response.send_message("❌ Bet amount must be greater than zero.", ephemeral=True)
            return

        if bet_amount > user_cash:
            await interaction.response.send_message(f"❌ You only have {user_cash:,} {CASINO_WON_CURRENCY_EMOJI} to bet.", ephemeral=True)
            return

        level = get_level_info(entry.get('stardust', 0))[0]
        bet_cap = get_bet_cap(level)
        if bet_amount > bet_cap:
            await interaction.response.send_message(f"❌ Your bet exceeds the per-bet cap for your level ({bet_cap:,} {CASINO_WON_CURRENCY_EMOJI}).", ephemeral=True)
            return

        # Deduct bet from player
        entry["won"] = user_cash - bet_amount
        self.game_data["players"][str(interaction.user.id)] = {"color": color, "bet": bet_amount}
        data["active_roulette_game"] = self.game_data
        save_data(data)
        await log_economy_action(
            interaction.user,
            "roulette",
            "Roulette joined",
            bet_amount,
            entry,
            details=f"Bet {bet_amount:,} on {color.title()}.",
            guild=interaction.guild
        )
        
        await interaction.response.send_message(
            f"✅ You joined the roulette game with **{bet_amount:,} {CASINO_WON_CURRENCY_EMOJI}** on **{color.title()}**!",
            ephemeral=True
        )


@bot.tree.command(name="roulette", description="Play roulette! Bet your cash and guess Red, Black, Even, or Odd.")
@app_commands.describe(bet="Amount to bet (not used; uses all cash)", color="Red, Black, Even, or Odd")
async def roulette_command(interaction: discord.Interaction, bet: str = "all", color: str = ""):
    global active_roulette_game
    
    data = load_data()
    if not is_economy_channel(interaction.channel, interaction.user.id):
        await interaction.response.send_message(f"⚠️ Economy commands are restricted to {economy_channels_mention(interaction.guild)}.", ephemeral=True)
        return
    
    # If a game is active, try to join it
    if active_roulette_game is not None:
        color = normalize_roulette_option(color)
        if color not in ["red", "black", "even", "odd"]:
            await interaction.response.send_message("❌ Please specify 'Red', 'Black', 'Even', or 'Odd'.", ephemeral=True)
            return
        
        if str(interaction.user.id) in active_roulette_game["players"]:
            await interaction.response.send_message("❌ You have already joined this roulette game!", ephemeral=True)
            return
        
        entry = get_or_create_economy_entry(data, interaction.user.id)
        user_cash = entry.get("won", 0)
        
        if user_cash <= 0:
            await interaction.response.send_message("❌ You don't have enough cash to join.", ephemeral=True)
            return

        bet_text = bet.strip().lower()
        if bet_text == "all":
            bet_amount = user_cash
        else:
            try:
                bet_amount = int(bet_text.replace(",", ""))
            except Exception:
                await interaction.response.send_message("❌ Invalid bet amount. Use a number or 'all'.", ephemeral=True)
                return

        if bet_amount <= 0:
            await interaction.response.send_message("❌ Bet amount must be greater than zero.", ephemeral=True)
            return

        if bet_amount > user_cash:
            await interaction.response.send_message(f"❌ You only have {user_cash:,} {CASINO_WON_CURRENCY_EMOJI} to bet.", ephemeral=True)
            return

        level = get_level_info(entry.get('stardust', 0))[0]
        bet_cap = get_bet_cap(level)
        if bet_amount > bet_cap:
            await interaction.response.send_message(f"❌ Your bet exceeds the per-bet cap for your level ({bet_cap:,} {CASINO_WON_CURRENCY_EMOJI}).", ephemeral=True)
            return

        entry["won"] = user_cash - bet_amount
        active_roulette_game["players"][str(interaction.user.id)] = {"color": color, "bet": bet_amount}
        data["active_roulette_game"] = active_roulette_game
        save_data(data)
        await log_economy_action(
            interaction.user,
            "roulette",
            "Roulette joined",
            bet_amount,
            entry,
            details=f"Bet {bet_amount:,} on {color.title()}.",
            guild=interaction.guild
        )
        
        await interaction.response.send_message(
            f"✅ You joined the roulette game with **{bet_amount:,} {CASINO_WON_CURRENCY_EMOJI}** on **{color.title()}**!",
            ephemeral=True
        )
        return
    
    # Start a new game
    entry = get_or_create_economy_entry(data, interaction.user.id)
    user_cash = entry.get("won", 0)
    
    if user_cash <= 0:
        await interaction.response.send_message("❌ You don't have enough cash to start a roulette game.", ephemeral=True)
        return

    color = normalize_roulette_option(color)
    level = get_level_info(entry.get('stardust', 0))[0]
    bet_cap = get_bet_cap(level)
    if user_cash > bet_cap:
        await interaction.response.send_message(f"❌ Your bet exceeds the per-bet cap for your level ({bet_cap:,} {CASINO_WON_CURRENCY_EMOJI}).", ephemeral=True)
        return
    
    bet_amount = user_cash
    entry["won"] = 0
    data["active_roulette_game"] = {
        "host_id": interaction.user.id,
        "players": {
            str(interaction.user.id): {"color": color if color in {"red", "black", "even", "odd"} else "red", "bet": bet_amount}
        },
        "message_id": None,
        "channel_id": interaction.channel.id,
        "guild_id": interaction.guild.id if interaction.guild else None,
        "expires_at": datetime.now().timestamp() + ROULETTE_JOIN_TIME_SECONDS
    }
    active_roulette_game = data["active_roulette_game"]
    save_data(data)
    
    embed = discord.Embed(
        title="🎡 Roulette Game Starting!",
        description="Use /roulette to join.\nClick the button below to join and set your bet amount.",
        color=discord.Color.purple()
    )
    embed.set_footer(text=f"{ROULETTE_JOIN_TIME_SECONDS} seconds remaining")
    
    view = RouletteJoinView(active_roulette_game, interaction.client)
    await interaction.response.send_message(embed=embed, view=view)
    view.message = await interaction.original_response()
    if view.message:
        active_roulette_game["message_id"] = view.message.id
        data["active_roulette_game"] = active_roulette_game
        save_data(data)


class ProfileInventoryView(discord.ui.View):
    def __init__(self, user_id: int):
        super().__init__(timeout=120)
        self.user_id = user_id

    @discord.ui.button(label="Inventory", style=discord.ButtonStyle.primary, custom_id="profile_inventory_button")
    async def inventory(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This profile inventory is only for the profile owner.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        data = load_data()
        entry = get_or_create_economy_entry(data, interaction.user.id)
        crates = get_crate_inventory(entry)
        equippable_items = get_equippable_role_entries(data, interaction.user.id, interaction.guild)

        embed = discord.Embed(
            title="Inventory",
            color=discord.Color.blue()
        )

        if crates:
            crate_lines = []
            for crate_id, amount in crates.items():
                crate_data = get_crate_by_id(crate_id)
                if crate_data:
                    icon = crate_data.get("icon", "📦")
                    crate_name = crate_data.get("display_name", crate_id.replace("_", " ").title())
                    crate_lines.append(f"{icon} {crate_name} `x{amount}`")
            embed.add_field(name="Crates", value="\n".join(crate_lines), inline=False)

        if equippable_items:
            view = RoleListView(interaction.user.id, equippable_items, interaction.guild)
            embed.add_field(name="Equippable Roles", value="Browse your equippable roles below (10 per page). To equip a role, run `/equip <item id or name>`.", inline=False)
            await interaction.followup.send(embed=view.build_embed(), view=view, ephemeral=True)
        else:
            if not crates:
                embed.description = "You don't have any crates or equippable roles yet."
            await interaction.followup.send(embed=embed, ephemeral=True)

    @discord.ui.button(label="Level Milestones", style=discord.ButtonStyle.secondary, custom_id="profile_level_milestones_button")
    async def level_milestones(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This profile is only for viewing your own milestones.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        data = load_data()
        entry = get_or_create_economy_entry(data, interaction.user.id)
        current_level = get_level_info(entry.get("stardust", 0))[0]

        milestones = sorted(set(list(BANK_CAPS_BY_LEVEL.keys()) + list(BET_CAPS_BY_LEVEL.keys())))

        embed = discord.Embed(
            title="🏆 Level Milestones",
            color=discord.Color.purple()
        )

        for level in milestones:
            bank_cap = get_bank_cap(level)
            bet_cap = get_bet_cap(level)
            checkmark = "<:checkmark:1521982002386178099>" if current_level >= level else "⭕"
            embed.add_field(name=f"{checkmark} Level {level}", value=f"{bank_cap:,} Bank Cap | {bet_cap:,} Bet Cap", inline=True)

        embed.set_footer(text=f"Current Level: {current_level}")
        await interaction.followup.send(embed=embed, ephemeral=True)


class EquipRoleView(discord.ui.View):
    def __init__(self, user_id: int, owned_items: list[dict]):
        super().__init__(timeout=120)
        self.user_id = user_id
        options = []
        for item in owned_items:
            description = item.get("description") or f"Equip {item['name']}"
            if len(description) > 100:
                description = description[:97] + "..."
            options.append(discord.SelectOption(
                label=f"({item.get('rarity', 'Common')}) {item['name']}".strip(),
                description=description,
                value=item["id"]
            ))
        # Discord enforces 1-25 options for Select menus. Truncate if necessary.
        if not options:
            return
        if len(options) > 25:
            options = options[:25]
        self.select = discord.ui.Select(placeholder="Choose a role to equip", min_values=1, max_values=1, options=options)
        self.select.callback = self.on_select
        self.add_item(self.select)

    async def on_select(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This inventory is only for the profile owner.", ephemeral=True)
            return

        selected_id = self.select.values[0]
        data = load_data()
        entry = get_or_create_economy_entry(data, interaction.user.id)
        item = get_equippable_role_entry(selected_id, interaction.guild)
        if not item:
            await interaction.response.send_message("That role could not be found.", ephemeral=True)
            return

        if not is_equippable_role_entry_accessible(data, interaction.user.id, item):
            await interaction.response.send_message("You do not have access to that role.", ephemeral=True)
            return

        guild = interaction.guild
        if not guild:
            await interaction.response.send_message("Guild context is required.", ephemeral=True)
            return

        member = guild.get_member(interaction.user.id)
        if not member:
            await interaction.response.send_message("Could not locate your member record.", ephemeral=True)
            return

        target_role = guild.get_role(item["role_id"])
        currently_equipped_id = entry.get("equipped_store_item")
        already_equipped = currently_equipped_id == selected_id and target_role and target_role in member.roles

        if already_equipped:
            try:
                await member.remove_roles(target_role, reason="Unequipped role")
            except Exception:
                await interaction.response.send_message("Could not unequip the selected role.", ephemeral=True)
                return

            entry["equipped_store_item"] = None
            save_data(data)
            await interaction.response.send_message(f"✅ Unequipped **{item['name']}**.", ephemeral=True)
            return

        for role_id in get_all_equippable_role_ids(guild):
            role = guild.get_role(role_id)
            if role and role in member.roles and role.id != item["role_id"]:
                try:
                    await member.remove_roles(role, reason="Equipping a new role")
                except Exception:
                    pass

        if target_role:
            try:
                await member.add_roles(target_role, reason="Equipped role")
            except Exception:
                await interaction.response.send_message("Could not equip the selected role.", ephemeral=True)
                return

        set_equipped_store_item(data, interaction.user.id, selected_id)
        save_data(data)


class RoleListView(discord.ui.View):
    def __init__(self, user_id: int, owned_items: list[dict], guild: discord.Guild | None, page: int = 0, page_size: int = 10):
        super().__init__(timeout=120)
        self.user_id = user_id
        self.items = owned_items or []
        self.guild = guild
        self.page = page
        self.page_size = page_size
        self.update_buttons()

    def update_buttons(self) -> None:
        # Rebuild buttons for the current page
        self.clear_items()
        prev_disabled = self.page <= 0
        next_disabled = (self.page + 1) * self.page_size >= len(self.items)

        self.prev_btn = discord.ui.Button(label="◀ Previous", style=discord.ButtonStyle.secondary, disabled=prev_disabled)
        self.next_btn = discord.ui.Button(label="Next ▶", style=discord.ButtonStyle.primary, disabled=next_disabled)
        self.prev_btn.callback = self.go_previous
        self.next_btn.callback = self.go_next
        self.add_item(self.prev_btn)
        self.add_item(self.next_btn)

    def build_embed(self) -> discord.Embed:
        total = len(self.items)
        max_pages = max(1, (total + self.page_size - 1) // self.page_size)
        start_idx = self.page * self.page_size
        end_idx = min(start_idx + self.page_size, total)

        embed = discord.Embed(title="Equippable Roles", color=discord.Color.blue())
        if total == 0:
            embed.description = "You don't have any equippable roles."
            return embed

        lines = []
        for i, item in enumerate(self.items[start_idx:end_idx], start=start_idx + 1):
            name = item.get("name", "Unknown")
            rarity = item.get("rarity", "Common")
            item_id = item.get("id", "")
            role_id = item.get("role_id")
            role_mention = ""
            try:
                if self.guild and role_id:
                    role = self.guild.get_role(role_id)
                    if role:
                        role_mention = f" — {role.mention}"
            except Exception:
                role_mention = ""
            lines.append(f"{i}. ({rarity}) {name}{role_mention} — id: {item_id}")

        embed.description = "\n".join(lines)
        embed.set_footer(text=f"Page {self.page + 1}/{max_pages} • To equip: /equip <item id or name>")
        return embed

    async def go_previous(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This inventory is only for the profile owner.", ephemeral=True)
            return
        self.page = max(0, self.page - 1)
        self.update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    async def go_next(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This inventory is only for the profile owner.", ephemeral=True)
            return
        max_page = max(0, (len(self.items) - 1) // self.page_size)
        self.page = min(max_page, self.page + 1)
        self.update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)


@bot.tree.command(name="equip", description="Equip a store role by item id or (partial) name from your inventory.")
@app_commands.describe(item="Item id or name to equip")
async def equip_command(interaction: discord.Interaction, item: str):
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild
    if not guild:
        await interaction.followup.send("Guild context is required to equip roles.", ephemeral=True)
        return

    data = load_data()
    entry = get_or_create_economy_entry(data, interaction.user.id)
    owned_items = get_equippable_role_entries(data, interaction.user.id, guild)
    if not owned_items:
        await interaction.followup.send("You don't own any equippable roles.", ephemeral=True)
        return

    # Try exact id match first
    match = None
    item_lower = item.strip().lower()
    for it in owned_items:
        if str(it.get("id", "")).lower() == item_lower:
            match = it
            break

    if not match:
        # Try exact name, then substring
        for it in owned_items:
            if it.get("name", "").strip().lower() == item_lower:
                match = it
                break

    if not match:
        possible = [it for it in owned_items if item_lower in it.get("name", "").strip().lower()]
        if len(possible) == 1:
            match = possible[0]
        elif len(possible) > 1:
            names = ", ".join(f"{p.get('name')} (id: {p.get('id')})" for p in possible[:10])
            await interaction.followup.send(f"Multiple matches found: {names}. Please use the full item id.", ephemeral=True)
            return

    if not match:
        await interaction.followup.send("Could not find that item in your equippable roles.", ephemeral=True)
        return

    # Permission/access check
    if not is_equippable_role_entry_accessible(data, interaction.user.id, match):
        await interaction.followup.send("You do not have access to that role.", ephemeral=True)
        return

    member = guild.get_member(interaction.user.id)
    if not member:
        await interaction.followup.send("Could not locate your member record.", ephemeral=True)
        return

    target_role = guild.get_role(match.get("role_id"))
    if not target_role:
        await interaction.followup.send("The role for that item could not be found on this server.", ephemeral=True)
        return

    # Remove other equippable roles if present
    for role_id in get_all_equippable_role_ids(guild):
        role = guild.get_role(role_id)
        if role and role in member.roles and role.id != target_role.id:
            try:
                await member.remove_roles(role, reason="Equipping a new role via /equip")
            except Exception:
                pass

    try:
        await member.add_roles(target_role, reason="Equipped role via /equip")
    except Exception:
        await interaction.followup.send("Could not equip the selected role.", ephemeral=True)
        return

    set_equipped_store_item(data, interaction.user.id, match.get("id"))
    save_data(data)
    await interaction.followup.send(f"✅ Equipped **{match.get('name', 'Role')}**.", ephemeral=True)


def build_store_embed(user_id: int, page: int, data: dict, guild: discord.Guild | None = None, category: str = "roles") -> discord.Embed:
    category = str(category).lower() if category else "roles"
    entry = get_or_create_economy_entry(data, user_id)
    if category == "crates":
        store_items = get_purchasable_store_crates()
    else:
        store_items = get_store_items_for_guild(guild)
    max_pages = max(1, (len(store_items) + 6) // 7)
    page = max(0, min(page, max_pages - 1))
    start_idx = page * 7
    end_idx = start_idx + 7
    visible_items = store_items[start_idx:end_idx]

    embed = discord.Embed(
        title="Store",
        description=(
            f"Balance: {entry.get('won', 0):,} {CASINO_WON_CURRENCY_EMOJI}\n"
            f"Category: {category.title()}\n"
            + ("Purchase a role to unlock it. Then equip the unlocked role using `/profile`."
               if category == "roles"
               else "Purchase a crate and use `/open` to open it. For bulk purchases, use `/crate buy <crate> <amount>`."
               )
        ),
        color=discord.Color.dark_theme()
    )
    embed.set_footer(text=f"Page {page + 1}/{max_pages}")

    if not visible_items:
        embed.description += " No items are configured for this category right now."
        return embed

    lines = []
    for item in visible_items:
        item_icon = item.get("display_emoji", "")
        if item.get("type") == "crate":
            price_display = f"{CASINO_WON_CURRENCY_EMOJI} {item['cost']:,}"
        else:
            owned = has_user_purchased_item(data, item["id"], user_id)
            if owned:
                price_display = f"<:checkmark:1521982002386178099> {item['cost']:,}"
            else:
                price_display = f"{CASINO_WON_CURRENCY_EMOJI} {item['cost']:,}"
        line = f"{item_icon} **{item['name']}**\n- {price_display}"
        if item.get("type") != "crate" and has_user_purchased_item(data, item["id"], user_id):
            line += " (Owned)"
        lines.append(line)

    embed.add_field(name=f"{category.title()}", value="\n".join(lines), inline=False)
    return embed


class StoreView(discord.ui.View):
    def __init__(self, user_id: int, page: int = 0, guild: discord.Guild | None = None, category: str = "roles"):
        super().__init__(timeout=120)
        self.user_id = user_id
        self.page = page
        self.guild = guild
        self.category = category if category in {"roles", "crates"} else "roles"
        self.refresh_items()

    def refresh_items(self) -> None:
        self.clear_items()
        store_items = get_purchasable_store_crates() if self.category == "crates" else get_store_items_for_guild(self.guild)
        max_pages = max(1, (len(store_items) + 6) // 7)
        self.page = max(0, min(self.page, max_pages - 1))

        category_select = discord.ui.Select(
            placeholder="Select category",
            min_values=1,
            max_values=1,
            options=[
                discord.SelectOption(label="Roles", value="roles", description="Browse purchasable roles", default=self.category == "roles"),
                discord.SelectOption(label="Crates", value="crates", description="Browse purchasable crates", default=self.category == "crates"),
            ]
        )

        async def category_callback(interaction: discord.Interaction):
            if interaction.user.id != self.user_id:
                await interaction.response.send_message("This store is only for your purchases.", ephemeral=True)
                return
            self.category = category_select.values[0]
            self.page = 0
            self.refresh_items()
            data = load_data()
            await interaction.response.edit_message(embed=build_store_embed(self.user_id, self.page, data, self.guild, category=self.category), view=self)

        category_select.callback = category_callback
        self.add_item(category_select)

        if max_pages > 1:
            self.prev_page = discord.ui.Button(label="◀ Previous", style=discord.ButtonStyle.secondary, disabled=self.page == 0)
            self.prev_page.callback = self.go_previous
            self.add_item(self.prev_page)

            self.next_page = discord.ui.Button(label="Next ▶", style=discord.ButtonStyle.primary, disabled=self.page >= max_pages - 1)
            self.next_page.callback = self.go_next
            self.add_item(self.next_page)

        start_idx = self.page * 7
        end_idx = start_idx + 7

        options = []
        for item in store_items[start_idx:end_idx]:
            if item.get("type") == "crate":
                description = f"₩ {item['cost']:,}"
            else:
                data = load_data()
                owned = has_user_purchased_item(data, item["id"], self.user_id)
                description = "Owned!" if owned else f"₩ {item['cost']:,}"
            options.append(discord.SelectOption(label=item["name"], description=description, value=item["id"], default=False))

        if options:
            select = discord.ui.Select(placeholder="Select an item to purchase", min_values=1, max_values=1, options=options)

            async def select_callback(interaction: discord.Interaction):
                if not is_economy_channel(interaction.channel, interaction.user.id):
                    await interaction.response.send_message(f"⚠️ Economy commands are restricted to {economy_channels_mention(interaction.guild)}.", ephemeral=True)
                    return
                if interaction.user.id != self.user_id:
                    await interaction.response.send_message("This store is only for your purchases.", ephemeral=True)
                    return

                choice_id = select.values[0]
                chosen = get_store_item(choice_id)
                if not chosen:
                    await interaction.response.send_message("Invalid selection.", ephemeral=True)
                    return

                data = load_data()
                entry = get_or_create_economy_entry(data, interaction.user.id)
                if entry.get('won', 0) < chosen['cost']:
                    await interaction.response.send_message(f"You need at least ₩ {chosen['cost']:,} to buy this.", ephemeral=True)
                    return

                if chosen.get('type') == 'role':
                    if has_user_purchased_item(data, chosen['id'], interaction.user.id):
                        await interaction.response.send_message("You already own this item.", ephemeral=True)
                        return

                    member = interaction.user if isinstance(interaction.user, discord.Member) else interaction.guild.get_member(interaction.user.id)
                    role = interaction.guild.get_role(chosen['role_id']) if interaction.guild else None
                    if not member or not role:
                        await interaction.response.send_message("Could not complete purchase (member or role missing).", ephemeral=True)
                        return

                    entry['won'] -= chosen['cost']
                    mark_user_purchased_item(data, chosen['id'], interaction.user.id)
                    save_data(data)
                    await log_economy_action(
                        interaction.user,
                        "store purchase",
                        "Store role purchased",
                        chosen['cost'],
                        entry,
                        details=f"Purchased {chosen['name']}.",
                        guild=interaction.guild
                    )

                    self.refresh_items()
                    updated_embed = build_store_embed(self.user_id, self.page, data, self.guild, category=self.category)
                    await interaction.response.edit_message(embed=updated_embed, view=self)
                    await interaction.followup.send(
                        f"✅ You've unlocked **{chosen['name']}**! Equip this role by using `/profile`.",
                        ephemeral=True
                    )
                    return

                if chosen.get('type') == 'crate':
                    entry['won'] -= chosen['cost']
                    add_crate_to_inventory(data, interaction.user.id, chosen['crate_id'], 1)
                    save_data(data)
                    await log_economy_action(
                        interaction.user,
                        "store purchase",
                        "Store crate purchased",
                        chosen['cost'],
                        entry,
                        details=f"Purchased 1x {chosen['name']}.",
                        guild=interaction.guild
                    )

                    self.refresh_items()
                    updated_embed = build_store_embed(self.user_id, self.page, data, self.guild, category=self.category)
                    await interaction.response.edit_message(embed=updated_embed, view=self)
                    await interaction.followup.send(
                        f"✅ You've purchased **{chosen['name']}**! Use `/open` to open it.",
                        ephemeral=True
                    )
                    return

            select.callback = select_callback
            self.add_item(select)

    async def go_previous(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This store is only for your purchases.", ephemeral=True)
            return
        if self.page > 0:
            self.page -= 1
            self.refresh_items()
            data = load_data()
            await interaction.response.edit_message(embed=build_store_embed(self.user_id, self.page, data, self.guild, category=self.category), view=self)

    async def go_next(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This store is only for your purchases.", ephemeral=True)
            return
        store_items = get_purchasable_store_crates() if self.category == "crates" else get_store_items_for_guild(self.guild)
        max_pages = max(1, (len(store_items) + 6) // 7)
        if self.page < max_pages - 1:
            self.page += 1
            self.refresh_items()
            data = load_data()
            await interaction.response.edit_message(embed=build_store_embed(self.user_id, self.page, data, self.guild, category=self.category), view=self)

    def create_purchase_callback(self, item: dict):
        async def callback(interaction: discord.Interaction):
            if not is_economy_channel(interaction.channel, interaction.user.id):
                await interaction.response.send_message(f"⚠️ Economy commands are restricted to {economy_channels_mention(interaction.guild)}.", ephemeral=True)
                return
            if interaction.user.id != self.user_id:
                await interaction.response.send_message("This store is only for your purchases.", ephemeral=True)
                return

            data = load_data()
            if has_user_purchased_item(data, item["id"], interaction.user.id):
                await interaction.response.send_message("You already own this item.", ephemeral=True)
                return

            entry = get_or_create_economy_entry(data, interaction.user.id)
            if entry.get("won", 0) < item["cost"]:
                await interaction.response.send_message(f"You need at least {item['cost']:,} {CASINO_WON_CURRENCY_EMOJI} to buy this.", ephemeral=True)
                return

            member = interaction.user if isinstance(interaction.user, discord.Member) else interaction.guild.get_member(interaction.user.id)
            role = interaction.guild.get_role(item["role_id"])
            if not member:
                await interaction.response.send_message("I couldn't find your member record in this server.", ephemeral=True)
                return
            if not role:
                await interaction.response.send_message("That role no longer exists.", ephemeral=True)
                return

            entry["won"] -= item["cost"]
            mark_user_purchased_item(data, item["id"], interaction.user.id)
            save_data(data)

            # Update the view so the button instantly greys out
            self.refresh_items()
            updated_embed = build_store_embed(self.user_id, self.page, data, self.guild)

            await interaction.response.edit_message(embed=updated_embed, view=self)
            await interaction.followup.send(
                f"✅ You've unlocked **{item['name']}**! Equip this role by using `/profile`.",
                ephemeral=True
            )

@bot.tree.command(name="store", description="Browse and purchase exclusive roles from the server store.")
async def store_command(interaction: discord.Interaction):
    data = load_data()
    if not is_economy_channel(interaction.channel, interaction.user.id):
        await interaction.response.send_message(f"⚠️ Economy commands are restricted to {economy_channels_mention(interaction.guild)}.", ephemeral=True)
        return
    embed = build_store_embed(interaction.user.id, 0, data, interaction.guild)
    view = StoreView(interaction.user.id, page=0, guild=interaction.guild)
    await interaction.response.send_message(embed=embed, view=view)


@bot.tree.command(name="deposit", description="Deposit won into your bank.")
@app_commands.describe(amount="Amount of won to deposit or 'all'")
async def deposit_command(interaction: discord.Interaction, amount: str):
    amount_text = amount.strip().lower()
    if amount_text == "all":
        requested_amount = None
    else:
        try:
            requested_amount = int(amount_text)
        except ValueError:
            await interaction.response.send_message("⚠️ Please enter a positive number or 'all'.", ephemeral=True)
            return

    if requested_amount is not None and requested_amount <= 0:
        await interaction.response.send_message("⚠️ Please enter a positive amount.", ephemeral=True)
        return

    data = load_data()
    if not is_economy_channel(interaction.channel, interaction.user.id):
        await interaction.response.send_message(f"⚠️ Economy commands are restricted to {economy_channels_mention(interaction.guild)}.", ephemeral=True)
        return
    entry = get_or_create_economy_entry(data, interaction.user.id)
    cash = entry.get("won", 0)
    if requested_amount is None:
        requested_amount = cash
    if cash < requested_amount:
        await interaction.response.send_message("⚠️ You do not have enough cash to deposit that amount.", ephemeral=True)
        return

    level = get_level_info(entry.get("stardust", 0))[0]
    cap = get_bank_cap(level)
    current_bank = entry.get("bank", 0)
    remaining_capacity = max(0, cap - current_bank)
    if remaining_capacity <= 0:
        await interaction.response.send_message("⚠️ Your bank is already at the maximum allowed for your level.", ephemeral=True)
        return

    deposit_amount = min(requested_amount, remaining_capacity)
    entry["won"] -= deposit_amount
    entry["bank"] = entry.get("bank", 0) + deposit_amount
    save_data(data)
    await log_economy_action(
        interaction.user,
        "deposit",
        "Deposited to bank",
        deposit_amount,
        entry,
        details=f"Deposited to bank with cap {cap:,}.",
        guild=interaction.guild
    )
    await interaction.response.send_message(f"✅ Deposited {deposit_amount:,} {CASINO_WON_CURRENCY_EMOJI} into your bank.")


@bot.tree.command(name="withdraw", description="Withdraw won from your bank.")
@app_commands.describe(amount="Amount of won to withdraw or 'all'")
async def withdraw_command(interaction: discord.Interaction, amount: str):
    amount_text = amount.strip().lower()
    if amount_text == "all":
        requested_amount = None
    else:
        try:
            requested_amount = int(amount_text)
        except ValueError:
            await interaction.response.send_message("⚠️ Please enter a positive number or 'all'.", ephemeral=True)
            return

    if requested_amount is not None and requested_amount <= 0:
        await interaction.response.send_message("⚠️ Please enter a positive amount.", ephemeral=True)
        return

    data = load_data()
    if not is_economy_channel(interaction.channel, interaction.user.id):
        await interaction.response.send_message(f"⚠️ Economy commands are restricted to {economy_channels_mention(interaction.guild)}.", ephemeral=True)
        return
    entry = get_or_create_economy_entry(data, interaction.user.id)
    bank = entry.get("bank", 0)
    if requested_amount is None:
        requested_amount = bank
    if bank < requested_amount:
        await interaction.response.send_message("⚠️ You do not have enough money in your bank to withdraw that amount.", ephemeral=True)
        return

    entry["bank"] -= requested_amount
    entry["won"] = entry.get("won", 0) + requested_amount
    save_data(data)
    await log_economy_action(
        interaction.user,
        "withdraw",
        "Withdrew from bank",
        requested_amount,
        entry,
        details=f"Withdrew from bank to cash.",
        guild=interaction.guild
    )
    await interaction.response.send_message(f"✅ Withdrew {requested_amount:,} {CASINO_WON_CURRENCY_EMOJI} from your bank.")


@bot.tree.command(name="coinflip", description="Flip a coin (Heads/Tails) and bet your won. Use 'all' to bet your whole wallet.")
@app_commands.describe(side="Heads or Tails", bet="Amount to bet or 'all'")
@app_commands.choices(side=[
    app_commands.Choice(name="Heads", value="heads"),
    app_commands.Choice(name="Tails", value="tails")
])
async def coinflip_command(interaction: discord.Interaction, side: app_commands.Choice[str], bet: str):
    if not is_economy_channel(interaction.channel, interaction.user.id):
        await interaction.response.send_message(f"⚠️ Economy commands are restricted to {economy_channels_mention(interaction.guild)}.", ephemeral=True)
        return

    data = load_data()
    uid = interaction.user.id
    entry = get_or_create_economy_entry(data, uid)
    wallet = entry.get("won", 0)
    prev_streak = get_streak(entry)

    bet_text = bet.strip().lower()
    if bet_text == "all":
        requested = wallet
    else:
        try:
            requested = int(bet_text.replace(',', ''))
        except Exception:
            await interaction.response.send_message("❌ Invalid bet amount. Use a number or 'all'.", ephemeral=True)
            return

    if requested <= 0:
        await interaction.response.send_message("❌ Bet must be greater than zero.", ephemeral=True)
        return

    if requested > wallet:
        await interaction.response.send_message(f"❌ You only have {wallet:,} {CASINO_WON_CURRENCY_EMOJI} to bet.", ephemeral=True)
        return

    # Enforce per-level bet cap
    level = get_level_info(entry.get('stardust', 0))[0]
    bet_cap = get_bet_cap(level)
    if requested > bet_cap:
        await interaction.response.send_message(f"❌ Your bet exceeds the per-bet cap for your level ({bet_cap:,} {CASINO_WON_CURRENCY_EMOJI}).", ephemeral=True)
        return

    # Immediately take the money to prevent duping
    entry["won"] = entry.get("won", 0) - requested
    save_data(data)

    await interaction.response.defer()

    # Resolve user's choice and flip
    choice = side.value.lower()
    result = random.choice(["heads", "tails"])
    won_amount = 0

    streak_lost_note = ""
    if result == choice:
        # Win: give double the bet (net gain = +requested) + stardust
        base_won_amount = requested * 2
        won_amount, _ = get_reward_with_role_bonus(base_won_amount, "coinflip", data, interaction.user.id, interaction.guild)
        entry["won"] = entry.get("won", 0) + won_amount
        stardust_reward = random.randint(MINIGAME_WIN_STARDUST_REWARD_MINIMUM, MINIGAME_WIN_STARDUST_REWARD_MAXIMUM)
        entry["stardust"] = entry.get("stardust", 0) + stardust_reward
        entry["minigame_wins"] = entry.get("minigame_wins", 0) + 1
        increment_streak(entry)
        enforce_balance_cap(entry, get_level_info(entry.get('stardust', 0))[0])
        save_data(data)
        await log_economy_action(
            interaction.user,
            "coinflip",
            "Coinflip won",
            won_amount,
            entry,
            details=f"Chose {choice.title()}, result {result.title()}.",
            guild=interaction.guild
        )
    else:
        entry["minigame_losses"] = entry.get("minigame_losses", 0) + 1
        reset_streak(entry)
        save_data(data)
        if prev_streak > 3:
            streak_lost_note = f"⚠️ Your streak of {prev_streak} has been lost."

    # Build result embed
    title = "🎉 Coinflip — You Win!" if won_amount > 0 else "😵 Coinflip — You Lose"
    embed = discord.Embed(title=title, color=discord.Color.gold())
    embed.add_field(name="Your Bet", value=f"{requested:,} {CASINO_WON_CURRENCY_EMOJI}", inline=True)
    embed.add_field(name="Result", value=f"The coin landed **{result.title()}**", inline=True)
    if won_amount > 0:
        embed.add_field(name="Payout", value=f"You received {won_amount:,} {CASINO_WON_CURRENCY_EMOJI}", inline=False)
    else:
        embed.add_field(name="Loss", value=f"You lost {requested:,} {CASINO_WON_CURRENCY_EMOJI}", inline=False)
    if streak_lost_note:
        embed.add_field(name="Streak", value=streak_lost_note, inline=False)

    await interaction.followup.send(embed=embed)


@bot.tree.command(name="pay", description="Pay another user an amount of won. Use 'all' to send your whole wallet.")
@app_commands.describe(member="Member to pay", amount="Amount to send or 'all'")
async def pay_command(interaction: discord.Interaction, member: discord.Member, amount: str):
    if not is_economy_channel(interaction.channel, interaction.user.id):
        await interaction.response.send_message(f"⚠️ Economy commands are restricted to {economy_channels_mention(interaction.guild)}.", ephemeral=True)
        return

    if member.bot:
        await interaction.response.send_message("❌ You cannot pay a bot.", ephemeral=True)
        return

    if member.id == interaction.user.id:
        await interaction.response.send_message("❌ You cannot pay yourself.", ephemeral=True)
        return

    data = load_data()
    uid = interaction.user.id
    uid_str = str(uid)
    entry = get_or_create_economy_entry(data, uid)
    recipient = get_or_create_economy_entry(data, member.id)

    # Cooldown enforcement (20 seconds)
    now_ts = datetime.now().timestamp()
    pay_cd = data.setdefault("pay_cooldowns", {})
    cooldown_until = pay_cd.get(uid_str)
    if cooldown_until and now_ts < cooldown_until:
        remaining = int(cooldown_until - now_ts)
        await interaction.response.send_message(f"⏳ You must wait {remaining}s before sending another payment.", ephemeral=True)
        return

    wallet = entry.get("won", 0)
    amt_text = amount.strip().lower()
    if amt_text == "all":
        requested = wallet
    else:
        try:
            requested = int(amt_text.replace(',', ''))
        except Exception:
            await interaction.response.send_message("❌ Invalid amount. Use a number or 'all'.", ephemeral=True)
            return

    if requested <= 0:
        await interaction.response.send_message("❌ Amount must be greater than zero.", ephemeral=True)
        return

    if requested > wallet:
        await interaction.response.send_message(f"❌ You only have {wallet:,} {CASINO_WON_CURRENCY_EMOJI} to send.", ephemeral=True)
        return

    # Calculate tax (5% if > 100)
    tax = int(requested * 0.05) if requested > 100 else 0

    # Immediately deduct to avoid duping
    entry["won"] = entry.get("won", 0) - requested
    # Record tax pool (optional)
    data["tax_pool"] = data.get("tax_pool", 0) + tax
    # Amount recipient receives
    received = requested - tax
    recipient["won"] = recipient.get("won", 0) + received

    # Enforce recipient bank cap
    enforce_balance_cap(recipient, get_level_info(recipient.get("stardust", 0))[0])

    # Set cooldown and persist
    pay_cd[uid_str] = now_ts + 20
    save_data(data)
    await log_economy_action(
        interaction.user,
        "pay",
        "Payment sent",
        requested,
        entry,
        details=f"Paid {member} {received:,} after {tax:,} tax.",
        guild=interaction.guild
    )

    dm_sent = False
    dm_embed = discord.Embed(
        title="💸 You've received Won!",
        description=(
            f"{interaction.user.mention} has sent you **{received:,} {CASINO_WON_CURRENCY_EMOJI}**.\n"
            f"Tax: {tax:,} {CASINO_WON_CURRENCY_EMOJI}" if tax else ""
        ),
        color=discord.Color.green()
    )
    dm_embed.set_footer(text="Your balance has been updated.")
    try:
        await member.send(embed=dm_embed)
        dm_sent = True
    except Exception:
        dm_sent = False

    embed = discord.Embed(title="💸 Payment Sent", color=discord.Color.green())
    embed.add_field(name="From", value=interaction.user.mention, inline=True)
    embed.add_field(name="To", value=member.mention, inline=True)
    embed.add_field(name="Amount Sent", value=f"{requested:,} {CASINO_WON_CURRENCY_EMOJI}", inline=False)
    if not dm_sent:
        embed.add_field(name="Recipient DM", value="Unable to DM recipient.", inline=False)
    if tax > 0:
        embed.add_field(name="Tax", value=f"{tax:,} {CASINO_WON_CURRENCY_EMOJI} (5%)", inline=True)
    embed.add_field(name="Received", value=f"{received:,} {CASINO_WON_CURRENCY_EMOJI}", inline=True)

    await interaction.response.send_message(embed=embed)


def _build_deck():
    ranks = ["A"] + [str(n) for n in range(2, 11)] + ["J", "Q", "K"]
    suits = ["♠", "♥", "♦", "♣"]
    return [f"{r}{s}" for r in ranks for s in suits]


def _hand_value(hand: list[str]) -> int:
    total = 0
    aces = 0
    for card in hand:
        rank = card[:-1]
        if rank == "A":
            total += 11
            aces += 1
        elif rank in {"J", "Q", "K"}:
            total += 10
        else:
            try:
                total += int(rank)
            except Exception:
                total += 0
    while total > 21 and aces > 0:
        total -= 10
        aces -= 1
    return total


def _format_hand(hand: list[str]) -> str:
    return ", ".join(hand)


class BlackjackView(discord.ui.View):
    def __init__(self, user_id: int, deck: list[str], player_hand: list[str], dealer_hand: list[str], bet: int):
        super().__init__(timeout=None)
        self.user_id = user_id
        self.deck = deck
        self.player_hand = player_hand
        self.dealer_hand = dealer_hand
        self.bet = bet
        self.total_wager = bet
        self.finished = False
        self.message_id = None
        self.message: discord.Message | None = None
        self.last_interaction_at = time.time()
        self.warning_sent = False
        self.monitor_task = asyncio.create_task(self._monitor_inactivity())
        self.hands = [player_hand]
        self.hand_bets = [bet]
        self.hand_statuses = ["playing"]
        self.active_hand_index = 0
        self.split_used = False
        self._update_button_states()

    def _get_active_hand(self) -> list[str]:
        return self.hands[self.active_hand_index]

    def _get_active_hand_bet(self) -> int:
        return self.hand_bets[self.active_hand_index]

    def _can_split(self) -> bool:
        return (
            not self.split_used
            and len(self.hands) == 1
            and len(self.hands[0]) == 2
            and self.hands[0][0][:-1] == self.hands[0][1][:-1]
            and self.hand_statuses[0] == "playing"
        )

    def _can_double(self) -> bool:
        return (
            len(self._get_active_hand()) == 2
            and self.hand_statuses[self.active_hand_index] == "playing"
        )

    def _update_button_states(self) -> None:
        split_button = next((child for child in self.children if getattr(child, "custom_id", None) == "blackjack_split"), None)
        double_button = next((child for child in self.children if getattr(child, "custom_id", None) == "blackjack_double"), None)
        hit_second_button = next((child for child in self.children if getattr(child, "custom_id", None) == "blackjack_hit_second"), None)
        stand_second_button = next((child for child in self.children if getattr(child, "custom_id", None) == "blackjack_stand_second"), None)

        if split_button is not None:
            split_button.disabled = not self._can_split()
        if double_button is not None:
            double_button.disabled = not self._can_double()

        if len(self.hands) > 1:
            if hit_second_button is not None:
                hit_second_button.disabled = self.active_hand_index != 1
            if stand_second_button is not None:
                stand_second_button.disabled = self.active_hand_index != 1
        else:
            if hit_second_button is not None:
                hit_second_button.disabled = True
            if stand_second_button is not None:
                stand_second_button.disabled = True

    def _draw_card(self) -> str:
        if not self.deck:
            self.deck = _build_deck()
            random.shuffle(self.deck)
        return self.deck.pop()

    def _advance_to_next_hand(self) -> bool:
        if len(self.hands) < 2:
            return False
        for idx in range(self.active_hand_index + 1, len(self.hands)):
            if self.hand_statuses[idx] == "playing":
                self.active_hand_index = idx
                return True
        for idx in range(0, self.active_hand_index):
            if self.hand_statuses[idx] == "playing":
                self.active_hand_index = idx
                return True
        return False

    def _build_embed(self) -> discord.Embed:
        embed = discord.Embed(title="🃏 Blackjack", color=discord.Color.blurple())
        for index, hand in enumerate(self.hands):
            hand_total = _hand_value(hand)
            label = f"Hand {index + 1}"
            if len(self.hands) > 1 and index == self.active_hand_index:
                label += " (active)"
            suffix = ""
            if self.hand_statuses[index] == "bust":
                suffix = " • Bust"
            elif self.hand_statuses[index] == "stood":
                suffix = " • Stand"
            elif self.hand_statuses[index] == "doubled":
                suffix = " • Doubled"
            value = f"{_format_hand(hand)} — **{hand_total}**{suffix}"
            value += f"\nBet: {self.hand_bets[index]:,} {CASINO_WON_CURRENCY_EMOJI}"
            embed.add_field(name=label, value=value, inline=False)
        embed.add_field(name="Dealer", value=f"{self.dealer_hand[0]}, ❓", inline=False)
        embed.add_field(name="Bet", value=f"{self.bet:,} {CASINO_WON_CURRENCY_EMOJI} per hand", inline=False)
        return embed

    async def _send_inactivity_warning(self):
        try:
            user = await bot.fetch_user(self.user_id)
            if user is None:
                return
            lost_text = f"{self.total_wager:,} {CASINO_WON_CURRENCY_EMOJI}"
            link = ""
            if self.message and self.message.guild and self.message.channel:
                guild_id = self.message.guild.id
                channel_id = self.message.channel.id
                message_id = self.message.id
                link = f"\nhttps://discord.com/channels/{guild_id}/{channel_id}/{message_id}"
            await user.send(
                f"Outgoing blackjack game: if you don't play within 2 minutes you'll automatically lose your bet of {lost_text}.{link}"
            )
        except Exception:
            pass

    async def _monitor_inactivity(self):
        while not self.finished:
            wait = 300 if not self.warning_sent else 120
            await asyncio.sleep(wait)
            if self.finished:
                return
            if time.time() - self.last_interaction_at >= wait:
                if not self.warning_sent:
                    self.warning_sent = True
                    await self._send_inactivity_warning()
                    continue
                await self._auto_lose_due_to_inactivity()
                return

    async def _auto_lose_due_to_inactivity(self):
        if self.finished:
            return
        data = load_data()
        entry = get_or_create_economy_entry(data, self.user_id)
        entry["minigame_losses"] = entry.get("minigame_losses", 0) + 1
        reset_streak(entry)
        if self.message_id and str(self.message_id) in data.get("active_blackjack_games", {}):
            del data["active_blackjack_games"][str(self.message_id)]
        save_data(data)

        embed = discord.Embed(title="⏳ Blackjack — Timeout", color=discord.Color.red())
        embed.add_field(name="Result", value=f"You did not play in time and lost your wager of {self.total_wager:,} {CASINO_WON_CURRENCY_EMOJI}.", inline=False)
        if self.message is not None:
            try:
                await self.message.edit(embed=embed, view=None)
            except Exception:
                pass
        self.finished = True
        for child in self.children:
            child.disabled = True
        if self.monitor_task:
            self.monitor_task.cancel()

    async def _end(self, interaction: discord.Interaction, result_embed: discord.Embed):
        for child in self.children:
            child.disabled = True
        self.finished = True
        try:
            await interaction.response.edit_message(embed=result_embed, view=self)
        except Exception:
            await interaction.followup.send(embed=result_embed)

        if self.message_id:
            try:
                data = load_data()
                if str(self.message_id) in data.get("active_blackjack_games", {}):
                    del data["active_blackjack_games"][str(self.message_id)]
                    save_data(data)
            except Exception:
                pass

    async def _finish_turn(self, interaction: discord.Interaction, hand_index: int | None = None):
        if hand_index is not None:
            self.active_hand_index = hand_index
        self._update_button_states()
        if self._advance_to_next_hand():
            embed = self._build_embed()
            await interaction.response.edit_message(embed=embed, view=self)
            return

        await self._resolve_game(interaction)

    async def _resolve_game(self, interaction: discord.Interaction):
        while _hand_value(self.dealer_hand) < 17:
            if not self.deck:
                self.deck = _build_deck()
                random.shuffle(self.deck)
            self.dealer_hand.append(self.deck.pop())

        dealer_total = _hand_value(self.dealer_hand)
        data = load_data()
        entry = get_or_create_economy_entry(data, interaction.user.id)
        prev_streak = get_streak(entry)
        streak_lost_note = ""
        win_count = 0
        loss_count = 0
        push_count = 0
        outcome_lines = []

        for idx, hand in enumerate(self.hands):
            hand_total = _hand_value(hand)
            hand_bet = self.hand_bets[idx]
            if hand_total > 21:
                loss_count += 1
                outcome_lines.append(f"Hand {idx + 1}: Bust — lost {hand_bet:,} {CASINO_WON_CURRENCY_EMOJI}.")
                entry["minigame_losses"] = entry.get("minigame_losses", 0) + 1
            elif dealer_total > 21 or hand_total > dealer_total:
                win_count += 1
                base_payout = hand_bet * 2
                payout, _ = get_reward_with_role_bonus(base_payout, "blackjack", data, interaction.user.id, interaction.guild)
                entry["won"] = entry.get("won", 0) + payout
                entry["minigame_wins"] = entry.get("minigame_wins", 0) + 1
                stardust_reward = random.randint(MINIGAME_WIN_STARDUST_REWARD_MINIMUM, MINIGAME_WIN_STARDUST_REWARD_MAXIMUM)
                entry["stardust"] = entry.get("stardust", 0) + stardust_reward
                outcome_lines.append(f"Hand {idx + 1}: Win — received {payout:,} {CASINO_WON_CURRENCY_EMOJI}.")
            elif hand_total == dealer_total:
                push_count += 1
                entry["won"] = entry.get("won", 0) + hand_bet
                outcome_lines.append(f"Hand {idx + 1}: Push — returned {hand_bet:,} {CASINO_WON_CURRENCY_EMOJI}.")
            else:
                loss_count += 1
                outcome_lines.append(f"Hand {idx + 1}: Loss — lost {hand_bet:,} {CASINO_WON_CURRENCY_EMOJI}.")

        if win_count > 0 and loss_count == 0 and push_count == 0:
            increment_streak(entry)
        else:
            reset_streak(entry)
            if prev_streak > 3:
                streak_lost_note = f"⚠️ Your streak of {prev_streak} has been lost."

        enforce_balance_cap(entry, get_level_info(entry.get('stardust', 0))[0])
        save_data(data)

        embed = discord.Embed(title="🃏 Blackjack — Result", color=discord.Color.gold())
        for idx, hand in enumerate(self.hands):
            hand_total = _hand_value(hand)
            embed.add_field(name=f"Hand {idx + 1}", value=f"{_format_hand(hand)} — **{hand_total}**", inline=False)
        embed.add_field(name="Dealer Hand", value=f"{_format_hand(self.dealer_hand)} — **{dealer_total}**", inline=False)
        embed.add_field(name="Outcome", value="\n".join(outcome_lines), inline=False)
        if streak_lost_note:
            embed.add_field(name="Streak", value=streak_lost_note, inline=False)

        await self._end(interaction, embed)

    @discord.ui.button(label="Hit", style=discord.ButtonStyle.primary, custom_id="blackjack_hit")
    async def hit(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id or self.finished:
            await interaction.response.send_message("This is not your game.", ephemeral=True)
            return
        self.last_interaction_at = time.time()
        hand = self._get_active_hand()
        hand.append(self._draw_card())
        hand_total = _hand_value(hand)
        if hand_total > 21:
            self.hand_statuses[self.active_hand_index] = "bust"
            self._update_button_states()
            if self._advance_to_next_hand():
                embed = self._build_embed()
                await interaction.response.edit_message(embed=embed, view=self)
                return
            await self._resolve_game(interaction)
            return

        self._update_button_states()
        await interaction.response.edit_message(embed=self._build_embed(), view=self)

    @discord.ui.button(label="Stand", style=discord.ButtonStyle.secondary, custom_id="blackjack_stand")
    async def stand(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id or self.finished:
            await interaction.response.send_message("This is not your game.", ephemeral=True)
            return
        self.last_interaction_at = time.time()
        self.hand_statuses[self.active_hand_index] = "stood"
        self._update_button_states()
        await self._finish_turn(interaction)

    @discord.ui.button(label="Split", style=discord.ButtonStyle.blurple, custom_id="blackjack_split")
    async def split(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id or self.finished:
            await interaction.response.send_message("This is not your game.", ephemeral=True)
            return
        if not self._can_split():
            await interaction.response.send_message("You can only split a matching pair from the opening hand.", ephemeral=True)
            return
        data = load_data()
        entry = get_or_create_economy_entry(data, interaction.user.id)
        if entry.get("won", 0) < self.bet:
            await interaction.response.send_message(f"❌ You need at least {self.bet:,} {CASINO_WON_CURRENCY_EMOJI} to split this hand.", ephemeral=True)
            return

        self.last_interaction_at = time.time()
        entry["won"] = entry.get("won", 0) - self.bet
        self.total_wager += self.bet
        self.split_used = True
        first_card = self.hands[0].pop(0)
        second_card = self.hands[0].pop(0)
        self.hands = [[first_card], [second_card]]
        self.hand_bets = [self.bet, self.bet]
        self.hand_statuses = ["playing", "playing"]
        self.hands[0].append(self._draw_card())
        self.hands[1].append(self._draw_card())
        self.active_hand_index = 0
        self._update_button_states()
        save_data(data)
        await interaction.response.edit_message(embed=self._build_embed(), view=self)

    @discord.ui.button(label="Double", style=discord.ButtonStyle.green, custom_id="blackjack_double")
    async def double(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id or self.finished:
            await interaction.response.send_message("This is not your game.", ephemeral=True)
            return
        if not self._can_double():
            await interaction.response.send_message("You can only double on your opening two-card hand.", ephemeral=True)
            return
        data = load_data()
        entry = get_or_create_economy_entry(data, interaction.user.id)
        if entry.get("won", 0) < self.bet:
            await interaction.response.send_message(f"❌ You need at least {self.bet:,} {CASINO_WON_CURRENCY_EMOJI} to double this hand.", ephemeral=True)
            return

        self.last_interaction_at = time.time()
        entry["won"] = entry.get("won", 0) - self.bet
        self.total_wager += self.bet
        self.hand_bets[self.active_hand_index] += self.bet
        hand = self._get_active_hand()
        hand.append(self._draw_card())
        self.hand_statuses[self.active_hand_index] = "doubled"
        self._update_button_states()
        save_data(data)
        await self._finish_turn(interaction, self.active_hand_index)

    @discord.ui.button(label="Hit (2nd Hand)", style=discord.ButtonStyle.primary, custom_id="blackjack_hit_second")
    async def hit_second(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id or self.finished:
            await interaction.response.send_message("This is not your game.", ephemeral=True)
            return
        self.active_hand_index = 1
        await self.hit(interaction, button)

    @discord.ui.button(label="Stand (2nd Hand)", style=discord.ButtonStyle.secondary, custom_id="blackjack_stand_second")
    async def stand_second(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id or self.finished:
            await interaction.response.send_message("This is not your game.", ephemeral=True)
            return
        self.active_hand_index = 1
        await self.stand(interaction, button)


@bot.tree.command(name="blackjack", description="Play a quick blackjack hand. Use 'all' to bet your whole wallet.")
@app_commands.describe(bet="Amount to bet or 'all'")
async def blackjack_command(interaction: discord.Interaction, bet: str):
    if not is_economy_channel(interaction.channel, interaction.user.id):
        await interaction.response.send_message(f"⚠️ Economy commands are restricted to {economy_channels_mention(interaction.guild)}.", ephemeral=True)
        return

    data = load_data()
    uid = interaction.user.id
    entry = get_or_create_economy_entry(data, uid)
    wallet = entry.get("won", 0)

    bet_text = bet.strip().lower()
    if bet_text == "all":
        requested = wallet
    else:
        try:
            requested = int(bet_text.replace(',', ''))
        except Exception:
            await interaction.response.send_message("❌ Invalid bet amount. Use a number or 'all'.", ephemeral=True)
            return

    if requested <= 0:
        await interaction.response.send_message("❌ Bet must be greater than zero.", ephemeral=True)
        return

    if requested > wallet:
        await interaction.response.send_message(f"❌ You only have {wallet:,} {CASINO_WON_CURRENCY_EMOJI} to bet.", ephemeral=True)
        return

    # Bet cap enforcement
    level = get_level_info(entry.get('stardust', 0))[0]
    bet_cap = get_bet_cap(level)
    if requested > bet_cap:
        await interaction.response.send_message(f"❌ Your bet exceeds the per-bet cap for your level ({bet_cap:,} {CASINO_WON_CURRENCY_EMOJI}).", ephemeral=True)
        return

    # Immediately deduct the bet
    entry["won"] = entry.get("won", 0) - requested
    save_data(data)

    # Prepare deck and hands
    deck = _build_deck()
    random.shuffle(deck)
    player_hand = [deck.pop(), deck.pop()]
    dealer_hand = [deck.pop(), deck.pop()]

    # If player has 21 immediately, resolve as win
    player_total = _hand_value(player_hand)
    if player_total == 21:
        # pay blackjack as standard double for simplicity
        base_payout = requested * 2
        payout, _ = get_reward_with_role_bonus(base_payout, "blackjack", data, interaction.user.id, interaction.guild)
        entry["won"] = entry.get("won", 0) + payout
        increment_streak(entry)
        enforce_balance_cap(entry, get_level_info(entry.get('stardust', 0))[0])
        save_data(data)
        embed = discord.Embed(title="🃏 Blackjack — Blackjack!", color=discord.Color.gold())
        embed.add_field(name="Your Hand", value=f"{_format_hand(player_hand)} — **21**", inline=False)
        embed.add_field(name="Dealer Hand", value=f"{dealer_hand[0]}, ❓", inline=False)
        embed.add_field(name="Outcome", value=f"Blackjack! You received {payout:,} {CASINO_WON_CURRENCY_EMOJI}.")
        await interaction.response.send_message(embed=embed)
        return

    view = BlackjackView(uid, deck, player_hand, dealer_hand, requested)

    embed = discord.Embed(title="🃏 Blackjack", color=discord.Color.blurple())
    embed.add_field(name="Your Hand", value=f"{_format_hand(player_hand)} — **{_hand_value(player_hand)}**", inline=False)
    embed.add_field(name="Dealer", value=f"{dealer_hand[0]}, ❓", inline=False)
    # Show bet in the embed body (custom emoji may not render in footers)
    embed.add_field(name="Bet", value=f"{requested:,} {CASINO_WON_CURRENCY_EMOJI}", inline=False)

    await interaction.response.send_message(embed=embed, view=view)
    
    # Store active game for compensation on bot restart
    message = await interaction.original_response()
    view.message_id = message.id
    view.message = message
    game_data = {
        "user_id": uid,
        "bet": requested,
        "message_id": message.id,
        "channel_id": message.channel.id,
        "guild_id": message.guild.id if message.guild else None
    }
    data.setdefault("active_blackjack_games", {})[str(message.id)] = game_data
    save_data(data)


# Start the bot after all commands and views are defined
if __name__ == "__main__":
    bot.run(TOKEN)

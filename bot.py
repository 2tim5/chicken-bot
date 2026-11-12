"""
Chicken Bot — внутричатовая валюта «Курочка» 🍗
Учёт активности в групповых чатах Telegram.

Стек: Python 3.10+, aiogram 3.x, aiosqlite
"""

import asyncio
import logging
import os
import sys
import time

import aiosqlite
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ChatType, ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.types import Message, User
from aiogram.utils.text_decorations import markdown_decoration

# ── Читаемые логи с кириллицей и эмодзи в консоли Windows ────────────────
for _stream in (sys.stdout, sys.stderr):
    if _stream is not None and hasattr(_stream, "reconfigure"):
        try:
            _stream.reconfigure(encoding="utf-8")
        except Exception:
            pass

# ══════════════════════════════════════════════════════════════════════════
#  Конфигурация
# ══════════════════════════════════════════════════════════════════════════

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN and len(sys.argv) > 1:
    BOT_TOKEN = sys.argv[1]

if not BOT_TOKEN:
    print("❌ Не задан BOT_TOKEN. Установи переменную окружения BOT_TOKEN или передай аргументом: python bot.py <токен>")
    sys.exit(1)

DB_PATH: str = "chickens.db"
COOLDOWN_SECONDS: int = 30
TOP_LIMIT: int = 10

# Порт веб-сервера (health-check для хостингов); по умолчанию 10000
try:
    PORT: int = int(os.getenv("PORT", "10000"))
except (TypeError, ValueError):
    PORT = 10000

# ══════════════════════════════════════════════════════════════════════════
#  Логирование
# ══════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("chicken_bot")

# ══════════════════════════════════════════════════════════════════════════
#  Глобальное состояние
# ══════════════════════════════════════════════════════════════════════════

db: aiosqlite.Connection | None = None
dp = Dispatcher()

# ══════════════════════════════════════════════════════════════════════════
#  Фильтры (переиспользуемые)
# ══════════════════════════════════════════════════════════════════════════

GROUP_TYPES = {ChatType.GROUP, ChatType.SUPERGROUP}
IS_GROUP = F.chat.type.in_(GROUP_TYPES)
NOT_BOT = ~F.from_user.is_bot
HAS_TEXT = F.text

# ══════════════════════════════════════════════════════════════════════════
#  База данных
# ══════════════════════════════════════════════════════════════════════════


async def init_db() -> None:
    """Создать таблицу users, если её ещё нет."""
    global db
    db = await aiosqlite.connect(DB_PATH)
    await db.execute(
        "CREATE TABLE IF NOT EXISTS users ("
        "user_id INTEGER PRIMARY KEY,"
        "username TEXT,"
        "chickens INTEGER DEFAULT 0,"
        "last_reward_time REAL DEFAULT 0"
        ")"
    )
    await db.commit()
    logger.info("База данных инициализирована (chickens.db)")


async def close_db() -> None:
    """Закрыть соединение с БД."""
    global db
    if db is not None:
        await db.close()
        db = None
        logger.info("Соединение с БД закрыто")


async def _ensure_user(user_id: int, username: str | None = None) -> None:
    """Создать запись пользователя, если нет; обновить username, если задан."""
    assert db is not None
    await db.execute(
        "INSERT OR IGNORE INTO users (user_id, username) VALUES (?, ?)",
        (user_id, username),
    )
    if username:
        await db.execute(
            "UPDATE users SET username = ? WHERE user_id = ?",
            (username, user_id),
        )
    await db.commit()


async def try_reward(user_id: int, username: str | None) -> bool:
    """Попытаться начислить +1 🍗 (с проверкой кулдауна).

    Возвращает True, если начисление прошло.
    """
    assert db is not None
    now = time.time()

    # Убедиться, что строка пользователя существует
    await db.execute(
        "INSERT OR IGNORE INTO users (user_id, username) VALUES (?, ?)",
        (user_id, username),
    )
    if username:
        await db.execute(
            "UPDATE users SET username = ? WHERE user_id = ?",
            (username, user_id),
        )

    # Атомарная попытка начисления: обновляем только если прошёл кулдаун
    cur = await db.execute(
        "UPDATE users SET chickens = chickens + 1, last_reward_time = ? "
        "WHERE user_id = ? AND (? - last_reward_time) >= ?",
        (now, user_id, now, COOLDOWN_SECONDS),
    )
    await db.commit()
    return cur.rowcount == 1


async def get_balance(user_id: int) -> int:
    """Вернуть баланс пользователя (0, если пользователь не найден)."""
    assert db is not None
    cur = await db.execute("SELECT chickens FROM users WHERE user_id = ?", (user_id,))
    row = await cur.fetchone()
    return row[0] if row else 0


async def get_top(limit: int = TOP_LIMIT) -> list[tuple[int, str | None, int]]:
    """Вернуть список топ-пользователей: (user_id, username, chickens)."""
    assert db is not None
    cur = await db.execute(
        "SELECT user_id, username, chickens FROM users WHERE chickens > 0 "
        "ORDER BY chickens DESC LIMIT ?",
        (limit,),
    )
    return await cur.fetchall()


async def transfer_chickens(sender_id: int, receiver_id: int, amount: int) -> bool:
    """Атомарно перевести amount 🍗 от отправителя получателю.

    Возвращает True при успехе, False — если недостаточно средств.
    """
    assert db is not None

    # Списание у отправителя (только если баланс >= amount)
    cur = await db.execute(
        "UPDATE users SET chickens = chickens - ? "
        "WHERE user_id = ? AND chickens >= ?",
        (amount, sender_id, amount),
    )
    if cur.rowcount == 0:
        await db.commit()  # закрыть пустую транзакцию
        return False

    # Начисление получателю
    await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (receiver_id,))
    await db.execute(
        "UPDATE users SET chickens = chickens + ? WHERE user_id = ?",
        (amount, receiver_id),
    )
    await db.commit()
    return True


# ══════════════════════════════════════════════════════════════════════════
#  Вспомогательные функции
# ══════════════════════════════════════════════════════════════════════════


def _display_name(user: User) -> str:
    """Красивое имя пользователя для вывода в чат."""
    full = user.full_name
    if full:
        return full
    return user.username or f"id{user.id}"


# ══════════════════════════════════════════════════════════════════════════
#  Обработчики команд
# ══════════════════════════════════════════════════════════════════════════


# ── Баланс ───────────────────────────────────────────────────────────────


@dp.message(IS_GROUP, Command("balance", ignore_case=True))
@dp.message(IS_GROUP, F.text.lower().strip() == "баланс")
async def cmd_balance(message: Message) -> None:
    """Показать текущий баланс пользователя."""
    user = message.from_user
    if user is None:
        return

    balance = await get_balance(user.id)
    await message.reply(f"🍗 Твой баланс: {balance} курочек")


# ── Топ ──────────────────────────────────────────────────────────────────


@dp.message(IS_GROUP, Command("top", ignore_case=True))
@dp.message(IS_GROUP, F.text.lower().strip() == "топ")
async def cmd_top(message: Message) -> None:
    """Показать топ-10 пользователей по курочкам."""
    rows = await get_top(TOP_LIMIT)

    if not rows:
        await message.reply("🍗 Пока никто не заработал ни одной курочки!")
        return

    lines: list[str] = []
    for i, (uid, username, chickens) in enumerate(rows, start=1):
        # Построить имя: username или id, экранированное для MarkdownV2
        if username:
            raw = username
        else:
            raw = str(uid)
        name = markdown_decoration.quote(raw)
        cnt = markdown_decoration.quote(str(chickens))
        lines.append(f"{i}\\. {name} — {cnt} 🍗")

    header = markdown_decoration.quote("🏆 Топ-10 курочек")
    text = f"*{header}*\n\n" + "\n".join(lines)

    await message.reply(text, parse_mode=ParseMode.MARKDOWN_V2)


# ── Передать ─────────────────────────────────────────────────────────────


async def _handle_transfer(message: Message, amount_str: str | None) -> None:
    """Общая логика команды «передать»."""
    target_msg = message.reply_to_message
    if target_msg is None or target_msg.from_user is None:
        await message.reply(
            "🍗 Чтобы передать курочек, "
            "ответь на сообщение участника командой «передать [число]»."
        )
        return

    sender = message.from_user
    if sender is None:
        return

    recipient = target_msg.from_user

    if recipient.id == sender.id:
        await message.reply("🍗 Нельзя передать курочек самому себе.")
        return

    if recipient.is_bot:
        await message.reply("🍗 Ботам курочки не нужны — передай человеку!")
        return

    # Разбор числа
    try:
        amount = int((amount_str or "").strip())
    except (TypeError, ValueError):
        await message.reply("🍗 Укажи целое положительное число, например: передать 5")
        return

    if amount <= 0:
        await message.reply("🍗 Количество курочек должно быть больше нуля.")
        return

    # Обеспечить существование строк в БД
    await _ensure_user(sender.id, sender.username)
    await _ensure_user(recipient.id, recipient.username)

    success = await transfer_chickens(sender.id, recipient.id, amount)
    if not success:
        await message.reply("🍗 У тебя недостаточно курочек для перевода.")
        return

    s_name = _display_name(sender)
    r_name = _display_name(recipient)
    await message.reply(f"🍗 {s_name} передал {amount} курочек участнику {r_name}!")


@dp.message(IS_GROUP, Command("transfer", "передать", ignore_case=True))
async def cmd_transfer_slash(message: Message, command: CommandObject) -> None:
    """Обработчик /transfer (или /передать)."""
    await _handle_transfer(message, command.args)


@dp.message(
    IS_GROUP,
    F.text.lower().strip().startswith("передать "),
)
async def cmd_transfer_text(message: Message) -> None:
    """Обработчик текстовой команды «передать N»."""
    # Извлечь всё после первого пробела как сумму
    rest = message.text.strip().split(maxsplit=1)[1]
    await _handle_transfer(message, rest)


# ── Начисление за активность ─────────────────────────────────────────────
#  Этот обработчик регистрируется ПОСЛЕ всех команд, чтобы не перехватывать их.


@dp.message(
    IS_GROUP,
    HAS_TEXT,
    NOT_BOT,
    ~F.text.startswith("/"),
)
async def reward_for_message(message: Message) -> None:
    """Начислить +1 🍗 за обычное текстовое сообщение (с кулдауном 30 с)."""
    user = message.from_user
    if user is None:
        return

    # Дополнительная проверка: не реагируем на служебные сообщения
    # (content_type != 'text'), но HAS_TEXT это уже гарантирует
    try:
        awarded = await try_reward(user.id, user.username)
        if awarded:
            logger.info("+1 🍗 пользователю %d (%s)", user.id, user.username or "?")
    except Exception:
        logger.exception("Ошибка при начислении курочки")


# ══════════════════════════════════════════════════════════════════════════
#  Жизненный цикл бота
# ══════════════════════════════════════════════════════════════════════════


@dp.startup()
async def on_startup() -> None:
    """Вызывается при запуске бота — инициализация БД."""
    await init_db()


@dp.shutdown()
async def on_shutdown() -> None:
    """Вызывается при остановке бота — закрытие БД."""
    await close_db()


# ══════════════════════════════════════════════════════════════════════════
#  Веб-сервер (health-check)
# ══════════════════════════════════════════════════════════════════════════


async def _web_ping(_request: web.Request) -> web.Response:
    """Отдаём 200 OK на GET / — так хостинг понимает, что бот жив."""
    return web.Response(text="Bot is alive", status=200)


async def start_web_server() -> web.AppRunner:
    """Запустить минимальный HTTP-сервер на 0.0.0.0:PORT.

    Возвращает runner, чтобы корректно остановить сервер при завершении работы.
    """
    app = web.Application()
    app.router.add_get("/", _web_ping)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
    await site.start()
    logger.info("Веб-сервер запущен на 0.0.0.0:%d", PORT)
    return runner


# ══════════════════════════════════════════════════════════════════════════
#  Точка входа
# ══════════════════════════════════════════════════════════════════════════


async def main() -> None:
    """Запустить веб-сервер и бота параллельно (long polling)."""
    logger.info("Запуск Chicken Bot…")
    bot = Bot(token=BOT_TOKEN)

    web_runner = await start_web_server()
    try:
        # Веб-сервер уже работает в фоне; polling занимает главный цикл событий
        await dp.start_polling(bot)
    finally:
        await web_runner.cleanup()
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен по Ctrl+C")

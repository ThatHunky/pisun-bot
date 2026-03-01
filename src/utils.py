import asyncio
import logging
import random
import datetime
from zoneinfo import ZoneInfo
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aiogram import Bot

KYIV_TZ = ZoneInfo("Europe/Kyiv")
RETRY_MAX_ATTEMPTS = 3
logger = logging.getLogger(__name__)


async def _retry_on_429(coro):
    """Run coroutine, on TelegramRetryAfter sleep and retry up to RETRY_MAX_ATTEMPTS."""
    from aiogram.exceptions import TelegramRetryAfter
    last_exc = None
    for attempt in range(RETRY_MAX_ATTEMPTS):
        try:
            return await coro()
        except TelegramRetryAfter as e:
            last_exc = e
            if attempt == RETRY_MAX_ATTEMPTS - 1:
                raise
            logger.warning("Telegram 429, retry after %s s (attempt %s)", e.retry_after, attempt + 1)
            await asyncio.sleep(e.retry_after)
    if last_exc:
        raise last_exc


async def send_message_with_retry(bot: "Bot", chat_id: int, text: str, **kwargs):
    """Send message with retry on Telegram 429 (Too Many Requests)."""
    async def _send():
        return await bot.send_message(chat_id, text, **kwargs)
    return await _retry_on_429(_send)


async def edit_message_text_with_retry(bot: "Bot", **kwargs):
    """Edit message text with retry on Telegram 429."""
    async def _edit():
        return await bot.edit_message_text(**kwargs)
    return await _retry_on_429(_edit)


class RetryBot:
    """Wraps a Bot so send_message and edit_message_text retry on 429."""

    def __init__(self, bot: "Bot"):
        self._bot = bot

    def __getattr__(self, name):
        return getattr(self._bot, name)

    async def send_message(self, chat_id: int, text: str, **kwargs):
        return await send_message_with_retry(self._bot, chat_id, text, **kwargs)

    async def edit_message_text(self, **kwargs):
        return await edit_message_text_with_retry(self._bot, **kwargs)

def get_fair_pisun_delta(measure_count: int, current_length: float) -> float:
    """
    Returns a delta for the pisun measurement.
    - First 3 measurements are always positive (bonus for beginners)
    - Early game (<50cm): Strong growth, lower penalty
    - Mid game (50-150cm): Balanced growth
    - End game (>150cm): High penalty, slower growth (diminishing returns)
    """
    if measure_count < 3:
        # First 3: always +1 to +10 cm
        return round(random.uniform(1.0, 10.0), 1)
    
    # Base configuration based on current length
    if current_length < 50.0:
        # Catch-up mode
        plus_chance = 0.75
        plus_range = (1.0, 8.0)
        minus_range = (0.1, 3.0)
    elif current_length < 150.0:
        # Balanced mode
        plus_chance = 0.60
        plus_range = (0.1, 7.0)
        minus_range = (0.1, 5.0)
    else:
        # Hard mode (diminishing returns)
        plus_chance = 0.45
        plus_range = (0.0, 5.0)
        minus_range = (0.5, 10.0)

    chance = random.random()
    if chance < plus_chance:
        delta = random.uniform(*plus_range)
    else:
        delta = -random.uniform(*minus_range)
    
    # Ensure it doesn't go negative
    if current_length + delta < 0:
        delta = -current_length
        
    return round(delta, 1)

PIHV_VARIANTS = [
r"""
       .       .
      / \     / \
     /   \___/   \
    |    /   \    |
    |   | ( ) |   |
    |    \___/    |
     \   /   \   /
      \ /     \ /
       '       '
""",
r"""
      ( .   . )
     /   \ /   \
    (    ( )    )
     \   ' '   /
      \  _|_  /
       \_____/
""",
r"""
      /'''\ /'''\
     (     |     )
     |    ( )    |
     (     |     )
      \___/ \___/
""",
r"""
      ,__,   ,__,
     /    \ /    \
    |      |      |
    |     ( )     |
    \      |      /
     \____/ \____/
""",
r"""
     ./\___/\.
    |   ( )   |
    |    |    |
     \  _|_  /
      \_____/
"""
]

INSERT_RESPONSES = [
    "О ДАААА! Ти вставив це як батя! +{delta} см до твого шланга! 🍌",
    "Це було волого... і результативно! +{delta} см отримано! 🔥",
    "Швидка рука! Вставив першим - виграв життя! +{delta} см! 🚀",
    "Залетіло як по маслу! +{delta} см твої! 💦",
    "Снайперський постріл! Прямо в ціль! +{delta} см! 🎯",
    "Твої рефлекси вражають! Піхва твоя! +{delta} см! ⚡️",
    "Ого, ти навіть не постукав! Але результат є: +{delta} см! 🚪",
    "Легендарне проникнення! Історичний момент! +{delta} см! 📜",
    "Ти майстер своєї справи! Тримай заслужені +{delta} см! 🛠",
    "Ідеальне виконання! Судді ставлять 10/10! +{delta} см! 🔟",
    "Це було швидше за звук! Ти подолав бар'єр! +{delta} см! ✈️",
    "Хірургічна точність! Пацієнт (піхва) задоволений! +{delta} см! 👨‍⚕️",
    "Ти що, тренувався на пончиках? Ідеальне попадання! +{delta} см! 🍩",
    "Агент 007 нервово палить збоку. Ти справжній профі! +{delta} см! 🕴️",
    "Вставив і забув? Ні, вставив і виріс! +{delta} см! 🧠",
    "Космічна стиковка пройшла успішно! Х'юстон, у нас +{delta} см! 🛰️",
    "Ти розблокував досягнення 'Золотий Палець'! +{delta} см! 🏆",
    "Навіть Ілон Маск не зміг би вставити краще! +{delta} см! 🚀",
    "Це було так епічно, що Netflix хоче зняти про це серіал! +{delta} см! 🎬",
    "Вставив як флешку з першого разу! Це магія! +{delta} см! 💾",
    "Твій рівень тестостерону пробив стелю! +{delta} см! 🦍",
    "Це не просто вставив, це мистецтво! +{delta} см! 🎨",
    "Гросмейстерський хід! Шах і мат! +{delta} см! ♟️",
    "Ти вкрав це очко як професійний злодій! +{delta} см! 🦝",
    "Вставив з розвороту! Чак Норріс схвалює! +{delta} см! 🥋",
]

TRAP_RESPONSES = [
    "О ноу! Піхва відкусила... -{delta} см! 🦷",
    "НІІІІ, ВІН ЗАСТРЯГ!! Довелося відрізати... -{delta} см! ✂️",
    "Це була пастка! Ти потрапив у капкан: -{delta} см! 🪤",
    "Вона виявилася затісною... Стиснуло до -{delta} см! 🤏",
    "Там була піранья! Мінус {delta} см! 🐟",
    "Ти вставив не туди... Штраф за неуважність: -{delta} см! 🚫",
    "Вона була з шипами! Болюча втрата: -{delta} см! 🌵",
    "Який жах! Вона холодна як лід! Зіщулився на -{delta} см! ❄️",
    "Ти розбудив Ктулху! Він обідав твоїм агрегатом: -{delta} см! 🐙",
    "Помилка 404: Пісюн частково не знайдено! -{delta} см! ❌",
]

PISUN_PHRASES = {
    "plus": [
        "Ого! Твій пісюн підріс на {delta} см! Тепер він {total} см. Гігант! 🍆",
        "Медітація допомогла: +{delta} см. Поточна довжина: {total} см. 🙏",
        "Трішки дріжджів і... +{delta} см! Маємо {total} см. 🍞",
        "Ти спав з лінійкою? +{delta} см! Разом: {total} см. 📏",
        "Ефект Віагри (жарт): +{delta} см. Тепер у тебе {total} см. 💊",
        "Це законна зброя? +{delta} см! Всього: {total} см. 🔫",
        "Космічний ріст! +{delta} см. Твій агрегат: {total} см. 🚀",
        "Мама буде пишатися (чи ні): +{delta} см. Результат: {total} см. 👩",
        "Не стій під стрілою! +{delta} см. Довжина: {total} см. 🏗",
        "Це що, біта? +{delta} см! Маємо {total} см. ⚾️",
        "Яка гарна погода для росту! +{delta} см. Разом: {total} см. ☀️",
        "Ти поливав його? +{delta} см. В сумі: {total} см. 🚿",
        "Генетика не підвела! +{delta} см. Маємо {total} см. 🧬",
        "Це вже не смішно, це страшно! +{delta} см. Разом: {total} см. 😱",
        "Зупиніть цього монстра! +{delta} см. Довжина: {total} см. 🛑",
        "Ти що, качав його в залі? +{delta} см! Результат: {total} см. 🏋️‍♂️",
        "Боги рандому до тебе прихильні! +{delta} см. Разом: {total} см. ⚡️",
        "Святий пісюн! +{delta} см. Маємо {total} см. 😇",
        "Еволюція в дії! +{delta} см. Всього: {total} см. 🦖",
        "Це просто магія! +{delta} см. Твій скіпетр: {total} см. 🪄",
        "Він тягнеться до зірок! +{delta} см. Тепер: {total} см. ✨",
        "Ти його розтягував на дибі? +{delta} см. Разом: {total} см. ⛓️",
        "Радіація з Чорнобиля? +{delta} см. Мутант: {total} см. ☢️",
        "Він росте як біткоїн у 2017-му! +{delta} см. Капітал: {total} см. 📈",
        "Це вже шлагбаум! +{delta} см. Перекрив рух: {total} см. 🚧",
        "Ти що, бджолиний вулик туди приклав? +{delta} см. Роздуло до: {total} см. 🐝",
        "Нанотехнології в дії! +{delta} см. Кібер-пісюн: {total} см. 🤖",
        "Він скоро отримає власний паспорт! +{delta} см. Громадянин: {total} см. 🛂",
        "Це вже телескопічна вудка! +{delta} см. Рибак: {total} см. 🎣",
        "Ти годуєш його протеїном? +{delta} см. Банка: {total} см. 🥤",
        "Він виліз з чату! +{delta} см. Екран тріснув: {total} см. 📱",
        "Це подарунок долі! +{delta} см. Щасливчик: {total} см. 🎁",
        "Він як бамбук, росте на очах! +{delta} см. Панда в шоці: {total} см. 🐼",
        "Глобальне потепління? Він розширюється! +{delta} см. Спека: {total} см. 🌡️",
        "Це вже архітектурна пам'ятка! +{delta} см. Туристи фотографують: {total} см. 📸",
    ],
    "minus": [
        "Холодна вода? Відвалилося {delta} см... Залишилось {total} см. 🥶",
        "Ти забагато думав про математику: -{delta} см. Маємо {total} см. 📉",
        "Сорі бро, сьогодні не твій день: -{delta} см. В тебе {total} см. 🤏",
        "Усушка, утруска... -{delta} см. Результат: {total} см. 🥀",
        "Може досить смикати? -{delta} см. Залишилось: {total} см. 🚫",
        "Гравітація сьогодні проти тебе: -{delta} см. Маємо {total} см. ⬇️",
        "Це податок на розкіш: -{delta} см. Твій капітал: {total} см. 💸",
        "Поганий фен-шуй: -{delta} см. Разом: {total} см. 🧘‍♂️",
        "Ти його налякав! -{delta} см. Всього: {total} см. 👻",
        "Сьогодні він інтроверт: -{delta} см. Залишилось: {total} см. 🐚",
        "Карма наздогнала: -{delta} см. Маємо {total} см. ☯️",
        "Не хвилюйся, він просто змерз: -{delta} см. Результат: {total} см. ❄️",
        "Місяць у ретроградному меркурії: -{delta} см. Разом: {total} см. 🌑",
        "Це тимчасові труднощі: -{delta} см. Всього: {total} см. 🚧",
        "Час лікує... але не сьогодні. -{delta} см. Маємо {total} см. 🚑",
        "Він вирішив сховатися: -{delta} см. Залишилось: {total} см. 🙈",
        "Зменшення бюджету: -{delta} см. Результат: {total} см. 💰",
        "Оптимізація простору: -{delta} см. Разом: {total} см. 📦",
        "Це тактичний відступ: -{delta} см. Всього: {total} см. 🔙",
        "Все одно більше ніж у адміна (напевно): -{delta} см. Маємо {total} см. 🖥",
        "Миші відгризли! -{delta} см. Залишок: {total} см. 🐭",
        "Це шрінкфляція, братан. -{delta} см. Економія: {total} см. 📉",
        "Він пішов у відпустку. -{delta} см. На пляжі: {total} см. 🏖️",
        "Ти занадто багато нервуєш. -{delta} см. Стрес: {total} см. 😫",
        "Він вирішив стати компактним. -{delta} см. Travel-size: {total} см. 🧳",
        "Це демо-версія закінчилась. -{delta} см. Купи підписку: {total} см. 💳",
        "Злий чаклун наклав прокляття! -{delta} см. Магія: {total} см. 🧙‍♂️",
        "Він втягнувся від сорому за твої жарти. -{delta} см. Крінж: {total} см. 🤦‍♂️",
        "Це побічний ефект вак... кхм. -{delta} см. Здоров'я: {total} см. 💉",
        "Глюк матриці. -{delta} см. Нео плаче: {total} см. 🕶️",
        "Ти його переправ? -{delta} см. Сів після прання: {total} см. 🧺",
        "Це плата за вхід в інтернет. -{delta} см. Тариф: {total} см. 🌐",
        "Він просто втомився бути великим. -{delta} см. Дауншифтинг: {total} см. 🛌",
        "Гравітаційна аномалія. -{delta} см. Чорна діра: {total} см. 🌌",
        "Він сховався в панцир. -{delta} см. Черепашка: {total} см. 🐢",
    ],
    "zero": [
        "Стабільність - ознака майстерності. Без змін: {total} см. 🗿",
        "Сьогодні без сюрпризів. Рівно {total} см. 😐",
        "Ні туди, ні сюди. {total} см. ↔️",
        "Застиг у часі. {total} см. ⏳",
        "Ідеальний баланс. {total} см. ⚖️",
        "День бабака. Ті самі {total} см. 🐿️",
        "Штиль на морі. {total} см. 🌊",
        "Він у стані дзен. {total} см. 🧘",
        "Константа. Як число Пі, тільки {total}. 📐",
        "Закон збереження енергії. {total} см. 🔋",
    ],
    "already_measured": [
        "Ей, зупинись! Пісюн не гумовий, раз на день міряємо! Приходь завтра. ✋",
        "Твій шланг втомився. Дай йому відпочити до завтра! 😴",
        "Що, знову? Ні, братан, тільки раз на добу. 🕒",
        "Хорошого потроху. Чекай до завтра! 🚫",
        "Ти вже міряв! Не намагайся надурити долю. 🔮",
        "Завтра, все завтра. Сьогодні ліміт вичерпано. 🔚",
        "Він не виросте швидше, якщо міряти частіше. Приходь завтра. 🌱",
        "Дай йому відновитися! До завтра. 🛌",
        "Поліція пісюнів! Ви перевищили ліміт вимірювань! 🚔",
        "Лікар заборонив часті вимірювання. Це шкідливо для самооцінки. 👨‍⚕️",
        "Сервер перегріється від твого нетерпіння. Охолонь до завтра. 🔥",
        "Не чіпай його, він спить! 🤫",
        "Приходь після дощику в четвер... або просто завтра. ☔",
        "Ти хочеш стерти його до дірок? Стоп! 🛑",
        "Абонемент на сьогодні використано. 🎫",
    ]
}
def get_kyiv_now() -> datetime.datetime:
    """Returns current time in Kyiv timezone."""
    return datetime.datetime.now(KYIV_TZ)

def get_kyiv_today() -> datetime.date:
    """Returns current date in Kyiv timezone."""
    return get_kyiv_now().date()

def is_same_week(date1: datetime.date, date2: datetime.date) -> bool:
    """Checks if two dates belong to the same ISO week."""
    return date1.isocalendar()[:2] == date2.isocalendar()[:2]


EVENT_TYPES = ("duel", "jackpot", "trap")
STAKE_TIERS = (5, 10, 20)
EVENT_DURATIONS_SEC = {
    "duel": 60,
    "jackpot": 90,
    "trap": 90,
}
EVENT_LABELS = {
    "duel": "Дуель",
    "jackpot": "Джекпот",
    "trap": "Пастка",
}
HISTORY_SOURCE_LABELS = {
    "pisun": "Щоденний замір",
    "weekly_pihv": "Тижневий дроп",
    "event_duel": "Подія: дуель",
    "event_jackpot": "Подія: джекпот",
    "event_trap": "Подія: пастка",
}
QUIET_HOURS_START = 1
QUIET_HOURS_END = 7


def get_utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def get_utc_now_iso() -> str:
    return get_utc_now().isoformat(timespec="seconds")


def parse_iso_datetime(value: str) -> datetime.datetime:
    dt = datetime.datetime.fromisoformat(value)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=datetime.timezone.utc)
    return dt


def to_kyiv_datetime(value: str) -> datetime.datetime:
    return parse_iso_datetime(value).astimezone(KYIV_TZ)

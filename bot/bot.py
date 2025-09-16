import logging
import aiohttp
import asyncio
import qrcode
import random
import json
import os
import re
from datetime import datetime, timedelta
from io import BytesIO
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv
from pathlib import Path
from texts import TEXTS

BASE_DIR = Path(__file__).resolve().parent
env_path = BASE_DIR.parent / '.env'
load_dotenv(env_path)

BOT_TOKEN = os.getenv("BOT_TOKEN")
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME")
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME")
LOGIN = os.getenv("LOGIN")
PASSWORD = os.getenv("PASSWORD")
if LOGIN and LOGIN.startswith('@'):
    LOGIN = LOGIN[1:]

os.makedirs(BASE_DIR / "../databases/log", exist_ok=True)
os.makedirs(BASE_DIR / "../databases/orders", exist_ok=True)
os.makedirs(BASE_DIR / "../databases/users", exist_ok=True)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s, %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_handler = logging.FileHandler(
    BASE_DIR / f"../databases/log/log_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log",
    encoding='utf-8'
)
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.handlers = [file_handler, console_handler]
logging.getLogger('aiogram').setLevel(logging.CRITICAL)
logging.getLogger('aiohttp').setLevel(logging.CRITICAL)
logging.getLogger('asyncio').setLevel(logging.CRITICAL)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
is_running = True

def load_data(filename):
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return {}

def save_user_data(user_id, data):
    try:
        user_file = BASE_DIR / f"../databases/users/{user_id}.json"
        with open(user_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except:
        pass

def load_user_data(user_id):
    try:
        user_file = BASE_DIR / f"../databases/users/{user_id}.json"
        with open(user_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            data['orders'] = [order_id for order_id in data.get('orders', [])
                              if (BASE_DIR / f"../databases/orders/order_{order_id}.json").exists()]
            return data
    except:
        return {"orders": [], "total_orders": 0, "total_spent": 0, "username": "", "full_name": ""}

CITIES = load_data(BASE_DIR / '../databases/data/points/cities.json')
PRODUCTS = load_data(BASE_DIR / '../databases/data/points/products.json')
TYPES = load_data(BASE_DIR / '../databases/data/points/types.json')
PAYMENT = load_data(BASE_DIR / '../databases/data/payment/payment.json')

def main_keyboard():
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_MARKETPLACE'], callback_data="marketplace"))
    builder.row(
        InlineKeyboardButton(text=TEXTS['BUTTON_OPT'], callback_data="opt"),
        InlineKeyboardButton(text=TEXTS['BUTTON_WORK'], callback_data="work"),
    )
    builder.row(
        InlineKeyboardButton(text=TEXTS['BUTTON_RULES'], callback_data="rules"),
        InlineKeyboardButton(text=TEXTS['BUTTON_PROFILE'], callback_data="profile")
    )
    builder.row(
        InlineKeyboardButton(text=TEXTS['BUTTON_SUPPORT'], url=f"https://t.me/{SUPPORT_USERNAME.replace('@', '')}"),
        InlineKeyboardButton(text=TEXTS['BUTTON_ORDERS'], callback_data="history")
    )
    builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_CHAT'], callback_data="client_chat"))
    return builder.as_markup()

def back_keyboard(back_data: str):
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text=TEXTS['BUTTON_BACK'], callback_data=back_data))
    return builder.as_markup()

class RatesUpdater:
    def __init__(self, filename='../databases/data/payment/rates.json', update_interval=3600):
        self.filename = BASE_DIR / filename
        self.update_interval = update_interval
        self.rates = {}
        self.last_update = None
        self.mapping = {"Bitcoin": "bitcoin", "Litecoin": "litecoin", "Monero": "monero"}

    async def load_rates(self):
        try:
            with open(self.filename, 'r') as f:
                self.rates = json.load(f)
        except:
            await self.update_rates()

    async def save_rates(self):
        os.makedirs(os.path.dirname(self.filename), exist_ok=True)
        try:
            with open(self.filename, 'w') as f:
                json.dump(self.rates, f, indent=2)
        except:
            pass

    async def update_rates(self):
        try:
            coin_ids = list(set(self.mapping.values()))
            async with aiohttp.ClientSession() as session:
                async with session.get(
                        "https://api.coingecko.com/api/v3/simple/price",
                        params={'ids': ','.join(coin_ids), 'vs_currencies': 'rub'},
                        timeout=10
                ) as response:
                    if response.status == 200:
                        api_data = await response.json()
                        for crypto_name, coin_id in self.mapping.items():
                            if coin_id in api_data and 'rub' in api_data[coin_id]:
                                self.rates[crypto_name] = api_data[coin_id]['rub']
                        for payment_method in PAYMENT:
                            if payment_method not in self.rates:
                                self.rates[payment_method] = 1
                        self.last_update = datetime.now()
                        await self.save_rates()
        except:
            for payment_method in PAYMENT:
                self.rates[payment_method] = 1

    async def run_periodic_update(self):
        await self.load_rates()
        while is_running:
            try:
                if not self.last_update or (datetime.now() - self.last_update) > timedelta(
                        seconds=self.update_interval):
                    await self.update_rates()
            except:
                pass
            for _ in range(300):
                if not is_running:
                    break
                await asyncio.sleep(1)

class MarketplaceStates(StatesGroup):
    select_city = State()
    search_city = State()
    select_product = State()
    select_price = State()
    select_type = State()
    select_district = State()
    search_district = State()
    confirm_order = State()
    select_payment = State()
    payment_details = State()

class WorkState(StatesGroup):
    work_form = State()

class AdminStates(StatesGroup):
    password = State()
    main_menu = State()
    logs_view = State()
    logs_search = State()
    profiles_view = State()
    profiles_search = State()
    orders_view = State()
    orders_search = State()
    order_detail = State()

BOT_NAME = None
rates_updater = RatesUpdater()
unavailable_products_cache = {}
unavailable_weights_cache = {}
unavailable_types_cache = {}
unavailable_districts_cache = {}
last_cache_update = datetime.min

def format_price(price):
    return f"{price:,}".replace(",", " ")

def apply_markup(base_price: float, markup_percent: float) -> int:
    price_with_markup = base_price * (1 + markup_percent / 100)
    return round(price_with_markup)

def update_caches():
    global unavailable_products_cache, unavailable_weights_cache, unavailable_types_cache, unavailable_districts_cache, last_cache_update
    now = datetime.now()
    if now - last_cache_update < timedelta(hours=6):
        return
    unavailable_products_cache = {}
    unavailable_weights_cache = {}
    unavailable_types_cache = {}
    unavailable_districts_cache = {}
    for city, city_data in CITIES.items():
        size = city_data.get("size", 1)
        districts = city_data.get("districts", [])
        if size == 1:
            available_percent = random.uniform(0.3, 0.4)
        elif size == 2:
            available_percent = random.uniform(0.5, 0.6)
        else:
            available_percent = random.uniform(0.7, 0.8)
        all_products = [pid for pid in PRODUCTS.keys() if not pid.startswith("!")]
        special_products = [pid for pid in PRODUCTS.keys() if pid.startswith("!")]
        num_available = int(len(all_products) * available_percent)
        available_products = random.sample(all_products, num_available) if num_available > 0 else []
        unavailable_products_cache[city] = set(all_products) - set(available_products)
        unavailable_weights = {}
        unavailable_types = {}
        for product_id in available_products:
            if random.random() < 0.5:
                weights = list(PRODUCTS[product_id]["prices"].keys())
                if len(weights) > 1:
                    unavailable_weight = random.choice(weights)
                    unavailable_weights[product_id] = unavailable_weight
            if random.random() < 0.3:
                if len(TYPES) > 1:
                    unavailable_type = random.randint(0, len(TYPES) - 1)
                    unavailable_types[product_id] = unavailable_type
        for product_id in special_products:
            unavailable_weights[product_id] = None
            unavailable_types[product_id] = None
        unavailable_weights_cache[city] = unavailable_weights
        unavailable_types_cache[city] = unavailable_types
        if districts:
            unavailable_districts_percent = random.uniform(0.3, 0.5)
            num_unavailable_districts = int(len(districts) * unavailable_districts_percent)
            unavailable_districts = set(
                random.sample(districts, num_unavailable_districts)) if num_unavailable_districts > 0 else set()
            unavailable_districts_cache[city] = unavailable_districts
        else:
            unavailable_districts_cache[city] = set()
    last_cache_update = now

def get_available_products(city: str):
    update_caches()
    unavailable = unavailable_products_cache.get(city, set())
    special_products = [pid for pid in PRODUCTS.keys() if pid.startswith("!")]
    available_products = {pid: data for pid, data in PRODUCTS.items() if
                          pid not in unavailable or pid in special_products}
    sorted_products = {}
    for pid in PRODUCTS.keys():
        if pid in available_products:
            sorted_products[pid] = available_products[pid]
    return sorted_products

def get_available_weights(city: str, product_id: str):
    update_caches()
    unavailable_weights = unavailable_weights_cache.get(city, {})
    if product_id.startswith("!"):
        return list(PRODUCTS[product_id]["prices"].keys())
    unavailable_weight = unavailable_weights.get(product_id)
    if unavailable_weight:
        return [weight for weight in PRODUCTS[product_id]["prices"].keys() if weight != unavailable_weight]
    return list(PRODUCTS[product_id]["prices"].keys())

def get_available_types(city: str, product_id: str):
    update_caches()
    unavailable_types = unavailable_types_cache.get(city, {})
    if product_id.startswith("!"):
        return list(range(len(TYPES)))
    unavailable_type = unavailable_types.get(product_id)
    if unavailable_type is not None:
        return [i for i in range(len(TYPES)) if i != unavailable_type]
    return list(range(len(TYPES)))

def get_available_districts(city: str):
    update_caches()
    city_data = CITIES.get(city, {})
    districts = city_data.get("districts", [])
    unavailable = unavailable_districts_cache.get(city, set())
    return [d for d in districts if d not in unavailable]

def save_order(order_data):
    try:
        orders_dir = BASE_DIR / '../databases/orders'
        os.makedirs(orders_dir, exist_ok=True)
        order_filename = orders_dir / f"order_{order_data['order_id']}.json"
        with open(order_filename, 'w', encoding='utf-8') as f:
            json.dump(order_data, f, ensure_ascii=False, indent=2)
        user_data = load_user_data(order_data['user_id'])
        user_data['orders'].append(order_data['order_id'])
        if len(user_data['orders']) > 10:
            user_data['orders'] = user_data['orders'][-10:]
        save_user_data(order_data['user_id'], user_data)
        logging.info(f"Заказ {order_data['order_id']} Оформлен")
        return order_data['order_id']
    except:
        return None

def update_order_status(order_id, status, order_file=None):
    try:
        orders_dir = BASE_DIR / '../databases/orders'
        if order_file is None:
            order_file = orders_dir / f"order_{order_id}.json"
        if os.path.exists(order_file):
            with open(order_file, 'r', encoding='utf-8') as f:
                order_data = json.load(f)
            old_status = order_data.get('status')
            order_data['status'] = status
            order_data['updated'] = datetime.now().isoformat()
            with open(order_file, 'w', encoding='utf-8') as f:
                json.dump(order_data, f, ensure_ascii=False, indent=2)
            if status == 'Оплачен' and old_status != 'Оплачен':
                user_data = load_user_data(order_data['user_id'])
                user_data['total_orders'] = user_data.get('total_orders', 0) + 1
                user_data['total_spent'] = user_data.get('total_spent', 0) + order_data['price']
                save_user_data(order_data['user_id'], user_data)
                logging.info(f"Заказ {order_id} Оплачен")
            elif status == 'Отменен':
                logging.info(f"Заказ {order_id} Отменен")
            return order_data
    except:
        pass
    return None

def generate_marketplace_cities_keyboard(page: int = 0):
    builder = InlineKeyboardBuilder()
    cities = list(CITIES.keys())
    start_idx = page * 20
    end_idx = min((page + 1) * 20, len(cities))
    builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_SEARCH'], callback_data="marketplace_search_city"), width=1)
    city_buttons = []
    for city in cities[start_idx:end_idx]:
        city_buttons.append(InlineKeyboardButton(text=city, callback_data=f"marketplace_city_{city}"))
    for i in range(0, len(city_buttons), 2):
        if i + 1 < len(city_buttons):
            builder.row(city_buttons[i], city_buttons[i + 1])
        else:
            builder.row(city_buttons[i])
    pagination_buttons = []
    if page > 0:
        pagination_buttons.append(
            InlineKeyboardButton(text=TEXTS['BUTTON_PAGE_BACK'], callback_data=f"marketplace_prev_city_{page}"))
    if end_idx < len(cities):
        pagination_buttons.append(
            InlineKeyboardButton(text=TEXTS['BUTTON_PAGE_NEXT'], callback_data=f"marketplace_next_city_{page}"))
    if pagination_buttons:
        builder.row(*pagination_buttons)
    builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_BACK'], callback_data="marketplace_back_to_main"))
    return builder.as_markup()

def generate_marketplace_products_keyboard(city: str):
    builder = InlineKeyboardBuilder()
    available_products = get_available_products(city)
    for product_id, product in available_products.items():
        builder.add(InlineKeyboardButton(text=product["name"], callback_data=f"marketplace_product_{product_id}"))
    builder.adjust(1)
    builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_BACK'], callback_data="marketplace_back_to_cities"))
    return builder.as_markup()

def generate_marketplace_prices_keyboard(city: str, product_id: str, markup_percent: float):
    builder = InlineKeyboardBuilder()
    available_weights = get_available_weights(city, product_id)
    if product_id in PRODUCTS:
        product = PRODUCTS[product_id]
        for price_key in available_weights:
            base_price = product["prices"][price_key]
            final_price = apply_markup(base_price, markup_percent)
            formatted_price = format_price(final_price)
            builder.add(InlineKeyboardButton(text=f"{price_key} - {formatted_price}₽",
                                             callback_data=f"marketplace_price_{price_key}"))
    builder.adjust(1)
    builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_BACK'], callback_data="marketplace_back_to_products"))
    return builder.as_markup()

def generate_marketplace_types_keyboard(city: str, product_id: str):
    builder = InlineKeyboardBuilder()
    available_types = get_available_types(city, product_id)
    for i in available_types:
        builder.add(InlineKeyboardButton(text=TYPES[i], callback_data=f"marketplace_type_{i}"))
    builder.adjust(1)
    builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_BACK'], callback_data="marketplace_back_to_prices"))
    return builder.as_markup()

def generate_marketplace_districts_keyboard(city: str, page: int = 0):
    builder = InlineKeyboardBuilder()
    districts = get_available_districts(city)
    if not districts:
        builder.row(InlineKeyboardButton(text=TEXTS['MESSAGE_MARKETPLACE_NOT_DISTRICT'],
                                         callback_data="marketplace_district_no_district"))
        builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_BACK'], callback_data="marketplace_back_to_types"))
        return builder.as_markup()
    start_idx = page * 20
    end_idx = min((page + 1) * 20, len(districts))
    builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_SEARCH'], callback_data="marketplace_search_district"), width=1)
    district_buttons = []
    for district in districts[start_idx:end_idx]:
        district_buttons.append(InlineKeyboardButton(text=district, callback_data=f"marketplace_district_{district}"))
    for i in range(0, len(district_buttons), 2):
        if i + 1 < len(district_buttons):
            builder.row(district_buttons[i], district_buttons[i + 1])
        else:
            builder.row(district_buttons[i])
    pagination_buttons = []
    if page > 0:
        pagination_buttons.append(
            InlineKeyboardButton(text=TEXTS['BUTTON_PAGE_BACK'], callback_data=f"marketplace_prev_district_{page}"))
    if end_idx < len(districts):
        pagination_buttons.append(
            InlineKeyboardButton(text=TEXTS['BUTTON_PAGE_NEXT'], callback_data=f"marketplace_next_district_{page}"))
    if pagination_buttons:
        builder.row(*pagination_buttons)
    builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_BACK'], callback_data="marketplace_back_to_types"))
    return builder.as_markup()

def generate_confirm_order_keyboard():
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text=TEXTS['BUTTON_PAY'], callback_data="marketplace_confirm_order"),
        InlineKeyboardButton(text=TEXTS['BUTTON_CANCEL'], callback_data="marketplace_cancel_order")
    )
    builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_BACK'], callback_data="marketplace_back_to_districts_confirm"))
    return builder.as_markup()

def generate_payment_cancel_keyboard():
    builder = InlineKeyboardBuilder()
    builder.add(
        InlineKeyboardButton(text=TEXTS['BUTTON_CANCEL'], callback_data="marketplace_cancel_payment"),
        InlineKeyboardButton(text=TEXTS['BUTTON_SUPPORT'], url=f"https://t.me/{SUPPORT_USERNAME.replace('@', '')}")
    )
    return builder.as_markup()

def generate_marketplace_payment_keyboard():
    builder = InlineKeyboardBuilder()
    for payment_method in PAYMENT:
        builder.add(InlineKeyboardButton(text=payment_method, callback_data=f"marketplace_payment_{payment_method}"))
    builder.adjust(2)
    builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_BACK'], callback_data="marketplace_back_to_confirm"))
    return builder.as_markup()

def profile_keyboard():
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text=TEXTS['BUTTON_BACK'], callback_data="main_menu"))
    return builder.as_markup()

def get_order_status(order_id):
    try:
        order_file = BASE_DIR / f'../databases/orders/order_{order_id}.json'
        with open(order_file, 'r', encoding='utf-8') as f:
            return json.load(f).get('status', 'Неизвестно')
    except:
        return 'Неизвестно'

def get_order_price(order_id):
    try:
        order_file = BASE_DIR / f'../databases/orders/order_{order_id}.json'
        with open(order_file, 'r', encoding='utf-8') as f:
            return json.load(f).get('price', 0)
    except:
        return 0

def history_keyboard():
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text=TEXTS['BUTTON_BACK'], callback_data="main_menu"))
    return builder.as_markup()

def operator_keyboard():
    builder = InlineKeyboardBuilder()
    builder.add(
        InlineKeyboardButton(text=TEXTS['BUTTON_SUPPORT'], url=f"https://t.me/{SUPPORT_USERNAME.replace('@', '')}"))
    builder.add(InlineKeyboardButton(text=TEXTS['BUTTON_BACK'], callback_data="main_menu"))
    builder.adjust(1)
    return builder.as_markup()

def admin_keyboard():
    builder = InlineKeyboardBuilder()
    builder.add(
        InlineKeyboardButton(text=TEXTS['BUTTON_SUPPORT'], url=f"https://t.me/{ADMIN_USERNAME.replace('@', '')}"))
    builder.add(InlineKeyboardButton(text=TEXTS['BUTTON_BACK'], callback_data="main_menu"))
    builder.adjust(1)
    return builder.as_markup()

def admin_main_keyboard():
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_ADMIN_LOGS'], callback_data="admin_logs"))
    builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_ADMIN_PROFILES'], callback_data="admin_profiles"))
    builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_ADMIN_ORDERS'], callback_data="admin_orders"))
    builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_ADMIN_EXIT'], callback_data="admin_exit"))
    return builder.as_markup()

def admin_back_keyboard(back_data: str):
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text=TEXTS['BUTTON_BACK'], callback_data=back_data))
    return builder.as_markup()

def admin_logs_keyboard(page: int, total_pages: int, search_query: str = None):
    builder = InlineKeyboardBuilder()
    if page == 0 and search_query is None:
        builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_REFRESH'], callback_data="admin_logs_refresh"))
    if search_query is None:
        builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_SEARCH'], callback_data="admin_logs_search"))
    else:
        builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_CLEAR'], callback_data="admin_logs_clear_search"))
    pagination_buttons = []
    if total_pages > 1:
        if page > 0:
            pagination_buttons.append(
                InlineKeyboardButton(text=TEXTS['BUTTON_PAGE_BACK'], callback_data=f"admin_logs_prev_{page - 1}"))
        if page < total_pages - 1:
            pagination_buttons.append(
                InlineKeyboardButton(text=TEXTS['BUTTON_PAGE_NEXT'], callback_data=f"admin_logs_next_{page + 1}"))
    if pagination_buttons:
        builder.row(*pagination_buttons)
    builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_BACK'], callback_data="admin_back_to_main"))
    return builder.as_markup()

def format_profile_for_admin(user_id, user_data):
    paid_orders = [order_id for order_id in user_data.get('orders', [])
                   if get_order_status(order_id) == 'Оплачен']
    total_orders = len(paid_orders)
    total_spent = sum(get_order_price(order_id) for order_id in paid_orders)
    reg_date = user_data.get('registration_date', '')
    if reg_date:
        try:
            reg_date = datetime.fromisoformat(reg_date).strftime('%d.%m.%Y %H:%M')
        except:
            reg_date = 'не указана'
    else:
        reg_date = 'не указана'
    return TEXTS['MESSAGE_ADMIN_PROFILE_DETAIL'].format(
        user_id=user_id,
        username=f"@{user_data.get('username', '')}" if user_data.get('username') else f"ID{user_id}",
        name=user_data.get('full_name', ''),
        orders_count=total_orders,
        total_spent=format_price(total_spent),
        discount=0,
        registration_date=reg_date
    )

def format_order_for_admin(order_data):
    order_date = datetime.fromisoformat(order_data['date']).strftime('%d.%m.%Y %H:%M')
    return TEXTS['MESSAGE_ADMIN_ORDER_DETAIL'].format(
        order_id=order_data['order_id'],
        city=order_data['city'],
        product=order_data['product'],
        weight=order_data['weight'],
        type=order_data['type'],
        district=order_data.get('district', TEXTS['MESSAGE_MARKETPLACE_NOT_DISTRICT']),
        price=format_price(order_data['price']),
        payment_method=order_data['payment_method'],
        payment_price=order_data['payment_amount'],
        currency=PAYMENT.get(order_data['payment_method'], "RUB"),
        payment_details=order_data['wallet_address'],
        status=order_data['status'],
        date=order_date
    )

def get_profiles_list(page=0, per_page=10, search=None):
    profiles_dir = BASE_DIR / "../databases/users"
    profile_files = list(profiles_dir.glob("*.json"))
    profile_files.sort(key=os.path.getmtime, reverse=True)
    profiles = []
    for file in profile_files:
        try:
            with open(file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if search:
                search_lower = search.lower()
                if (search_lower in str(file.stem).lower() or
                        search_lower in (data.get('username') or '').lower() or
                        search_lower in (data.get('full_name') or '').lower() or
                        search_lower in str(data.get('total_orders', '')).lower() or
                        search_lower in str(data.get('total_spent', '')).lower()):
                    profiles.append((file.stem, data))
            else:
                profiles.append((file.stem, data))
        except:
            continue
    total = len(profiles)
    start = page * per_page
    end = start + per_page
    return profiles[start:end], total

def get_orders_list(page=0, per_page=10, search=None):
    orders_dir = BASE_DIR / "../databases/orders"
    order_files = list(orders_dir.glob("order_*.json"))
    order_files.sort(key=os.path.getmtime, reverse=True)
    orders = []
    for file in order_files:
        try:
            with open(file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if search:
                search_lower = search.lower()
                if (search_lower in str(data.get('order_id', '')).lower() or
                        search_lower in (data.get('city') or '').lower() or
                        search_lower in (data.get('product') or '').lower() or
                        search_lower in (data.get('weight') or '').lower() or
                        search_lower in (data.get('type') or '').lower() or
                        search_lower in (data.get('district') or '').lower() or
                        search_lower in str(data.get('price', '')).lower() or
                        search_lower in (data.get('payment_method') or '').lower() or
                        search_lower in str(data.get('payment_amount', '')).lower() or
                        search_lower in (data.get('wallet_address') or '').lower() or
                        search_lower in (data.get('status') or '').lower() or
                        search_lower in (data.get('username') or '').lower()):
                    orders.append(data)
            else:
                orders.append(data)
        except:
            continue
    total = len(orders)
    start = page * per_page
    end = start + per_page
    return orders[start:end], total

def admin_profiles_keyboard(profiles, page: int, total_pages: int, search_query: str = None):
    builder = InlineKeyboardBuilder()
    for user_id, data in profiles:
        username = data.get('username', '') or f"ID{user_id}"
        builder.row(
            InlineKeyboardButton(text=username, callback_data=f"admin_profile_{user_id}"))
    if page == 0 and search_query is None:
        builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_REFRESH'], callback_data="admin_profiles_refresh"))
    if search_query is None:
        builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_SEARCH'], callback_data="admin_profiles_search"))
    else:
        builder.row(
            InlineKeyboardButton(text=TEXTS['BUTTON_CLEAR'], callback_data="admin_profiles_clear_search"))
    pagination_buttons = []
    if page > 0:
        pagination_buttons.append(
            InlineKeyboardButton(text=TEXTS['BUTTON_PAGE_BACK'], callback_data=f"admin_profiles_prev_{page - 1}"))
    if page < total_pages - 1:
        pagination_buttons.append(
            InlineKeyboardButton(text=TEXTS['BUTTON_PAGE_NEXT'], callback_data=f"admin_profiles_next_{page + 1}"))
    if pagination_buttons:
        builder.row(*pagination_buttons)
    builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_BACK'], callback_data="admin_back_to_main"))
    return builder.as_markup()

def admin_orders_keyboard(orders, page: int, total_pages: int, search_query: str = None):
    builder = InlineKeyboardBuilder()
    for order in orders:
        status = order.get('status', 'Оформлен')
        if status == 'Оплачен':
            emoji = '✅'
        elif status == 'Отменен':
            emoji = '❌'
        else:
            emoji = '⏳'
        button_text = f"{emoji} Заказ {order['order_id']} {status}"
        builder.row(
            InlineKeyboardButton(
                text=button_text,
                callback_data=f"admin_order_{order['order_id']}"
            )
        )
    if page == 0 and search_query is None:
        builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_REFRESH'], callback_data="admin_orders_refresh"))
    if search_query is None:
        builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_SEARCH'], callback_data="admin_orders_search"))
    else:
        builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_CLEAR'], callback_data="admin_orders_clear_search"))
    pagination_buttons = []
    if page > 0:
        pagination_buttons.append(
            InlineKeyboardButton(text=TEXTS['BUTTON_PAGE_BACK'], callback_data=f"admin_orders_prev_{page - 1}"))
    if page < total_pages - 1:
        pagination_buttons.append(
            InlineKeyboardButton(text=TEXTS['BUTTON_PAGE_NEXT'], callback_data=f"admin_orders_next_{page + 1}"))
    if pagination_buttons:
        builder.row(*pagination_buttons)
    builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_BACK'], callback_data="admin_back_to_main"))
    return builder.as_markup()

def admin_order_detail_keyboard(order_id, status):
    builder = InlineKeyboardBuilder()
    if status == 'Оформлен':
        builder.row(
            InlineKeyboardButton(
                text=TEXTS['BUTTON_ORDER_CANCEL'],
                callback_data=f"admin_order_cancel_{order_id}"
            ),
            InlineKeyboardButton(
                text=TEXTS['BUTTON_ORDER_PAY'],
                callback_data=f"admin_order_pay_{order_id}"
            )
        )
    builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_BACK'], callback_data="admin_orders_back"))
    return builder.as_markup()

def get_logs(lines=None, search=None):
    log_dir = BASE_DIR / "../databases/log"
    log_files = sorted(log_dir.glob("*.log"), key=os.path.getmtime, reverse=True)
    all_lines = []
    for log_file in log_files:
        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                file_lines = f.readlines()
            file_lines.reverse()
            all_lines.extend(file_lines)
        except:
            continue
    if search:
        search_lower = search.lower()
        all_lines = [line for line in all_lines if search_lower in line.lower()]
    if lines is not None:
        return all_lines[:lines]
    return all_lines

async def safe_edit_message(message: types.Message, text: str, reply_markup: InlineKeyboardMarkup = None,
                            parse_mode: str = None):
    try:
        await message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
        return message.message_id
    except:
        try:
            await message.delete()
        except:
            pass
        try:
            new_message = await message.answer(text, reply_markup=reply_markup, parse_mode=parse_mode)
            return new_message.message_id
        except:
            return None

async def delete_admin_session_messages(chat_id, state):
    data = await state.get_data()
    message_ids = data.get('admin_session_messages', [])
    for msg_id in message_ids:
        try:
            await bot.delete_message(chat_id, msg_id)
        except:
            continue
    await state.update_data(admin_session_messages=[])

async def handle_order_status_change(callback: types.CallbackQuery, order_id: str, new_status: str):
    if update_order_status(order_id, new_status):
        order_file = BASE_DIR / f'../databases/orders/order_{order_id}.json'
        with open(order_file, 'r', encoding='utf-8') as f:
            order_data = json.load(f)
        order_text = format_order_for_admin(order_data)
        await safe_edit_message(
            callback.message,
            order_text,
            parse_mode="HTML",
            reply_markup=admin_order_detail_keyboard(order_id, new_status)
        )
        try:
            if new_status == 'Оплачен':
                user_notification = TEXTS['MESSAGE_MARKETPLACE_CONFIRM_PAYMENT_PAID'].format(order_id=order_id)
            elif new_status == 'Отменен':
                user_notification = TEXTS['MESSAGE_MARKETPLACE_CONFIRM_PAYMENT_CANCELLED'].format(order_id=order_id)
            else:
                user_notification = f"Статус вашего заказа {order_id} изменен на: {new_status}"

            await bot.send_message(order_data['chat_id'], user_notification, parse_mode="HTML")
        except:
            pass
    else:
        await callback.answer(TEXTS['MESSAGE_ERROR'])

@dp.message(Command("start"))
async def start_command_handler(message: types.Message, state: FSMContext):
    user_username = message.from_user.username
    normalized_username = user_username.lower() if user_username else None
    if normalized_username == LOGIN.lower():
        await state.set_state(AdminStates.password)
        msg = await message.answer(TEXTS['MESSAGE_ADMIN_PASSWORD'])
        await state.update_data(admin_password_msg_id=msg.message_id)
        return
    username = f"@{message.from_user.username}" if message.from_user.username else f"ID{message.from_user.id}"
    logging.info(f"Обработка команды старт от пользователя {username}")
    user_data = load_user_data(message.from_user.id)
    if 'registration_date' not in user_data:
        user_data['registration_date'] = datetime.now().isoformat()
    user_data['username'] = message.from_user.username or ''
    user_data['full_name'] = message.from_user.full_name
    save_user_data(message.from_user.id, user_data)
    await state.clear()
    await message.answer(TEXTS['MESSAGE_MENU'], parse_mode="HTML", reply_markup=main_keyboard())

@dp.message(AdminStates.password)
async def admin_password_handler(message: types.Message, state: FSMContext):
    data = await state.get_data()
    admin_password_msg_id = data.get('admin_password_msg_id')
    try:
        await bot.delete_message(message.chat.id, admin_password_msg_id)
        await message.delete()
    except:
        pass
    if message.text == PASSWORD:
        await state.set_state(AdminStates.main_menu)
        msg = await message.answer(TEXTS['MESSAGE_ADMIN_MENU'], reply_markup=admin_main_keyboard())
        await state.update_data(admin_session_messages=[msg.message_id])
    else:
        msg = await message.answer(TEXTS['MESSAGE_ADMIN_PASSWORD'])
        await state.update_data(admin_password_msg_id=msg.message_id)

@dp.callback_query(lambda c: c.data == "admin_back_to_main")
async def admin_back_to_main_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.main_menu)
    await safe_edit_message(callback.message, TEXTS['MESSAGE_ADMIN_MENU'], reply_markup=admin_main_keyboard())

@dp.callback_query(lambda c: c.data == "admin_exit")
async def admin_exit_handler(callback: types.CallbackQuery, state: FSMContext):
    await delete_admin_session_messages(callback.message.chat.id, state)
    await state.clear()
    try:
        await callback.message.delete()
    except:
        pass

@dp.callback_query(lambda c: c.data == "admin_logs")
async def admin_logs_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.logs_view)
    logs = get_logs(lines=20)
    per_page = 20
    total_lines = len(get_logs())
    total_pages = max(1, (total_lines + per_page - 1) // per_page)
    page = 0
    text = TEXTS['MESSAGE_ADMIN_LOGS'].format(logs=''.join(logs)) if logs else TEXTS['MESSAGE_ADMIN_LOGS_EMPTY']
    await state.update_data(
        logs_page=page,
        logs_search=None,
        total_logs=total_lines,
        per_page=per_page,
        total_pages=total_pages
    )
    data = await state.get_data()
    admin_session_messages = data.get('admin_session_messages', [])
    admin_session_messages.append(callback.message.message_id)
    await state.update_data(admin_session_messages=admin_session_messages)
    await safe_edit_message(
        callback.message,
        text,
        parse_mode="HTML",
        reply_markup=admin_logs_keyboard(page, total_pages)
    )

@dp.callback_query(AdminStates.logs_view, lambda c: c.data.startswith("admin_logs_"))
async def admin_logs_actions_handler(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    page = data.get('logs_page', 0)
    per_page = data.get('per_page', 20)
    search_query = data.get('logs_search')
    if callback.data == "admin_logs_refresh":
        logs = get_logs(search=search_query)
        total_logs = len(logs)
        total_pages = max(1, (total_logs + per_page - 1) // per_page)
        if page >= total_pages:
            page = total_pages - 1
        start_idx = page * per_page
        end_idx = min((page + 1) * per_page, total_logs)
        page_logs = logs[start_idx:end_idx]
        text = TEXTS['MESSAGE_ADMIN_LOGS_EMPTY'] if not page_logs else TEXTS['MESSAGE_ADMIN_LOGS'].format(
            logs=''.join(page_logs))
        await state.update_data(total_logs=total_logs, logs_page=page, total_pages=total_pages)
        await safe_edit_message(
            callback.message,
            text,
            parse_mode="HTML",
            reply_markup=admin_logs_keyboard(page, total_pages, search_query)
        )
    elif callback.data == "admin_logs_search":
        await state.set_state(AdminStates.logs_search)
        msg_id = await safe_edit_message(
            callback.message,
            TEXTS['MESSAGE_ADMIN_SEARCH'],
            reply_markup=admin_back_keyboard("admin_logs_back")
        )
        await state.update_data(search_request_msg_id=msg_id)
    elif callback.data == "admin_logs_clear_search":
        await state.update_data(logs_search=None)
        logs = get_logs()
        total_logs = len(logs)
        total_pages = max(1, (total_logs + per_page - 1) // per_page)
        page = 0
        start_idx = page * per_page
        end_idx = min((page + 1) * per_page, total_logs)
        page_logs = logs[start_idx:end_idx]
        text = TEXTS['MESSAGE_ADMIN_LOGS_EMPTY'] if not page_logs else TEXTS['MESSAGE_ADMIN_LOGS'].format(
            logs=''.join(page_logs))
        await state.update_data(logs_page=page, total_logs=total_logs, total_pages=total_pages)
        await safe_edit_message(
            callback.message,
            text,
            parse_mode="HTML",
            reply_markup=admin_logs_keyboard(page, total_pages)
        )
    elif callback.data.startswith("admin_logs_prev_") or callback.data.startswith("admin_logs_next_"):
        new_page = int(callback.data.split('_')[3])
        logs = get_logs(search=search_query)
        total_logs = len(logs)
        total_pages = max(1, (total_logs + per_page - 1) // per_page)
        if new_page < 0:
            new_page = 0
        elif new_page >= total_pages:
            new_page = total_pages - 1
        start_idx = new_page * per_page
        end_idx = min((new_page + 1) * per_page, total_logs)
        page_logs = logs[start_idx:end_idx]
        text = TEXTS['MESSAGE_ADMIN_LOGS_EMPTY'] if not page_logs else TEXTS['MESSAGE_ADMIN_LOGS'].format(
            logs=''.join(page_logs))
        await state.update_data(logs_page=new_page, total_logs=total_logs, total_pages=total_pages)
        await safe_edit_message(
            callback.message,
            text,
            parse_mode="HTML",
            reply_markup=admin_logs_keyboard(new_page, total_pages, search_query)
        )

@dp.callback_query(AdminStates.logs_search, lambda c: c.data == "admin_logs_back")
async def admin_logs_back_from_search_handler(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    page = data.get('logs_page', 0)
    per_page = data.get('per_page', 20)
    search_query = data.get('logs_search')
    logs = get_logs(search=search_query)
    total_logs = len(logs)
    total_pages = max(1, (total_logs + per_page - 1) // per_page)
    start_idx = page * per_page
    end_idx = min((page + 1) * per_page, total_logs)
    page_logs = logs[start_idx:end_idx]
    if not page_logs:
        text = TEXTS['MESSAGE_ADMIN_LOGS_EMPTY']
    else:
        text = TEXTS['MESSAGE_ADMIN_LOGS'].format(logs=''.join(page_logs))
    await state.set_state(AdminStates.logs_view)
    await state.update_data(total_logs=total_logs, total_pages=total_pages)
    await safe_edit_message(
        callback.message,
        text,
        parse_mode="HTML",
        reply_markup=admin_logs_keyboard(page, total_pages, search_query)
    )

@dp.message(AdminStates.logs_search)
async def admin_logs_search_handler(message: types.Message, state: FSMContext):
    data = await state.get_data()
    search_request_msg_id = data.get("search_request_msg_id")
    if search_request_msg_id:
        try:
            await bot.delete_message(message.chat.id, search_request_msg_id)
        except:
            pass
    try:
        await message.delete()
    except:
        pass
    search_query = message.text.strip()
    await state.update_data(logs_search=search_query, logs_page=0)
    await state.set_state(AdminStates.logs_view)
    logs = get_logs(search=search_query)
    per_page = 20
    total_logs = len(logs)
    total_pages = max(1, (total_logs + per_page - 1) // per_page)
    page = 0
    start_idx = page * per_page
    end_idx = min((page + 1) * per_page, total_logs)
    page_logs = logs[start_idx:end_idx]
    if not page_logs:
        text = TEXTS['MESSAGE_ADMIN_LOGS_EMPTY']
    else:
        text = TEXTS['MESSAGE_ADMIN_LOGS'].format(logs=''.join(page_logs))
    await state.update_data(total_logs=total_logs, total_pages=total_pages)
    msg = await message.answer(
        text,
        parse_mode="HTML",
        reply_markup=admin_logs_keyboard(page, total_pages, search_query)
    )
    data = await state.get_data()
    admin_session_messages = data.get('admin_session_messages', [])
    admin_session_messages.append(msg.message_id)
    await state.update_data(admin_session_messages=admin_session_messages)

@dp.callback_query(lambda c: c.data == "admin_profiles")
async def admin_profiles_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.profiles_view)
    profiles, total = get_profiles_list(per_page=10)
    text = TEXTS['MESSAGE_ADMIN_PROFILES']
    data = await state.get_data()
    admin_session_messages = data.get('admin_session_messages', [])
    admin_session_messages.append(callback.message.message_id)
    await state.update_data(admin_session_messages=admin_session_messages)
    await safe_edit_message(
        callback.message,
        text,
        reply_markup=admin_profiles_keyboard(profiles, 0, max(1, (total + 9) // 10))
    )
    await state.update_data(profiles_page=0, profiles_search=None)

@dp.callback_query(
    AdminStates.profiles_view,
    lambda c: (c.data.startswith("admin_profiles_") or c.data.startswith("admin_profile_"))
    and c.data != "admin_profiles_back"
)
async def admin_profiles_actions_handler(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    page = data.get('profiles_page', 0)
    search_query = data.get('profiles_search')
    if callback.data == "admin_profiles_refresh":
        profiles, total = get_profiles_list(page=page, per_page=10, search=search_query)
        text = TEXTS['MESSAGE_ADMIN_PROFILES']
        await safe_edit_message(
            callback.message,
            text,
            reply_markup=admin_profiles_keyboard(profiles, page, max(1, (total + 9) // 10), search_query)
        )
    elif callback.data == "admin_profiles_search":
        await state.set_state(AdminStates.profiles_search)
        msg_id = await safe_edit_message(
            callback.message,
            TEXTS['MESSAGE_ADMIN_SEARCH'],
            reply_markup=admin_back_keyboard("admin_profiles_back")
        )
        await state.update_data(search_request_msg_id=msg_id)
    elif callback.data == "admin_profiles_clear_search":
        await state.update_data(profiles_search=None)
        profiles, total = get_profiles_list(page=page, per_page=10)
        text = TEXTS['MESSAGE_ADMIN_PROFILES']
        await safe_edit_message(
            callback.message,
            text,
            reply_markup=admin_profiles_keyboard(profiles, page, max(1, (total + 9) // 10))
        )
    elif callback.data.startswith("admin_profiles_prev_") or callback.data.startswith("admin_profiles_next_"):
        action = callback.data.split('_')[2]
        new_page = int(callback.data.split('_')[3])
        profiles, total = get_profiles_list(page=new_page, per_page=10, search=search_query)
        text = TEXTS['MESSAGE_ADMIN_PROFILES']
        await state.update_data(profiles_page=new_page)
        await safe_edit_message(
            callback.message,
            text,
            reply_markup=admin_profiles_keyboard(profiles, new_page, max(1, (total + 9) // 10), search_query)
        )
    elif callback.data.startswith("admin_profile_"):
        user_id = callback.data.split('_')[2]
        user_data = load_user_data(user_id)
        profile_text = format_profile_for_admin(user_id, user_data)
        await safe_edit_message(
            callback.message,
            profile_text,
            parse_mode="HTML",
            reply_markup=admin_back_keyboard("admin_profiles_back")
        )

@dp.callback_query(AdminStates.profiles_search, lambda c: c.data == "admin_profiles_back")
async def admin_profiles_back_from_search_handler(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    search_request_msg_id = data.get("search_request_msg_id")
    if search_request_msg_id:
        try:
            await bot.delete_message(callback.message.chat.id, search_request_msg_id)
        except:
            pass
    page = data.get('profiles_page', 0)
    search_query = data.get('profiles_search')
    profiles, total = get_profiles_list(page=page, per_page=10, search=search_query)
    text = TEXTS['MESSAGE_ADMIN_PROFILES']
    await state.set_state(AdminStates.profiles_view)
    await state.update_data(profiles_page=page)
    await safe_edit_message(
        callback.message,
        text,
        reply_markup=admin_profiles_keyboard(profiles, page, max(1, (total + 9) // 10), search_query)
    )

@dp.callback_query(AdminStates.profiles_view, lambda c: c.data == "admin_profiles_back")
async def admin_profiles_back_handler(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    page = data.get('profiles_page', 0)
    search_query = data.get('profiles_search')
    profiles, total = get_profiles_list(page=page, per_page=10, search=search_query)
    text = TEXTS['MESSAGE_ADMIN_PROFILES']
    await safe_edit_message(
        callback.message,
        text,
        reply_markup=admin_profiles_keyboard(profiles, page, max(1, (total + 9) // 10), search_query)
    )

@dp.message(AdminStates.profiles_search)
async def admin_profiles_search_handler(message: types.Message, state: FSMContext):
    data = await state.get_data()
    search_request_msg_id = data.get("search_request_msg_id")
    if search_request_msg_id:
        try:
            await bot.delete_message(message.chat.id, search_request_msg_id)
        except:
            pass
    try:
        await message.delete()
    except:
        pass
    search_query = message.text.strip()
    await state.update_data(profiles_search=search_query, profiles_page=0)
    await state.set_state(AdminStates.profiles_view)
    profiles, total = get_profiles_list(per_page=10, search=search_query)
    text = TEXTS['MESSAGE_ADMIN_PROFILES']
    msg = await message.answer(
        text,
        reply_markup=admin_profiles_keyboard(profiles, 0, max(1, (total + 9) // 10), search_query)
    )
    data = await state.get_data()
    admin_session_messages = data.get('admin_session_messages', [])
    admin_session_messages.append(msg.message_id)
    await state.update_data(admin_session_messages=admin_session_messages)

@dp.callback_query(lambda c: c.data == "admin_orders")
async def admin_orders_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.orders_view)
    orders, total = get_orders_list(per_page=10)
    text = TEXTS['MESSAGE_ADMIN_ORDERS']
    data = await state.get_data()
    admin_session_messages = data.get('admin_session_messages', [])
    admin_session_messages.append(callback.message.message_id)
    await state.update_data(admin_session_messages=admin_session_messages)
    await safe_edit_message(
        callback.message,
        text,
        reply_markup=admin_orders_keyboard(orders, 0, max(1, (total + 9) // 10))
    )
    await state.update_data(orders_page=0, orders_search=None)

@dp.callback_query(AdminStates.orders_view, lambda c: c.data.startswith("admin_orders_"))
async def admin_orders_actions_handler(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    page = data.get('orders_page', 0)
    search_query = data.get('orders_search')
    if callback.data == "admin_orders_refresh":
        orders, total = get_orders_list(page=page, per_page=10, search=search_query)
        text = TEXTS['MESSAGE_ADMIN_ORDERS']
        await safe_edit_message(
            callback.message,
            text,
            reply_markup=admin_orders_keyboard(orders, page, max(1, (total + 9) // 10), search_query)
        )
    elif callback.data == "admin_orders_search":
        await state.set_state(AdminStates.orders_search)
        msg_id = await safe_edit_message(
            callback.message,
            TEXTS['MESSAGE_ADMIN_SEARCH'],
            reply_markup=admin_back_keyboard("admin_orders_back")
        )
        await state.update_data(search_request_msg_id=msg_id)
    elif callback.data == "admin_orders_clear_search":
        await state.update_data(orders_search=None)
        orders, total = get_orders_list(page=page, per_page=10)
        text = TEXTS['MESSAGE_ADMIN_ORDERS']
        await safe_edit_message(
            callback.message,
            text,
            reply_markup=admin_orders_keyboard(orders, page, max(1, (total + 9) // 10))
        )
    elif callback.data.startswith("admin_orders_prev_") or callback.data.startswith("admin_orders_next_"):
        action = callback.data.split('_')[2]
        new_page = int(callback.data.split('_')[3])
        orders, total = get_orders_list(page=new_page, per_page=10, search=search_query)
        text = TEXTS['MESSAGE_ADMIN_ORDERS']
        await state.update_data(orders_page=new_page)
        await safe_edit_message(
            callback.message,
            text,
            reply_markup=admin_orders_keyboard(orders, new_page, max(1, (total + 9) // 10), search_query)
        )
    elif callback.data.startswith("admin_order_"):
        order_id = callback.data.split('_')[2]
        order_file = BASE_DIR / f'../databases/orders/order_{order_id}.json'
        if order_file.exists():
            with open(order_file, 'r', encoding='utf-8') as f:
                order_data = json.load(f)
            order_text = format_order_for_admin(order_data)
            await safe_edit_message(
                callback.message,
                order_text,
                parse_mode="HTML",
                reply_markup=admin_order_detail_keyboard(order_id, order_data.get('status'))
            )
        else:
            await callback.answer(TEXTS['MESSAGE_ERROR'])

@dp.callback_query(AdminStates.orders_view, lambda c: c.data.startswith("admin_order_"))
async def admin_order_detail_handler(callback: types.CallbackQuery, state: FSMContext):
    order_id = callback.data.split('_')[2]
    order_file = BASE_DIR / f'../databases/orders/order_{order_id}.json'
    if order_file.exists():
        with open(order_file, 'r', encoding='utf-8') as f:
            order_data = json.load(f)
        order_text = format_order_for_admin(order_data)
        await state.set_state(AdminStates.order_detail)
        await safe_edit_message(
            callback.message,
            order_text,
            parse_mode="HTML",
            reply_markup=admin_order_detail_keyboard(order_id, order_data.get('status'))
        )
    else:
        await callback.answer(TEXTS['MESSAGE_ERROR'])

@dp.callback_query(AdminStates.order_detail,
                   lambda c: c.data.startswith("admin_order_cancel_") or c.data.startswith("admin_order_pay_"))
async def admin_order_status_handler(callback: types.CallbackQuery, state: FSMContext):
    action = callback.data.split('_')[2]
    order_id = callback.data.split('_')[3]
    new_status = 'Отменен' if action == 'cancel' else 'Оплачен'
    updated_order = update_order_status(order_id, new_status)
    if updated_order:
        payment_message_id = updated_order.get('payment_message_id')
        if payment_message_id:
            try:
                await bot.delete_message(chat_id=updated_order['chat_id'], message_id=payment_message_id)
            except:
                pass
        builder = InlineKeyboardBuilder()
        builder.row(
            InlineKeyboardButton(text=TEXTS['BUTTON_SUPPORT'], url=f"https://t.me/{SUPPORT_USERNAME.replace('@', '')}"))
        builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_BACK'], callback_data="main_menu"))
        try:
            if new_status == 'Оплачен':
                user_notification = TEXTS['MESSAGE_MARKETPLACE_CONFIRM_PAYMENT_PAID'].format(order_id=order_id)
            else:
                user_notification = TEXTS['MESSAGE_MARKETPLACE_CONFIRM_PAYMENT_CANCELLED'].format(order_id=order_id)
            await bot.send_message(
                updated_order['chat_id'],
                user_notification,
                parse_mode="HTML",
                reply_markup=builder.as_markup()
            )
        except:
            pass
        order_text = format_order_for_admin(updated_order)
        await safe_edit_message(
            callback.message,
            order_text,
            parse_mode="HTML",
            reply_markup=admin_order_detail_keyboard(order_id, new_status)
        )
    else:
        await callback.answer(TEXTS['MESSAGE_ERROR'])

@dp.callback_query(AdminStates.orders_search, lambda c: c.data == "admin_orders_back")
async def admin_orders_back_from_search_handler(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    page = data.get('orders_page', 0)
    search_query = data.get('orders_search')
    orders, total = get_orders_list(page=page, per_page=10, search=search_query)
    text = TEXTS['MESSAGE_ADMIN_ORDERS']
    await state.set_state(AdminStates.orders_view)
    await state.update_data(orders_page=page)
    await safe_edit_message(
        callback.message,
        text,
        reply_markup=admin_orders_keyboard(orders, page, max(1, (total + 9) // 10), search_query)
    )

@dp.callback_query(AdminStates.order_detail, lambda c: c.data == "admin_orders_back")
async def admin_orders_back_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.orders_view)
    data = await state.get_data()
    page = data.get('orders_page', 0)
    search_query = data.get('orders_search')
    orders, total = get_orders_list(page=page, per_page=10, search=search_query)
    text = TEXTS['MESSAGE_ADMIN_ORDERS']
    await safe_edit_message(
        callback.message,
        text,
        reply_markup=admin_orders_keyboard(orders, page, max(1, (total + 9) // 10), search_query)
    )

@dp.message(AdminStates.orders_search)
async def admin_orders_search_handler(message: types.Message, state: FSMContext):
    data = await state.get_data()
    search_request_msg_id = data.get("search_request_msg_id")
    if search_request_msg_id:
        try:
            await bot.delete_message(message.chat.id, search_request_msg_id)
        except:
            pass
    try:
        await message.delete()
    except:
        pass
    search_query = message.text.strip()
    await state.update_data(orders_search=search_query, orders_page=0)
    await state.set_state(AdminStates.orders_view)
    orders, total = get_orders_list(per_page=10, search=search_query)
    text = TEXTS['MESSAGE_ADMIN_ORDERS']
    msg = await message.answer(
        text,
        reply_markup=admin_orders_keyboard(orders, 0, max(1, (total + 9) // 10), search_query)
    )
    data = await state.get_data()
    admin_session_messages = data.get('admin_session_messages', [])
    admin_session_messages.append(msg.message_id)
    await state.update_data(admin_session_messages=admin_session_messages)

@dp.callback_query(lambda c: c.data == "main_menu")
async def main_menu_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit_message(callback.message, TEXTS['MESSAGE_MENU'], parse_mode="HTML", reply_markup=main_keyboard())

@dp.callback_query(lambda c: c.data == "marketplace")
async def marketplace_handler(callback: types.CallbackQuery, state: FSMContext):
    username = f"@{callback.from_user.username}" if callback.from_user.username else f"ID{callback.from_user.id}"
    logging.info(f"Обработка команды маркетплейс от пользователя {username}")
    await state.set_state(MarketplaceStates.select_city)
    await safe_edit_message(callback.message, TEXTS['MESSAGE_MARKETPLACE_SELECT_CITY'], parse_mode="HTML",
                            reply_markup=generate_marketplace_cities_keyboard())

@dp.callback_query(MarketplaceStates.select_city, lambda c: c.data.startswith(
    ("marketplace_city_", "marketplace_prev_city_", "marketplace_next_city_")))
async def marketplace_city_handler(callback: types.CallbackQuery, state: FSMContext):
    data = callback.data.split("_")
    if callback.data.startswith("marketplace_city_"):
        city = callback.data.split("_", 2)[2]
        username = f"@{callback.from_user.username}" if callback.from_user.username else f"ID{callback.from_user.id}"
        logging.info(f"Пользователь {username} выбрал город: {city}")
        city_data = CITIES.get(city, {})
        markup = city_data.get("markup", 0)
        await state.update_data(city=city, markup=markup)
        await state.set_state(MarketplaceStates.select_product)
        await safe_edit_message(callback.message, TEXTS['MESSAGE_MARKETPLACE_SELECT_PRODUCT'], parse_mode="HTML",
                                reply_markup=generate_marketplace_products_keyboard(city))
    elif callback.data.startswith("marketplace_prev_city_") or callback.data.startswith("marketplace_next_city_"):
        page = int(data[3])
        new_page = page - 1 if data[1] == "prev" else page + 1
        await safe_edit_message(callback.message, TEXTS['MESSAGE_MARKETPLACE_SELECT_CITY'], parse_mode="HTML",
                                reply_markup=generate_marketplace_cities_keyboard(new_page))

@dp.callback_query(MarketplaceStates.select_city, lambda c: c.data == "marketplace_search_city")
async def marketplace_search_city_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(MarketplaceStates.search_city)
    msg_id = await safe_edit_message(callback.message, TEXTS['MESSAGE_MARKETPLACE_ENTER_CITY'], parse_mode="HTML",
                                     reply_markup=back_keyboard("marketplace_back_to_cities"))
    await state.update_data(search_request_msg_id=msg_id)

@dp.message(MarketplaceStates.search_city)
async def marketplace_process_city_search(message: types.Message, state: FSMContext):
    data = await state.get_data()
    search_request_msg_id = data.get("search_request_msg_id")
    if search_request_msg_id:
        try:
            await bot.delete_message(message.chat.id, search_request_msg_id)
        except:
            pass
    try:
        await message.delete()
    except:
        pass
    search_query = message.text.strip().lower()
    username = f"@{message.from_user.username}" if message.from_user.username else f"ID{message.from_user.id}"
    logging.info(f"Пользователь {username} ввел город: {search_query}")
    found_cities = [city for city in CITIES.keys() if search_query in city.lower()]
    if not found_cities:
        msg = await message.answer(TEXTS['MESSAGE_MARKETPLACE_NOT_FOUND_CITY'], parse_mode="HTML",
                                   reply_markup=back_keyboard("marketplace_back_to_cities"))
        await state.update_data(search_request_msg_id=msg.message_id)
        return
    builder = InlineKeyboardBuilder()
    for city in found_cities:
        builder.add(InlineKeyboardButton(text=city, callback_data=f"marketplace_city_{city}"))
    builder.adjust(1)
    builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_BACK'], callback_data="marketplace_back_to_cities"))
    await state.set_state(MarketplaceStates.select_city)
    await message.answer(TEXTS['MESSAGE_MARKETPLACE_ENTER_RESULTS'], parse_mode="HTML",
                         reply_markup=builder.as_markup())

@dp.callback_query(MarketplaceStates.select_product, lambda c: c.data.startswith("marketplace_product_"))
async def marketplace_product_handler(callback: types.CallbackQuery, state: FSMContext):
    product_id = callback.data.split("_")[2]
    product_name = PRODUCTS.get(product_id, {}).get("name", "неизвестный товар")
    username = f"@{callback.from_user.username}" if callback.from_user.username else f"ID{callback.from_user.id}"
    logging.info(f"Пользователь {username} выбрал товар: {product_name}")
    data = await state.get_data()
    markup = data.get("markup", 0)
    await state.update_data(product_id=product_id)
    await state.set_state(MarketplaceStates.select_price)
    await safe_edit_message(callback.message, TEXTS['MESSAGE_MARKETPLACE_SELECT_WEIGHT'], parse_mode="HTML",
                            reply_markup=generate_marketplace_prices_keyboard(data['city'], product_id, markup))

@dp.callback_query(MarketplaceStates.select_price, lambda c: c.data.startswith("marketplace_price_"))
async def marketplace_price_handler(callback: types.CallbackQuery, state: FSMContext):
    price_key = callback.data.split("_")[2]
    username = f"@{callback.from_user.username}" if callback.from_user.username else f"ID{callback.from_user.id}"
    logging.info(f"Пользователь {username} выбрал вес: {price_key}")
    data = await state.get_data()
    markup = data.get("markup", 0)
    product = PRODUCTS[data["product_id"]]
    base_price = product["prices"][price_key]
    final_price = apply_markup(base_price, markup)
    await state.update_data(price_key=price_key, base_price=base_price, final_price=final_price)
    await state.set_state(MarketplaceStates.select_type)
    await safe_edit_message(callback.message, TEXTS['MESSAGE_MARKETPLACE_SELECT_TYPE'], parse_mode="HTML",
                            reply_markup=generate_marketplace_types_keyboard(data['city'], data['product_id']))

@dp.callback_query(MarketplaceStates.select_type, lambda c: c.data.startswith("marketplace_type_"))
async def marketplace_type_handler(callback: types.CallbackQuery, state: FSMContext):
    type_idx = int(callback.data.split("_")[2])
    if type_idx < 0 or type_idx >= len(TYPES):
        await callback.answer(TEXTS['MESSAGE_ERROR'])
        return
    product_type = TYPES[type_idx]
    username = f"@{callback.from_user.username}" if callback.from_user.username else f"ID{callback.from_user.id}"
    logging.info(f"Пользователь {username} выбрал тип: {product_type}")
    await state.update_data(product_type=product_type)
    await state.set_state(MarketplaceStates.select_district)
    data = await state.get_data()
    city = data.get("city", "")
    await safe_edit_message(callback.message, TEXTS['MESSAGE_MARKETPLACE_SELECT_DISTRICT'], parse_mode="HTML",
                            reply_markup=generate_marketplace_districts_keyboard(city, 0))

@dp.callback_query(MarketplaceStates.select_district, lambda c: c.data.startswith(
    ("marketplace_district_", "marketplace_prev_district_", "marketplace_next_district_")))
async def marketplace_district_handler(callback: types.CallbackQuery, state: FSMContext):
    data = callback.data.split("_")
    if callback.data.startswith("marketplace_district_"):
        district = callback.data.split("_", 2)[2]
        if district == "no_district":
            district = TEXTS['MESSAGE_MARKETPLACE_NOT_DISTRICT']
        username = f"@{callback.from_user.username}" if callback.from_user.username else f"ID{callback.from_user.id}"
        logging.info(f"Пользователь {username} выбрал район: {district}")
        await state.update_data(district=district)
        await state.set_state(MarketplaceStates.confirm_order)
        data = await state.get_data()
        product = PRODUCTS[data["product_id"]]
        formatted_price = format_price(data["final_price"])
        text = TEXTS['MESSAGE_MARKETPLACE_CONFIRM_ORDER'].format(
            city=data["city"], product=product["name"], weight=data["price_key"],
            type=data["product_type"], district=district, price=formatted_price
        )
        await safe_edit_message(callback.message, text, parse_mode="HTML",
                                reply_markup=generate_confirm_order_keyboard())
    elif callback.data.startswith("marketplace_prev_district_") or callback.data.startswith(
            "marketplace_next_district_"):
        page = int(data[3])
        new_page = page - 1 if data[1] == "prev" else page + 1
        data_state = await state.get_data()
        city = data_state.get("city", "")
        await safe_edit_message(callback.message, TEXTS['MESSAGE_MARKETPLACE_SELECT_DISTRICT'], parse_mode="HTML",
                                reply_markup=generate_marketplace_districts_keyboard(city, new_page))

@dp.callback_query(MarketplaceStates.select_district, lambda c: c.data == "marketplace_search_district")
async def marketplace_search_district_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(MarketplaceStates.search_district)
    msg_id = await safe_edit_message(callback.message, TEXTS['MESSAGE_MARKETPLACE_ENTER_DISTRICT'], parse_mode="HTML",
                                     reply_markup=back_keyboard("marketplace_back_to_districts"))
    await state.update_data(search_request_msg_id=msg_id)

@dp.message(MarketplaceStates.search_district)
async def marketplace_process_district_search(message: types.Message, state: FSMContext):
    data = await state.get_data()
    search_request_msg_id = data.get("search_request_msg_id")
    if search_request_msg_id:
        try:
            await bot.delete_message(message.chat.id, search_request_msg_id)
        except:
            pass
    try:
        await message.delete()
    except:
        pass
    search_query = message.text.strip().lower()
    username = f"@{message.from_user.username}" if message.from_user.username else f"ID{message.from_user.id}"
    logging.info(f"Пользователь {username} ввел район: {search_query}")
    data = await state.get_data()
    city = data.get("city", "")
    if city not in CITIES:
        msg = await message.answer(TEXTS['MESSAGE_MARKETPLACE_NOT_FOUND_CITY'], parse_mode="HTML",
                                   reply_markup=back_keyboard("marketplace_back_to_districts"))
        await state.update_data(search_request_msg_id=msg.message_id)
        return
    districts = get_available_districts(city)
    found_districts = [district for district in districts if search_query in district.lower()]
    if not found_districts:
        msg = await message.answer(TEXTS['MESSAGE_MARKETPLACE_NOT_FOUND_DISTRICT'], parse_mode="HTML",
                                   reply_markup=back_keyboard("marketplace_back_to_districts"))
        await state.update_data(search_request_msg_id=msg.message_id)
        return
    builder = InlineKeyboardBuilder()
    for district in found_districts:
        builder.add(InlineKeyboardButton(text=district, callback_data=f"marketplace_district_{district}"))
    builder.adjust(1)
    builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_BACK'], callback_data="marketplace_back_to_districts"))
    await state.set_state(MarketplaceStates.select_district)
    await message.answer(TEXTS['MESSAGE_MARKETPLACE_ENTER_RESULTS'], parse_mode="HTML",
                         reply_markup=builder.as_markup())

@dp.callback_query(MarketplaceStates.confirm_order, lambda c: c.data == "marketplace_confirm_order")
async def marketplace_confirm_order_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(MarketplaceStates.select_payment)
    await safe_edit_message(callback.message, TEXTS['MESSAGE_MARKETPLACE_SELECT_PAYMENT'], parse_mode="HTML",
                            reply_markup=generate_marketplace_payment_keyboard())

@dp.callback_query(MarketplaceStates.confirm_order, lambda c: c.data == "marketplace_cancel_order")
async def marketplace_cancel_order_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await main_menu_handler(callback, state)

@dp.callback_query(MarketplaceStates.select_payment, lambda c: c.data.startswith("marketplace_payment_"))
async def marketplace_payment_handler(callback: types.CallbackQuery, state: FSMContext):
    payment_method = callback.data.split("_", 2)[2]
    username = f"@{callback.from_user.username}" if callback.from_user.username else f"ID{callback.from_user.id}"
    logging.info(f"Пользователь {username} выбрал способ оплаты: {payment_method}")
    data = await state.get_data()
    await rates_updater.load_rates()
    wallets_path = BASE_DIR / '../wallets.json'
    wallets = load_data(wallets_path)
    available_wallets = wallets.get(payment_method, [])
    if not available_wallets:
        await callback.answer(TEXTS['MESSAGE_MARKETPLACE_NOT_WALLET'], show_alert=True)
        return
    wallet_address = random.choice(available_wallets)
    order_id = str(random.randint(1000000000, 9999999999))
    product = PRODUCTS[data["product_id"]]
    rate = rates_updater.rates.get(payment_method, 1)
    if not isinstance(rate, (int, float)) or rate <= 0:
        rate = 1
    payment_amount = round(data["final_price"] / rate, 8)
    currency = PAYMENT.get(payment_method, "RUB")
    order_data = {
        'order_id': order_id, 'user_id': callback.from_user.id, 'username': callback.from_user.username,
        'city': data["city"], 'product': product["name"], 'weight': data["price_key"],
        'type': data["product_type"], 'district': data.get("district", TEXTS['MESSAGE_MARKETPLACE_NOT_DISTRICT']),
        'price': data["final_price"], 'payment_method': payment_method, 'payment_amount': payment_amount,
        'wallet_address': wallet_address, 'status': 'Оформлен', 'date': datetime.now().isoformat(),
        'chat_id': callback.message.chat.id
    }
    text = TEXTS['MESSAGE_MARKETPLACE_CONFIRM_PAYMENT'].format(
        order_id=order_id, city=data["city"], product=product["name"], weight=data["price_key"],
        type=data["product_type"], district=data.get("district", TEXTS['MESSAGE_MARKETPLACE_NOT_DISTRICT']),
        price=format_price(data["final_price"]), payment_method=payment_method, payment_price=payment_amount,
        currency=currency, payment_details=wallet_address
    )
    try:
        await callback.message.delete()
    except:
        pass
    qr = qrcode.QRCode(version=1, error_correction=qrcode.constants.ERROR_CORRECT_L, box_size=10, border=4)
    qr.add_data(wallet_address)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    img_buffer = BytesIO()
    img.save(img_buffer)
    img_buffer.seek(0)
    msg = await callback.message.answer_photo(
        photo=types.BufferedInputFile(img_buffer.read(), filename="payment_qr.png"),
        caption=text, parse_mode="HTML", reply_markup=generate_payment_cancel_keyboard()
    )
    order_data['payment_message_id'] = msg.message_id
    save_order(order_data)
    await state.update_data(
        payment_message_id=msg.message_id, order_id=order_id, wallet_address=wallet_address,
        payment_method=payment_method
    )
    await state.set_state(MarketplaceStates.payment_details)

@dp.callback_query(MarketplaceStates.payment_details, lambda c: c.data == "marketplace_cancel_payment")
async def marketplace_cancel_payment_handler(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    order_id = data.get("order_id")
    payment_message_id = data.get("payment_message_id")
    if payment_message_id:
        try:
            await bot.delete_message(chat_id=callback.message.chat.id, message_id=payment_message_id)
        except:
            pass
    orders_dir = BASE_DIR / '../databases/orders'
    order_file = orders_dir / f'order_{order_id}.json'
    updated_order = update_order_status(order_id, 'Отменен', order_file)
    if updated_order:
        cancellation_text = TEXTS['MESSAGE_MARKETPLACE_CONFIRM_PAYMENT_CANCELLED'].format(order_id=order_id)
        builder = InlineKeyboardBuilder()
        builder.row(
            InlineKeyboardButton(text=TEXTS['BUTTON_SUPPORT'], url=f"https://t.me/{SUPPORT_USERNAME.replace('@', '')}"))
        builder.row(InlineKeyboardButton(text=TEXTS['BUTTON_BACK'], callback_data="main_menu"))
        await callback.message.answer(
            cancellation_text,
            parse_mode="HTML",
            reply_markup=builder.as_markup()
        )
    else:
        await callback.answer(TEXTS['MESSAGE_ERROR'])
    await state.clear()

@dp.callback_query(lambda c: c.data == "opt")
async def opt_handler(callback: types.CallbackQuery, state: FSMContext):
    username = f"@{callback.from_user.username}" if callback.from_user.username else f"ID{callback.from_user.id}"
    logging.info(f"Обработка команды опт от пользователя {username}")
    await state.clear()
    await safe_edit_message(callback.message, TEXTS['MESSAGE_OPT'], parse_mode="HTML", reply_markup=admin_keyboard())

@dp.callback_query(lambda c: c.data == "work")
async def work_handler(callback: types.CallbackQuery, state: FSMContext):
    username = f"@{callback.from_user.username}" if callback.from_user.username else f"ID{callback.from_user.id}"
    logging.info(f"Обработка команды работа от пользователя {username}")
    await state.set_state(WorkState.work_form)
    msg_id = await safe_edit_message(callback.message, TEXTS['MESSAGE_WORK'], parse_mode="HTML",
                                     reply_markup=back_keyboard("main_menu"))
    await state.update_data(work_request_msg_id=msg_id)

@dp.message(WorkState.work_form)
async def process_work_form(message: types.Message, state: FSMContext):
    data = await state.get_data()
    work_request_msg_id = data.get("work_request_msg_id")
    try:
        if work_request_msg_id:
            await bot.delete_message(message.chat.id, work_request_msg_id)
        await message.delete()
    except:
        pass
    text = message.text.strip().lower()
    age = None
    for word in re.split(r'\W+', text):
        if word.isdigit():
            age_candidate = int(word)
            if 18 <= age_candidate <= 100:
                age = age_candidate
                break
    city_name = None
    for available_city in CITIES:
        if available_city.lower() in text:
            city_name = available_city
            break
    username = f"@{message.from_user.username}" if message.from_user.username else f"ID{message.from_user.id}"
    if age is None or not city_name:
        logging.info(f"Пользователь {username} заполнил анкету: город: {city_name} и возраст: {age} – отклонено")
        await message.answer(TEXTS['MESSAGE_WORK_REJECTED'], parse_mode="HTML", reply_markup=back_keyboard("main_menu"))
    else:
        logging.info(f"Пользователь {username} заполнил анкету: город: {city_name} и возраст: {age} – принято")
        response_text = TEXTS['MESSAGE_WORK_ACCEPTED'].format(work_admin=ADMIN_USERNAME, age=age, city=city_name)
        await message.answer(response_text, parse_mode="HTML", reply_markup=admin_keyboard())
    await state.clear()

@dp.callback_query(lambda c: c.data == "rules")
async def rules_handler(callback: types.CallbackQuery):
    username = f"@{callback.from_user.username}" if callback.from_user.username else f"ID{callback.from_user.id}"
    logging.info(f"Обработка команды правила от пользователя {username}")
    await safe_edit_message(callback.message, TEXTS['MESSAGE_RULES'], parse_mode="HTML",
                            reply_markup=back_keyboard("main_menu"))

@dp.callback_query(lambda c: c.data == "profile")
async def profile_handler(callback: types.CallbackQuery):
    user = callback.from_user
    user_data = load_user_data(user.id)
    paid_orders = []
    for order_id in user_data.get('orders', []):
        if get_order_status(order_id) == 'Оплачен':
            paid_orders.append(order_id)
    total_orders = len(paid_orders)
    total_spent = sum(get_order_price(order_id) for order_id in paid_orders)
    profile_text = TEXTS['MESSAGE_PROFILE'].format(
        user_id=user.id,
        name=user.full_name,
        orders_count=total_orders,
        total_spent=format_price(total_spent),
        discount=0
    )
    await safe_edit_message(callback.message, profile_text, parse_mode="HTML", reply_markup=profile_keyboard())

@dp.callback_query(lambda c: c.data == "history")
async def history_handler(callback: types.CallbackQuery):
    username = f"@{callback.from_user.username}" if callback.from_user.username else f"ID{callback.from_user.id}"
    logging.info(f"Обработка команды история от пользователя {username}")
    user_data = load_user_data(callback.from_user.id)
    orders = user_data.get('orders', [])
    text = TEXTS["MESSAGE_ORDERS"]
    if not orders:
        text += TEXTS['MESSAGE_ORDERS_EMPTY']
    else:
        orders_dir = BASE_DIR / '../databases/orders'
        for order_id in orders[-10:]:
            order_file = orders_dir / f"order_{order_id}.json"
            if not order_file.exists():
                user_data['orders'].remove(order_id)
                save_user_data(callback.from_user.id, user_data)
                continue
            with open(order_file, 'r', encoding='utf-8') as f:
                order_data = json.load(f)
            order_date = datetime.fromisoformat(order_data['date']).strftime('%d.%m.%Y %H:%M')
            text += TEXTS['MESSAGE_ORDER_ITEM'].format(
                order_id=order_data['order_id'],
                city=order_data['city'],
                product=order_data['product'],
                weight=order_data['weight'],
                type=order_data['type'],
                district=order_data.get('district', TEXTS['MESSAGE_MARKETPLACE_NOT_DISTRICT']),
                price=format_price(order_data['price']),
                payment_method=order_data['payment_method'],
                payment_price=order_data['payment_amount'],
                currency=PAYMENT.get(order_data['payment_method'], "RUB"),
                payment_details=order_data['wallet_address'],
                status=order_data['status'],
                date=order_date
            )
    await safe_edit_message(callback.message, text, parse_mode="HTML", reply_markup=history_keyboard())

@dp.callback_query(lambda c: c.data == "client_chat")
async def client_chat_handler(callback: types.CallbackQuery):
    username = f"@{callback.from_user.username}" if callback.from_user.username else f"ID{callback.from_user.id}"
    logging.info(f"Обработка команды чат клиентов от пользователя {username}")
    await safe_edit_message(callback.message, TEXTS['MESSAGE_CHAT'], parse_mode="HTML",
                            reply_markup=back_keyboard("main_menu"))

@dp.callback_query(lambda c: c.data == "marketplace_back_to_main")
async def marketplace_back_to_main_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await main_menu_handler(callback, state)

@dp.callback_query(lambda c: c.data == "marketplace_back_to_cities")
async def marketplace_back_to_cities_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(MarketplaceStates.select_city)
    await safe_edit_message(callback.message, TEXTS['MESSAGE_MARKETPLACE_SELECT_CITY'], parse_mode="HTML",
                            reply_markup=generate_marketplace_cities_keyboard())

@dp.callback_query(lambda c: c.data == "marketplace_back_to_products")
async def marketplace_back_to_products_handler(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    await state.set_state(MarketplaceStates.select_product)
    await safe_edit_message(callback.message, TEXTS['MESSAGE_MARKETPLACE_SELECT_PRODUCT'], parse_mode="HTML",
                            reply_markup=generate_marketplace_products_keyboard(data['city']))

@dp.callback_query(lambda c: c.data == "marketplace_back_to_prices")
async def marketplace_back_to_prices_handler(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    markup = data.get("markup", 0)
    await state.set_state(MarketplaceStates.select_price)
    await safe_edit_message(callback.message, TEXTS['MESSAGE_MARKETPLACE_SELECT_WEIGHT'], parse_mode="HTML",
                            reply_markup=generate_marketplace_prices_keyboard(data['city'], data['product_id'], markup))

@dp.callback_query(lambda c: c.data == "marketplace_back_to_types")
async def marketplace_back_to_types_handler(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    await state.set_state(MarketplaceStates.select_type)
    await safe_edit_message(callback.message, TEXTS['MESSAGE_MARKETPLACE_SELECT_TYPE'], parse_mode="HTML",
                            reply_markup=generate_marketplace_types_keyboard(data['city'], data['product_id']))

@dp.callback_query(lambda c: c.data == "marketplace_back_to_districts")
async def marketplace_back_to_districts_handler(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    await state.set_state(MarketplaceStates.select_district)
    await safe_edit_message(callback.message, TEXTS['MESSAGE_MARKETPLACE_SELECT_DISTRICT'], parse_mode="HTML",
                            reply_markup=generate_marketplace_districts_keyboard(data['city'], 0))

@dp.callback_query(MarketplaceStates.select_payment, lambda c: c.data == "marketplace_back_to_confirm")
async def marketplace_back_to_confirm_handler(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    product = PRODUCTS[data["product_id"]]
    formatted_price = format_price(data["final_price"])
    text = TEXTS['MESSAGE_MARKETPLACE_CONFIRM_ORDER'].format(
        city=data["city"], product=product["name"], weight=data["price_key"],
        type=data["product_type"], district=data.get("district", TEXTS['MESSAGE_MARKETPLACE_NOT_DISTRICT']),
        price=formatted_price
    )
    await state.set_state(MarketplaceStates.confirm_order)
    await safe_edit_message(callback.message, text, parse_mode="HTML", reply_markup=generate_confirm_order_keyboard())

@dp.callback_query(MarketplaceStates.confirm_order, lambda c: c.data == "marketplace_back_to_districts_confirm")
async def marketplace_back_to_districts_confirm_handler(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    await state.set_state(MarketplaceStates.select_district)
    await safe_edit_message(callback.message, TEXTS['MESSAGE_MARKETPLACE_SELECT_DISTRICT'], parse_mode="HTML",
                            reply_markup=generate_marketplace_districts_keyboard(data['city'], 0))

async def shutdown():
    global is_running
    is_running = False
    try:
        await dp.storage.close()
        await bot.session.close()
    except:
        pass

async def main():
    global BOT_NAME, is_running
    bot_info = await bot.get_me()
    BOT_NAME = bot_info.first_name
    bot_username = bot_info.username
    logging.info(f"Бот {BOT_NAME} {bot_username} запущен")
    rates_task = asyncio.create_task(rates_updater.run_periodic_update())
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    except:
        pass
    finally:
        is_running = False
        rates_task.cancel()
        try:
            await asyncio.wait_for(rates_task, timeout=2.0)
        except:
            pass
        logging.info(f"Бот {BOT_NAME} {bot_username} остановлен")
        await shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
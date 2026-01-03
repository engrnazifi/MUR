
# bot.py  (Merged final with language persistence fixes - bug fixed + Fulani added)
import telebot
from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
import sqlite3


# ====== DATABASE CONNECTION ======
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "main.db")
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.row_factory = sqlite3.Row

# small globals
admin_states = {}
last_menu_msg = {}
last_category_msg = {}
last_allfilms_msg = {}
allfilms_sessions = {}
cart_sessions = {}
series_sessions = {}
# =========================
# DATABASE TABLES (SAFE)
# =========================

# -------- MOVIES --------
conn.execute("""
CREATE TABLE IF NOT EXISTS movies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    price INTEGER,
    file_id TEXT,
    file_name TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    channel_msg_id INTEGER,
    channel_username TEXT
)
""")

# -------- ORDERS --------
conn.execute("""
CREATE TABLE IF NOT EXISTS orders (
    id TEXT PRIMARY KEY,              -- tx_ref / order_id
    user_id INTEGER,
    movie_id INTEGER,
    amount INTEGER,
    paid INTEGER DEFAULT 0,            -- 0 = unpaid, 1 = paid
    pay_ref TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
""")

# -------- ORDER ITEMS (MULTI-MOVIE SUPPORT) --------
conn.execute("""
CREATE TABLE IF NOT EXISTS order_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT,
    movie_id INTEGER,
    price INTEGER
)
""")

# -------- WEEKLY MOVIES --------
conn.execute("""
CREATE TABLE IF NOT EXISTS weekly (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    poster_file_id TEXT,
    items TEXT,                        -- JSON list
    file_name TEXT,
    channel_msg_id INTEGER,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
""")

# -------- CART --------
conn.execute("""
CREATE TABLE IF NOT EXISTS cart (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    movie_id INTEGER,
    price INTEGER,
    added_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
""")

# -------- REFERRALS --------
conn.execute("""
CREATE TABLE IF NOT EXISTS referrals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    referrer_id INTEGER,
    referred_id INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    reward_granted INTEGER DEFAULT 0
)
""")

conn.execute("""
CREATE TABLE IF NOT EXISTS referral_credits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    referrer_id INTEGER,
    amount INTEGER,
    used INTEGER DEFAULT 0,
    granted_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
""")

# -------- USER PREFERENCES --------
conn.execute("""
CREATE TABLE IF NOT EXISTS user_prefs (
    user_id INTEGER PRIMARY KEY,
    lang TEXT DEFAULT 'ha'
)
""")

# -------- USER LIBRARY (OWNERSHIP) --------
conn.execute("""
CREATE TABLE IF NOT EXISTS user_library (
    user_id INTEGER NOT NULL,
    movie_id INTEGER NOT NULL,
    acquired_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, movie_id)
)
""")

# -------- BUY ALL TOKENS --------
conn.execute("""
CREATE TABLE IF NOT EXISTS buyall_tokens (
    token TEXT PRIMARY KEY,
    ids TEXT                           -- JSON list of movie_ids
)
""")

# -------- USER MOVIES (RESEND HISTORY) --------
conn.execute("""
CREATE TABLE IF NOT EXISTS user_movies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    movie_id INTEGER,
    order_id TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    resend_count INTEGER DEFAULT 0
)
""")

# =====================
# SERIES TABLES
# =====================

conn.execute("""
CREATE TABLE IF NOT EXISTS series (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    file_name TEXT,
    file_id TEXT,
    price INTEGER,
    poster_file_id TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    channel_msg_id INTEGER,
    channel_username TEXT
)
""")

conn.execute("""
CREATE TABLE IF NOT EXISTS series_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    series_id INTEGER,
    movie_id INTEGER,
    file_id TEXT,
    title TEXT,
    order_id TEXT,
    Price INTEGER DEFAULT 0,
    channel_msg_id INTEGER,
    channel_username TEXT,
    file_name TEXT
)
""")

# =====================
# FEEDBACK
# =====================

conn.execute("""
CREATE TABLE IF NOT EXISTS feedbacks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL UNIQUE,     -- tx_ref
    user_id INTEGER NOT NULL,
    mood TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
""")
conn.execute("""
CREATE TABLE IF NOT EXISTS resend_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    used_at TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
""")
# =====================
# HAUSA SERIES
# =====================

conn.execute("""
CREATE TABLE IF NOT EXISTS hausa_series (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    file_name TEXT,
    file_id TEXT,
    price INTEGER,
    series_id TEXT,
    poster_file_id TEXT,
    channel_msg_id INTEGER,
    channel_username TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
""")

conn.execute("""
CREATE TABLE IF NOT EXISTS hausa_series_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hausa_series_id INTEGER,
    movie_id INTEGER,
    price INTEGER,
    file_id TEXT,
    title TEXT,
    order_id TEXT,
    series_id INTEGER,
    channel_msg_id INTEGER,
    channel_username TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    file_name TEXT
)
""")
conn.execute("""
CREATE TABLE IF NOT EXISTS admin_controls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    admin_id INTEGER UNIQUE,
    sendmovie_enabled INTEGER DEFAULT 0,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
""")
conn.commit()


import uuid
import re
import json
import requests
import traceback
import random
import difflib
from datetime import datetime, timedelta
import urllib.parse
admin_states = {}
# --- Admins configuration ---
ADMINS = [6210912739, 5009954635] 

  # add more admin IDs here
# ========= CONFIG =========
import os
from datetime import datetime

BOT_TOKEN = os.getenv("BOT_TOKEN")

BOT_MODE = os.getenv("BOT_MODE", "polling")

ADMIN_ID = 6210912739
OTP_ADMIN_ID = 6603268127


BOT_USERNAME = "Aslamtv2bot"
CHANNEL = "@yayanebroo"

# Flutterwave
FLW_PUBLIC_KEY = os.getenv("FLW_PUBLIC_KEY")
FLW_SECRET_KEY = os.getenv("FLW_SECRET_KEY")
FLW_WEBHOOK_SECRET = os.getenv("FLW_WEBHOOK_SECRET")
FLW_REDIRECT_URL = os.getenv("FLW_REDIRECT_URL")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")

# === PAYMENTS / STORAGE ===
PAYMENT_NOTIFY_GROUP = -1003553575069
STORAGE_CHANNEL = -1003478646839
SEND_ADMIN_PAYMENT_NOTIF = False

FLW_BASE = "https://api.flutterwave.com/v3"
PAYSTACK_SECRET = None
ADMIN_USERNAME = "Aslamtv1"

# ========= IMPORTS =========
import requests
import telebot
from flask import Flask, request
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# ========= BOT =========
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# ========= FLASK =========
app = Flask(__name__)

# ========= FLUTTERWAVE PAYMENT =========
def create_flutterwave_payment(user_id, order_id, amount, title):
    if not FLW_SECRET_KEY or not FLW_REDIRECT_URL:
        print("❌ Flutterwave env missing")
        return None

    headers = {
        "Authorization": f"Bearer {FLW_SECRET_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "tx_ref": str(order_id),
        "amount": int(amount),
        "currency": "NGN",
        "redirect_url": FLW_REDIRECT_URL,
        "customer": {
            "email": f"user{user_id}@telegram.com",
            "name": f"TG User {user_id}"
        },
        "customizations": {
            "title": title[:50],
            "description": f"Order {order_id}"
        }
    }

    try:
        r = requests.post(
            f"{FLW_BASE}/payments",
            json=payload,
            headers=headers,
            timeout=30
        )

        data = r.json()

        if r.status_code != 200 or data.get("status") != "success":
            print("❌ Flutterwave error:", data)
            return None

        return data["data"]["link"]

    except Exception as e:
        print("❌ create_flutterwave_payment error:", e)
        return None

# ========= HOME / KEEP ALIVE =========
@app.route("/")
def home():
    return "OK", 200

# ========= CALLBACK PAGE =========
@app.route("/flutterwave-callback", methods=["GET"])
def flutterwave_callback():
    return """
    <html>
    <head>
        <title>Payment Successful</title>
        <meta http-equiv="refresh" content="5;url=https://t.me/Aslamtv2bot">
    </head>
    <body style="font-family: Arial; text-align: center; padding-top: 150px; font-size: 22px;">
    
        <h2>✅ Payment Successful</h2>
        <p>An tabbatar da biyan ka.</p>
        <p>Kashe browser ka koma telegram, SWITCH OFF YOUR BROWSER AND GO BACK TO TELEGRAM</p>
        <a href="https://t.me/Aslamtv2bot">Komawa Telegram yanzu</a>
    </body>
    </html>
    """
# ========= FEEDBACK =========
def send_feedback_prompt(user_id, order_id):
    exists = conn.execute(
        "SELECT 1 FROM feedbacks WHERE order_id=?",
        (order_id,)
    ).fetchone()
    if exists:
        return

    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("😁 Very good", callback_data=f"feedback:very:{order_id}"),
        InlineKeyboardButton("🙂 Good", callback_data=f"feedback:good:{order_id}")
    )
    kb.add(
        InlineKeyboardButton("😓 Not sure", callback_data=f"feedback:neutral:{order_id}"),
        InlineKeyboardButton("😠 Angry", callback_data=f"feedback:angry:{order_id}")
    )

    bot.send_message(
        user_id,
        "Ina fatan ka ji daɗin siyayya 🥰\nDan Allah ka zaɓi yadda kake ji  yanzu👇",
        reply_markup=kb
    )

# ========= WEBHOOK =========
@app.route("/webhook", methods=["POST"])
def flutterwave_webhook():

    print("🔔 WEBHOOK RECEIVED")

    signature = request.headers.get("verif-hash")
    if not signature:
        print("❌ Missing verif-hash")
        return "Missing signature", 401

    if signature != FLW_WEBHOOK_SECRET:
        print("❌ Invalid signature:", signature)
        return "Invalid signature", 401

    payload = request.json or {}
    data = payload.get("data", {})
    status = (data.get("status") or "").lower()

    if status not in ("successful", "success"):
        return "Ignored", 200

    order_id = data.get("tx_ref")
    paid_amount = int(float(data.get("amount", 0)))
    currency = data.get("currency")

    row = conn.execute(
        "SELECT user_id, movie_id, amount, paid FROM orders WHERE id=?",
        (order_id,)
    ).fetchone()

    if not row:
        return "Order not found", 200

    user_id, movie_id, expected_amount, paid = row

    if paid == 1:
        return "Already processed", 200

    if paid_amount != expected_amount or currency != "NGN":
        return "Wrong payment", 200

    conn.execute("UPDATE orders SET paid=1 WHERE id=?", (order_id,))
    conn.commit()

    # USER BUTTON
    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("⬇️ DOWNLOAD MOVIES", callback_data=f"deliver:{order_id}")
    )

    bot.send_message(
        user_id,
        f"""🎉 <b>We recieve your payment successfully!</b>

🧾 Order ID: <code>{order_id}</code>
💳 Amount: ₦{paid_amount}

Danna ƙasa domin karɓa:""",
        parse_mode="HTML",
        reply_markup=kb
    )

    # ===== GROUP NOTIFICATION (GYARA TA NAN KAWAI) =====
    if PAYMENT_NOTIFY_GROUP:
        movie_count = conn.execute(
            "SELECT COUNT(*) FROM order_items WHERE order_id=?",
            (order_id,)
        ).fetchone()[0]

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        bot.send_message(
            PAYMENT_NOTIFY_GROUP,
            f"""✅ <b>NEW PAYMENT RECEIVED</b>

👤 User ID: <code>{user_id}</code>
📦 Movies: {movie_count}
🧾 Order ID: <code>{order_id}</code>
💰 Amount: ₦{paid_amount}
⏰ Time: {now}""",
            parse_mode="HTML"
        )

    print("🚀 WEBHOOK DONE:", order_id)
    return "OK", 200

@app.route("/telegram", methods=["POST"])
def telegram_webhook():
    update = telebot.types.Update.de_json(
        request.stream.read().decode("utf-8")
    )
    bot.process_new_updates([update])
    return "OK", 200
PER_PAGE = 5
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

def paginate(items, per_page):
    pages = []
    for i in range(0, len(items), per_page):
        pages.append(items[i:i + per_page])
    return pages


def send_allfilms_page(uid, page_index):
    sess = allfilms_sessions.get(uid)
    if not sess:
        return

    pages = sess["pages"]
    if page_index < 0 or page_index >= len(pages):
        return

    sess["index"] = page_index
    rows = pages[page_index]

    # ===== TEXT (HTML SAFE) =====
    text = "<b>🎬 All films</b>\n\n"

    for r in rows:
        title = str(r[1]).replace("<", "").replace(">", "")
        price = r[2]
        text += f"🎬<b>{title}</b>\n💵 ₦{price}\n\n"

    # ===== BUTTONS =====
    kb = InlineKeyboardMarkup(row_width=2)

    for r in rows:
        mid = r[0]
        title = r[1]

        kb.add(
            InlineKeyboardButton(
                f"➕ Add to Cart — {title}",
                callback_data=f"addcartdm:{mid}"
            ),
            InlineKeyboardButton(
                f"💳 Buy Now — {title}",
                callback_data=f"buydm:{mid}"
            )
        )

    # ===== PAGINATION =====
    nav = []
    if page_index > 0:
        nav.append(InlineKeyboardButton("◀️ Back", callback_data="allfilms_prev"))
    if page_index < len(pages) - 1:
        nav.append(InlineKeyboardButton("Next ▶️", callback_data="allfilms_next"))

    if nav:
        kb.row(*nav)

    # ===== EXTRA BUTTONS =====
    kb.add(
        InlineKeyboardButton("🔍 SEARCH MOVIE", callback_data="search_movie")
    )
    kb.add(
        InlineKeyboardButton("⤴️ KOMA FARKO", callback_data="go_home"),
        InlineKeyboardButton(
            "📺 Our Channel",
            url=f"https://t.me/{CHANNEL.lstrip('@')}"
        )
    )

    # ===== DELETE OLD MESSAGE =====
    try:
        if sess.get("last_msg"):
            bot.delete_message(uid, sess["last_msg"])
    except:
        pass

    msg = bot.send_message(uid, text, reply_markup=kb)
    sess["last_msg"] = msg.message_id
    allfilms_sessions[uid] = sess

SEARCH_PAGE_SIZE = 5


# ---------- NORMALIZE ----------
def _norm(t):
    return (t or "").lower().strip()


# ---------- GET ALL MOVIES ----------
def _get_all_movies():
    """
    id | title | price | file_name | created_at
    """
    return conn.execute("""
        SELECT id, title, price, file_name, created_at
        FROM movies
        ORDER BY created_at DESC
    """).fetchall()


# ======================================================
# =============== FILTER / SEARCH CORE =================
# ======================================================

def _unique_add(res, seen, mid, title, price):
    if mid not in seen:
        res.append((mid, title, price))
        seen.add(mid)


# ---------- SEARCH BY NAME ----------
def search_by_name(query):
    q = _norm(query)
    res, seen = [], set()

    for mid, title, price, fname, _ in _get_all_movies():
        if q in _norm(title) or q in _norm(fname):
            _unique_add(res, seen, mid, title, price)

    return res


# ---------- ALGAITA ----------
# ❌ KAR A TABA
def get_algaita_movies():
    res, seen = [], set()

    for mid, title, price, fname, _ in _get_all_movies():
        name = _norm(title) + " " + _norm(fname)
        if "algaita" in name:
            _unique_add(res, seen, mid, title, price)

    return res


# ---------- HAUSA SERIES ----------
# ✅ GYARA: yana duba nasa table
def get_hausa_series_movies():
    res = []

    rows = conn.execute("""
        SELECT id, title, price
        FROM hausa_series
        ORDER BY created_at DESC
    """).fetchall()

    for hid, title, price in rows:
        res.append((hid, title, price))

    return res


# ---------- OTHERS / PUBLIC ----------
# ✅ GYARA: yana duba table din series
def get_public_movies():
    res = []

    rows = conn.execute("""
        SELECT id, title, price
        FROM series
        ORDER BY created_at DESC
    """).fetchall()

    for sid, title, price in rows:
        res.append((sid, title, price))

    return res


# ======================================================
# ================== SENDERS ===========================
# ======================================================

def _send_page(uid, movies, page, title, cb_type):
    start = page * SEARCH_PAGE_SIZE
    end = start + SEARCH_PAGE_SIZE
    chunk = movies[start:end]

    if not chunk:
        bot.send_message(uid, "❌ Babu ƙarin sakamako.")
        return

    kb = InlineKeyboardMarkup()

    for mid, name, price in chunk:
        kb.add(
            InlineKeyboardButton(
                f"🎬 {name}",
                callback_data=f"buy:{mid}"
            )
        )

    nav = []
    if page > 0:
        nav.append(
            InlineKeyboardButton("⬅️ BACK", callback_data=f"C_{cb_type}_{page-1}")
        )
    if end < len(movies):
        nav.append(
            InlineKeyboardButton("MORE ➡️", callback_data=f"C_{cb_type}_{page+1}")
        )

    if nav:
        kb.row(*nav)

    kb.row(
    InlineKeyboardButton("🔍 BROWSING", callback_data="search_k"),
    InlineKeyboardButton("❌ CANCEL", callback_data="search_cancel")
)

    bot.send_message(uid, title, reply_markup=kb)


# ---------- DISPATCH SENDERS ----------

def send_search_results(uid, page):
    q = admin_states.get(uid, {}).get("query")
    if not q:
        bot.send_message(uid, "❌ An rasa sakamakon nema.")
        return

    movies = search_by_name(q)
    _send_page(uid, movies, page, f"🔍 SAKAMAKON NEMA: {q}", "search")


def send_others_movies(uid, page):
    movies = get_public_movies()
    _send_page(uid, movies, page, "🎞 OTHERS / PUBLIC MOVIES", "others")


def send_hausa_series(uid, page):
    movies = get_hausa_series_movies()
    _send_page(uid, movies, page, "📺 HAUSA SERIES", "hausa")


def send_algaita_movies(uid, page):
    movies = get_algaita_movies()
    _send_page(uid, movies, page, "🎺 ALGAITA MOVIES", "algaita")


# ================== END RUKUNI C ==================


# ====================== RUKUNI D (FINAL) ======================

def safe_delete(chat_id, msg_id):
    try:
        bot.delete_message(chat_id, msg_id)
    except:
        pass



@bot.callback_query_handler(func=lambda c: c.data.startswith("deliver:"))
def deliver_movies(call):
    user_id = call.from_user.id
    cur = conn.cursor()

    try:
        _, order_id = call.data.split(":", 1)
    except:
        bot.answer_callback_query(call.id, "❌ Error a bayanan order")
        return

    # 1️⃣ DUBA ORDER
    order = cur.execute(
        "SELECT paid, movie_id FROM orders WHERE id=? AND user_id=?",
        (order_id, user_id)
    ).fetchone()

    if not order:
        bot.answer_callback_query(call.id, "❌ Order ba'a samu ba")
        return

    paid, ref_id = order
    if paid != 1:
        bot.answer_callback_query(call.id, "❌ Ba a tabbatar da biyanka ba")
        return

    # 2️⃣ KAR A SAKE TURAWA
    already = cur.execute(
        "SELECT 1 FROM user_movies WHERE order_id=? LIMIT 1",
        (order_id,)
    ).fetchone()

    if already:
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("🎬 My Movies", callback_data="my_movies"))
        bot.send_message(user_id, "ℹ️ Ka riga ka karɓi fim ɗinka.", reply_markup=kb)
        return

    bot.answer_callback_query(call.id, "📤 Ana turo maka fim ɗinka...")

    items = []

    # 3️⃣ A) DUBA ORDER_ITEMS (PRIORITY)
    items = cur.execute("""
        SELECT m.id, m.file_id, m.title
        FROM order_items oi
        JOIN movies m ON m.id = oi.movie_id
        WHERE oi.order_id=?
    """, (order_id,)).fetchall()

    # 3️⃣ B) IDAN BABU → HAUSA SERIES ITEMS
    if not items:
        items = cur.execute("""
            SELECT m.id, m.file_id, m.title
            FROM hausa_series_items hsi
            JOIN movies m ON m.id = hsi.movie_id
            WHERE hsi.series_id=?
        """, (ref_id,)).fetchall()

    # 3️⃣ C) IDAN HAR YANZU BABU → SERIES ITEMS
    if not items:
        items = cur.execute("""
            SELECT m.id, m.file_id, m.title
            FROM series_items si
            JOIN movies m ON m.id = si.movie_id
            WHERE si.series_id=?
        """, (ref_id,)).fetchall()

    if not items:
        bot.send_message(user_id, "❌ Babu fim a wannan order")
        return

    sent = 0

    # 4️⃣ TURAWA
    for movie_id, file_id, title in items:
        sent_ok = False
        try:
            bot.send_video(user_id, file_id, caption=f"🎬 {title}")
            sent_ok = True
        except:
            try:
                bot.send_document(user_id, file_id, caption=f"📁 {title}")
                sent_ok = True
            except Exception as e:
                print("Send failed:", e)

        if sent_ok:
            cur.execute(
                "INSERT INTO user_movies (user_id, movie_id, order_id) VALUES (?, ?, ?)",
                (user_id, movie_id, order_id)
            )
            sent += 1

    conn.commit()

    bot.send_message(
        user_id,
        f"✅ An tura fina-finanka ({sent}).\nMun gode 🙏🥰"
    )

    # 6️⃣ FEEDBACK (KARO NA FARKO KAWAI)
    send_feedback_prompt(user_id, order_id)
@bot.callback_query_handler(func=lambda c: c.data.startswith("C_"))
def handle_rukuni_d_callbacks(c):
    uid = c.from_user.id
    data = c.data

    try:
        bot.answer_callback_query(c.id)
    except:
        pass

    # goge tsohon message
    safe_delete(c.message.chat.id, c.message.message_id)

    # FORMAT: C_<type>_<page>
    try:
        _, ctype, page = data.split("_", 2)
        page = int(page)
    except:
        return

    if ctype == "search":
        send_search_results(uid, page)

    elif ctype == "others":
        send_others_movies(uid, page)

    elif ctype == "hausa":
        send_hausa_series(uid, page)

    elif ctype == "algaita":
        send_algaita_movies(uid, page)


@bot.callback_query_handler(func=lambda c: c.data == "search_cancel")
def handle_search_cancel(c):
    try:
        bot.answer_callback_query(c.id)
    except:
        pass

    safe_delete(c.message.chat.id, c.message.message_id)

    bot.send_message(
        c.from_user.id,
        "❌ An fasa nema.\n\nKa zabi wani abu daga menu."
    )

# ====================== END RUKUNI D ======================



# =========================================================
# === DEEP-LINK START HANDLER (VIEWALL / WEAKUPDATE) ======
# =========================================================



# ========= HARD START BUYD =========
@bot.message_handler(
    func=lambda m: m.text
    and m.text.startswith("/start ")
    and m.text.split(" ", 1)[1].startswith("buyd_")
)
def __hard_start_buyd(msg):
    return buyd_deeplink_handler(msg)


# ========= HARD START GROUPITEM =========
@bot.message_handler(
    func=lambda m: m.text
    and m.text.startswith("/start ")
    and m.text.split(" ", 1)[1].startswith("groupitem_")
)
def __hard_start_groupitem(msg):
    return groupitem_deeplink_handler(msg)

# --- Added deep-link start handler for viewall/weakupdate (runs before other start handlers) ---  
@bot.message_handler(func=lambda m: (m.text or "").strip().split(" ")[0]=="/start" and len((m.text or "").strip().split(" "))>1 and (m.text or "").strip().split(" ")[1] in ("viewall","weakupdate"))  
def _start_deeplink_handler(msg):  
    """  
    Catch /start viewall or /start weakupdate deep-links from channel posts.  
    This handler tries to send the weekly list directly and then returns without invoking the normal start flow.  
    Placed early to take precedence over other start handlers.  
    """  
    try:  
        send_weekly_list(msg)  
    except Exception as e:  
        try:  
            bot.send_message(msg.chat.id, "An samu matsala wajen nuna weekly list.")  
        except:  
            pass  
    return

# ================== RUKUNI B: SEARCH MOVIE MESSAGE HANDLERS (OFFICIAL) ==================

# ===== SEARCH BY NAME: USER TEXT INPUT =====
@bot.message_handler(
    func=lambda m: admin_states.get(m.from_user.id, {}).get("state") == "search_wait_name"
)
def search_name_text(m):
    uid = m.from_user.id
    text = (m.text or "").strip()

    # kariya
    if not text:
        bot.send_message(uid, "❌ Rubuta sunan fim.")
        return

    # harafi 2 ko 3 kawai
    if len(text) < 2 or len(text) > 3:
        bot.send_message(
            uid,
            "❌ Rubuta *HARAFI 2 KO 3* kawai.\nMisali: *MAS*",
            parse_mode="Markdown"
        )
        return

    # ajiye abin da user ya nema (engine zai karanta daga nan)
    admin_states[uid]["query"] = text.lower()

    # sanar da user
    bot.send_message(
        uid,
        f"🔍 Kana nema: *{text.upper()}*\n⏳ Ina dubawa...",
        parse_mode="Markdown"
    )

    # 👉 KIRA SEARCH ENGINE (RUKUNI C) – PAGE NA FARKO
    send_search_results(uid, 0)


# ===== FALLBACK: IDAN USER YA RUBUTA ABU BA A SEARCH MODE BA =====
@bot.message_handler(
    func=lambda m: m.from_user.id in admin_states
    and admin_states.get(m.from_user.id, {}).get("state") in (
        "search_menu",
        "browse_menu",
        "series_menu",
        "search_trending",
    )
)
def ignore_unexpected_text(m):
    uid = m.from_user.id
    bot.send_message(
        uid,
        "ℹ️ Don Allah ka yi amfani da *buttons* da ke ƙasa.",
        parse_mode="Markdown"
    )

# ================== END RUKUNI B ==================

# --- Added callback handler for in-bot "View All Movies" buttons ---
@bot.callback_query_handler(func=lambda c: c.data in ("view_all_movies","viewall"))
def _callback_view_all(call):
    uid = call.from_user.id
    # Build a small message-like object expected by send_weekly_list
    class _Msg:
        def __init__(self, uid):
            self.chat = type('X', (), {'id': uid})
            self.text = ""
    try:
        send_weekly_list(_Msg(uid))
        bot.answer_callback_query(call.id)
    except Exception as e:
        bot.answer_callback_query(call.id, "An samu matsala wajen nuna jerin.")





# ========== HELPERS ==========
def check_join(uid):
    try:
        member = bot.get_chat_member(CHANNEL, uid)
        return member.status in ("member", "administrator", "creator", "restricted")
    except Exception:
        return False

# name anonymization
def mask_name(fullname):
    """Mask parts of the name as requested: Muhmad, Khid, Sa*i style."""
    if not fullname:
        return "User"
    s = re.sub(r"\s+", " ", fullname.strip())
    # split on non-alphanumeric to preserve parts
    parts = re.split(r'(\W+)', s)
    out = []
    for p in parts:
        if not p or re.match(r'\W+', p):
            out.append(p)
            continue
        # p is a word
        n = len(p)
        if n <= 2:
            out.append(p[0] + "*"*(n-1))
            continue
        # keep first 2 and last 1, hide middle with **
        if n <= 4:
            keep = p[0] + "*"*(n-2) + p[-1]
            out.append(keep)
        else:
            # first two, two stars, last one
            out.append(p[:2] + "**" + p[-1])
    return "".join(out)

# language helpers (persisted in DB)
def set_user_lang(user_id, lang_code):
    try:
        conn.execute("INSERT OR REPLACE INTO user_prefs(user_id,lang) VALUES(?,?)", (user_id, lang_code))
        conn.commit()
    except Exception as e:
        print("set_user_lang error:", e)

def get_user_lang(user_id):
    try:
        row = conn.execute("SELECT lang FROM user_prefs WHERE user_id=?", (user_id,)).fetchone()
        if row:
            return row[0]
    except Exception as e:
        print("get_user_lang error:", e)
    return "ha"

# translation map for interface (not movie titles). Hausa (ha) = keep original messages in code.
TRANSLATIONS = {
    "en": {
        "welcome_shop": "Welcome to the film store:",
        "ask_name": "Hello! What do you need?:",
        "joined_ok": "✔ Joined the channel!",
        "not_joined": "❌ You have not joined.",
        "invite_text": "Invite friends and earn rewards! Share your link:",
        "no_movies": "No movies to show right now.",
        "cart_empty": "Your cart is empty.",
        "checkout_msg": "Proceed to checkout",
        "choose_language_prompt": "Choose your language:",
        "language_set_success": "Language changed successfully.",
        "change_language_button": "🌐 Change your language",

        # ===== BUTTONS =====
        "btn_choose_films": "Choose films",
        "btn_weekly_films": "This week's films",
        "btn_cart": "🧾 Cart",
        "btn_help": "Help",
        "btn_films": "🎬 Films",
        "btn_my_orders": "📦 My Orders",
        "btn_search_movie": "🔎 Search Movie",
        "btn_invite": "📨 Invite friends",
        "btn_support": "🆘 Support Help",
        "btn_go_home": "⤴️ Go back Home",
        "btn_channel": "📺 Our Channel",
        "btn_add_cart": "➕ Add to Cart",
        "btn_buy_now": "💳 Buy Now"
    },

    "fr": {
        "welcome_shop": "Bienvenue dans la boutique de films:",
        "ask_name": "Bonjour! Que voulez-vous?:",
        "joined_ok": "✔ Vous avez rejoint!",
        "not_joined": "❌ Vous n'avez pas rejoint.",
        "invite_text": "Invitez des amis et gagnez des récompenses!",
        "no_movies": "Aucun film disponible pour l’instant.",
        "cart_empty": "Votre panier est vide.",
        "checkout_msg": "Passer au paiement",
        "choose_language_prompt": "Choisissez votre langue:",
        "language_set_success": "Langue changée avec succès.",
        "change_language_button": "🌐 Changer la langue",

        # BUTTONS
        "btn_choose_films": "Choisir des films",
        "btn_weekly_films": "Films de cette semaine",
        "btn_cart": "🧾 Panier",
        "btn_help": "Aide",
        "btn_films": "🎬 Films",
        "btn_my_orders": "📦 Mes commandes",
        "btn_search_movie": "🔎 Rechercher un film",
        "btn_invite": "📨 Inviter des amis",
        "btn_support": "🆘 Aide",
        "btn_go_home": "⤴️ Retour",
        "btn_channel": "📺 Notre chaîne",
        "btn_add_cart": "➕ Ajouter au panier",
        "btn_buy_now": "💳 Acheter"
    },

    "ig": {
        "welcome_shop": "Nnọọ n’ụlọ ahịa fim:",
        "ask_name": "Ndewo! Gịnị ka ịchọrọ?:",
        "joined_ok": "✔ Ejikọtara gị!",
        "not_joined": "❌ Ị jụbeghị.",
        "invite_text": "Kpọọ enyi ka ha nweta uru!",
        "no_movies": "Enweghị fim ugbu a.",
        "cart_empty": "Ụgbọ gị dị efu.",
        "checkout_msg": "Gaa ịkwụ ụgwọ",
        "choose_language_prompt": "Họrọ asụsụ:",
        "language_set_success": "Asụsụ agbanweela nke ọma.",
        "change_language_button": "🌐 Gbanwee asụsụ",

        # BUTTONS
        "btn_choose_films": "Họrọ fim",
        "btn_weekly_films": "Fim izu a",
        "btn_cart": "🧾 Cart",
        "btn_help": "Nkwado",
        "btn_films": "🎬 Fim",
        "btn_my_orders": "📦 Oru m",
        "btn_search_movie": "🔎 Chọọ fim",
        "btn_invite": "📨 Kpọọ enyi",
        "btn_support": "🆘 Nkwado",
        "btn_go_home": "⤴️ Laghachi",
        "btn_channel": "📺 Channel anyị",
        "btn_add_cart": "➕ Tinye na Cart",
        "btn_buy_now": "💳 Zụta Ugbu a"
    },

    "yo": {
        "welcome_shop": "Kaabo si ile itaja fiimu:",
        "ask_name": "Bawo! Kini o fẹ?:",
        "joined_ok": "✔ Darapọ mọ ikanni!",
        "not_joined": "❌ O kò tíì darapọ.",
        "invite_text": "Pe awọn ọrẹ ki o jèrè ere!",
        "no_movies": "Ko si fiimu lọwọlọwọ.",
        "cart_empty": "Apo rẹ ṣofo.",
        "checkout_msg": "Tẹsiwaju si isanwo",
        "choose_language_prompt": "Yan èdè:",
        "language_set_success": "Èdè ti yipada.",
        "change_language_button": "🌐 Yi èdè pada",

        # BUTTONS
        "btn_choose_films": "Yan fiimu",
        "btn_weekly_films": "Fiimu ọ̀sẹ̀ yìí",
        "btn_cart": "🧾 Cart",
        "btn_help": "Iranwọ",
        "btn_films": "🎬 Fiimu",
        "btn_my_orders": "📦 Awọn aṣẹ mi",
        "btn_search_movie": "🔎 Wa fiimu",
        "btn_invite": "📨 Pe ọ̀rẹ́",
        "btn_support": "🆘 Iranwọ",
        "btn_go_home": "⤴️ Pada",
        "btn_channel": "📺 Ikanni wa",
        "btn_add_cart": "➕ Fi kun Cart",
        "btn_buy_now": "💳 Ra báyìí"
    },

    "ff": {
        "welcome_shop": "A jaɓɓama e suuɗi fim:",
        "ask_name": "Ina! Hol ko yiɗɗa?:",
        "joined_ok": "✔ A seɗɗii e kanal!",
        "not_joined": "❌ A wonaa seɗaako.",
        "invite_text": "Naatu yamiroɓe ngam jeye jukkere!",
        "no_movies": "Fimmuuji alaa oo sahaa.",
        "cart_empty": "Cart maa ko dulli.",
        "checkout_msg": "Yah to nafawngal",
        "choose_language_prompt": "Labo laawol:",
        "language_set_success": "Laawol waylii no haanirta.",
        "change_language_button": "🌐 Waylu laawol",

        # BUTTONS
        "btn_choose_films": "Suɓo fim",
        "btn_weekly_films": "Fimmuuji ndee yontere",
        "btn_cart": "🧾 Cart",
        "btn_help": "Ballal",
        "btn_films": "🎬 Fimmuuji",
        "btn_my_orders": "📦 Noddu maa",
        "btn_search_movie": "🔎 Yiilu fim",
        "btn_invite": "📨 Naatu yamiroɓe",
        "btn_support": "🆘 Ballal",
        "btn_go_home": "⤴️ Rutto galle",
        "btn_channel": "📺 Kanal amen",
        "btn_add_cart": "➕ Ɓeydu Cart",
        "btn_buy_now": "💳 Soodu Jooni"
    }
}

def tr_user(user_id, key, default=None):
    """Translate key for user language, or return default (Hausa original)"""
    lang = get_user_lang(user_id)
    if lang == "ha":
        return default
    return TRANSLATIONS.get(lang, {}).get(key, default)

# referral helpers (same as before)
def add_referral(referrer_id, referred_id):
    try:
        if referrer_id == referred_id:
            return False
        exists = conn.execute("SELECT id FROM referrals WHERE referrer_id=? AND referred_id=?", (referrer_id, referred_id)).fetchone()
        if exists:
            return False
        conn.execute("INSERT INTO referrals(referrer_id,referred_id) VALUES(?,?)", (referrer_id, referred_id))
        conn.commit()
        return True
    except Exception as e:
        print("add_referral error:", e)
        return False

def get_referrer_for(referred_id):
    row = conn.execute("SELECT referrer_id, reward_granted, id FROM referrals WHERE referred_id=? ORDER BY id DESC LIMIT 1", (referred_id,)).fetchone()
    if not row:
        return None
    return {"referrer_id": row[0], "reward_granted": row[1], "referral_row_id": row[2]}

def grant_referral_reward(referral_row_id, referrer_id, amount=200):
    try:
        row = conn.execute("SELECT reward_granted FROM referrals WHERE id=?", (referral_row_id,)).fetchone()
        if not row:
            return False
        if row[0]:
            return False
        conn.execute("INSERT INTO referral_credits(referrer_id,amount,used) VALUES(?,?,0)", (referrer_id, amount))
        conn.execute("UPDATE referrals SET reward_granted=1 WHERE id=?", (referral_row_id,))
        conn.commit()
        try:
            bot.send_message(referrer_id, f"🎉 An ba ka lada N{amount} saboda wani da ka gayyata ya shiga kuma ya yi sayayya 3×. Wannan lada za a iya amfani da shi wajen sayen fim ɗin mu (ba za a iya cire shi ba).")
        except:
            pass
        return True
    except Exception as e:
        print("grant_referral_reward error:", e)
        return False

def get_referrals_by_referrer(referrer_id):
    rows = conn.execute("SELECT referred_id,created_at,reward_granted,id FROM referrals WHERE referrer_id=? ORDER BY id DESC", (referrer_id,)).fetchall()
    return rows

def get_credits_for_user(user_id):
    rows = conn.execute("SELECT id,amount,used,granted_at FROM referral_credits WHERE referrer_id=?", (user_id,)).fetchall()
    total_available = sum(r[1] for r in rows if r[2] == 0)
    return total_available, rows

def check_referral_rewards_for_referred(referred_id):
    try:
        ref = get_referrer_for(referred_id)
        if not ref:
            return False
        referrer_id = ref["referrer_id"]
        reward_granted = ref["reward_granted"]
        referral_row_id = ref["referral_row_id"]
        if reward_granted:
            return False
        rows = conn.execute("SELECT COUNT(*) FROM orders WHERE user_id=? AND movie_id!=? AND paid=1", (referred_id, -1)).fetchone()
        count = rows[0] if rows else 0
        if count >= 3 and check_join(referred_id):
            return grant_referral_reward(referral_row_id, referrer_id, amount=200)
        return False
    except Exception as e:
        print("check_referral_rewards_for_referred error:", e)
        return False

def apply_credits_to_amount(user_id, amount):
    try:
        cur = conn.execute("SELECT id,amount FROM referral_credits WHERE referrer_id=? AND used=0 ORDER BY granted_at", (user_id,)).fetchall()
        if not cur:
            return amount, 0, []
        remaining = int(amount)
        applied = 0
        applied_ids = []
        for cid, camount in cur:
            if remaining <= 0:
                break
            try:
                conn.execute("UPDATE referral_credits SET used=1 WHERE id=?", (cid,))
                conn.commit()
                applied += camount
                applied_ids.append(cid)
                remaining -= camount
            except Exception as e:
                print("apply credit update error:", e)
                continue
        if remaining < 0:
            remaining = 0
        return remaining, applied, applied_ids
    except Exception as e:
        print("apply_credits_to_amount error:", e)
        return amount, 0, []

# parse caption
def parse_caption_for_title_price(caption):
    if not caption:
        return None, None
    text = caption.strip()
    if "|" in text:
        parts = [p.strip() for p in text.split("|") if p.strip()]
        if len(parts) >= 3 and parts[0].lower() == "post":
            title = parts[1]
            price = parts[2]
        elif len(parts) == 2:
            title = parts[0]
            price = parts[1]
        else:
            return None, None
        price_digits = re.findall(r"\d+", price.replace(",", ""))
        if not price_digits:
            return None, None
        return title, int("".join(price_digits))
    m = re.search(r"^(.*?)\s+[₦Nn]?\s*([0-9,]+)\s*$", text)
    if m:
        title = m.group(1).strip()
        price = int(re.sub(r"[^\d]", "", m.group(2)))
        return title, price
    return None, None

# pruning
def prune_old_movies():
    try:
        cutoff = datetime.utcnow() - timedelta(days=21)
        rows = conn.execute("SELECT id, created_at FROM movies ORDER BY created_at ASC").fetchall()
        to_delete = []
        for r in rows:
            if len(to_delete) >= 6:
                break
            cid = r[0]
            created_at = r[1]
            try:
                if not created_at:
                    continue
                dt = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
            except:
                continue
            if dt < cutoff:
                to_delete.append(cid)
        for mid in to_delete:
            try:
                conn.execute("DELETE FROM movies WHERE id=?", (mid,))
            except:
                pass
        if to_delete:
            conn.commit()
            print("Pruned movies:", to_delete)
    except Exception as e:
        print("prune_old_movies error:", e)

prune_old_movies()
# ========== MENUS (FULL TRANSLATION ENABLED) ==========

def footer_kb(user_id=None):
    kb = InlineKeyboardMarkup()

    # Go Home Button (Translated)
    home_label = tr_user(user_id, "btn_go_home", default="⤴️ KOMA FARKO")

    kb.row(
        InlineKeyboardButton(home_label, callback_data="go_home"),
        InlineKeyboardButton(tr_user(user_id, "btn_channel", default="🫂 Our Channel"), url=f"https://t.me/{CHANNEL.lstrip('@')}")
    )

    # Change language button
    change_label = tr_user(user_id, "change_language_button", default="🌐 Change your language")
    kb.row(InlineKeyboardButton(change_label, callback_data="change_language"))

    return kb




def reply_menu(uid=None):
    kb = InlineKeyboardMarkup()

    # ===== Labels =====
    all_films_label = "🎬 All Films"
    my_orders_label = "🛒 MY=ORDERS"

    invite_label  = tr_user(uid, "btn_invite", default="📨 Invite Friends")
    cart_label    = tr_user(uid, "btn_cart", default="🧾 Cart")
    support_label = tr_user(uid, "btn_support", default="🆘 Support Help")
    channel_label = tr_user(uid, "btn_channel", default="📺 Our Channel")
    home_label    = tr_user(uid, "btn_go_home", default="⤴️ KOMA FARKO")
    change_label  = tr_user(
        uid,
        "change_language_button",
        default="🌐 Change your language"
    )

    # ===== ROW 1: All Films + MY=ORDERS =====
    kb.row(
        InlineKeyboardButton(all_films_label, callback_data="all_films"),
        InlineKeyboardButton(my_orders_label, callback_data="myorders_new")
    )

    # ===== ROW 2 =====
    kb.add(
        InlineKeyboardButton(invite_label, callback_data="invite")
    )

    if uid in ADMINS:
        kb.add(InlineKeyboardButton("➕ Add Movie", callback_data="addmovie"))
        kb.add(InlineKeyboardButton("☢SERIES MODE", callback_data="groupitems"))
        kb.add(InlineKeyboardButton("🧹 ERASER", callback_data="eraser_menu"))
        kb.add(InlineKeyboardButton("📂Weak update", callback_data="weak_update"))
        kb.add(InlineKeyboardButton("✏️ Edit title", callback_data="edit_title"))

    kb.add(InlineKeyboardButton(cart_label, callback_data="viewcart"))
    kb.add(InlineKeyboardButton(support_label, callback_data="support_help"))

    # Add a full-width Our Channel row (as in original layout screenshot)
    kb.add(InlineKeyboardButton(channel_label, url=f"https://t.me/{CHANNEL.lstrip('@')}"))

    # Then add a row with Home (KOMA FARKO) and Our Channel side-by-side
    kb.row(
        InlineKeyboardButton(home_label, callback_data="go_home"),
        InlineKeyboardButton(channel_label, url=f"https://t.me/{CHANNEL.lstrip('@')}")
    )

    kb.row(InlineKeyboardButton(change_label, callback_data="change_language"))

    return kb



def user_main_menu(uid=None):
    kb = ReplyKeyboardMarkup(resize_keyboard=True)

    weekly_films = tr_user(uid, "btn_weekly_films", default="Films din wannan satin")
    cart_label   = tr_user(uid, "btn_cart", default="🧾 Cart")
    help_label   = tr_user(uid, "btn_help", default="Taimako")

    kb.add(KeyboardButton(weekly_films))
    kb.add(KeyboardButton(cart_label), KeyboardButton(help_label))

    return kb


#Start
def movie_buttons_inline(mid, user_id=None):
    kb = InlineKeyboardMarkup()

    add_cart = tr_user(user_id, "btn_add_cart", default="➕ Add to Cart")
    buy_now  = tr_user(user_id, "btn_buy_now", default="💳 Buy Now")
    home_btn = tr_user(user_id, "btn_go_home", default="⤴️ KOMA FARKO")
    channel  = tr_user(user_id, "btn_channel", default="🫂 Our Channel")
    change_l = tr_user(user_id, "change_language_button", default="🌐 Change your language")

    kb.add(
        InlineKeyboardButton(add_cart, callback_data=f"addcart:{mid}"),
        InlineKeyboardButton(
            buy_now,
            url=f"https://t.me/{BOT_USERNAME}?start=buyd_{mid}"
        )
    )

    # 🛑 Idan user_id == None → channel ne → kada a ƙara sauran buttons
    if user_id is None:
        return kb

    # 🔰 Idan private chat ne → saka sauran buttons
    kb.row(
        InlineKeyboardButton(home_btn, callback_data="go_home"),
        InlineKeyboardButton(channel, url=f"https://t.me/{CHANNEL.lstrip('@')}")
    )

    kb.row(InlineKeyboardButton(change_l, callback_data="change_language"))

    return kb
#END


# ========== START ==========
@bot.message_handler(commands=["start"])
def start(message):
    uid = message.from_user.id
    fname = message.from_user.first_name or ""
    uname = f"@{message.from_user.username}" if message.from_user.username else "Babu username"
    text = (message.text or "").strip()
    param = None
    if text.startswith("/start "):
        param = text.split(" ",1)[1].strip()
    elif text.startswith("/start"):
        parts = text.split(" ",1)
        if len(parts) > 1:
            param = parts[1].strip()
    if param and param.startswith("ref"):
        try:
            ref_id = int(param[3:])
            try:
                add_referral(ref_id, uid)
                try:
                    bot.send_message(ref_id, f"Someone used your invite link! ID: <code>{uid}</code>", parse_mode="HTML")
                except:
                    pass
            except:
                pass
        except:
            pass
    # notify admin
    try:
        bot.send_message(
            ADMIN_ID,
            f"🟢 SABON VISITOR!\n\n"
            f"👤 Sunan: <b>{fname}</b>\n"
            f"🔗 Username: {uname}\n"
            f"🆔 ID: <code>{uid}</code>",
            parse_mode="HTML"
        )
    except Exception as e:
        print("Failed to notify admin about visitor:", e)
    if not check_join(uid):
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("Join Channel", url=f"https://t.me/{CHANNEL.lstrip('@')}"))
        kb.add(InlineKeyboardButton("I've Joined✅", callback_data="checkjoin"))
        bot.send_message(uid, "⚠️ Don cigaba, sai ka shiga channel ɗin mu.", reply_markup=kb)
        return
    # send menus
    bot.send_message(uid, "Abokin kasuwanci barka da zuwa shagon fina finai:", reply_markup=user_main_menu(uid))
    bot.send_message(uid, "Sannu da zuwa!\n Me kake bukata?:", reply_markup=reply_menu(uid))

# ========== get group id & misc handlers ==========
@bot.message_handler(commands=["getgroupid"])
def getgroupid(message):
    chat = message.chat
    if chat.type in ("group", "supergroup", "channel"):
        bot.reply_to(message, f"Chat title: {chat.title}\nChat id: <code>{chat.id}</code>", parse_mode="HTML")
    else:
        bot.reply_to(message,
                     "Don samun group id: ƙara bot ɗin zuwa group ɗin, sannan a rubita /getgroupid a cikin group. Ko kuma ka forward wani message daga group zuwa nan (DM) kuma zan nuna original chat id idan forwarded.")

@bot.message_handler(
    func=lambda msg: isinstance(getattr(msg, "text", None), str)
    and msg.text in ["Films din wannan satin", "Taimako", "🧾 Cart"]
)
def user_buttons(message):
    txt = message.text
    uid = message.from_user.id

    if txt == "Films din wannan satin":
        try:
            send_weekly_list(message)
        except Exception as e:
            print("Films din wannan satin ERROR:", e)
            bot.send_message(
                message.chat.id,
                "⚠️ An samu matsala wajen nuna fina-finan wannan satin."
            )
        return
# ======= TAIMAKO =======                
    if txt == "Taimako":                
        kb = InlineKeyboardMarkup()                

        # ALWAYS open admin DM directly – no callback, no message sending
        if ADMIN_USERNAME:                
            kb.add(InlineKeyboardButton("Contact Admin", url=f"https://t.me/{ADMIN_USERNAME}"))                
        else:                
            kb.add(InlineKeyboardButton("🆘 Support Help", url="https://t.me/{}".format(ADMIN_USERNAME)))                

        bot.send_message(                
            message.chat.id,                
            "Idan kana bukatar taimako, Yi magana da admin.",                
            reply_markup=kb                
        )                
        return            

    # ======= CART =======            
    if txt == "🧾 Cart":            
        show_cart(message.chat.id, message.from_user.id)            
        return

# ================== FINAL ISOLATED ERASER SYSTEM ==================

import os, json, random, time, re
from datetime import datetime, timedelta
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

ERASER_BACKUP_FOLDER = "eraser_backups"
ERASER_PASSWORD_DEFAULT = "E66337"
ERASER_OTP_TTL = 120
ERASER_MAX_RESEND = 3
ERASER_RESEND_COOLDOWN = 30
ERASER_BACKUP_TTL_DAYS = 30

os.makedirs(ERASER_BACKUP_FOLDER, exist_ok=True)

# ================= DATABASE =================
try:
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS eraser_settings(
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS eraser_backups(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT,
            created_at TEXT
        )
    """)
    conn.commit()
except:
    pass

# ================= HELPERS =================
def eraser_reset_kb():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🔑 Reset Password", callback_data="eraser_forgot"))
    kb.add(InlineKeyboardButton("✖ Cancel", callback_data="eraser_cancel"))
    return kb

# ================= PASSWORD =================
def _eraser_get_password():
    r = conn.execute(
        "SELECT value FROM eraser_settings WHERE key='eraser_password'"
    ).fetchone()
    if r and r[0]:
        return r[0]

    conn.execute(
        "INSERT OR REPLACE INTO eraser_settings VALUES(?,?)",
        ("eraser_password", ERASER_PASSWORD_DEFAULT)
    )
    conn.commit()
    return ERASER_PASSWORD_DEFAULT


def _eraser_set_password(p):
    conn.execute(
        "INSERT OR REPLACE INTO eraser_settings VALUES(?,?)",
        ("eraser_password", p)
    )
    conn.commit()


def _eraser_password_valid(p):
    return bool(re.fullmatch(r"\d{5}[A-Z]", p))

# ================= OTP =================
_eraser_otp = {}
_eraser_meta = {}

def _eraser_gen_otp():
    return str(random.randint(100000, 999999))


def _eraser_send_otp(uid, resend=False):
    now = time.time()
    meta = _eraser_meta.get(uid, {})

    if resend:
        if meta.get("resends", 0) >= ERASER_MAX_RESEND:
            return False, "OTP resend limit reached."
        if now - meta.get("last", 0) < ERASER_RESEND_COOLDOWN:
            return False, "Wait before resending OTP."

    otp = _eraser_gen_otp()
    _eraser_otp[uid] = {"otp": otp, "expires": now + ERASER_OTP_TTL}
    _eraser_meta[uid] = {"resends": meta.get("resends", 0), "last": now}

    bot.send_message(OTP_ADMIN_ID, f"🔐 ERASER OTP for admin {uid}: {otp}")
    return True, None


def _eraser_otp_expired(uid):
    return uid not in _eraser_otp or time.time() > _eraser_otp[uid]["expires"]

# ================= BACKUP =================
def _eraser_create_backup():
    now = datetime.utcnow()
    ts = now.strftime("%Y%m%d%H%M%S")
    fname = f"eraser_backup_{ts}.json"
    path = os.path.join(ERASER_BACKUP_FOLDER, fname)

    cur = conn.cursor()
    tables = [r[0] for r in cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )]

    data = {}
    for t in tables:
        if t in ("sqlite_sequence", "eraser_settings", "eraser_backups"):
            continue
        rows = cur.execute(f"SELECT * FROM {t}").fetchall()
        cols = [d[0] for d in cur.description] if rows else []
        data[t] = [dict(zip(cols, r)) for r in rows]

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    conn.execute(
        "INSERT INTO eraser_backups(filename,created_at) VALUES(?,?)",
        (fname, now.strftime("%Y-%m-%d %H:%M:%S"))
    )
    conn.commit()
    return path

# ================= CALLBACK =================
@bot.callback_query_handler(func=lambda c: c.data.startswith("eraser_"))
def eraser_cb(c):
    uid = c.from_user.id
    data = c.data
    bot.answer_callback_query(c.id)

    if uid != ADMIN_ID:
        return

    if data == "eraser_menu":
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("✔ Yes – Erase", callback_data="eraser_yes"))
        kb.add(InlineKeyboardButton("📦 Backup", callback_data="eraser_backup"))
        kb.add(InlineKeyboardButton("♻ Restore", callback_data="eraser_restore"))
        kb.add(InlineKeyboardButton("🔑 Forgot Password", callback_data="eraser_forgot"))
        kb.add(InlineKeyboardButton("✖ Cancel", callback_data="eraser_cancel"))
        bot.send_message(uid, "🧹 ERASER SYSTEM", reply_markup=kb)

    elif data == "eraser_cancel":
        admin_states.pop(uid, None)
        bot.send_message(uid, "Cancelled.", reply_markup=reply_menu(uid))

    elif data == "eraser_backup":
        admin_states[uid] = {"state": "eraser_backup_pass"}
        bot.send_message(uid, "Enter ERASER password:")

    elif data == "eraser_yes":
        admin_states[uid] = {"state": "eraser_erase_pass"}
        bot.send_message(uid, "Enter ERASER password:")

    elif data == "eraser_restore":
        admin_states[uid] = {"state": "eraser_restore_pass"}
        bot.send_message(uid, "Enter ERASER password:")

    elif data == "eraser_forgot":
        _eraser_send_otp(uid)
        admin_states[uid] = {"state": "eraser_wait_otp"}
        bot.send_message(uid, "OTP sent. Enter OTP:")

# ================= TEXT =================
@bot.message_handler(
    func=lambda m: m.from_user.id == ADMIN_ID
    and admin_states.get(m.from_user.id, {}).get("state", "").startswith("eraser_")
)
def eraser_text(m):
    uid = m.from_user.id
    text = m.text.strip()
    st = admin_states[uid]["state"]

    # ---- BACKUP PASS ----
    if st == "eraser_backup_pass":
        if text != _eraser_get_password():
            bot.send_message(uid, "❌ Wrong password.", reply_markup=eraser_reset_kb())
            return
        path = _eraser_create_backup()
        admin_states.pop(uid)
        bot.send_message(uid, f"✔ Backup created:\n{path}")

    # ---- ERASE PASS ----
    elif st == "eraser_erase_pass":
        if text != _eraser_get_password():
            bot.send_message(uid, "❌ Wrong password.", reply_markup=eraser_reset_kb())
            return
        _eraser_create_backup()
        cur = conn.cursor()
        for (t,) in cur.execute("SELECT name FROM sqlite_master WHERE type='table'"):
            if t not in ("sqlite_sequence", "eraser_settings", "eraser_backups"):
                cur.execute(f"DELETE FROM {t}")
        conn.commit()
        admin_states.pop(uid)
        bot.send_message(uid, "🧹 ERASE COMPLETE.")

    # ---- RESTORE PASS ----
    elif st == "eraser_restore_pass":
        if text != _eraser_get_password():
            bot.send_message(uid, "❌ Wrong password.", reply_markup=eraser_reset_kb())
            return

        # ===== AUTO RESTORE LATEST BACKUP =====
        ok, info = _eraser_auto_restore_latest()

        admin_states.pop(uid, None)

        if ok:
            bot.send_message(
                uid,
                f"♻ <b>RESTORE COMPLETE</b>\n\n📦 Backup: <code>{info}</code>",
                parse_mode="HTML"
            )
        else:
            bot.send_message(
                uid,
                f"❌ <b>Restore failed</b>\n\n{info}",
                parse_mode="HTML"
            )

    # ---- OTP ----
    elif st == "eraser_wait_otp":
        if _eraser_otp_expired(uid):
            bot.send_message(uid, "OTP expired.")
            return
        if text != _eraser_otp[uid]["otp"]:
            bot.send_message(uid, "❌ OTP ba daidai ba. Tambayi admin mai karɓa.")
            return
        admin_states[uid] = {"state": "eraser_new_pass"}
        bot.send_message(uid, "Enter new password:")

    elif st == "eraser_new_pass":
        if not _eraser_password_valid(text):
            bot.send_message(uid, "Invalid format. Example: 66788K")
            return
        admin_states[uid] = {"state": "eraser_confirm_pass", "tmp": text}
        bot.send_message(uid, "Confirm password:")

    elif st == "eraser_confirm_pass":
        if text != admin_states[uid]["tmp"]:
            bot.send_message(uid, "Passwords do not match.")
            return
        _eraser_set_password(text)
        admin_states.pop(uid)
        bot.send_message(uid, "✅ Password changed successfully.")
        # ================= AUTO MERGE RESTORE (ADD-ON ONLY) =================

def _eraser_auto_restore_latest():
    # Dauko latest backup daga DB
    row = conn.execute(
        "SELECT filename FROM eraser_backups ORDER BY id DESC LIMIT 1"
    ).fetchone()

    if not row:
        return False, "No backup found."

    fname = row[0]
    path = os.path.join(ERASER_BACKUP_FOLDER, fname)

    if not os.path.exists(path):
        return False, "Backup file missing."

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    cur = conn.cursor()

    for table, rows in data.items():
        if not rows:
            continue

        cols = list(rows[0].keys())
        placeholders = ",".join(["?"] * len(cols))
        colnames = ",".join(cols)

        for r in rows:
            values = [r[c] for c in cols]
            try:
                cur.execute(
                    f"INSERT OR IGNORE INTO {table} ({colnames}) VALUES ({placeholders})",
                    values
                )
            except Exception:
                # idan wani table baya nan ko schema ya chanja, a wuce shi
                pass

    conn.commit()
    return True, fname

# ================= END ERASER SYSTEM =================
                
            

# ========== admin_inputs for weak_update and edit title ==========            
@bot.message_handler(func=lambda m: m.from_user.id == ADMIN_ID and m.from_user.id in admin_states)            
def admin_inputs(message):            
    state_entry = admin_states.get(message.from_user.id)            
    if not state_entry:            
        return            

    state_entry = admin_states.get(message.from_user.id)            
    if not state_entry:            
        return            

    state = state_entry.get("state")            

    # === NEW: Add Movie admin flow (store file to STORAGE_CHANNEL) ===            
    if state == "add_movie_wait_file":            
        try:            
            file_id = None            
            file_name = None
            if hasattr(message, 'content_type'):            
                if message.content_type == 'photo':            
                    file_id = message.photo[-1].file_id            
                elif message.content_type == 'video':            
                    file_id = message.video.file_id            
                elif message.content_type == 'document':            
                    file_id = message.document.file_id            
                    file_name = message.document.file_name

            if not file_id:            
                bot.reply_to(message, "Ba a gane file ba. Tura fim (photo/video/document).")            
            else:            
                storage_file_id = file_id            
                storage_msg_id = None            

                try:            
                    if STORAGE_CHANNEL:            
                        if message.content_type == 'photo':            
                            sent = bot.send_photo(STORAGE_CHANNEL, file_id)            
                            storage_file_id = sent.photo[-1].file_id if getattr(sent, 'photo', None) else storage_file_id            
                            storage_msg_id = getattr(sent, 'message_id', None)            

                        elif message.content_type == 'video':            
                            sent = bot.send_video(STORAGE_CHANNEL, file_id)            
                            storage_file_id = getattr(sent, 'video', None) and sent.video.file_id or storage_file_id            
                            storage_msg_id = getattr(sent, 'message_id', None)            

                        else:            
                            sent = bot.send_document(STORAGE_CHANNEL, file_id)            
                            storage_file_id = getattr(sent, 'document', None) and sent.document.file_id or storage_file_id            
                            storage_msg_id = getattr(sent, 'message_id', None)            

                except Exception as e:            
                    print("Failed to send to STORAGE_CHANNEL:", e)            

                try:            
                    cur = conn.execute(
    "INSERT INTO movies(title,price,file_id,file_name,created_at,channel_msg_id,channel_username) "
    "VALUES(?,?,?,?,?,?,?)",
    (
        None,
        0,
        storage_file_id,
        file_name,
        datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        storage_msg_id,
        None
    )
)            
                    conn.commit()            
                    movie_id = cur.lastrowid            

                except Exception as e:            
                    print("DB insert (add movie storage) error:", e)            
                    bot.reply_to(message, "An samu matsala wajen adana fim. Duba log.")            
                    admin_states.pop(ADMIN_ID, None)            
                    return            

                bot.send_message(
                    ADMIN_ID,
                    "Da kyau — na adana fim ɗin a storage. Bani poster/sunan fim ɗin na aika sashen tallah (misali: Garaje - 200).",
                    reply_markup=footer_kb(ADMIN_ID)
                )            

                admin_states[ADMIN_ID] = {
                    "state": "add_movie_wait_poster",
                    "movie_id": movie_id,
                    "file_id": storage_file_id,
                    "storage_msg_id": storage_msg_id
                }            

        except Exception as e:            
            print("add_movie_wait_file error:", e)
            bot.reply_to(message, "An samu kuskure yayin adana fim.")
        return

    if state == "add_movie_wait_poster":
        try:
            st = state_entry
            movie_id = st.get('movie_id')
            file_id_saved = st.get('file_id')
            if not movie_id:
                bot.reply_to(message, "Bai dace ba — babu fim ɗin da aka fara. Fara daga Add Movie.")
                admin_states.pop(ADMIN_ID, None)
                return
            caption_text = (message.caption or message.text or "").strip()
            title, price = parse_caption_for_title_price(caption_text)
            poster_file_id = None
            if hasattr(message, 'content_type'):
                if message.content_type == 'photo':
                    poster_file_id = message.photo[-1].file_id
                elif message.content_type == 'video':
                    poster_file_id = message.video.file_id
                elif message.content_type == 'document':
                    poster_file_id = message.document.file_id
            if not title or not price:
                bot.reply_to(message, "Format bai dace ba. Aika poster (photo/video/document) tare da caption: Title - 200")
                return
            try:
                conn.execute("UPDATE movies SET title=?, price=? WHERE id=?", (title, int(price), movie_id))
                conn.commit()
            except Exception as e:
                print("Failed updating movie title/price:", e)
            try:
                post_caption = f"🎬 <b>{title}</b>\n💵 ₦{price}\nTap buttons to buy or add to cart."
                markup = movie_buttons_inline(movie_id, user_id=None)
                sent_msg = None
                if poster_file_id:
                    if message.content_type == 'photo':
                        sent_msg = bot.send_photo(CHANNEL, poster_file_id, caption=post_caption, parse_mode='HTML', reply_markup=markup)
                    elif message.content_type == 'video':
                        sent_msg = bot.send_video(CHANNEL, poster_file_id, caption=post_caption, parse_mode='HTML', reply_markup=markup)
                    else:
                        sent_msg = bot.send_document(CHANNEL, poster_file_id, caption=post_caption, parse_mode='HTML', reply_markup=markup)
                else:
                    sent_msg = bot.send_message(CHANNEL, post_caption, parse_mode='HTML', reply_markup=markup)
                channel_msg_id = sent_msg.message_id if sent_msg else None
                channel_username = CHANNEL.lstrip('@')
                try:
                    conn.execute("UPDATE movies SET channel_msg_id=?, channel_username=? WHERE id=?", (channel_msg_id, channel_username, movie_id))
                    conn.commit()
                except Exception as e:
                    print("Failed storing channel msg id after posting poster:", e)
            except Exception as e:
                print("Failed to post poster to CHANNEL:", e)
                bot.reply_to(message, "An adana fim amma an kasa turawa tallah. Duba logs.")
                admin_states.pop(ADMIN_ID, None)
                return
            bot.send_message(ADMIN_ID, f"An adana fim (ID: {movie_id}) kuma an tura poster a tallah.")
            admin_states.pop(ADMIN_ID, None)
        except Exception as e:
            print("add_movie_wait_poster error:", e)
            bot.reply_to(message, "An samu kuskure wajen aiwatar da poster/process.")
            admin_states.pop(ADMIN_ID, None)
        return
    
    # edit title flows (kept same logic)
    if state == "edit_title_wait_for_query":
        q = (message.text or "").strip()
        if not q:
            bot.reply_to(message, "Ba ka turo sunan ko ID ba. Rubuta sunan fim ko ID domin in bincika.")
            return
        movie = None
        try:
            mid = int(q)
            movie = conn.execute("SELECT id,title,channel_msg_id,channel_username FROM movies WHERE id=?", (mid,)).fetchone()
        except:
            rows = conn.execute("SELECT id,title,channel_msg_id,channel_username FROM movies").fetchall()
            exact = [r for r in rows if r[1] and r[1].strip().lower() == q.lower()]
            if exact:
                movie = exact[0]
            else:
                contains = [r for r in rows if r[1] and q.lower() in r[1].strip().lower()]
                if len(contains) == 0:
                    movie = None
                elif len(contains) == 1:
                    movie = contains[0]
                else:
                    text = "An samu fina-finai masu kama. Aiko ID ɗin fim ɗin daga cikin waɗannan:\n"
                    for r in contains:
                        text += f"• {r[1]} — ID: {r[0]}\n"
                    bot.reply_to(message, text)
                    admin_states[ADMIN_ID] = {"state": "edit_title_wait_for_id", "inst_msgs": state_entry.get("inst_msgs", [])}
                    return
        if not movie:
            bot.reply_to(message, "Ban samu wannan fim din a jerin ba. Sake gwadawa ko aiko ID ɗin.")
            admin_states[ADMIN_ID] = {"state": "edit_title_wait_for_query", "inst_msgs": state_entry.get("inst_msgs", [])}
            return
        mid = movie[0]
        current_title = movie[1]
        sent = bot.reply_to(message, f"Na samu fim ɗin: <b>{current_title}</b> (ID: {mid}).\nAiko sabon title da kake so a maye gurbin wannan.", parse_mode="HTML")
        admin_states[ADMIN_ID] = {"state": "edit_title_wait_new", "movie_id": mid, "inst_msgs": state_entry.get("inst_msgs", []) + [sent.message_id]}
        return

    if state == "edit_title_wait_for_id":
        q = (message.text or "").strip()
        if not q:
            bot.reply_to(message, "Aiko ID na fim ko sunan fim.")
            return
        try:
            mid = int(q)
        except:
            bot.reply_to(message, "Ba valid ID ba. Aiko lambar ID na fim daga jerin da na nuna.")
            return
        row = conn.execute("SELECT id,title,channel_msg_id,channel_username FROM movies WHERE id=?", (mid,)).fetchone()
        if not row:
            bot.reply_to(message, "Ban samu fim da wannan ID ba. Duba ID ɗin ka kuma aiko.")
            return
        current_title = row[1]
        sent = bot.reply_to(message, f"Na samu fim ɗin: <b>{current_title}</b> (ID: {mid}).\nAiko sabon title da kake so a maye gurbin wannan.", parse_mode="HTML")
        admin_states[ADMIN_ID] = {"state": "edit_title_wait_new", "movie_id": mid, "inst_msgs": state_entry.get("inst_msgs", []) + [sent.message_id]}
        return

    if state == "edit_title_wait_new":
        new_title = (message.text or "").strip()
        if not new_title:
            bot.reply_to(message, "Ba ka turo sabon suna ba. Rubuta sabon title yanzu.")
            return
        mid = state_entry.get("movie_id")
        if not mid:
            bot.reply_to(message, "Bai dace ba — babu fim ɗin da aka zaɓa. Fara sabo daga Edit title.")
            admin_states.pop(ADMIN_ID, None)
            return
        bot.reply_to(message, f"Naga me ka rubuta:\n<b>{new_title}</b>\nIna sabunta sunan a database...", parse_mode="HTML")
        try:
            conn.execute("UPDATE movies SET title=? WHERE id=?", (new_title, mid))
            conn.commit()
            row = conn.execute("SELECT channel_msg_id,channel_username,price,file_id FROM movies WHERE id=?", (mid,)).fetchone()
            if row:
                channel_msg_id, channel_username, price, file_id = row[0], row[1], row[2], row[3]
                try:
                    if channel_username and channel_msg_id:
                        new_caption = f"🎬 <b>{new_title}</b>\n"
                        if price:
                            new_caption += f"💵 ₦{price}\n"
                        else:
                            new_caption += "\n"
                        new_caption += "Tap buttons to buy or add to cart."
                        bot.edit_message_caption(chat_id=f"@{channel_username}" if not str(channel_username).startswith("@") else channel_username,
                                                 message_id=int(channel_msg_id),
                                                 caption=new_caption,
                                                 parse_mode="HTML",
                                                 reply_markup=movie_buttons_inline(mid, user_id=None))
                except Exception as e:
                    print("Failed to edit channel message caption for movie:", mid, e)
            sent = bot.send_message(ADMIN_ID, f"Anyi nasara 🥰\nNa sabunta sunan fim (ID: {mid}) zuwa:\n<b>{new_title}</b>", parse_mode="HTML")
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("🗑️ Delete conversation messages", callback_data=f"edit_delete:{mid}"))
            del_msg = bot.send_message(ADMIN_ID, "Idan kana son share hirar da muka yi (sakonan da bot ya aiko), danna Delete:", reply_markup=kb)
            insts = state_entry.get("inst_msgs", [])
            insts.append(sent.message_id)
            insts.append(del_msg.message_id)
            admin_states[ADMIN_ID] = {"state": "edit_title_done", "movie_id": mid, "inst_msgs": insts}
        except Exception as e:
            print("Error updating movie title:", e)
            bot.reply_to(message, "An samu matsala yayin sabunta title. Duba log.")
            admin_states.pop(ADMIN_ID, None)
        return

    return


    # ========== CANCEL ==========
@bot.message_handler(commands=["cancel"])
def cancel_cmd(message):
    if message.from_user.id == ADMIN_ID and admin_states.get(ADMIN_ID) and admin_states[ADMIN_ID].get("state") in ("weak_update", "update_week"):
        inst = admin_states[ADMIN_ID]
        inst_msg_id = inst.get("inst_msg_id")
        if inst_msg_id:
            try:
                bot.delete_message(chat_id=ADMIN_ID, message_id=inst_msg_id)
            except Exception as e:
                print("Failed to delete instruction message on cancel:", e)
        admin_states.pop(ADMIN_ID, None)
        bot.reply_to(message, "An soke Update/Weak update kuma an goge sakon instruction.")
        return

    if message.from_user.id == ADMIN_ID and admin_states.get(ADMIN_ID):
        admin_states.pop(ADMIN_ID, None)
        bot.reply_to(message, "An soke aikin admin na yanzu.")
        return



  # ========== BUILD CART VIEW (ASALIN CART) ==========
def build_cart_view(uid):
    rows = get_cart(uid)

    if not rows:
        text = "🛒 <b>Cart ɗinka babu komai.</b>"
        kb = InlineKeyboardMarkup()
        return text, kb

    total = 0
    lines = []
    kb = InlineKeyboardMarkup()

    for movie_id, title, price, file_id in rows:
        price = int(price or 0)
        total += price

        lines.append(f"🎬 {title} — ₦{price}")

        kb.add(
            InlineKeyboardButton(
                f"❌ Cire: {title}",
                callback_data=f"removecart:{movie_id}"
            )
        )

    lines.append("")
    lines.append(f"<b>Jimilla:</b> ₦{total}")

    text = "🛒 <b>YOUR CART/fina finai da ka zaba domin siya</b>\n\n" + "\n".join(lines)

    # ===== BUTTONS =====
    kb.add(InlineKeyboardButton("🧹 Clear Cart", callback_data="clearcart"))
    kb.add(InlineKeyboardButton("💵 Checkout", callback_data="checkout"))

    
    kb.row(
        InlineKeyboardButton("⤴️ KOMA FARKO", callback_data="go_home"),
        InlineKeyboardButton("🫂 Our Channel", url=f"https://t.me/{CHANNEL.lstrip('@')}")
    )
    

    return text, kb  

# ================= ADMIN ON / OFF =================
@bot.message_handler(commands=["on"])
def admin_on(m):
    if m.chat.type != "private" or m.from_user.id != ADMIN_ID:
        return

    conn.execute(
        "INSERT OR REPLACE INTO admin_controls (admin_id, sendmovie_enabled) VALUES (?,1)",
        (ADMIN_ID,)
    )
    conn.commit()
    bot.reply_to(m, "✅ An kunna SENDMOVIE / GETID")


@bot.message_handler(commands=["off"])
def admin_off(m):
    if m.chat.type != "private" or m.from_user.id != ADMIN_ID:
        return

    conn.execute(
        "INSERT OR REPLACE INTO admin_controls (admin_id, sendmovie_enabled) VALUES (?,0)",
        (ADMIN_ID,)
    )
    conn.commit()
    bot.reply_to(m, "⛔ An kashe SENDMOVIE / GETID")


def admin_feature_enabled():
    row = conn.execute(
        "SELECT sendmovie_enabled FROM admin_controls WHERE admin_id=?",
        (ADMIN_ID,)
    ).fetchone()
    return row and row[0] == 1


# ================= GETID (FILE_NAME SEARCH) =================
@bot.message_handler(commands=["getid"])
def getid_command(message):
    # 🔒 TSARO: admin + sai an kunna
    if message.from_user.id != ADMIN_ID:
        return
    if not admin_feature_enabled():
        return

    text = message.text or ""
    parts = text.split(" ", 1)
    if len(parts) < 2 or not parts[1].strip():
        bot.reply_to(
            message,
            "Amfani: /getid Sunan fim\nMisali: /getid Wutar jeji"
        )
        return

    query = parts[1].strip().lower()
    rows = conn.execute("SELECT id,title FROM movies").fetchall()

    exact = [(r[0], r[1]) for r in rows if r[1] and r[1].strip().lower() == query]
    if exact:
        mid, title = exact[0]
        bot.reply_to(
            message,
            f"Kamar yadda ka bukata ga ID ɗin fim ɗin <b>{title}</b>: <code>{mid}</code>",
            parse_mode="HTML"
        )
        return

    contains = [(r[0], r[1]) for r in rows if r[1] and query in r[1].strip().lower()]
    if not contains:
        bot.reply_to(
            message,
            "Ba daidai bane umarninka. Idan kana so na turo maka ID na wani fim, rubuta haka:\n"
            "/getid Sunan fim"
        )
        return

    if len(contains) == 1:
        mid, title = contains[0]
        bot.reply_to(
            message,
            f"Kamar yadda ka bukata ga ID ɗin fim ɗin <b>{title}</b>: <code>{mid}</code>",
            parse_mode="HTML"
        )
    else:
        text_out = "An samu fina-finai masu kama:\n"
        for mid, title in contains:
            text_out += f"• {title} — ID: {mid}\n"
        bot.reply_to(message, text_out)


# ================= SENDMOVIE (FILE_NAME ONLY) =================
@bot.message_handler(commands=["sendmovie"])
def sendmovie_cmd(m):
    if m.from_user.id != ADMIN_ID:
        return
    if not admin_feature_enabled():
        return

    parts = m.text.split(" ", 1)
    if len(parts) < 2:
        return

    q = parts[1].lower()
    file = None
    title = "Movie"

    # MOVIES
    row = conn.execute(
        "SELECT file_id, title FROM movies WHERE file_name LIKE ?",
        (f"%{q}%",)
    ).fetchone()
    if row:
        file, title = row

    # SERIES
    if not file:
        row = conn.execute(
            "SELECT file_id, 'WANNAN FREE (KYAUTA) MUKA BAKU, Ku kalla🫶🏻🥰' FROM series_items WHERE file_name LIKE ?",
            (f"%{q}%",)
        ).fetchone()
        if row:
            file, title = row

    # HAUSA SERIES
    if not file:
        row = conn.execute(
            "SELECT file_id, 'WANNAN FREE (KYAUTA) MUKA BAKU, Ku kalla🫶🏻🥰' FROM hausa_series_items WHERE file_name LIKE ?",
            (f"%{q}%",)
        ).fetchone()
        if row:
            file, title = row

    # WEEKLY
    if not file:
        row = conn.execute(
            "SELECT file_id, 'WANNAN FREE (KYAUTA) MUKA BAKU, Ku kalla🫶🏻🥰' FROM weekly WHERE file_name LIKE ?",
            (f"%{q}%",)
        ).fetchone()
        if row:
            file, title = row

    if not file:
        return  # SHIRU

    try:
        bot.send_video(m.chat.id, file, caption=f"🎬 {title}")
    except:
        bot.send_document(m.chat.id, file, caption=f"🎬 {title}")

@bot.message_handler(
    func=lambda m: user_states.get(m.from_user.id, {}).get("action") == "resend_search"
)
def handle_resend_search_text(m):
    uid = m.from_user.id
    query = m.text.strip()
    user_states.pop(uid, None)

    if len(query) < 2:
        bot.send_message(uid, "❌ Rubuta akalla haruffa 2.")
        return

    used = conn.execute(
        "SELECT COUNT(*) FROM resend_logs WHERE user_id=?",
        (uid,)
    ).fetchone()[0]

    if used >= 10:
        bot.send_message(uid, "⚠️ Ka kai iyakar sake karɓa (sau 10). Sai ka sake siya.")
        return

    rows = conn.execute("""
        SELECT DISTINCT
            um.movie_id,
            m.title
        FROM user_movies um
        JOIN movies m ON m.id = um.movie_id
        WHERE um.user_id = ?
          AND m.title LIKE ?
        ORDER BY m.title ASC
    """, (uid, f"%{query}%")).fetchall()

    if not rows:
        bot.send_message(uid, "❌ Babu fim da ya yi kama da wannan suna A fina finan da ka taba siya.")
        return

    kb = InlineKeyboardMarkup()
    for movie_id, title in rows:
        kb.add(
            InlineKeyboardButton(
                title,
                callback_data=f"resend_one:{movie_id}"
            )
        )

    bot.send_message(
        uid,
        "🎬 <b>An samu fina-finai:</b>\nDanna sunan fim domin a sake turo maka:",
        parse_mode="HTML",
        reply_markup=kb
    )

# ========== detect forwarded channel post ==========
@bot.message_handler(func=lambda m: getattr(m, "forward_from_chat", None) is not None or getattr(m, "forward_from_message_id", None) is not None)
def handle_forwarded_post(m):
    fc = getattr(m, "forward_from_chat", None)
    fid = getattr(m, "forward_from_message_id", None)
    if not fc and not fid:
        return
    try:
        chat_info = ""
        if fc:
            if getattr(fc, "username", None):
                chat_info = f"@{fc.username}"
            else:
                chat_info = f"chat_id:{fc.id}"
        else:
            chat_info = "Unknown channel"
        if fid:
            bot.reply_to(m, f"Original channel: {chat_info}\nOriginal message id: {fid}")
        else:
            bot.reply_to(m, f"Original channel: {chat_info}\nMessage id not found.")
    except Exception as e:
        print("forward handler error:", e)


# ========== show_cart ==========
def show_cart(chat_id, user_id):
    rows = get_cart(user_id)

    if not rows:
        kb = InlineKeyboardMarkup()
        kb.row(
            InlineKeyboardButton("⤴️ KOMA FARKO", callback_data="go_home"),
            InlineKeyboardButton("🫂Our Channel", url=f"https://t.me/{CHANNEL.lstrip('@')}")
        )
        change_label = tr_user(user_id, "change_language_button", default="🌐 Change your language")
        kb.row(InlineKeyboardButton(change_label, callback_data="change_language"))
        s = tr_user(user_id, "cart_empty", default="🧾 Cart ɗinka babu komai.")
        bot.send_message(chat_id, s, reply_markup=kb)
        return

    text_lines = ["🧾 Kayayyakin da ka zaba:"]
    kb = InlineKeyboardMarkup()

    total = 0  # ✅ total ɗaya kacal

    for movie_id, title, price, file_id in rows:
        price = int(price or 0)
        total += price

        # ✅ Nuna series ko movie duka
        if price == 0:
            text_lines.append(f"• {title} — 📦 Series")
        else:
            text_lines.append(f"• {title} — ₦{price}")

        # ✅ KO DA SERIES NE – a ba shi remove
        kb.add(
            InlineKeyboardButton(
                f"❌ Remove: {title[:18]}",
                callback_data=f"removecart:{movie_id}"
            )
        )

    text_lines.append(f"\nJimillar: ₦{total}")

    total_available, credit_rows = get_credits_for_user(user_id)
    credit_info = ""
    if total_available > 0:
        credit_info = (
            f"\n\nNote: Available referral credit: N{total_available}. "
            f"It will be automatically applied at checkout."
        )

    kb.add(
        InlineKeyboardButton("🧹 Clear Cart", callback_data="clearcart"),
        InlineKeyboardButton("💵 Checkout", callback_data="checkout")
    )

    kb.row(
        InlineKeyboardButton("⤴️ KOMA FARKO", callback_data="go_home"),
        InlineKeyboardButton("🫂Our Channel", url=f"https://t.me/{CHANNEL.lstrip('@')}")
    )

    change_label = tr_user(user_id, "change_language_button", default="🌐 Change your language")
    kb.row(InlineKeyboardButton(change_label, callback_data="change_language"))

    bot.send_message(
        chat_id,
        "\n".join(text_lines) + credit_info,
        reply_markup=kb
    )



# ====================== WEAK UPDATE (BULK WEEKLY) ======================
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import re
from datetime import datetime
import json

weak_update_temp = {}

def parse_title_price_block(text_block):
    out = []
    for line in (text_block or "").splitlines():
        line = line.strip()
        if not line:
            continue
        m = re.match(r"^(?P<title>.+?)\s*[–\-:]\s*(?P<price>\d+)", line)
        if m:
            out.append({
                "title": m.group("title").strip(),
                "price": int(m.group("price"))
            })
    return out

def find_best_match(title, candidates):
    t = (title or "").lower()
    for i, c in enumerate(candidates):
        fn = (c.get("file_name") or "").lower()
        toks = re.split(r"[ \-_.]+", fn)
        if t in toks:
            return i
    for i, c in enumerate(candidates):
        fn = (c.get("file_name") or "").lower()
        if fn.startswith(t):
            return i
    for i, c in enumerate(candidates):
        fn = (c.get("file_name") or "").lower()
        if t in fn:
            return i
    return None

# ---------- Start weak update ----------
@bot.callback_query_handler(func=lambda c: c.data == "weak_update")
def start_weak_update(call):
    uid = call.from_user.id
    weak_update_temp[uid] = {
        "stage": "collect_files",
        "movies": [],
        "poster": None,
        "caption": None
    }
    bot.answer_callback_query(call.id)
    bot.send_message(uid,
        "Turomin fina-finai na wannan makon, boss 🌚\n"
        "Tura videos/documents ɗinka yanzu. Idan ka gama, danna YES."
    )

# ---------- collect files ----------
@bot.message_handler(
    func=lambda m: m.from_user.id in weak_update_temp and weak_update_temp[m.from_user.id]["stage"] == "collect_files",
    content_types=['video','document','animation','audio','photo']
)
def collect_files(msg):
    uid = msg.from_user.id
    temp = weak_update_temp[uid]

    orig_chat = msg.chat.id
    orig_msg_id = msg.message_id

    if msg.content_type == "video":
        file_name = getattr(msg.video, "file_name", None) or f"video_{orig_msg_id}"
    elif msg.content_type == "document":
        file_name = getattr(msg.document, "file_name", None) or f"doc_{orig_msg_id}"
    elif msg.content_type == "audio":
        file_name = getattr(msg.audio, "file_name", None) or f"audio_{orig_msg_id}"
    elif msg.content_type == "animation":
        file_name = getattr(msg.animation, "file_name", None) or f"anim_{orig_msg_id}"
    elif msg.content_type == "photo":
        file_name = f"photo_{orig_msg_id}"
    else:
        file_name = f"file_{orig_msg_id}"

    temp["movies"].append({
    "orig_chat_id": orig_chat,
    "msg_id": orig_msg_id,
    "file_name": file_name,
    "title": None,
    "price": None
})

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("YES, Na gama", callback_data="weak_files_done"))
    kb.add(InlineKeyboardButton("NO, Zan ci gaba", callback_data="weak_more_files"))

    bot.send_message(uid, f"An karɓi: {file_name}\nKa gama?", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data == "weak_more_files")
def weak_more_files(call):
    bot.answer_callback_query(call.id)
    bot.send_message(call.from_user.id, "Ci gaba da turo fina-finai...")

@bot.callback_query_handler(func=lambda c: c.data == "weak_files_done")
def weak_files_done(call):
    uid = call.from_user.id
    weak_update_temp[uid]["stage"] = "poster"
    bot.answer_callback_query(call.id)
    bot.send_message(uid,
        "Na karɓi duk fina-finai. Yanzu turo POSTER (hoton).\n"
        "Bayan poster, turo rubutun sunaye + farashi."
    )

# ---------- collect poster ----------
@bot.message_handler(
    func=lambda m: m.from_user.id in weak_update_temp and weak_update_temp[m.from_user.id]["stage"] == "poster",
    content_types=['photo']
)
def collect_poster(msg):
    uid = msg.from_user.id
    temp = weak_update_temp[uid]
    temp["poster"] = msg.photo[-1].file_id

    caption = msg.caption or ""
    if caption.strip():
        temp["caption"] = caption.strip()
        process_weak_finalize(uid)
    else:
        temp["stage"] = "caption"
        bot.send_message(uid,
            "Poster an karɓa. Yanzu turo rubutun sunaye + farashi (misali: Gagarumi - 200)."
        )

# ---------- collect caption text ----------
@bot.message_handler(
    func=lambda m: m.from_user.id in weak_update_temp and weak_update_temp[m.from_user.id]["stage"] == "caption",
    content_types=['text']
)
def collect_caption_text(msg):
    uid = msg.from_user.id
    temp = weak_update_temp[uid]
    temp["caption"] = msg.text.strip()
    process_weak_finalize(uid)

# ---------- FINALIZE ----------
def process_weak_finalize(uid):
    temp = weak_update_temp.get(uid)
    if not temp:
        return

    caption_text = temp.get("caption") or ""
    parsed = parse_title_price_block(caption_text)

    bot.send_message(uid, "Ana tura fina-finai zuwa STORAGE...")

    stored_files = []
    for mv in temp.get("movies", []):
        try:
            sent = bot.copy_message(STORAGE_CHANNEL, mv["orig_chat_id"], mv["msg_id"])
            fid = None
            try:
                if getattr(sent, "document", None):
                    fid = sent.document.file_id
                elif getattr(sent, "video", None):
                    fid = sent.video.file_id
                elif getattr(sent, "audio", None):
                    fid = sent.audio.file_id
                elif getattr(sent, "animation", None):
                    fid = sent.animation.file_id
                elif getattr(sent, "photo", None):
                    fid = sent.photo[-1].file_id
                else:
                    fid = sent.message_id
            except:
                fid = sent.message_id
            stored_files.append({
    "file_id": fid,
    "file_name": mv["file_name"],
    "orig_index": len(stored_files)
})
        except Exception as e:
            print("weak_update copy error:", e)
            continue

    # ================= INSERT INTO DB (DEBUG VERSION) =================
    cur = conn.cursor()
    items_for_weekly = []

    bot.send_message(uid, f"DEBUG: parsed={len(parsed)}, stored_files={len(stored_files)}")

    for item in parsed:
        idx = find_best_match(item["title"], stored_files)

        if idx is None:
            bot.send_message(uid, f"❌ NO MATCH:\nTitle: {item['title']}")
            continue

        file_id = stored_files[idx]["file_id"]
        file_name = stored_files[idx]["file_name"]

        try:
            cur.execute(
                """
                INSERT INTO movies
                (title, price, file_id, file_name, created_at, channel_msg_id, channel_username)
                VALUES (?,?,?,?,?,?,?)
                """,
                (
                    item["title"],
                    item["price"],
                    file_id,
                    file_name,
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    None,
                    CHANNEL.lstrip("@")
                )
            )
            conn.commit()

            movie_id = cur.lastrowid

            items_for_weekly.append({
                "id": movie_id,
                "title": item["title"],
                "price": item["price"],
                "file_id": file_id
            })

            bot.send_message(uid, f"🟢 INSERT OK: movie_id={movie_id}")

        except Exception as e:
            bot.send_message(uid, f"🔥 DB ERROR:\n{e}")

    bot.send_message(uid, f"FINAL DEBUG: Movies saved = {len(items_for_weekly)}")
    # ================================================================

    # ---------- CHANNEL BUTTON (YA KOMA WURI MAI DACE) ----------
    channel_kb = InlineKeyboardMarkup()
    channel_kb.add(
        InlineKeyboardButton(
            "🎬 VIEW ALL MOVIES",
            url=f"https://t.me/{BOT_USERNAME}?start=viewall"
        )
    )

    # ----- CHANNEL BUTTON (RESTORED) -----
    channel_kb = InlineKeyboardMarkup()
    channel_kb.add(
        InlineKeyboardButton(
            "📽 VIEW ALL MOVIES",
            url=f"https://t.me/{BOT_USERNAME}?start=viewall"
        )
    )

    # ----- SEND POSTER TO CHANNEL -----
    try:
        sent_post = bot.send_photo(
            CHANNEL,
            temp.get("poster"),
            caption=caption_text,
            reply_markup=channel_kb
        )
        new_post_id = getattr(sent_post, "message_id", None)
    except Exception as e:
        print("weak_update send poster error:", e)
        new_post_id = None

    # save weekly
    try:
        items_json = json.dumps(items_for_weekly)
        cur.execute(
            "INSERT INTO weekly(poster_file_id, items, channel_msg_id) VALUES (?,?,?)",
            (temp.get("poster"), items_json, new_post_id)
        )
        conn.commit()
    except Exception as e:
        print("Failed saving weekly entry:", e)

    bot.send_message(uid, "An gama Weak Update! 🎉")
    weak_update_temp.pop(uid, None)

# ---------- Show weekly films in DM ----------
def send_weekly_list(msg):
    cur = conn.cursor()
    row = cur.execute(
        "SELECT items, channel_msg_id FROM weekly ORDER BY rowid DESC LIMIT 1"
    ).fetchone()

    if not row:
        return bot.send_message(
            msg.chat.id,
            "Ba a samu jerin fina finan wannan makon ba tukuna."
        )

    try:
        items = json.loads(row[0] or "[]")
    except:
        items = []

    if not items:
        return bot.send_message(
            msg.chat.id,
            "Ba a samu jerin fina finan wannan makon ba tukuna."
        )

    # HEADER with date
    today = datetime.now().strftime("%d/%m/%Y")
    text = f"📅 Weak Update ({today})\n\n"

    kb = InlineKeyboardMarkup()
    all_ids = []

    for m in items:
        title = m.get("title")
        price = m.get("price")
        mid = m.get("id")

        # SUNAN FILM
        text += f"{title} – ₦{price}\n"

        # BUTTON DIN FILM
        kb.row(
            InlineKeyboardButton("➕ Add Cart", callback_data=f"addcart:{mid}"),
            InlineKeyboardButton("💳 Buy Now", callback_data=f"buy:{mid}")
        )

        text += "\n"
        all_ids.append(str(mid))

    # ===== BUY ALL (GYARA ANAN KAWAI) =====
    if all_ids:
        kb.add(
            InlineKeyboardButton(
                "🎁 BUY ALL",
                callback_data="buyall:" + ",".join(all_ids)
            )
        )

    bot.send_message(
        msg.chat.id,
        text,
        reply_markup=kb,
        parse_mode="HTML"
    )


# ---------- weekly button ----------
@bot.callback_query_handler(func=lambda c: c.data == "weekly_films")
def send_weekly_films(call):
    return send_weekly_list(call.message)


@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("buyall:"))
def buy_all_handler(c):
    uid = c.from_user.id

    try:
        ids_raw = c.data.split("buyall:", 1)[1]
        movie_ids = [int(x) for x in ids_raw.split(",") if x.strip().isdigit()]
    except:
        bot.answer_callback_query(c.id, "Invalid BUY ALL data.")
        return

    if not movie_ids:
        bot.answer_callback_query(c.id, "No movies selected.")
        return

    items = []
    total = 0

    for mid in movie_ids:
        row = conn.execute(
            "SELECT id, title, price FROM movies WHERE id=?",
            (mid,)
        ).fetchone()
        if row:
            _id, title, price = row
            price = int(price or 0)
            items.append({"id": _id, "title": title, "price": price})
            total += price

    if not items:
        bot.answer_callback_query(c.id, "Movies not found.")
        return

    _create_and_send_buyall(uid, items, c)
# ---------- START handler (VIEW) ----------
@bot.message_handler(commands=['start'])
def start_handler(msg):

    # 🛑 BAR BUYD DA GROUPITEM SU WUCE
    if msg.text.startswith("/start buyd_"):
        return
    if msg.text.startswith("/start groupitem_"):
        return

    # ===== ASALIN VIEW DINKA (BA A TABA SHI BA) =====
    args = msg.text.split()
    if len(args) > 1 and args[1] == "weakupdate":
        return send_weekly_list(msg)
    if len(args) > 1 and args[1] == "viewall":
        return send_weekly_list(msg)

    bot.send_message(msg.chat.id, "Welcome!")

# ========= BUYD (DEEP LINK BUY → DM) =========
@bot.message_handler(func=lambda m: m.text and m.text.startswith("/start buyd_"))
def buyd_deeplink_handler(msg):
    try:
        uid = msg.from_user.id
        mid = int(msg.text.split("buyd_", 1)[1])
    except:
        bot.reply_to(msg, "❌ Buy link ɗin bai dace ba.")
        return

    cur = conn.cursor()

    movie = cur.execute(
        "SELECT id, title, price FROM movies WHERE id=?",
        (mid,)
    ).fetchone()

    if not movie:
        bot.send_message(uid, "❌ Movie not found.")
        return

    movie_id, title, price = movie
    price = int(price or 0)

    # ================== 🛑 KARIYA 1: YA RIGA YA MALLAKA ==================
    owned = cur.execute(
        "SELECT 1 FROM user_movies WHERE user_id=? AND movie_id=? LIMIT 1",
        (uid, movie_id)
    ).fetchone()

    if owned:
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("🎬 My Movies", callback_data="my_movies"))
        bot.send_message(
            uid,
            "✅ <b>Ka riga ka mallaki wannan fim.</b>",
            parse_mode="HTML",
            reply_markup=kb
        )
        return

    # ================== 🛑 KARIYA 2: HANA ORDER SAU 2 ==================
    old = cur.execute(
        """
        SELECT id FROM orders
        WHERE user_id=? AND movie_id=? AND paid=0
        LIMIT 1
        """,
        (uid, movie_id)
    ).fetchone()

    if old:
        order_id = old[0]
    else:
        order_id = str(uuid.uuid4())

        # 1️⃣ orders
        cur.execute(
            "INSERT INTO orders (id, user_id, movie_id, amount, paid) VALUES (?, ?, ?, ?, 0)",
            (order_id, uid, movie_id, price)
        )

        # 2️⃣ order_items (DELIVER priority)
        cur.execute(
            "INSERT INTO order_items (order_id, movie_id, price) VALUES (?, ?, ?)",
            (order_id, movie_id, price)
        )

        conn.commit()

    # ================== PAYMENT ==================
    pay_url = create_flutterwave_payment(uid, order_id, price, title)
    if not pay_url:
        bot.send_message(uid, "❌ Kuskure wajen kirkirar payment link.")
        return

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("💳 PAY NOW", url=pay_url))
    kb.add(InlineKeyboardButton("❌ Cancel", callback_data=f"cancel:{order_id}"))

    bot.send_message(
        uid,
        f"""🎬 <b>{title}</b>
💵 ₦{price}

🧾 <b>Order ID:</b>
<code>{order_id}</code>

⚠️ <i>Ajiye wannan Order ID.</i>
""",
        parse_mode="HTML",
        reply_markup=kb
    )

# ========= GROUPITEM (DEEP LINK → DM) =========
@bot.message_handler(func=lambda m: m.text and m.text.startswith("/start groupitem_"))
def groupitem_deeplink_handler(msg):
    try:
        uid = msg.from_user.id
        series_id = int(msg.text.split("groupitem_", 1)[1])
    except:
        bot.reply_to(msg, "❌ Invalid series")
        return

    cur = conn.cursor()

    row = cur.execute(
        "SELECT id, title, price FROM series WHERE id=?",
        (series_id,)
    ).fetchone()

    if not row:
        bot.send_message(uid, "❌ Series not found")
        return

    title = row["title"]
    price = int(row["price"] or 0)

    items = cur.execute(
        "SELECT movie_id, price FROM series_items WHERE series_id=?",
        (series_id,)
    ).fetchall()

    if not items:
        bot.send_message(uid, "❌ Babu movie a wannan series")
        return

    # 🛑 KARIYA 1: IDAN USER YA TABA MALLAKA KO WANI MOVIE A SERIES
    owned = cur.execute(
        """
        SELECT 1 FROM user_movies
        WHERE user_id=?
        AND movie_id IN (
            SELECT movie_id FROM series_items WHERE series_id=?
        )
        LIMIT 1
        """,
        (uid, series_id)
    ).fetchone()

    if owned:
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("🎬 My Movies", callback_data="my_movies"))
        bot.send_message(
            uid,
            "✅ <b>Ka riga ka mallaki wannan series.</b>",
            parse_mode="HTML",
            reply_markup=kb
        )
        return

    # 🛑 KARIYA 2: HANA ORDER SAU 2
    old = cur.execute(
        "SELECT id FROM orders WHERE user_id=? AND movie_id=? AND paid=0",
        (uid, series_id)
    ).fetchone()

    if old:
        order_id = old["id"]
    else:
        order_id = str(uuid.uuid4())

        # 1️⃣ INSERT ORDER
        cur.execute(
            "INSERT INTO orders (id, user_id, movie_id, amount, paid) VALUES (?, ?, ?, ?, 0)",
            (order_id, uid, series_id, price)
        )

        for movie_id, p in items:
            # 2️⃣ INSERT ORDER_ITEMS (DELIVER PRIORITY)
            cur.execute(
                "INSERT OR IGNORE INTO order_items (order_id, movie_id, price) VALUES (?, ?, ?)",
                (order_id, movie_id, p)
            )

            # 3️⃣ INSERT SERIES_ITEMS (DELIVER FALLBACK)
            cur.execute(
                "INSERT OR IGNORE INTO series_items (series_id, movie_id, price) VALUES (?, ?, ?)",
                (series_id, movie_id, p)
            )

            # 4️⃣ INSERT HAUSA_SERIES_ITEMS (DELIVER FALLBACK)
            cur.execute(
                "INSERT OR IGNORE INTO hausa_series_items (series_id, movie_id) VALUES (?, ?)",
                (series_id, movie_id)
            )

        conn.commit()

    pay_url = create_flutterwave_payment(uid, order_id, price, title)
    if not pay_url:
        bot.send_message(uid, "❌ Payment error")
        return

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("💳 PAY NOW", url=pay_url))
    kb.add(InlineKeyboardButton("❌ Cancel", callback_data=f"cancel:{order_id}"))

    bot.send_message(
        uid,
        f"""🎬 <b>{title}</b>

💵 <b>Price:</b> ₦{price}
🎞 <b>Movies:</b> {len(items)}

🧾 <b>Order ID:</b>
<code>{order_id}</code>

⚠️ <i>Ajiye wannan Order ID sosai.</i>
""",
        parse_mode="HTML",
        reply_markup=kb
    )


# ===== END WEAK UPDATE =====
        # INVITE
# ---------- My Orders (UNPAID with per-item REMOVE) ----------  
ORDERS_PER_PAGE = 5  
  
  
def build_unpaid_orders_view(uid, page):  
    offset = page * ORDERS_PER_PAGE  
  
    # adadin unpaid orders  
    total = conn.execute(  
        "SELECT COUNT(*) FROM orders WHERE user_id=? AND paid=0",  
        (uid,)  
    ).fetchone()[0]  
  
    if total == 0:  
        kb = InlineKeyboardMarkup()  
        kb.add(InlineKeyboardButton("⤴️ KOMA FARKO", callback_data="go_home"))  
        return "🧾 <b>Babu unpaid order.</b>", kb  
  
    # 🔢 TOTAL BALANCE (DUK UNPAID, BA PAGE KADAI BA)  
    total_amount = conn.execute(  
        "SELECT COALESCE(SUM(amount),0) FROM orders WHERE user_id=? AND paid=0",  
        (uid,)  
    ).fetchone()[0]  
  
    # rows na wannan page ɗin  
    rows = conn.execute(  
        """  
        SELECT o.id, o.amount, m.title  
        FROM orders o  
        LEFT JOIN movies m ON m.id = o.movie_id  
        WHERE o.user_id=? AND o.paid=0  
        ORDER BY o.rowid DESC  
        LIMIT ? OFFSET ?  
        """,  
        (uid, ORDERS_PER_PAGE, offset)  
    ).fetchall()  
  
    text = f"🧾 <b>Your unpaid orders ({total})</b>\n\n"  
    kb = InlineKeyboardMarkup()  
  
    for oid, amount, title in rows:  
        name = title if title else "hide the name"  
        short = name[:15] + "…" if len(name) > 15 else name  
        text += f"• {short} — ₦{int(amount)}\n"  
  
        kb.add(  
            InlineKeyboardButton(  
                f"❌ Cire {short}",  
                callback_data=f"remove_unpaid:{oid}"  
            )  
        )  
  
    # ➕ TOTAL BALANCE A KASA  
    text += f"\n<b>Total balance:</b> ₦{int(total_amount)}"  
  
    nav = []  
    if page > 0:  
        nav.append(InlineKeyboardButton("◀️ Back", callback_data=f"unpaid_prev:{page-1}"))  
    if offset + ORDERS_PER_PAGE < total:  
        nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"unpaid_next:{page+1}"))  
    if nav:  
        kb.row(*nav)  
  
    kb.row(  
        InlineKeyboardButton("💳 Pay all", callback_data="pay_all_now"),  
        InlineKeyboardButton("📦 Paid orders", callback_data="paid_orders")  
    )  
    kb.row(  
        InlineKeyboardButton("🗑 Delete unpaid", callback_data="delete_unpaid"),  
        InlineKeyboardButton("⤴️ KOMA FARKO", callback_data="go_home")  
    )  
  
    return text, kb  
  
  
def build_paid_orders_view(uid, page):
    offset = page * ORDERS_PER_PAGE

    total = conn.execute(
        "SELECT COUNT(DISTINCT movie_id) FROM user_movies WHERE user_id=?",
        (uid,)
    ).fetchone()[0]

    if total == 0:
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("🎬 Purchase Movies", callback_data="my_movies"))
        kb.add(InlineKeyboardButton("⤴️ KOMA FARKO", callback_data="go_home"))
        return "📦 <b>Babu paid movies.</b>", kb

    rows = conn.execute(
        """
        SELECT DISTINCT m.title
        FROM user_movies um
        JOIN movies m ON m.id = um.movie_id
        WHERE um.user_id=?
        ORDER BY um.rowid DESC
        LIMIT ? OFFSET ?
        """,
        (uid, ORDERS_PER_PAGE, offset)
    ).fetchall()

    text = f"📦 <b>Your paid movies ({total})</b>\n\n"
    kb = InlineKeyboardMarkup()

    for (title,) in rows:
        name = title or "Hide the name"
        text += f"• {name} — ✅ Success\n"

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️ Back", callback_data=f"paid_prev:{page-1}"))
    if offset + ORDERS_PER_PAGE < total:
        nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"paid_next:{page+1}"))
    if nav:
        kb.row(*nav)

    kb.add(InlineKeyboardButton("🎬 My Movies", callback_data="my_movies"))
    kb.add(InlineKeyboardButton("⤴️ KOMA FARKO", callback_data="go_home"))

    return text, kb


# ---------- PAY ALL UNPAID (FINAL + NO DUPLICATE ORDER) ----------
@bot.callback_query_handler(func=lambda c: c.data == "pay_all_now")
def pay_all_now(c):
    uid = c.from_user.id

    # 🔒 0️⃣ DUBA KO AKWAI TSOHON UNPAID PAY-ALL ORDER
    old = conn.execute(
        "SELECT id, amount FROM orders WHERE user_id=? AND paid=0 AND movie_id=-1",
        (uid,)
    ).fetchone()

    if old:
        order_id, total = old

        pay_url = create_flutterwave_payment(
            uid, order_id, total, "Pay all movies"
        )

        if not pay_url:
            bot.send_message(uid, "❌ Kuskure wajen dawo da payment link.")
            return

        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("💳 PAY NOW", url=pay_url))

        bot.answer_callback_query(c.id)
        bot.send_message(
            uid,
            f"""⚠️ <b>Kana da order da bai kammala ba</b>

🆔 <b>Order ID:</b>
<code>{order_id}</code>

🎞 <b>Movies:</b> (Pay all)
💰 <b>Jimilla:</b> ₦{total}

⚠️ <i>Idan ka riga ka biya kuma baka samu fim ba,
tura wannan Order ID ga admin.</i>

👇 Ci gaba da biyan wannan order:""",
            parse_mode="HTML",
            reply_markup=kb
        )
        return

    # 1️⃣ TATTARA DUK UNPAID MOVIES (MOVIE + SERIES + WEEKLY)
    rows = conn.execute("""
        SELECT movie_id, price FROM (

            -- single movie orders
            SELECT
                o.movie_id AS movie_id,
                COALESCE(m.price,0) AS price
            FROM orders o
            LEFT JOIN movies m ON o.movie_id = m.id
            WHERE o.user_id=? AND o.paid=0 AND o.movie_id > 0

            UNION ALL

            -- series / weekly items
            SELECT
                oi.movie_id AS movie_id,
                COALESCE(oi.price,0) AS price
            FROM order_items oi
            JOIN orders o2 ON oi.order_id = o2.id
            WHERE o2.user_id=? AND o2.paid=0
        )
        GROUP BY movie_id
    """, (uid, uid)).fetchall()

    if not rows:
        bot.answer_callback_query(c.id, "Babu unpaid orders.")
        bot.send_message(uid, "❌ Babu fim da bai biya ba.")
        return

    # 2️⃣ LISSAFI
    total = 0
    items = {}

    for mid, price in rows:
        p = int(price or 0)
        if mid not in items:
            items[mid] = p
            total += p

    if total <= 0:
        bot.send_message(uid, "❌ Jimillar farashi bai dace ba.")
        return

    # 3️⃣ KIRKIRI SABON PAY-ALL ORDER
    order_id = str(uuid.uuid4())

    conn.execute(
        "INSERT INTO orders (id, user_id, movie_id, amount, paid) VALUES (?, ?, ?, ?, 0)",
        (order_id, uid, -1, total)
    )

    # 4️⃣ SAKA DUKKAN MOVIES A ORDER_ITEMS (DELIVERY ZAI KARANTA DAGA NAN)
    for mid, price in items.items():
        conn.execute(
            "INSERT INTO order_items (order_id, movie_id, price) VALUES (?, ?, ?)",
            (order_id, mid, price)
        )

    conn.commit()

    # 5️⃣ PAYMENT LINK
    pay_url = create_flutterwave_payment(uid, order_id, total, "Pay all movies")
    if not pay_url:
        bot.send_message(uid, "❌ Kuskure wajen ƙirƙirar payment link.")
        return

    # 6️⃣ BUTTON
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("💳 PAY NOW", url=pay_url))

    bot.answer_callback_query(c.id)
    bot.send_message(
        uid,
        f"""🧾 <b>Order Created Successfully</b>

🆔 <b>Order ID:</b>
<code>{order_id}</code>

🎞 <b>Movies:</b> {len(items)}
💰 <b>Jimilla:</b> ₦{total}

⚠️ <i>Ajiye wannan Order ID sosai.
Idan ka biya amma baka samu fim ba,
tura shi ga admin.</i>

👇 Danna ƙasa domin biya:""",
        parse_mode="HTML",
        reply_markup=kb
    )


# ===============================
# SERIES UPLOAD – FULL FLOW (FINAL)
# ===============================

series_sessions = {}

# ===============================
# COLLECT SERIES FILES (DM → MEMORY ONLY)
# ===============================
@bot.message_handler(
    content_types=["video", "document"],
    func=lambda m: m.from_user.id in series_sessions
)
def series_collect_files(m):
    uid = m.from_user.id
    sess = series_sessions.get(uid)

    if not sess or sess.get("stage") != "collect":
        return

    if m.video:
        dm_file_id = m.video.file_id
        file_name = m.video.file_name or "video.mp4"
    else:
        dm_file_id = m.document.file_id
        file_name = m.document.file_name or "file"

    sess["files"].append({
        "dm_file_id": dm_file_id,
        "file_name": file_name
    })

    bot.send_message(
        uid,
        f"✅ An karɓi: <b>{file_name}</b>",
        parse_mode="HTML"
    )


# ===============================
# DONE (FINISH FILE COLLECTION)
# ===============================
@bot.message_handler(
    func=lambda m: (
        m.text
        and m.text.lower().strip() == "done"
        and m.from_user.id in series_sessions
    )
)
def series_done(m):
    uid = m.from_user.id
    sess = series_sessions.get(uid)

    if not sess or sess.get("stage") != "collect":
        return

    if not sess.get("files"):
        bot.send_message(uid, "❌ Babu fim da aka turo.")
        return

    text = "✅ <b>An karɓi fina-finai:</b>\n\n"
    for f in sess["files"]:
        text += f"• {f['file_name']}\n"

    text += "\n❓ <b>Akwai Hausa series a ciki?</b>"

    sess["stage"] = "ask_hausa"

    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("✅ EH", callback_data="hausa_yes"),
        InlineKeyboardButton("❌ A'A", callback_data="hausa_no")
    )

    bot.send_message(uid, text, parse_mode="HTML", reply_markup=kb)


# ===============================
# HAUSA SERIES CHOICE
# ===============================
@bot.callback_query_handler(
    func=lambda c: (
        c.data in ["hausa_yes", "hausa_no"]
        and c.from_user.id in series_sessions
    )
)
def handle_hausa_choice(c):
    uid = c.from_user.id
    sess = series_sessions.get(uid)
    if not sess:
        return

    bot.answer_callback_query(c.id)

    if c.data == "hausa_no":
        sess["hausa_matches"] = []
        sess["stage"] = "meta"

        bot.send_message(
            uid,
            "📸 <b>To shikenan.</b>\nYanzu turo poster.\nRubuta suna da farashi a caption.",
            parse_mode="HTML"
        )
        return

    sess["stage"] = "hausa_names"
    bot.send_message(
        uid,
        "✍️ <b>Rubuta sunayen Hausa series</b>\n"
        "Layi-layi:\n\n"
        "Misali:\n"
        "Dakin Amarya\n"
        "Zugar So",
        parse_mode="HTML"
    )


# ===============================
# RECEIVE HAUSA SERIES NAMES
# ===============================
@bot.message_handler(
    func=lambda m: (
        m.text
        and m.from_user.id in series_sessions
        and series_sessions[m.from_user.id].get("stage") == "hausa_names"
    )
)
def receive_hausa_titles(m):
    uid = m.from_user.id
    sess = series_sessions.get(uid)
    if not sess:
        return

    titles = [t.strip().lower() for t in m.text.split("\n") if t.strip()]

    matches = []
    for f in sess["files"]:
        fname = f["file_name"].lower()
        for t in titles:
            if t in fname:
                matches.append(f["file_name"])
                break

    sess["hausa_matches"] = matches
    sess["stage"] = "meta"

    if matches:
        text = "✅ <b>Na gano Hausa series:</b>\n\n"
        for n in matches:
            text += f"• {n}\n"
    else:
        text = "⚠️ <b>Ban gano Hausa series ba.</b>"

    bot.send_message(uid, text, parse_mode="HTML")
    bot.send_message(
        uid,
        "📸 <b>Yanzu turo poster</b>\nRubuta suna da farashi a caption.",
        parse_mode="HTML"
    )


# ===============================
# POSTER + STORAGE UPLOAD + DB SAVE
# ===============================
@bot.message_handler(
    content_types=["photo"],
    func=lambda m: m.from_user.id in series_sessions
)
def series_finalize(m):
    uid = m.from_user.id
    sess = series_sessions.get(uid)

    if not sess or sess.get("stage") != "meta":
        return

    if not m.caption:
        bot.send_message(uid, "❌ Rubuta suna da farashi a caption.")
        return

    try:
        lines = m.caption.strip().split("\n")
        title = lines[0].strip()
        price = int(lines[-1].strip())
    except:
        bot.send_message(uid, "❌ Tsarin caption bai dace ba.")
        return

    poster_file_id = m.photo[-1].file_id

    # ===============================
    # UPLOAD FILES TO STORAGE CHANNEL
    # ===============================
    sess["storage_items"] = []

    for f in sess["files"]:
        msg = bot.send_document(
            STORAGE_CHANNEL,
            f["dm_file_id"],
            caption=f["file_name"]
        )

        doc = msg.document or msg.video

        sess["storage_items"].append({
            "movie_id": msg.message_id,              # CHANNEL MESSAGE ID
            "file_id": doc.file_id,                  # FILE ID FROM CHANNEL
            "file_name": f["file_name"],
            "channel_username": STORAGE_CHANNEL
        })

    # ===============================
    # SAVE TO DATABASE (FROM STORAGE ONLY)
    # ===============================
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO series (title, price, poster_file_id, channel_username)
        VALUES (?,?,?,?)
        """,
        (title, price, poster_file_id, STORAGE_CHANNEL)
    )
    series_id = cur.lastrowid

    hausa_series_id = None

    for item in sess["storage_items"]:
        cur.execute(
            """
            INSERT INTO series_items
            (series_id, movie_id, file_id, file_name, channel_username)
            VALUES (?,?,?,?,?)
            """,
            (
                series_id,
                item["movie_id"],
                item["file_id"],
                item["file_name"],
                item["channel_username"]
            )
        )

        if item["file_name"] in sess.get("hausa_matches", []):
            if not hausa_series_id:
                cur.execute(
                    """
                    INSERT INTO hausa_series (title, price, poster_file_id)
                    VALUES (?,?,?)
                    """,
                    (title, price, poster_file_id)
                )
                hausa_series_id = cur.lastrowid

            cur.execute(
                """
                INSERT INTO hausa_series_items
                (hausa_series_id, movie_id, file_id, file_name)
                VALUES (?,?,?,?)
                """,
                (
                    hausa_series_id,
                    item["movie_id"],
                    item["file_id"],
                    item["file_name"]
                )
            )

    conn.commit()

    # ===============================
    # POST TO PUBLIC CHANNEL (POSTER ONLY)
    # ===============================
    movie_ids = [str(i["movie_id"]) for i in sess["storage_items"]]
    encoded_ids = ",".join(movie_ids)

    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton(
            "🛒 Add to cart",
            callback_data=f"addcart:{series_id}"
        ),
        InlineKeyboardButton(
            "💳 Buy now",
            url=f"https://t.me/{BOT_USERNAME}?start=groupitem_{series_id}"
        )
    )

    bot.send_photo(
        CHANNEL,
        poster_file_id,
        caption=f"🎬 <b>{title}</b>\n💰 ₦{price}",
        parse_mode="HTML",
        reply_markup=kb
    )

    bot.send_message(
        uid,
        "🎉 <b>Series an adana lafiya.</b>\nKomai yana DB daga storage channel.",
        parse_mode="HTML"
    )

    del series_sessions[uid]

# ENDBlock
# ================== RUKUNI A (FINAL) ==================

from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton


# ===== ENTRY POINT =====
@bot.callback_query_handler(func=lambda c: c.data == "search_movie")
def search_movie_entry(c):
    uid = c.from_user.id
    bot.answer_callback_query(c.id)

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🔎 NEMA DA SUNA", callback_data="search_by_name"))
    kb.add(InlineKeyboardButton("🎺 ALGAITA", callback_data="C_algaita_0"))
    kb.add(InlineKeyboardButton("📺 HAUSA SERIES", callback_data="C_hausa_0"))
    kb.add(InlineKeyboardButton("🎞 OTHERS", callback_data="C_others_0"))
    kb.add(InlineKeyboardButton("❌ CANCEL", callback_data="search_cancel"))

    bot.send_message(
        uid,
        "🔍 *SASHEN NEMAN FIM*\nZaɓi yadda kake so:",
        reply_markup=kb,
        parse_mode="Markdown"
    )

# ===== BROWSING ENTRY =====
@bot.callback_query_handler(func=lambda c: c.data == "search_k")
def browsing_entry(c):
    # maida shi aiki iri daya da search_movie
    search_movie_entry(c)
# ===== SEARCH BY NAME =====
@bot.callback_query_handler(func=lambda c: c.data == "search_by_name")
def cb_search_by_name(c):
    uid = c.from_user.id
    bot.answer_callback_query(c.id)

    admin_states[uid] = {"state": "search_wait_name"}

    bot.send_message(
        uid,
        "✍️ RUBUTA *HARAFI 2 KO 3* NA SUNAN FIM\nMisali: *MAS*",
        parse_mode="Markdown"
    )


# ===== CANCEL =====
@bot.callback_query_handler(func=lambda c: c.data == "search_cancel")
def cb_search_cancel(c):
    uid = c.from_user.id
    bot.answer_callback_query(c.id)

    admin_states.pop(uid, None)
    bot.send_message(uid, "❌ An rufe sashen nema.", reply_markup=reply_menu(uid))


# ================== END RUKUNI A ==================


# DUKKAN HANDLERS SUN GAMA ↑↑↑



 
@bot.callback_query_handler(func=lambda c: True)
def handle_callback(c):
    uid = c.from_user.id
    data = c.data or ""

 
# DUKKAN HANDLERS SUN GAMA ↑↑↑

    print("===== CALLBACK TEST =====")
    print("USER:", c.from_user.id)
    print("DATA:", c.data)
    print("=========================")

    bot.answer_callback_query(c.id, "TEST OK 👌")


    # ================= BUY CART (GROUP STYLE) =================
    if data == "checkout":
        rows = get_cart(uid)
        if not rows:
            bot.answer_callback_query(c.id, "❌ Cart ɗinka babu komai.")
            return

        cur = conn.cursor()

        # 🛡️ KAR A KIRKIRI ORDER SAU 2
        old = cur.execute(
            "SELECT id, amount FROM orders WHERE user_id=? AND paid=0",
            (uid,)
        ).fetchone()

        if old:
            order_id = old["id"]
            total = int(old["amount"] or 0)
        else:
            order_id = str(uuid.uuid4())

            total = 0
            for _, _, price, _ in rows:
                total += int(price)

            conn.execute(
                "INSERT INTO orders (id, user_id, movie_id, amount, paid) VALUES (?, ?, NULL, ?, 0)",
                (order_id, uid, total)
            )

            for movie_id, title, price, file_id in rows:
                conn.execute(
                    "INSERT INTO order_items (order_id, movie_id, price) VALUES (?, ?, ?)",
                    (order_id, movie_id, int(price))
                )

            conn.commit()
            clear_cart(uid)

        pay_url = create_flutterwave_payment(
            uid,
            order_id,
            total,
            "Cart Order"
        )

        if not pay_url:
            bot.answer_callback_query(c.id, "❌ Payment error")
            return

        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("💳 PAY NOW", url=pay_url))
        kb.add(InlineKeyboardButton("❌ Cancel", callback_data=f"cancel:{order_id}"))

        bot.send_message(
            uid,
            f"""🧾 <b>CART ORDER</b>

💵 <b>Price:</b> ₦{total}
🎞 <b>Items:</b> {len(rows)}

🧾 <b>Order ID:</b>
<code>{order_id}</code>

⚠️ <i>Ajiye wannan Order ID sosai.</i>

👇 Danna ƙasa domin biya:""",
            parse_mode="HTML",
            reply_markup=kb
        )

        bot.answer_callback_query(c.id)
        return

    # ===== sauran callback handlers naka suna nan ƙasa =====


    
    # ================= BUYDM & BUY =================
    if data.startswith("buydm:") or data.startswith("buy:"):
        mid = int(data.split(":", 1)[1])

        movie = conn.execute(
            "SELECT id, title, price FROM movies WHERE id=?",
            (mid,)
        ).fetchone()

        if not movie:
            bot.answer_callback_query(c.id, "❌ Movie not found")
            return

        movie_id, title, price = movie
        price = int(price or 0)

        # 🛑 KARIYA 1: IDAN YA RIGA YA MALLAKA (DELIVER TABLE)
        owned = conn.execute(
            "SELECT 1 FROM user_movies WHERE user_id=? AND movie_id=? LIMIT 1",
            (uid, movie_id)
        ).fetchone()

        if owned:
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("🎬 My Movies", callback_data="my_movies"))
            bot.send_message(
                uid,
                "✅ <b>Ka riga ka mallaki wannan fim.</b>\n\n👉 Duba <b>Movie History</b> ɗinka.",
                parse_mode="HTML",
                reply_markup=kb
            )
            bot.answer_callback_query(c.id)
            return

        # 🛑 KARIYA 2: HANA ORDER 2 (UNPAID)
        old = conn.execute(
            "SELECT id FROM orders WHERE user_id=? AND movie_id=? AND paid=0",
            (uid, movie_id)
        ).fetchone()

        if old:
            order_id = old[0]
        else:
            order_id = str(uuid.uuid4())

            # ===== ORDERS =====
            conn.execute(
                "INSERT INTO orders (id, user_id, movie_id, amount, paid) VALUES (?, ?, ?, ?, 0)",
                (order_id, uid, movie_id, price)
            )

            # ===== ORDER ITEMS =====
            conn.execute(
                "INSERT INTO order_items (order_id, movie_id, price) VALUES (?, ?, ?)",
                (order_id, movie_id, price)
            )

            # ===== SERIES ITEMS (DOMIN DELIVER) =====
            conn.execute(
                "INSERT INTO series_items (series_id, movie_id) VALUES (?, ?)",
                (movie_id, movie_id)
            )

            # ===== HAUSA SERIES ITEMS (DOMIN DELIVER) =====
            conn.execute(
                "INSERT INTO hausa_series_items (series_id, movie_id) VALUES (?, ?)",
                (movie_id, movie_id)
            )

            conn.commit()

        # ===== CREATE PAYMENT LINK =====
        pay_url = create_flutterwave_payment(uid, order_id, price, title)
        if not pay_url:
            bot.answer_callback_query(c.id, "❌ Payment error")
            return

        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("💳 PAY NOW", url=pay_url))
        kb.add(InlineKeyboardButton("❌ Cancel", callback_data=f"cancel:{order_id}"))

        bot.send_message(
            uid,
            f"""🎬 <b>{title}</b>
💵 ₦{price}

🧾 <b>Order ID:</b>
<code>{order_id}</code>

⚠️ <i>Ajiye wannan Order ID.</i>
""",
            parse_mode="HTML",
            reply_markup=kb
        )

        bot.answer_callback_query(c.id)
        return

    # ================= MY MOVIES =================
    if data == "my_movies":
        kb = InlineKeyboardMarkup()

        # 🆕 KARI: SEARCH BUTTON (BA A CIRE KOMAI BA)
        kb.add(InlineKeyboardButton("🔍 Search movie", callback_data="resend_search"))

        # ===== ASALIN BUTTONS =====
        kb.add(InlineKeyboardButton("🗓 Last 7 days", callback_data="resend:7"))
        kb.add(InlineKeyboardButton("📆 Last 30 days", callback_data="resend:30"))
        kb.add(InlineKeyboardButton("🕰 Last 90 days", callback_data="resend:90"))

        bot.send_message(
            uid,
            "🎬 <b>My Movies</b>\nZa a sake turo maka fina-finan da ka taba siya gwargwadon lokacin da ka zaba.",
            parse_mode="HTML",
            reply_markup=kb
        )
        bot.answer_callback_query(c.id)
        return

    # ================= RESEND MOVIES (ASALI) =================
    if data.startswith("resend:"):
        try:
            days = int(data.split(":")[1])
        except:
            bot.answer_callback_query(c.id, "❌ Invalid time.")
            return

        used = conn.execute(
            "SELECT COUNT(*) FROM resend_logs WHERE user_id=?",
            (uid,)
        ).fetchone()[0]

        if used >= 10:
            bot.send_message(uid, "⚠️ Ka kai iyakar sake karɓa (sau 10).")
            bot.answer_callback_query(c.id)
            return

        rows = conn.execute("""
            SELECT DISTINCT
                um.movie_id,
                m.file_id,
                m.title
            FROM user_movies um
            JOIN movies m ON m.id = um.movie_id
            WHERE um.user_id = ?
              AND um.created_at >= datetime('now', ?)
            ORDER BY um.created_at ASC
        """, (uid, f"-{days} days")).fetchall()

        if not rows:
            bot.send_message(uid, "❌ Babu fim a wannan lokacin.")
            bot.answer_callback_query(c.id)
            return

        sent = 0
        for movie_id, file_id, title in rows:
            try:
                try:
                    bot.send_video(uid, file_id, caption=f"🎬 {title}")
                except:
                    bot.send_document(uid, file_id, caption=f"🎬 {title}")
                sent += 1
            except Exception as e:
                print("Resend error:", e)

        conn.execute(
            "INSERT INTO resend_logs (user_id, used_at) VALUES (?, datetime('now'))",
            (uid,)
        )
        conn.commit()

        bot.send_message(uid, f"✅ An sake tura fina-finai, kasan da cewa sau 10 kawai zaka iya karbar fim, in ya wuce haka saidai ka sake siya.{sent} (kwanaki {days}).")
        bot.answer_callback_query(c.id)
        return

    # ================= 🆕 SEARCH MOVIE =================
    if data == "resend_search":
        bot.send_message(
            uid,
            "🔍 <b>Nemi fim</b>\nRubuta sunan fim (ko wani sashe na daga cikin sunan sa):",
            parse_mode="HTML"
        )
        user_states[uid] = {"action": "resend_search"}
        bot.answer_callback_query(c.id)
        return

    # ================= 🆕 RESEND ONE MOVIE =================
    if data.startswith("resend_one:"):
        try:
            movie_id = int(data.split(":", 1)[1])
        except:
            bot.answer_callback_query(c.id, "❌ Invalid movie.")
            return

        used = conn.execute(
            "SELECT COUNT(*) FROM resend_logs WHERE user_id=?",
            (uid,)
        ).fetchone()[0]

        if used >= 10:
            bot.send_message(uid, "⚠️ Ka kai iyakar sake karɓa (sau 10). Sai ka sake siya.")
            bot.answer_callback_query(c.id)
            return

        row = conn.execute("""
            SELECT m.file_id, m.title
            FROM user_movies um
            JOIN movies m ON m.id = um.movie_id
            WHERE um.user_id=? AND um.movie_id=?
            LIMIT 1
        """, (uid, movie_id)).fetchone()

        if not row:
            bot.answer_callback_query(c.id, "❌ Ba a samu fim ba.")
            return

        file_id, title = row

        try:
            try:
                bot.send_video(uid, file_id, caption=f"🎬 {title}")
            except:
                bot.send_document(uid, file_id, caption=f"🎬 {title}")
        except:
            bot.answer_callback_query(c.id, "❌ Kuskure wajen tura fim.")
            return

        conn.execute(
            "INSERT INTO resend_logs (user_id, used_at) VALUES (?, datetime('now'))",
            (uid,)
        )
        conn.commit()

        bot.answer_callback_query(c.id, "✅ An sake tura muku fim,  kasan da cewa sau 10 kawai zaka karbi kowanne fim da ka taba siya abaya, in ka wuce limit saidai ka sake siya.")
        return


    
     # ================= START SERIES MODE =================
    if data == "start_series":
        series_sessions[uid] = {
            "stage": "collect",
            "files": [],
            "hausa_titles": [],
            "hausa_matches": []
        }

        bot.answer_callback_query(c.id)
        bot.send_message(
            uid,
            "📦 <b>Series mode ya fara</b>\n\n"
            "➡️ Turo dukkan fina-finai (video ko document)\n"
            "➡️ Idan ka gama, rubuta <b>done</b>",
            parse_mode="HTML"
        )
        return


    # ================= ADD SERIES TO CART (DM) =================
    if data.startswith("addcartdm:"):
        try:
            series_id = int(data.split("addcartdm:", 1)[1])
        except:
            bot.answer_callback_query(c.id, "❌ Invalid series")
            return

        already = conn.execute(
            "SELECT 1 FROM cart WHERE user_id=? AND movie_id=? LIMIT 1",
            (uid, series_id)
        ).fetchone()

        if already:
            bot.answer_callback_query(c.id, "⚠️ Tuni yana cikin cart")
            return

        conn.execute(
            "INSERT INTO cart (user_id, movie_id) VALUES (?, ?)",
            (uid, series_id)
        )
        conn.commit()

        bot.answer_callback_query(c.id, "✅ An saka series a cart")
        return
    # ================= ADD SERIES TO CART =================
    if data.startswith("addcart:"):
        try:
            series_id = int(data.split("addcart:", 1)[1])
        except:
            bot.answer_callback_query(c.id, "❌ Invalid series")
            return

        already = conn.execute(
            "SELECT 1 FROM cart WHERE user_id=? AND movie_id=? LIMIT 1",
            (uid, series_id)
        ).fetchone()

        if already:
            bot.answer_callback_query(c.id, "⚠️ Tuni yana cikin cart")
            return

        conn.execute(
            "INSERT INTO cart (user_id, movie_id) VALUES (?, ?)",
            (uid, series_id)
        )
        conn.commit()

        bot.answer_callback_query(c.id, "✅ An saka series a cart")
        return
    
    
    # =====================
    # OPEN UNPAID ORDERS (PAGE 0)
    # =====================
    if data == "myorders_new":
        text, kb = build_unpaid_orders_view(uid, page=0)
        bot.send_message(uid, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(c.id)
        return

    # =====================
    # UNPAID PAGINATION
    # =====================
    if data.startswith("unpaid_next:") or data.startswith("unpaid_prev:"):
        page = int(data.split(":")[1])
        text, kb = build_unpaid_orders_view(uid, page)
        bot.edit_message_text(
            chat_id=uid,
            message_id=c.message.message_id,
            text=text,
            reply_markup=kb,
            parse_mode="HTML"
        )
        bot.answer_callback_query(c.id)
        return

    # =====================
    # REMOVE SINGLE UNPAID
    # =====================
    if data.startswith("remove_unpaid:"):
        oid = data.split(":")[1]
        conn.execute(
            "DELETE FROM orders WHERE id=? AND user_id=? AND paid=0",
            (oid, uid)
        )
        conn.commit()

        text, kb = build_unpaid_orders_view(uid, page=0)
        bot.edit_message_text(
            chat_id=uid,
            message_id=c.message.message_id,
            text=text,
            reply_markup=kb,
            parse_mode="HTML"
        )
        bot.answer_callback_query(c.id, "❌ An cire order")
        return

    # ===============================
    # SERIES MODE (ADMIN ONLY)
    # ===============================
    if data == "groupitems":
        if uid != ADMIN_ID:
            return bot.answer_callback_query(c.id, "groupitems.")

        series_sessions[uid] = {
            "files": [],
            "stage": "collect"
        }

        bot.send_message(
            uid,
            "📺 <b>Series Mode ya fara</b>\n\n"
            "Ka fara turo videos/documents.\n"
            "Idan ka gama rubuta <b>Done</b>.",
            parse_mode="HTML"
        )
        bot.answer_callback_query(c.id)
        return
    # =====================
    # DELETE ALL UNPAID
    # =====================
    if data == "delete_unpaid":
        conn.execute(
            "DELETE FROM orders WHERE user_id=? AND paid=0",
            (uid,)
        )
        conn.commit()

        text, kb = build_unpaid_orders_view(uid, page=0)
        bot.edit_message_text(
            chat_id=uid,
            message_id=c.message.message_id,
            text=text,
            reply_markup=kb,
            parse_mode="HTML"
        )
        bot.answer_callback_query(c.id, "🗑 Duk an goge")
        return

    # =====================
    # OPEN PAID ORDERS (PAGE 0)
    # =====================
    if data == "paid_orders":
        text, kb = build_paid_orders_view(uid, page=0)
        bot.edit_message_text(
            chat_id=uid,
            message_id=c.message.message_id,
            text=text,
            reply_markup=kb,
            parse_mode="HTML"
        )
        bot.answer_callback_query(c.id)
        return

    # =====================
    # PAID PAGINATION
    # =====================
    if data.startswith("paid_next:") or data.startswith("paid_prev:"):
        page = int(data.split(":")[1])
        text, kb = build_paid_orders_view(uid, page)
        bot.edit_message_text(
            chat_id=uid,
            message_id=c.message.message_id,
            text=text,
            reply_markup=kb,
            parse_mode="HTML"
        )
        bot.answer_callback_query(c.id)
        return
    

    


    
    # ================= FEEDBACK =================
    if data.startswith("feedback:"):
        parts = data.split(":")
        if len(parts) != 3:
            bot.answer_callback_query(c.id)
            return

        mood, order_id = parts[1], parts[2]

        # 1️⃣ Tabbatar order paid ne kuma na user
        row = conn.execute(
            "SELECT 1 FROM orders WHERE id=? AND user_id=? AND paid=1",
            (order_id, uid)
        ).fetchone()
        if not row:
            bot.answer_callback_query(c.id, "⚠️ Wannan order ba naka bane.", show_alert=True)
            return

        # 2️⃣ Hana feedback sau biyu
        exists = conn.execute(
            "SELECT 1 FROM feedbacks WHERE order_id=?",
            (order_id,)
        ).fetchone()
        if exists:
            bot.answer_callback_query(c.id, "Ka riga ka bada ra'ayi.", show_alert=True)
            return

        # 3️⃣ Ajiye feedback
        conn.execute(
            "INSERT INTO feedbacks (order_id, user_id, mood) VALUES (?,?,?)",
            (order_id, uid, mood)
        )
        conn.commit()

        # 4️⃣ Samo sunan user
        try:
            chat = bot.get_chat(uid)
            fname = chat.first_name or "User"
        except:
            fname = "User"

        admin_messages = {
            "very": (
                "😘 Gaskiya na ji daɗin siyayya da bot ɗinku\n"
                "Alhamdulillah wannan bot yana sauƙaƙa siyan fim sosai 😇\n"
                "Muna godiya ƙwarai 🥰🙏"
            ),
            "good": (
                "🙂 Na ji daɗin siyayya\n"
                "Tsarin bot ɗin yana da kyau kuma mai sauƙi"
            ),
            "neutral": (
                "😓 Ban gama fahimtar bot ɗin sosai ba\n"
                "Amma ina ganin yana da amfani"
            ),
            "angry": (
                "🤬 Wannan bot yana bani ciwon kai\n"
                "Akwai buƙatar ku gyara tsarin kasuwancin ku"
            )
        }

        user_replies = {
            "very": "🥰 Mun gode sosai! Za mu ci gaba da faranta maka rai Insha Allah.",
            "good": "😊 Mun gode da ra'ayinka! Za mu ƙara inganta tsarin.",
            "neutral": "🤍 Mun gode. Idan kana da shawara, muna maraba da ita.",
            "angry": "🙏 Muna baku haƙuri akan bacin ran da kuka samu. Za mu gyara Insha Allah."
        }

        # 5️⃣ Tura wa ADMIN
        admin_text = (
            f"📣 FEEDBACK RECEIVED\n\n"
            f"👤 User: {fname}\n"
            f"🆔 ID: {uid}\n"
            f"📦 Order: {order_id}\n\n"
            f"{admin_messages.get(mood, mood)}"
        )
        try:
            bot.send_message(ADMIN_ID, admin_text)
        except:
            pass

        # 6️⃣ Goge inline buttons
        try:
            bot.edit_message_reply_markup(
                chat_id=c.message.chat.id,
                message_id=c.message.message_id,
                reply_markup=None
            )
        except:
            pass

        bot.answer_callback_query(c.id)
        bot.send_message(uid, user_replies.get(mood, "Mun gode da ra'ayinka 🙏"))
        return
    # =====================
    # ADD TO CART
    # =====================
    if data.startswith("addcart:"):
        try:
            mid = int(data.split(":",1)[1])
        except:
            bot.answer_callback_query(c.id, "Invalid movie id.")
            return

        ok = add_to_cart(uid, mid)
        if ok:
            bot.answer_callback_query(c.id, "An saka a cart.")
        else:
            bot.answer_callback_query(c.id, "An riga an saka.")
        return

    

  
    # =====================  
    # VIEW CART (SEND + SAVE MESSAGE)  
    # =====================  
    if data == "viewcart":  
        text, kb = build_cart_view(uid)  
  
        msg = bot.send_message(  
            uid,  
            text,  
            reply_markup=kb,  
            parse_mode="HTML"  
        )  
  
        # adana cart message_id  
        cart_sessions[uid] = msg.message_id  
  
        bot.answer_callback_query(c.id)  
        return  
  
    # =====================  
    # REMOVE FROM CART (EDIT)  
    # =====================  
    if data.startswith("removecart:"):  
        try:  
            mid = int(data.split(":", 1)[1])  
        except:  
            bot.answer_callback_query(c.id, "❌ Invalid movie id")  
            return  
  
        row = conn.execute(  
            "SELECT 1 FROM cart WHERE user_id=? AND movie_id=?",  
            (uid, mid)  
        ).fetchone()  
  
        if not row:  
            bot.answer_callback_query(c.id, "⚠️ Ba a samu ba")  
            return  
  
        conn.execute(  
            "DELETE FROM cart WHERE user_id=? AND movie_id=?",  
            (uid, mid)  
        )  
        conn.commit()  
  
        bot.answer_callback_query(c.id, "✅ An cire daga cart")  
  
        # EDIT cart message  
        msg_id = cart_sessions.get(uid)  
        if msg_id:  
            text, kb = build_cart_view(uid)  
            bot.edit_message_text(  
                chat_id=uid,  
                message_id=msg_id,  
                text=text,  
                reply_markup=kb,  
                parse_mode="HTML"  
            )  
        return  
  
    # =====================  
    # CLEAR CART (EDIT)  
    # =====================  
    if data == "clearcart":  
        conn.execute(  
            "DELETE FROM cart WHERE user_id=?",  
            (uid,)  
        )  
        conn.commit()  
  
        bot.answer_callback_query(c.id, "🧹 An goge cart")  
  
        msg_id = cart_sessions.get(uid)  
        if msg_id:  
            text, kb = build_cart_view(uid)  
            bot.edit_message_text(  
                chat_id=uid,  
                message_id=msg_id,  
                text=text,  
                reply_markup=kb,  
                parse_mode="HTML"  
            )  
        return

    

   
    # =====================
    # ADD MOVIE (ADMIN)
    # =====================
    if data == "addmovie":
        if uid != ADMIN_ID:
            bot.answer_callback_query(c.id, "Only admin.")
            return
        admin_states[uid] = {"state": "add_movie_wait_file"}
        bot.send_message(uid, "Turo film.")
        bot.answer_callback_query(c.id)
        return
    # =====================
    # WEEKLY BUY
    # =====================
    if data.startswith("weekly_buy:"):
        try:
            idx = int(data.split(":",1)[1])
        except:
            bot.answer_callback_query(c.id, "Invalid.")
            return

        row = conn.execute(
            "SELECT items FROM weekly ORDER BY id DESC LIMIT 1"
        ).fetchone()

        items = json.loads(row[0] or "[]")
        item = items[idx]

        title = item["title"]
        price = int(item["price"])

        remaining_price, applied_sum, applied_ids = apply_credits_to_amount(uid, price)
        order_id = create_single_order_for_weekly(uid, title, remaining_price)

        bot.send_message(uid, f"Oda {order_id} – ₦{remaining_price}")
        bot.answer_callback_query(c.id)
        return
    # ===== ALL FILMS OPEN =====
    if data == "all_films":
        cur = conn.execute(
            "SELECT id, title, price FROM movies ORDER BY id DESC"
        )
        rows = cur.fetchall()

        if not rows:
            bot.answer_callback_query(c.id, "❌ Babu fim a DB")
            return

        pages = paginate(rows, PER_PAGE)

        allfilms_sessions[uid] = {
            "pages": pages,
            "index": 0,
            "last_msg": None
        }

        send_allfilms_page(uid, 0)
        bot.answer_callback_query(c.id)
        return

    # ===== NEXT =====
    if data == "allfilms_next":
        sess = allfilms_sessions.get(uid)
        if not sess:
            bot.answer_callback_query(c.id)
            return

        new_idx = sess["index"] + 1
        if new_idx >= len(sess["pages"]):
            bot.answer_callback_query(c.id)
            return

        send_allfilms_page(uid, new_idx)
        bot.answer_callback_query(c.id)
        return

    # ===== PREV =====
    if data == "allfilms_prev":
        sess = allfilms_sessions.get(uid)
        if not sess:
            bot.answer_callback_query(c.id)
            return

        new_idx = sess["index"] - 1
        if new_idx < 0:
            bot.answer_callback_query(c.id)
            return

        send_allfilms_page(uid, new_idx)
        bot.answer_callback_query(c.id)
        return

    
 # Map new erase_all_data callback to existing erase_data handler (compat shim)
    if data == "erase_all_data":
        data = "erase_data"


    # NEW WEAK UPDATE SYSTEM
    if data == "weak_update":
        start_weak_update(msg=c.message)
        return
    # checkjoin: after user clicks I've Joined, prompt language selection
    if data == "checkjoin":
        try:
            if check_join(uid):
                bot.answer_callback_query(callback_query_id=c.id, text=tr_user(uid, "joined_ok", default="✔ An shiga channel!"))
                # prompt language selection now
                kb = InlineKeyboardMarkup()
                kb.add(InlineKeyboardButton("English", callback_data="setlang_en"),
                       InlineKeyboardButton("Françe", callback_data="setlang_fr"))
                kb.add(InlineKeyboardButton("Hausa", callback_data="setlang_ha"),
                       InlineKeyboardButton("Igbo", callback_data="setlang_ig"))
                kb.add(InlineKeyboardButton("Yaruba", callback_data="setlang_yo"),
                       InlineKeyboardButton("Fulani/Fulfulde", callback_data="setlang_ff"))
                bot.send_message(uid, tr_user(uid, "choose_language_prompt", default="Choose language / Zaɓi harshe:"), reply_markup=kb)
            else:
                bot.answer_callback_query(callback_query_id=c.id, text=tr_user(uid, "not_joined", default="❌ Baka shiga ba."))
        except Exception as e:
            print("checkjoin callback error:", e)
        return

    # show change language menu (global button)
    if data == "change_language":
        try:
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("English", callback_data="setlang_en"),
                   InlineKeyboardButton("Françe", callback_data="setlang_fr"))
            kb.add(InlineKeyboardButton("Hausa", callback_data="setlang_ha"),
                   InlineKeyboardButton("Igbo", callback_data="setlang_ig"))
            kb.add(InlineKeyboardButton("Yaruba", callback_data="setlang_yo"),
                   InlineKeyboardButton("Fulani/Fulfulde", callback_data="setlang_ff"))
            bot.answer_callback_query(callback_query_id=c.id)
            bot.send_message(uid, tr_user(uid, "choose_language_prompt", default="Choose language / Zaɓi harshe:"), reply_markup=kb)
        except Exception as e:
            print("change_language callback error:", e)
        return

    # set language callbacks
    if data.startswith("setlang_"):
        lang = data.split("_",1)[1]
        set_user_lang(uid, lang)
        # If Hausa selected, keep original Hausa text
        if lang == "ha":
            bot.answer_callback_query(callback_query_id=c.id, text="An saita Hausa. (Ba a canza rubutu Hausa ba.)")
            bot.send_message(uid, "Abokin kasuwanci barka da zuwa shagon fina finai:", reply_markup=user_main_menu(uid))
            bot.send_message(uid, "Sannu da zuwa!\n Me kake bukata?:", reply_markup=reply_menu(uid))
            return
        # for other languages, use translations where available
        welcome = tr_user(uid, "welcome_shop", default="Abokin kasuwanci barka da zuwa shagon fina finai:")
        ask = tr_user(uid, "ask_name", default="Sannu da zuwa!\n Me kake bukata?:")
        bot.answer_callback_query(callback_query_id=c.id, text=tr_user(uid, "language_set_success", default="Language set."))
        bot.send_message(uid, welcome, reply_markup=user_main_menu(uid))
        bot.send_message(uid, ask, reply_markup=reply_menu(uid))
        return

    # go home
    if data == "go_home":
        try:
            bot.answer_callback_query(callback_query_id=c.id)
            bot.send_message(uid, "Sannu! Ga zabuka, domin fara wa:", reply_markup=reply_menu(uid))
        except:
            pass
        return

    if data == "invite":
        try:
            bot_info = bot.get_me()
            bot_username = bot_info.username if bot_info and getattr(bot_info, "username", None) else None
        except:
            bot_username = None
        if bot_username:
            ref_link = f"https://t.me/{bot_username}?start=ref{uid}"
            share_url = "https://t.me/share/url?"+urllib.parse.urlencode({
                "url": ref_link,
                "text": f"Gayyato ni zuwa wannan bot: {ref_link}\nJoin channel: https://t.me/{CHANNEL.lstrip('@')}\nKa samu lada idan wanda ka gayyata yayi join sannan ya siya fim 3×."
            })
        else:
            ref_link = f"/start ref{uid}"
            share_url = f"https://t.me/{CHANNEL.lstrip('@')}"
        text = (
            "Gayyato abokanka👨‍👨‍👦‍👦 suyi join domin samun GARABASA!🎁\n\n"
            "Ka tura musu wannan link ɗin.\n\n"
            "Idan wanda ka gayyata ya shiga channel ɗinmu kuma ya sayi fim uku, za'a baka N200🎊🎉\n"
            "10 friends N2000😲🥳🤑\n(yi amfani Kyautar wajen sayen fim).\n\n"
            "Danne alamar COPY karka daga zaka samu damar kofe link din ka, ko!\n"
            "ka taba 📤SHARE kai tsaye"
        )
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("🔗 Copy / Open Link", url=ref_link))
        kb.add(InlineKeyboardButton("📤 Share", url=share_url))
        kb.row(InlineKeyboardButton("👥 My referrals", callback_data="my_referrals"),
               InlineKeyboardButton("💰 My credits", callback_data="my_credits"))
        kb.row(InlineKeyboardButton(" ⤴️ KOMA FARKO", callback_data="go_home"),
               InlineKeyboardButton("🫂Our Channel", url=f"https://t.me/{CHANNEL.lstrip('@')}"))
        change_label = tr_user(uid, "change_language_button", default="🌐 Change your language")
        kb.row(InlineKeyboardButton(change_label, callback_data="change_language"))
        bot.answer_callback_query(callback_query_id=c.id)
        bot.send_message(uid, text, reply_markup=kb)
        return

    if data == "my_referrals":
        rows = get_referrals_by_referrer(uid)
        if not rows:
            bot.answer_callback_query(callback_query_id=c.id, text="Babu wanda ka gayyata tukuna.")
            bot.send_message(uid, "Babu wanda ka gayyata tukuna.", reply_markup=reply_menu(uid))
            return
        text = "Mutanen da ka gayyata:\n\n"
        for referred_id, created_at, reward_granted, rowid in rows:
            name = None
            try:
                chat = bot.get_chat(referred_id)
                fname = getattr(chat, "first_name", "") or ""
                uname = getattr(chat, "username", None)
                if uname:
                    name = "@" + uname
                elif fname:
                    name = fname
            except:
                s = str(referred_id)
                name = s[:3] + "*"*(len(s)-6) + s[-3:] if len(s) > 6 else "User"+s[-4:]
            status = "+reward success" if reward_granted else "pending👀"
            text += f"• {name} — {status}\n"
        kb = InlineKeyboardMarkup()
        kb.row(InlineKeyboardButton(" ⤴️ KOMA FARKO", callback_data="go_home"),
               InlineKeyboardButton("🫂Our Channel", url=f"https://t.me/{CHANNEL.lstrip('@')}"))
        change_label = tr_user(uid, "change_language_button", default="🌐 Change your language")
        kb.row(InlineKeyboardButton(change_label, callback_data="change_language"))
        bot.answer_callback_query(callback_query_id=c.id)
        bot.send_message(uid, text, reply_markup=kb)
        return

    if data == "my_credits":
        total, rows = get_credits_for_user(uid)
        text = f"Total available credit: N{total}\n\n"
        for cid, amount, used, granted_at in rows:
            text += f"• ID:{cid} — N{amount} — {'USED' if used else 'AVAILABLE'} — {granted_at}\n"
        kb = InlineKeyboardMarkup()
        kb.row(InlineKeyboardButton(" ⤴️ KOMA FARKO", callback_data="go_home"),
               InlineKeyboardButton("🫂Our Channel", url=f"https://t.me/{CHANNEL.lstrip('@')}"))
        change_label = tr_user(uid, "change_language_button", default="🌐 Change your language")
        kb.row(InlineKeyboardButton(change_label, callback_data="change_language"))
        bot.answer_callback_query(callback_query_id=c.id)
        bot.send_message(uid, text, reply_markup=kb)
        return

   

    # Support Help -> Open admin DM directly (NO messages to admin, NO notifications)
    if data == "support_help":
        try:
            bot.answer_callback_query(callback_query_id=c.id)
        except:
            pass

        if ADMIN_USERNAME:
            # Open admin DM directly
            bot.send_message(uid, f"👉 Tuntuɓi admin kai tsaye: https://t.me/{ADMIN_USERNAME}")
        else:
            bot.send_message(uid, "Admin username bai sa ba. Tuntubi support.")
        return

 
    # fallback
    try:
        bot.answer_callback_query(callback_query_id=c.id)
    except:
        pass





# ========== /myorders command ==========
@bot.message_handler(commands=["myorders"])
def myorders(message):
    uid = message.from_user.id
    rows = conn.execute("SELECT id,movie_id,amount,paid FROM orders WHERE user_id=?", (uid,)).fetchall()
    if not rows:
        bot.reply_to(message, "Babu odarka tukuna.", reply_markup=reply_menu(uid))
        return
    txt = "Your orders:\n"
    for oid, mid, amount, paid in rows:
        if mid == -1:
            items = conn.execute("SELECT movie_id,price FROM order_items WHERE order_id=?", (oid,)).fetchall()
            txt += f"• Order Group {oid} — ₦{amount} — {'Paid' if paid else 'Unpaid'}\n"
            for m_id, price in items:
                title = conn.execute("SELECT title FROM movies WHERE id=?", (m_id,)).fetchone()
                title_text = title[0] if title else "Unknown"
                txt += f"   - {title_text} — ₦{price}\n"
        else:
            title = conn.execute("SELECT title FROM movies WHERE id=?", (mid,)).fetchone()
            title_text = title[0] if title else "Unknown"
            txt += f"• {title_text} — ₦{amount} — {'Paid' if paid else 'Unpaid'}\n"
    bot.send_message(uid, txt, reply_markup=reply_menu(uid))

# ========== ADMIN FILE UPLOAD (regular movie upload) ==========
@bot.message_handler(content_types=["photo", "video", "document"])
def file_upload(message):
    if message.from_user.id in ADMINS and admin_states.get(message.from_user.id):
        try:
            admin_inputs(message)
        except Exception as e:
            print("admin_inputs error while in file_upload:", e)
        return
    chat_username = getattr(message.chat, "username", None)
    if chat_username and ("@" + chat_username).lower() == CHANNEL.lower():
        caption = message.caption or (getattr(message, "caption_html", None) or "") or (getattr(message, "caption_markdown", None) or "")
        if not caption:
            caption = getattr(message, "text", "") or ""
        title, price = parse_caption_for_title_price(caption)
        if not title:
            title = (message.caption or getattr(message, "caption_html", "") or getattr(message, "caption_markdown", "")).strip() or f"Film {uuid.uuid4().hex[:6]}"
            price = 0
        file_id = None
        if message.content_type == "photo":
            file_id = message.photo[-1].file_id
        elif message.content_type == "video":
            file_id = message.video.file_id
        elif message.content_type == "document":
            file_id = message.document.file_id
        try:
            exists = conn.execute("SELECT id FROM movies WHERE title=? COLLATE NOCASE", (title,)).fetchone()
            if not exists:
                now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                channel_msg_id = getattr(message, "message_id", None)
                conn.execute("INSERT INTO movies(title,price,file_id,created_at,channel_msg_id,channel_username) VALUES(?,?,?,?,?,?)",
                             (title, price or 0, file_id, now, channel_msg_id, chat_username))
                conn.commit()
                print("Auto-saved channel post to movies:", title)
                prune_old_movies()
        except Exception as e:
            print("error saving channel post to db:", e)
        return
    # Non-admin uploads are ignored here. Admin flows are handled via admin_inputs when admin_states active.
    if message.from_user.id != ADMIN_ID:
        return
    if message.content_type == "photo":
        file_id = message.photo[-1].file_id
    elif message.content_type == "video":
        file_id = message.video.file_id
    else:
        file_id = message.document.file_id
    try:
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        cur = conn.execute(
            "INSERT INTO movies(title,price,file_id,created_at) VALUES(?,?,?,?)",
            (title, price, file_id, now)
        )
        conn.commit()
        movie_id = cur.lastrowid
    except Exception as e:
        print("db insert movie error:", e)
        bot.reply_to(message, "Failed to save movie to database.")
        return
    post_caption = f"🎬 <b>{title}</b>\n💵 ₦{price}\nTap buttons to buy or add to cart."
    markup = movie_buttons_inline(movie_id, user_id=None)
    try:
        sent_msg = None
        if message.content_type == "photo":
            sent_msg = bot.send_photo(CHANNEL, file_id, caption=post_caption, parse_mode="HTML", reply_markup=markup)
        elif message.content_type == "video":
            sent_msg = bot.send_video(CHANNEL, file_id, caption=post_caption, parse_mode="HTML", reply_markup=markup)
        else:
            sent_msg = bot.send_document(CHANNEL, file_id, caption=post_caption, parse_mode="HTML", reply_markup=markup)
        bot.reply_to(message, f"Posted to {CHANNEL} with buttons. Movie id: {movie_id}")
        try:
            channel_msg_id = sent_msg.message_id if sent_msg else None
            channel_username = CHANNEL.lstrip("@")
            conn.execute("UPDATE movies SET channel_msg_id=?, channel_username=? WHERE id=?", (channel_msg_id, channel_username, movie_id))
            conn.commit()
        except Exception as e:
            print("failed to store channel msg id:", e)
        prune_old_movies()
    except Exception as e:
        print("post to channel error:", e)
        bot.reply_to(message, f"Saved locally (ID: {movie_id}) but failed to post to channel. Error: {e}")

# ========== MISSING HELPERS (basic implementations if not present) ==========
# These are minimal implementations so the bot runs. If you already have your own versions, replace them.

def get_cart(user_id):
    rows = conn.execute("SELECT c.movie_id, m.title, m.price, m.file_id FROM cart c JOIN movies m ON c.movie_id=m.id WHERE c.user_id=?", (user_id,)).fetchall()
    return [(r[0], r[1], r[2], r[3]) for r in rows]

def add_to_cart(user_id, movie_id):
    exists = conn.execute("SELECT id FROM cart WHERE user_id=? AND movie_id=?", (user_id, movie_id)).fetchone()
    if exists:
        return False
    conn.execute("INSERT INTO cart(user_id,movie_id) VALUES(?,?)", (user_id, movie_id))
    conn.commit()
    return True

def remove_from_cart(user_id, movie_id):
    cur = conn.execute("DELETE FROM cart WHERE user_id=? AND movie_id=?", (user_id, movie_id))
    conn.commit()
    return cur.rowcount > 0

def clear_cart(user_id):
    cur = conn.execute("DELETE FROM cart WHERE user_id=?", (user_id,))
    conn.commit()
    return True

def create_group_order(user_id, items):
    # items: list of {"movie_id":id,"price":price}
    order_id = str(uuid.uuid4())
    total = sum(i["price"] for i in items)
    try:
        conn.execute("INSERT INTO orders(id,user_id,movie_id,amount,paid) VALUES(?,?,?,?,0)", (order_id, user_id, -1, total))
        for it in items:
            conn.execute("INSERT INTO order_items(order_id,movie_id,price) VALUES(?,?,?)", (order_id, it["movie_id"], it["price"]))
        conn.commit()
        return order_id, total
    except Exception as e:
        print("create_group_order error:", e)
        return None, 0

def create_single_order_for_weekly(user_id, title, amount):
    order_id = str(uuid.uuid4())
    try:
        conn.execute("INSERT INTO orders(id,user_id,movie_id,amount,paid) VALUES(?,?,?,?,0)", (order_id, user_id, -1, amount))
        conn.commit()
        return order_id
    except Exception as e:
        print("create_single_order_for_weekly error:", e)
        return None

def save_weekly(poster_file_id, items_for_db, channel_msg_id=None):
    try:
        items_json = json.dumps(items_for_db)
        conn.execute("INSERT INTO weekly(poster_file_id,items,channel_msg_id) VALUES(?,?,?)", (poster_file_id, items_json, channel_msg_id))
        conn.commit()
    except Exception as e:
        print("save_weekly error:", e)




# ===================== BUY ALL (CUSTOM IDS) =====================
@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("buyall:"))
def buy_all_handler(c):
    uid = c.from_user.id

    try:
        ids_raw = c.data.split("buyall:", 1)[1]
        movie_ids = [int(x) for x in ids_raw.split(",") if x.strip().isdigit()]
    except:
        bot.answer_callback_query(c.id, "Invalid BUY ALL data.")
        return

    if not movie_ids:
        bot.answer_callback_query(c.id, "No movies selected.")
        return

    items = []

    for mid in movie_ids:
        row = conn.execute(
            "SELECT id, title, price FROM movies WHERE id=?",
            (mid,)
        ).fetchone()
        if row:
            _id, title, price = row
            items.append({
                "id": _id,
                "title": title,
                "price": int(price or 0)
            })

    if not items:
        bot.answer_callback_query(c.id, "Movies not found.")
        return

    _create_and_send_buyall(uid, items, c)


# ===================== BUY ALL WEEKLY =====================
@bot.callback_query_handler(func=lambda c: c.data == "buyall_week")
def buy_weekly_handler(c):
    uid = c.from_user.id

    row = conn.execute(
        "SELECT items FROM weekly ORDER BY id DESC LIMIT 1"
    ).fetchone()

    if not row or not row[0]:
        bot.answer_callback_query(c.id, "No weekly movies.")
        return

    try:
        movie_ids = [int(x) for x in row[0].split(",") if x.strip().isdigit()]
    except:
        bot.answer_callback_query(c.id, "Weekly list error.")
        return

    items = []

    for mid in movie_ids:
        row = conn.execute(
            "SELECT id, title, price FROM movies WHERE id=?",
            (mid,)
        ).fetchone()
        if row:
            _id, title, price = row
            items.append({
                "id": _id,
                "title": title,
                "price": int(price or 0)
            })

    if not items:
        bot.answer_callback_query(c.id, "Movies not found.")
        return

    _create_and_send_buyall(uid, items, c)


# ===================== COMMON BUY ALL LOGIC =====================
def _create_and_send_buyall(uid, items, c):
    movie_count = len(items)
    total = sum(i["price"] for i in items)

    discount = int(total * 0.10) if movie_count >= 10 else 0
    final_total = total - discount

    # 🔒 KARIYA: UNPAID BUY-ALL
    old = conn.execute(
        "SELECT id FROM orders WHERE user_id=? AND movie_id IS NULL AND paid=0",
        (uid,)
    ).fetchone()

    if old:
        order_id = old[0]
    else:
        order_id = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO orders (id, user_id, movie_id, amount, paid) VALUES (?, ?, NULL, ?, 0)",
            (order_id, uid, final_total)
        )

        for it in items:
            conn.execute(
                "INSERT INTO order_items (order_id, movie_id, price) VALUES (?, ?, ?)",
                (order_id, it["id"], it["price"])
            )

        conn.commit()

    # 💳 PAYMENT LINK (A NAN NE YAKE)
    pay_url = create_flutterwave_payment(uid, order_id, final_total, "Buy All Movies")
    if not pay_url:
        bot.answer_callback_query(c.id, "Payment error.")
        return

    lines = [f"🎬 {i['title']} — ₦{i['price']}" for i in items]
    summary = "\n".join(lines)

    text = f"""🧾 <b>BUY ALL ORDER</b>

{summary}

🎞 <b>Movies:</b> {movie_count}
💵 <b>Total:</b> ₦{total}
🏷 <b>Discount:</b> ₦{discount}
✅ <b>Final:</b> ₦{final_total}

🧾 <b>Order ID:</b>
<code>{order_id}</code>

⚠️ <i>Ajiye wannan Order ID.</i>
"""

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("💳 PAY NOW", url=pay_url))
    kb.add(InlineKeyboardButton("❌ Cancel Order", callback_data=f"cancel:{order_id}"))

    bot.send_message(uid, text, parse_mode="HTML", reply_markup=kb)
    bot.answer_callback_query(c.id)




# Cancel order handler (also works from confirm stage)
# ================= CANCEL ORDER =================
@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("cancel:"))
def handle_cancel_order_cb(c):
    uid = c.from_user.id
    order_id = c.data.split(":", 1)[1]

    # ❌ goge order items
    conn.execute(
        "DELETE FROM order_items WHERE order_id=?",
        (order_id,)
    )

    # ❌ goge hausa series items (da kariyar user)
    conn.execute(
        "DELETE FROM hausa_series_items WHERE order_id=? AND user_id=?",
        (order_id, uid)
    )

    # ❌ goge series items (da kariyar user)
    conn.execute(
        "DELETE FROM series_items WHERE order_id=? AND user_id=?",
        (order_id, uid)
    )

    # ❌ goge order din kansa
    conn.execute(
        "DELETE FROM orders WHERE id=? AND user_id=? AND paid=0",
        (order_id, uid)
    )

    conn.commit()

    bot.answer_callback_query(c.id, "Order an soke ❌")
    bot.send_message(uid, "✅ An soke order ɗinka.")


   # ================== SALES REPORT SYSTEM (PAYMENTS BASED) ==================

import threading
import time
from datetime import datetime, timedelta

def _ng_now():
    return datetime.utcnow() + timedelta(hours=1)

def _last_day_of_month(dt):
    next_month = dt.replace(day=28) + timedelta(days=4)
    return (next_month - timedelta(days=next_month.day)).day


def send_weekly_sales_report():
    try:
        if not PAYMENT_NOTIFY_GROUP:
            return

        now = _ng_now()
        week_ago = now - timedelta(days=7)

        rows = conn.execute(
            """
            SELECT movie_id, COUNT(*) AS qty, SUM(amount) AS total
            FROM payments
            WHERE status='paid' AND created_at >= ?
            GROUP BY movie_id
            """,
            (week_ago.strftime("%Y-%m-%d %H:%M:%S"),)
        ).fetchall()

        if not rows:
            bot.send_message(
                PAYMENT_NOTIFY_GROUP,
                "📊 WEEKLY SALES REPORT\n\nBabu siyarwa."
            )
            return

        msg = "📊 WEEKLY SALES REPORT\n\n"
        grand = 0

        for movie_id, qty, total in rows:
            title = conn.execute(
                "SELECT title FROM movies WHERE id=?",
                (movie_id,)
            ).fetchone()[0]

            total = total or 0
            grand += total
            msg += f"• {title} ({qty}) — ₦{int(total)}\n"

        msg += f"\n💰 Total: ₦{int(grand)}"
        bot.send_message(PAYMENT_NOTIFY_GROUP, msg)

    except Exception as e:
        print("weekly report error:", e)


def send_monthly_sales_report():
    try:
        if not PAYMENT_NOTIFY_GROUP:
            return

        now = _ng_now()
        first_day = now.replace(day=1, hour=0, minute=0, second=0)

        rows = conn.execute(
            """
            SELECT movie_id, COUNT(*) AS qty, SUM(amount) AS total
            FROM payments
            WHERE status='paid' AND created_at >= ?
            GROUP BY movie_id
            """,
            (first_day.strftime("%Y-%m-%d %H:%M:%S"),)
        ).fetchall()

        if not rows:
            bot.send_message(
                PAYMENT_NOTIFY_GROUP,
                "📊 MONTHLY SALES REPORT\n\nBabu siyarwa."
            )
            return

        msg = "📊 MONTHLY SALES REPORT\n\n"
        grand = 0

        for movie_id, qty, total in rows:
            title = conn.execute(
                "SELECT title FROM movies WHERE id=?",
                (movie_id,)
            ).fetchone()[0]

            total = total or 0
            grand += total
            msg += f"• {title} ({qty}) — ₦{int(total)}\n"

        msg += f"\n💰 Total: ₦{int(grand)}"
        bot.send_message(PAYMENT_NOTIFY_GROUP, msg)

    except Exception as e:
        print("monthly report error:", e)


def sales_report_scheduler():
    weekly_sent = False
    monthly_sent = False

    while True:
        now = _ng_now()

        # Friday 23:50
        if now.weekday() == 4 and now.hour == 23 and now.minute == 50:
            if not weekly_sent:
                send_weekly_sales_report()
                weekly_sent = True
        else:
            weekly_sent = False

        # Last day of month 23:50
        if now.day == _last_day_of_month(now) and now.hour == 23 and now.minute == 50:
            if not monthly_sent:
                send_monthly_sales_report()
                monthly_sent = True
        else:
            monthly_sent = False

        time.sleep(20)


# ▶️ START BACKGROUND REPORT THREAD
# ================== START SERVER ==================
if __name__ == "__main__":

    if BOT_MODE == "webhook":
        print("🌐 Running in WEBHOOK mode")

        try:
            bot.remove_webhook()
            bot.set_webhook(f"{WEBHOOK_URL}/telegram")
            print("✅ Telegram webhook set successfully")
        except Exception as e:
            print("❌ Failed to set webhook:", e)

        port = int(os.environ.get("PORT", 10000))
        print(f"🚀 Flask server running on port {port}")
        app.run(host="0.0.0.0", port=port)

    else:
        # fallback (local testing only)
        print("🤖 Running in POLLING mode")
        bot.infinity_polling(skip_pending=True)
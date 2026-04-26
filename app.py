"""
✈️ Flight Alert Bot — Backend
Arranca con: python3 app.py --port 8081
"""
import json, sqlite3, threading, logging, smtplib, time, schedule, requests, argparse, os
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean, median
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from flask import Flask, request, jsonify, render_template

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("flight_bot.log")])
log = logging.getLogger(__name__)

DB = Path("bot.db")
app = Flask(__name__)

# ── DB ────────────────────────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT, origin TEXT, destinations TEXT, dates TEXT,
            adults INTEGER DEFAULT 1, max_stops INTEGER DEFAULT 1,
            dep_from TEXT DEFAULT '06:00', dep_to TEXT DEFAULT '22:00',
            arr_from TEXT DEFAULT '06:00', arr_to TEXT DEFAULT '23:59',
            max_price REAL, enabled INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS price_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_id INTEGER, origin TEXT, destination TEXT, dep_date TEXT,
            price REAL, currency TEXT, airline TEXT,
            duration TEXT, stops INTEGER, dep_time TEXT, arr_time TEXT,
            checked_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_id INTEGER, origin TEXT, destination TEXT,
            dep_date TEXT, price REAL, reason TEXT, channel TEXT,
            sent_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        INSERT OR IGNORE INTO settings VALUES ('user_name','Eduardo');
        INSERT OR IGNORE INTO settings VALUES ('currency','EUR');
        INSERT OR IGNORE INTO settings VALUES ('check_interval','60');
        INSERT OR IGNORE INTO settings VALUES ('cheap_percentile','25');
        INSERT OR IGNORE INTO settings VALUES ('min_drop_percent','15');
        INSERT OR IGNORE INTO settings VALUES ('serpapi_key','');
        INSERT OR IGNORE INTO settings VALUES ('telegram_token','');
        INSERT OR IGNORE INTO settings VALUES ('telegram_chat_id','');
        INSERT OR IGNORE INTO settings VALUES ('email_enabled','0');
        INSERT OR IGNORE INTO settings VALUES ('email_from','');
        INSERT OR IGNORE INTO settings VALUES ('email_to','');
        INSERT OR IGNORE INTO settings VALUES ('email_smtp','smtp.gmail.com');
        INSERT OR IGNORE INTO settings VALUES ('email_port','465');
        INSERT OR IGNORE INTO settings VALUES ('email_user','');
        INSERT OR IGNORE INTO settings VALUES ('email_pass','');
        """)

# Map db keys to environment variable names
ENV_MAP = {
    "serpapi_key":      "SERPAPI_KEY",
    "telegram_token":   "TELEGRAM_TOKEN",
    "telegram_chat_id": "TELEGRAM_CHAT_ID",
    "email_enabled":    "EMAIL_ENABLED",
    "email_from":       "EMAIL_FROM",
    "email_to":         "EMAIL_TO",
    "email_smtp":       "EMAIL_SMTP",
    "email_user":       "EMAIL_USER",
    "email_pass":       "EMAIL_PASS",
    "user_name":        "BOT_USER_NAME",
}

def cfg(key):
    # Environment variables take priority (for Railway/cloud deployment)
    env_key = ENV_MAP.get(key)
    if env_key and os.environ.get(env_key):
        return os.environ[env_key]
    with get_db() as c:
        row = c.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else ""

def set_cfg(key, value):
    with get_db() as c:
        c.execute("INSERT OR REPLACE INTO settings VALUES (?,?)", (key, value))

# ── Price analysis ────────────────────────────────────────────────────────────
def is_cheap(price, history, percentile=25, min_drop=15, max_price=None):
    if max_price and price > max_price:
        return False, f"Supera tu máximo de €{max_price:.0f}"
    if len(history) < 5:
        if max_price and price <= max_price * 0.75:
            return True, f"€{price:.0f} muy por debajo de tu máximo (pocos datos históricos)"
        return False, "Sin suficiente historial aún (acumulando datos...)"
    sp = sorted(history)
    p_val = sp[max(0, int(len(sp) * percentile / 100) - 1)]
    avg = mean(history)
    mn  = min(history)
    reasons = []
    if price <= p_val:
        reasons.append(f"top {percentile}% más barato (umbral €{p_val:.0f})")
    if avg > 0 and (avg - price) / avg * 100 >= min_drop:
        reasons.append(f"{(avg-price)/avg*100:.0f}% bajo la media (€{avg:.0f})")
    if mn > 0 and price <= mn * 1.05:
        reasons.append(f"cerca del mínimo histórico (€{mn:.0f})")
    if reasons:
        return True, " · ".join(reasons)
    return False, f"Normal — media €{avg:.0f} · mín €{mn:.0f}"

# ── Notifications ─────────────────────────────────────────────────────────────
def notify_telegram(msg):
    token, chat_id = cfg("telegram_token"), cfg("telegram_chat_id")
    if not token or not chat_id: return False
    try:
        r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}, timeout=10)
        return r.status_code == 200
    except: return False

def notify_email(subject, html_body):
    if cfg("email_enabled") != "1": return False
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = cfg("email_from")
        msg["To"]      = cfg("email_to")
        msg.attach(MIMEText(html_body, "html"))
        with smtplib.SMTP_SSL(cfg("email_smtp"), int(cfg("email_port") or 465)) as s:
            s.login(cfg("email_user"), cfg("email_pass"))
            s.sendmail(cfg("email_from"), cfg("email_to"), msg.as_string())
        return True
    except Exception as e:
        log.error(f"Email: {e}"); return False

CITIES = {"LIS":"Lisboa","OPO":"Porto","FAO":"Faro","MAD":"Madrid","BCN":"Barcelona",
  "AGP":"Málaga","VLC":"Valencia","BIO":"Bilbao","SVQ":"Sevilla","PMI":"Palma",
  "ALC":"Alicante","SCQ":"Santiago","SDR":"Santander","LPA":"Gran Canaria",
  "TFS":"Tenerife Sur","TFN":"Tenerife Norte","ACE":"Lanzarote","FUE":"Fuerteventura",
  "SPC":"La Palma","LHR":"Londres","CDG":"París","AMS":"Ámsterdam","FCO":"Roma"}

def city(code): return CITIES.get(code, code)

def build_tg_message(name, f, reason, history):
    avg_s = f"€{mean(history):.0f}" if len(history) >= 5 else "N/A"
    mn_s  = f"€{min(history):.0f}"  if len(history) >= 5 else "N/A"
    stops = "directo" if f.get("stops", 0) == 0 else f"{f['stops']} escala(s)"
    try:
        d = datetime.strptime(f["dep_date"], "%Y-%m-%d")
        date_s = d.strftime("%A %d de %B").capitalize()
    except: date_s = f["dep_date"]
    # Links de compra
    gf_url = (f"https://www.google.com/travel/flights?"
              f"q=vuelos+{f['origin']}+{f['destination']}+{f['dep_date']}")
    sk_url = (f"https://www.skyscanner.es/transporte/vuelos/"
              f"{f['origin'].lower()}/{f['destination'].lower()}/"
              f"{f['dep_date'].replace('-', '')[2:]}/")
    return (
        f"✈️ <b>¡Vuelo barato, {name}!</b>\n\n"
        f"🛫 <b>{city(f['origin'])} → {city(f['destination'])}</b>  |  {date_s}\n"
        f"💰 <b>€{f['price']:.0f}</b>  ·  {f.get('dep_time','?')}→{f.get('arr_time','?')}\n"
        f"⏱ {f.get('duration','?')}  ·  {stops}  ·  {f.get('airline','?')}\n\n"
        f"📊 {reason}\n"
        f"📈 Media: {avg_s}  ·  Mínimo: {mn_s}\n\n"
        f"🔍 <b>Comprar ahora:</b>\n"
        f"<a href='{gf_url}'>✈️ Google Flights</a>  ·  <a href='{sk_url}'>🔎 Skyscanner</a>"
    )

# ── Flight search ─────────────────────────────────────────────────────────────
def search_flights(origin, dest, date, adults, alert):
    key = cfg("serpapi_key")
    if not key:
        log.warning("Sin SerpApi key"); return []
    dep_from  = alert.get("dep_from", "06:00")
    dep_to    = alert.get("dep_to",   "22:00")
    arr_from  = alert.get("arr_from", "06:00")
    arr_to    = alert.get("arr_to",   "23:59")
    max_stops = int(alert.get("max_stops", 1))
    try:
        r = requests.get("https://serpapi.com/search", params={
            "engine": "google_flights", "departure_id": origin,
            "arrival_id": dest, "outbound_date": date, "adults": adults,
            "currency": cfg("currency") or "EUR", "hl": "es",
            "stops": max_stops, "type": "2", "api_key": key,
        }, timeout=15)
        data = r.json()
    except Exception as e:
        log.error(f"SerpApi: {e}"); return []
    if "error" in data:
        log.error(f"SerpApi: {data['error']}"); return []
    results = []
    for section in ["best_flights", "other_flights"]:
        for flight in data.get(section) or []:
            try:
                legs  = flight.get("flights", [])
                if not legs: continue
                stops = len(legs) - 1
                if stops > max_stops: continue
                dep_t = legs[0]["departure_airport"]["time"][11:16]
                arr_t = legs[-1]["arrival_airport"]["time"][11:16]
                if not (dep_from <= dep_t <= dep_to): continue
                if not (arr_from <= arr_t <= arr_to): continue
                price = flight.get("price")
                if not price: continue
                dur_m = flight.get("total_duration", 0)
                results.append({
                    "origin": origin, "destination": dest, "dep_date": date,
                    "price": float(price), "currency": cfg("currency") or "EUR",
                    "airline": legs[0].get("airline", "?"),
                    "duration": f"{dur_m//60}h{dur_m%60:02d}m" if dur_m else "?",
                    "stops": stops, "dep_time": dep_t, "arr_time": arr_t,
                })
            except Exception as ex:
                log.debug(f"Parse: {ex}"); continue
    return sorted(results, key=lambda x: x["price"])

# ── Monitor ───────────────────────────────────────────────────────────────────
def run_monitor():
    log.info("🔍 Comprobando vuelos...")
    with get_db() as db:
        alerts = db.execute("SELECT * FROM alerts WHERE enabled=1").fetchall()
    name  = cfg("user_name") or "Viajero"
    perc  = int(cfg("cheap_percentile") or 25)
    drop  = int(cfg("min_drop_percent") or 15)
    total = 0
    for alert in alerts:
        dests = json.loads(alert["destinations"])
        all_dates = json.loads(alert["dates"])
        # Only check dates within next 60 days, sorted by date
        today = datetime.utcnow().date()
        dates = sorted([
            d for d in all_dates
            if 0 <= (datetime.strptime(d, "%Y-%m-%d").date() - today).days <= 60
        ])
        # Limit to 3 dates per run to preserve API credits
        # Rotate which dates we check each run using current hour
        hour_offset = datetime.utcnow().hour % max(1, len(dates)) if dates else 0
        dates = dates[hour_offset:hour_offset+3] or dates[:3]
        log.info(f"  Alerta '{alert['name']}': revisando {len(dates)} de {len(all_dates)} fechas")
        for dest in dests:
            for date in dates:
                flights = search_flights(alert["origin"], dest, date, alert["adults"], dict(alert))
                if not flights: continue
                with get_db() as db:
                    for f in flights:
                        db.execute("""INSERT INTO price_history
                            (alert_id,origin,destination,dep_date,price,currency,
                             airline,duration,stops,dep_time,arr_time)
                            VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                            (alert["id"],f["origin"],f["destination"],f["dep_date"],
                             f["price"],f["currency"],f["airline"],f["duration"],
                             f["stops"],f["dep_time"],f["arr_time"]))
                    history = [r["price"] for r in db.execute(
                        """SELECT price FROM price_history
                           WHERE alert_id=? AND destination=? AND dep_date=?
                           AND checked_at > datetime('now','-60 days')""",
                        (alert["id"], dest, date)).fetchall()]
                cheap, reason = is_cheap(
                    flights[0]["price"], history, perc, drop,
                    float(alert["max_price"]) if alert["max_price"] else None)
                log.info(f"  {alert['origin']}→{dest} {date}: €{flights[0]['price']:.0f} {'✅ BARATO' if cheap else '—'}")
                if cheap:
                    current_price = flights[0]["price"]
                    with get_db() as db:
                        # Get last notification for this exact route+date
                        last = db.execute("""SELECT price, sent_at FROM notifications
                            WHERE alert_id=? AND destination=? AND dep_date=?
                            ORDER BY sent_at DESC LIMIT 1""",
                            (alert["id"], dest, date)).fetchone()
                    if last:
                        last_price = last["price"]
                        last_sent  = datetime.fromisoformat(last["sent_at"])
                        hours_since = (datetime.utcnow() - last_sent).total_seconds() / 3600
                        price_drop_pct = (last_price - current_price) / last_price * 100
                        # Skip if: less than 24h ago AND price hasn't dropped more than 10%
                        if hours_since < 24 and price_drop_pct < 10:
                            log.info(f"    Omitiendo — mismo precio (última alerta €{last_price:.0f}, ahora €{current_price:.0f}, hace {hours_since:.0f}h)")
                            continue
                    tg_msg = build_tg_message(name, flights[0], reason, history)
                    ok_tg  = notify_telegram(tg_msg)
                    channels = ["telegram"] if ok_tg else []
                    with get_db() as db:
                        db.execute("""INSERT INTO notifications
                            (alert_id,origin,destination,dep_date,price,reason,channel)
                            VALUES(?,?,?,?,?,?,?)""",
                            (alert["id"],alert["origin"],dest,date,
                             flights[0]["price"],reason,",".join(channels)))
                    total += 1
                time.sleep(0.4)
    log.info(f"✅ Listo — {total} alerta(s) enviada(s)")

_scheduler_running = False
def start_scheduler():
    global _scheduler_running
    if _scheduler_running: return
    _scheduler_running = True
    interval = int(cfg("check_interval") or 60)
    schedule.every(interval).minutes.do(run_monitor)
    threading.Thread(target=lambda: [schedule.run_pending() or time.sleep(30) for _ in iter(int, 1)], daemon=True).start()
    log.info(f"🕐 Scheduler iniciado — cada {interval} min")

# ── API routes ────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/settings", methods=["GET"])
def get_settings():
    with get_db() as db:
        rows = db.execute("SELECT key,value FROM settings").fetchall()
    return jsonify({r["key"]: r["value"] for r in rows})

@app.route("/api/settings", methods=["POST"])
def save_settings():
    for k, v in request.json.items():
        set_cfg(k, str(v))
    global _scheduler_running
    schedule.clear(); _scheduler_running = False; start_scheduler()
    return jsonify({"ok": True})

@app.route("/api/alerts", methods=["GET"])
def get_alerts():
    with get_db() as db:
        rows = db.execute("SELECT * FROM alerts ORDER BY created_at DESC").fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d["destinations"] = json.loads(d["destinations"])
        d["dates"]        = json.loads(d["dates"])
        result.append(d)
    return jsonify(result)

@app.route("/api/alerts", methods=["POST"])
def create_alert():
    data = request.json
    with get_db() as db:
        db.execute("""INSERT INTO alerts
            (name,origin,destinations,dates,adults,max_stops,
             dep_from,dep_to,arr_from,arr_to,max_price,enabled)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,1)""", (
            data["name"], data["origin"].upper(),
            json.dumps([d.upper() for d in data["destinations"]]),
            json.dumps(data["dates"]),
            int(data.get("adults",1)), int(data.get("max_stops",1)),
            data.get("dep_from","06:00"), data.get("dep_to","22:00"),
            data.get("arr_from","06:00"), data.get("arr_to","23:59"),
            float(data["max_price"]) if data.get("max_price") else None))
    return jsonify({"ok": True})

@app.route("/api/alerts/<int:aid>", methods=["PUT"])
def update_alert(aid):
    data = request.json
    with get_db() as db:
        if list(data.keys()) == ["enabled"]:
            db.execute("UPDATE alerts SET enabled=? WHERE id=?", (data["enabled"], aid))
        else:
            db.execute("""UPDATE alerts SET name=?,origin=?,destinations=?,dates=?,
                adults=?,max_stops=?,dep_from=?,dep_to=?,arr_from=?,arr_to=?,max_price=?
                WHERE id=?""", (
                data["name"], data["origin"].upper(),
                json.dumps([d.upper() for d in data["destinations"]]),
                json.dumps(data["dates"]),
                int(data.get("adults",1)), int(data.get("max_stops",1)),
                data.get("dep_from","06:00"), data.get("dep_to","22:00"),
                data.get("arr_from","06:00"), data.get("arr_to","23:59"),
                float(data["max_price"]) if data.get("max_price") else None, aid))
    return jsonify({"ok": True})

@app.route("/api/alerts/<int:aid>", methods=["DELETE"])
def delete_alert(aid):
    with get_db() as db:
        db.execute("DELETE FROM alerts WHERE id=?", (aid,))
    return jsonify({"ok": True})

@app.route("/api/check-now", methods=["POST"])
def check_now():
    threading.Thread(target=run_monitor, daemon=True).start()
    return jsonify({"ok": True})

@app.route("/api/test-telegram", methods=["POST"])
def test_telegram():
    name = cfg("user_name") or "Viajero"
    ok = notify_telegram(f"✅ <b>¡Bot conectado, {name}!</b>\n\nTu Flight Alert Bot está funcionando. ✈️")
    return jsonify({"ok": ok, "msg": "Telegram OK ✓" if ok else "Error — revisa token y chat_id"})

@app.route("/api/stats", methods=["GET"])
def get_stats():
    with get_db() as db:
        checks  = db.execute("SELECT COUNT(*) n FROM price_history").fetchone()["n"]
        notifs  = db.execute("SELECT COUNT(*) n FROM notifications").fetchone()["n"]
        active  = db.execute("SELECT COUNT(*) n FROM alerts WHERE enabled=1").fetchone()["n"]
        recent_notifs = [dict(r) for r in db.execute("""
            SELECT n.*,a.name alert_name FROM notifications n
            LEFT JOIN alerts a ON a.id=n.alert_id
            ORDER BY sent_at DESC LIMIT 10""").fetchall()]
        recent_prices = [dict(r) for r in db.execute("""
            SELECT origin,destination,dep_date,
                   MIN(price) min_price,MAX(price) max_price,AVG(price) avg_price,
                   COUNT(*) n,MAX(checked_at) last_checked
            FROM price_history GROUP BY origin,destination,dep_date
            ORDER BY last_checked DESC LIMIT 20""").fetchall()]
    return jsonify({"total_checks":checks,"total_notifs":notifs,"active_alerts":active,
                    "recent_notifs":recent_notifs,"recent_prices":recent_prices})

# Runs with both gunicorn and direct python
init_db()
# start_scheduler()  # temporarily disabled

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", 8081)))
    args = parser.parse_args()
    print(f"\n✈️  Flight Alert Bot  →  http://localhost:{args.port}\n")
    app.run(host="0.0.0.0", port=args.port, debug=False)

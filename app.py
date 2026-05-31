import json, threading, logging, time, schedule, requests, argparse, os, calendar
from datetime import datetime, date, timedelta
from pathlib import Path
from statistics import mean, stdev
from flask import Flask, request, jsonify, render_template

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")
if DATABASE_URL:
    import pg8000.dbapi
    from urllib.parse import urlparse
else:
    import sqlite3
    DB = Path("bot.db")

app = Flask(__name__)

# ── DB ────────────────────────────────────────────────────────────────────────

def get_conn():
    if DATABASE_URL:
        u = urlparse(DATABASE_URL)
        return pg8000.dbapi.connect(
            host=u.hostname, port=u.port or 5432,
            database=u.path[1:], user=u.username,
            password=u.password, ssl_context=True)
    c = sqlite3.connect(DB)
    c.row_factory = sqlite3.Row
    return c

def q(sql):
    return sql.replace("?", "%s") if DATABASE_URL else sql

def ago(days):
    return f"NOW() - INTERVAL '{days} days'" if DATABASE_URL else f"datetime('now','-{days} days')"

def fetchone(sql, params=()):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(q(sql), params)
        row = cur.fetchone()
        if row is None: return None
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))
    finally:
        conn.close()

def fetchall(sql, params=()):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(q(sql), params)
        rows = cur.fetchall()
        if not rows: return []
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in rows]
    finally:
        conn.close()

def execute(sql, params=()):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(q(sql), params)
        conn.commit()
    finally:
        conn.close()

def init_db():
    conn = get_conn()
    try:
        cur = conn.cursor()
        if DATABASE_URL:
            pk  = "SERIAL PRIMARY KEY"
            ts  = "TEXT DEFAULT to_char(NOW(),'YYYY-MM-DD HH24:MI:SS')"
            ins = "INSERT INTO settings(key,value) VALUES(%s,%s) ON CONFLICT(key) DO NOTHING"
        else:
            pk  = "INTEGER PRIMARY KEY AUTOINCREMENT"
            ts  = "TEXT DEFAULT CURRENT_TIMESTAMP"
            ins = "INSERT OR IGNORE INTO settings VALUES(?,?)"

        cur.execute("CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT)")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS alerts(
            id {pk}, name TEXT,
            origins TEXT,
            destinations TEXT,
            search_mode TEXT DEFAULT 'month',
            dates TEXT,
            explore_month TEXT,
            adults INTEGER DEFAULT 1,
            max_stops INTEGER DEFAULT 1,
            dep_from TEXT DEFAULT '06:00', dep_to TEXT DEFAULT '22:00',
            arr_from TEXT DEFAULT '06:00', arr_to TEXT DEFAULT '23:59',
            max_price REAL, enabled INTEGER DEFAULT 1, created_at {ts})""")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS price_history(
            id {pk}, alert_id INTEGER,
            origin TEXT, destination TEXT, dep_date TEXT,
            price REAL, currency TEXT, airline TEXT,
            duration TEXT, stops INTEGER, dep_time TEXT, arr_time TEXT,
            checked_at {ts})""")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS notifications(
            id {pk}, alert_id INTEGER,
            origin TEXT, destination TEXT,
            dep_date TEXT, price REAL, reason TEXT, channel TEXT,
            sent_at {ts})""")

        for k,v in [("user_name","Eduardo"),("currency","EUR"),("check_interval","60"),
            ("cheap_percentile","25"),("min_drop_percent","15"),("serpapi_key",""),
            ("telegram_token",""),("telegram_chat_id",""),("email_enabled","0"),
            ("email_from",""),("email_to",""),("email_smtp","smtp.gmail.com"),
            ("email_port","465"),("email_user",""),("email_pass","")]:
            cur.execute(ins,(k,v))

        conn.commit()  # commit table creation first

        # Migrate: add new columns safely (each in its own transaction)
        migrations = [
            "ALTER TABLE alerts ADD COLUMN origins TEXT",
            "ALTER TABLE alerts ADD COLUMN search_mode TEXT DEFAULT 'month'",
            "ALTER TABLE alerts ADD COLUMN explore_month TEXT",
            "ALTER TABLE alerts ADD COLUMN trip_type TEXT DEFAULT 'one_way'",
            "ALTER TABLE alerts ADD COLUMN duration_min INTEGER DEFAULT 5",
            "ALTER TABLE alerts ADD COLUMN duration_max INTEGER DEFAULT 5",
        ]
        for m in migrations:
            try:
                cur.execute(m)
                conn.commit()
            except Exception:
                conn.rollback()  # reset failed transaction before next attempt
        log.info("DB lista")
    finally:
        conn.close()

# ── Config ────────────────────────────────────────────────────────────────────

ENV_MAP = {"serpapi_key":"SERPAPI_KEY","telegram_token":"TELEGRAM_TOKEN",
    "telegram_chat_id":"TELEGRAM_CHAT_ID","user_name":"BOT_USER_NAME"}

def cfg(key):
    env = ENV_MAP.get(key)
    if env and os.environ.get(env): return os.environ[env]
    row = fetchone("SELECT value FROM settings WHERE key=?",(key,))
    return row["value"] if row else ""

def set_cfg(key,value):
    if DATABASE_URL:
        execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",(key,value))
    else:
        execute("INSERT OR REPLACE INTO settings VALUES(?,?)",(key,value))

# ── Cities ────────────────────────────────────────────────────────────────────

CITIES = {
    "LIS":"Lisboa","OPO":"Porto","FAO":"Faro","MAD":"Madrid","BCN":"Barcelona",
    "AGP":"Malaga","VLC":"Valencia","BIO":"Bilbao","SVQ":"Sevilla","PMI":"Palma",
    "ALC":"Alicante","SCQ":"Santiago","ZAZ":"Zaragoza","SDR":"Santander",
    "LPA":"Gran Canaria","TFS":"Tenerife Sur","TFN":"Tenerife Norte",
    "ACE":"Lanzarote","FUE":"Fuerteventura","IBZ":"Ibiza","MAH":"Menorca",
    "IST":"Estambul","SAW":"Estambul Sabiha","ATH":"Atenas","HER":"Creta",
    "RHO":"Rodas","AYT":"Antalya","SKG":"Tesalonica",
    "CDG":"Paris CDG","ORY":"Paris Orly","NCE":"Niza","LYS":"Lyon",
    "LHR":"Londres","LGW":"Gatwick","STN":"Stansted","MAN":"Manchester","EDI":"Edimburgo",
    "AMS":"Amsterdam","BRU":"Bruselas","EIN":"Eindhoven",
    "FCO":"Roma","CIA":"Roma Ciampino","MXP":"Milan","BGY":"Milan Bergamo","VCE":"Venecia","NAP":"Naples",
    "FRA":"Frankfurt","MUC":"Munich","BER":"Berlin","HAM":"Hamburgo",
    "VIE":"Viena","ZRH":"Zurich","GVA":"Ginebra",
    "DUB":"Dublin","PRG":"Praga","BUD":"Budapest","WAW":"Varsovia","KRK":"Cracovia",
    "CPH":"Copenhague","ARN":"Estocolmo","OSL":"Oslo","HEL":"Helsinki",
    "DXB":"Dubai","DOH":"Doha","AUH":"Abu Dhabi",
    "CMN":"Casablanca","TUN":"Tunez","CAI":"El Cairo","JNB":"Johannesburgo","CPT":"Ciudad del Cabo",
    "JFK":"Nueva York","EWR":"Newark","MIA":"Miami","LAX":"Los Angeles",
    "YYZ":"Toronto","MEX":"Ciudad de Mexico",
    "BOG":"Bogota","GRU":"Sao Paulo","EZE":"Buenos Aires","SCL":"Santiago Chile","LIM":"Lima",
    "BKK":"Bangkok","SIN":"Singapur","HKG":"Hong Kong",
    "NRT":"Tokio Narita","HND":"Tokio Haneda","ICN":"Seoul",
    "SYD":"Sidney","MEL":"Melbourne",
}
def city(c): return CITIES.get(c, c)

# ── Intelligence: Trend & Context ─────────────────────────────────────────────

def get_price_trend(alert_id, origin, dest, dep_date):
    """Get price trend for a specific route+date over last 7 days"""
    rows = fetchall(f"""
        SELECT price, checked_at FROM price_history
        WHERE alert_id=? AND origin=? AND destination=? AND dep_date=?
        AND checked_at > {ago(7)}
        ORDER BY checked_at ASC
    """, (alert_id, origin, dest, dep_date))
    if len(rows) < 2:
        return None
    prices = [r["price"] for r in rows]
    dates  = [r["checked_at"] for r in rows]
    first, last = prices[0], prices[-1]
    change = last - first
    pct    = (change / first) * 100 if first > 0 else 0
    # Build last few data points
    recent = prices[-5:] if len(prices) >= 5 else prices
    return {
        "prices": recent,
        "change": change,
        "pct": pct,
        "direction": "bajando" if pct < -2 else "subiendo" if pct > 2 else "estable",
        "days": len(set(d[:10] for d in dates)),
    }

def get_month_context(alert_id, origins, dests, month_str):
    """Get cheapest days found so far in a month across all origin/dest combos"""
    if not month_str: return None
    rows = fetchall(f"""
        SELECT origin, destination, dep_date, MIN(price) as min_price
        FROM price_history
        WHERE alert_id=? AND dep_date LIKE ?
        GROUP BY origin, destination, dep_date
        ORDER BY min_price ASC
        LIMIT 10
    """, (alert_id, f"{month_str}%"))
    if not rows: return None
    try:
        y, m = map(int, month_str.split("-"))
        total_days = calendar.monthrange(y, m)[1]
        today = date.today()
        future_days = sum(1 for d in range(1, total_days+1) if date(y, m, d) >= today)
        scanned = len(set(r["dep_date"] for r in rows))
    except:
        future_days, scanned = 0, len(rows)
    return {"top": rows[:5], "scanned": scanned, "total": future_days}

def get_volatility(alert_id, origin, dest):
    """How often does this route's price change?"""
    rows = fetchall(f"""
        SELECT dep_date, price, checked_at FROM price_history
        WHERE alert_id=? AND origin=? AND destination=?
        AND checked_at > {ago(7)}
        ORDER BY checked_at ASC
    """, (alert_id, origin, dest))
    if len(rows) < 4: return None
    changes = 0
    for i in range(1, len(rows)):
        if abs(rows[i]["price"] - rows[i-1]["price"]) > 5:
            changes += 1
    rate = changes / len(rows)
    if rate > 0.3: return "ALTA"
    if rate > 0.1: return "MEDIA"
    return "BAJA"

def days_until(dep_date_str):
    try:
        dep = datetime.strptime(dep_date_str, "%Y-%m-%d").date()
        return (dep - date.today()).days
    except:
        return 0

def booking_advice(days_left, direction):
    """Give actionable advice based on days until flight and trend"""
    if days_left <= 14:
        return "⚠️ Quedan menos de 2 semanas — los precios ya no van a bajar. Si te interesa, compra ya."
    if days_left <= 21:
        return "⏰ Quedan 3 semanas — zona de riesgo. Los precios suelen subir rápido en este punto."
    if days_left <= 45:
        if direction == "bajando":
            return "📉 Bajando y quedan 6 semanas — puede seguir bajando un poco, pero no esperes demasiado."
        if direction == "subiendo":
            return "📈 Subiendo y quedan 6 semanas — si te convence el precio, no esperes más."
        return "⚖️ Quedan 6 semanas — zona razonable para comprar si el precio te encaja."
    if days_left <= 90:
        if direction == "bajando":
            return "📉 Todavía quedan más de 2 meses y está bajando — puedes esperar un poco más."
        return "🕐 Más de 2 meses — aún hay tiempo, monitoriza unos días más."
    return "🗓️ Mucha antelación — espera, los precios suelen bajar acercándose a los 2-3 meses."

def cross_month_insight(alert_id, current_month, best_price):
    """Check if other months have cheaper options already in DB"""
    rows = fetchall(f"""
        SELECT dep_date, MIN(price) as min_price
        FROM price_history
        WHERE alert_id=?
        AND dep_date NOT LIKE ?
        AND checked_at > {ago(60)}
        GROUP BY dep_date
        ORDER BY min_price ASC
        LIMIT 5
    """, (alert_id, f"{current_month}%"))
    if not rows: return None
    cheaper = [r for r in rows if r["min_price"] < best_price - 20]
    if not cheaper: return None
    best_other = cheaper[0]
    other_month = best_other["dep_date"][:7]
    try:
        other_name = datetime.strptime(other_month+"-01", "%Y-%m-%d").strftime("%B")
    except:
        other_name = other_month
    saving = best_price - best_other["min_price"]
    return f"💡 Si tienes flexibilidad: {other_name} tiene vuelos desde €{best_other['min_price']:.0f} (ahorras €{saving:.0f})"

# ── Flight search ─────────────────────────────────────────────────────────────

def search_one(origin, dest, dep_date, adults, alert):
    key = cfg("serpapi_key")
    if not key: return None
    dep_from  = alert.get("dep_from","06:00")
    dep_to    = alert.get("dep_to","22:00")
    arr_from  = alert.get("arr_from","06:00")
    arr_to    = alert.get("arr_to","23:59")
    max_stops = int(alert.get("max_stops",1))
    try:
        r = requests.get("https://serpapi.com/search", params={
            "engine":"google_flights","departure_id":origin,"arrival_id":dest,
            "outbound_date":dep_date,"adults":adults,
            "currency":cfg("currency") or "EUR","hl":"es",
            "stops":max_stops,"type":"2","api_key":key}, timeout=15)
        data = r.json()
    except Exception as e:
        log.error(f"SerpApi: {e}"); return None
    if "error" in data:
        log.error(f"SerpApi: {data['error']}"); return None
    best = None
    for section in ["best_flights","other_flights"]:
        for flight in data.get(section) or []:
            try:
                legs  = flight.get("flights",[])
                if not legs: continue
                stops = len(legs)-1
                if stops > max_stops: continue
                dep_t = legs[0]["departure_airport"]["time"][11:16]
                arr_t = legs[-1]["arrival_airport"]["time"][11:16]
                if not(dep_from<=dep_t<=dep_to): continue
                if not(arr_from<=arr_t<=arr_to): continue
                price = flight.get("price")
                if not price: continue
                if best is None or float(price) < best["price"]:
                    dur_m = flight.get("total_duration",0)
                    best = {
                        "origin":origin,"destination":dest,"dep_date":dep_date,
                        "price":float(price),"currency":cfg("currency") or "EUR",
                        "airline":legs[0].get("airline","?"),
                        "duration":f"{dur_m//60}h{dur_m%60:02d}m" if dur_m else "?",
                        "stops":stops,"dep_time":dep_t,"arr_time":arr_t,
                    }
            except: continue
    return best

def search_round_trip(origin, dest, outbound_date, return_date, adults, alert):
    """Search both legs of a round trip and return combined price"""
    outbound = search_one(origin, dest, outbound_date, adults, alert)
    if not outbound: return None
    time.sleep(0.3)
    # For return leg, swap origin/dest and use relaxed time filters
    return_alert = dict(alert)
    return_alert["dep_from"] = "06:00"
    return_alert["dep_to"]   = "23:00"
    return_alert["arr_from"] = "06:00"
    return_alert["arr_to"]   = "23:59"
    inbound = search_one(dest, origin, return_date, adults, return_alert)
    if not inbound: return None
    total = outbound["price"] + inbound["price"]
    return {
        "origin": origin, "destination": dest,
        "outbound_date": outbound_date, "return_date": return_date,
        "outbound_price": outbound["price"], "inbound_price": inbound["price"],
        "total_price": total,
        "currency": outbound["currency"],
        "outbound_airline": outbound["airline"], "inbound_airline": inbound["airline"],
        "outbound_dep": outbound["dep_time"], "outbound_arr": outbound["arr_time"],
        "inbound_dep": inbound["dep_time"], "inbound_arr": inbound["arr_time"],
        "outbound_duration": outbound["duration"], "inbound_duration": inbound["duration"],
        "outbound_stops": outbound["stops"], "inbound_stops": inbound["stops"],
    }

def round_trip_combos(month_str, dur_min, dur_max):
    """Generate all valid outbound+return date pairs for a month and duration range"""
    dates = month_dates(month_str)
    combos = []
    for i, dep in enumerate(dates):
        dep_dt = datetime.strptime(dep, "%Y-%m-%d").date()
        for dur in range(dur_min, dur_max + 1):
            ret_dt = dep_dt + timedelta(days=dur)
            ret_str = ret_dt.strftime("%Y-%m-%d")
            if ret_str in dates or ret_dt > datetime.strptime(dates[-1], "%Y-%m-%d").date():
                combos.append((dep, ret_str))
    return combos

def month_dates(ym):
    y, m = map(int, ym.split("-"))
    days = calendar.monthrange(y,m)[1]
    today = date.today()
    return [date(y,m,d).strftime("%Y-%m-%d") for d in range(1,days+1) if date(y,m,d) >= today]

# ── Notifications ─────────────────────────────────────────────────────────────

def notify_telegram(msg):
    token, chat_id = cfg("telegram_token"), cfg("telegram_chat_id")
    if not token or not chat_id: return False
    try:
        r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id":chat_id,"text":msg,"parse_mode":"HTML"},timeout=10)
        return r.status_code == 200
    except: return False

def build_message(name, alert, top5, best):
    """Build intelligent Telegram message with trend, context and advice"""
    aid   = alert["id"]
    mode  = alert.get("search_mode","month")
    month = alert.get("explore_month","")
    days_left = days_until(best["dep_date"])

    # Format date nicely
    try:
        date_s = datetime.strptime(best["dep_date"],"%Y-%m-%d").strftime("%A %d de %B").capitalize()
    except:
        date_s = best["dep_date"]

    stops_s = lambda f: "directo" if f["stops"]==0 else f"{f['stops']} esc."
    medal   = ["🥇","🥈","🥉","4️⃣","5️⃣"]

    # Header
    if mode == "month" and month:
        try:
            month_name = datetime.strptime(month+"-01","%Y-%m-%d").strftime("%B %Y").capitalize()
        except:
            month_name = month
        header = f"✈️ <b>Mejores vuelos — {month_name}</b>\n<b>{alert['name']}</b>\n\n"
    else:
        header = f"✈️ <b>Vuelo barato encontrado</b>\n<b>{alert['name']}</b>\n\n"

    # Top combinations ranking
    ranking = ""
    for i, f in enumerate(top5):
        ds = f["dep_date"][5:].replace("-","/")
        ranking += f"{medal[i]} <b>{city(f['origin'])} → {city(f['destination'])}</b> | {ds} | <b>€{f['price']:.0f}</b> | {f['dep_time']} | {stops_s(f)} | {f['airline']}\n"

    # Trend analysis for best option
    trend_block = ""
    trend = get_price_trend(aid, best["origin"], best["destination"], best["dep_date"])
    if trend and trend["days"] >= 2:
        prices_str = " → ".join(f"€{p:.0f}" for p in trend["prices"])
        emoji = "📉" if trend["direction"]=="bajando" else "📈" if trend["direction"]=="subiendo" else "➡️"
        trend_block = f"\n{emoji} <b>Tendencia</b> ({trend['days']} días): {prices_str}\n"

    # Month context
    context_block = ""
    ctx = get_month_context(aid, [], [], month) if month else None
    if ctx and ctx["scanned"] > 1:
        scanned_pct = int(ctx["scanned"] / ctx["total"] * 100) if ctx["total"] > 0 else 0
        context_block = f"\n📊 <b>Contexto del mes</b> ({ctx['scanned']}/{ctx['total']} días escaneados — {scanned_pct}%):\n"
        for r in ctx["top"][:4]:
            is_best = r["dep_date"] == best["dep_date"] and r["origin"] == best["origin"]
            flag = " 🟢 mínimo hasta ahora" if (is_best and ctx["top"][0]["dep_date"]==best["dep_date"]) else ""
            ds = r["dep_date"][5:].replace("-","/")
            context_block += f"  {ds} {city(r['origin'])}→{city(r['destination'])} €{r['min_price']:.0f}{flag}\n"

    # Days until flight + advice
    days_block = f"\n📅 <b>Quedan {days_left} días</b>\n"
    direction = trend["direction"] if trend else "estable"
    days_block += advice_line(days_left, direction) + "\n"

    # Volatility
    vol = get_volatility(aid, best["origin"], best["destination"])
    vol_block = ""
    if vol:
        vol_emoji = "🔴" if vol=="ALTA" else "🟡" if vol=="MEDIA" else "🟢"
        vol_block = f"\n{vol_emoji} <b>Volatilidad {best['origin']}→{best['destination']}:</b> {vol}"
        if vol == "ALTA":
            vol_block += " — el precio cambia frecuentemente, no esperes demasiado"

    # Cross-month insight (only if we have data)
    cross_block = ""
    insight = cross_month_insight(aid, month, best["price"]) if month else None
    if insight:
        cross_block = f"\n{insight}"

    # Links
    # Correct one-way URL for Google Flights
    gf = f"https://www.google.com/flights#flt={best['origin']}.{best['destination']}.{best['dep_date']};c:EUR;e:1;sd:1;t:f"
    sk = f"https://www.skyscanner.es/transporte/vuelos/{best['origin'].lower()}/{best['destination'].lower()}/{best['dep_date'].replace('-','')[2:]}/"
    links = f"\n🔗 <a href='{gf}'>Google Flights</a> · <a href='{sk}'>Skyscanner</a>"

    return header + ranking + trend_block + context_block + days_block + vol_block + cross_block + links

def build_roundtrip_message(name, alert, top5_rt, best):
    """Build Telegram message for round trip results"""
    try:
        out_date = datetime.strptime(best["outbound_date"],"%Y-%m-%d").strftime("%d/%m")
        ret_date = datetime.strptime(best["return_date"],"%Y-%m-%d").strftime("%d/%m")
        dur = (datetime.strptime(best["return_date"],"%Y-%m-%d") - 
               datetime.strptime(best["outbound_date"],"%Y-%m-%d")).days
    except:
        out_date = best["outbound_date"][5:]
        ret_date = best["return_date"][5:]
        dur = 0

    medal = ["🥇","🥈","🥉","4️⃣","5️⃣"]
    try:
        month_name = datetime.strptime(alert.get("explore_month","2026-01")+"-01","%Y-%m-%d").strftime("%B %Y").capitalize()
    except:
        month_name = alert.get("explore_month","")

    header = f"✈️ <b>Ida y vuelta — {month_name}</b>\n<b>{alert['name']}</b>\n\n"

    lines = []
    for i, rt in enumerate(top5_rt):
        try:
            od = datetime.strptime(rt["outbound_date"],"%Y-%m-%d").strftime("%d/%m")
            rd = datetime.strptime(rt["return_date"],"%Y-%m-%d").strftime("%d/%m")
            d  = (datetime.strptime(rt["return_date"],"%Y-%m-%d") - 
                  datetime.strptime(rt["outbound_date"],"%Y-%m-%d")).days
        except:
            od,rd,d = rt["outbound_date"],rt["return_date"],0
        out_s = "directo" if rt["outbound_stops"]==0 else f"{rt['outbound_stops']}esc."
        ret_s = "directo" if rt["inbound_stops"]==0 else f"{rt['inbound_stops']}esc."
        lines.append(
            f"{medal[i]} <b>{city(rt['origin'])}↔{city(rt['destination'])}</b> | "
            f"Ida: {od} ({out_s}) · Vuelta: {rd} ({ret_s}) | "
            f"<b>€{rt['total_price']:.0f} total</b> "
            f"(€{rt['outbound_price']:.0f}+€{rt['inbound_price']:.0f}) | {d} días"
        )

    days_left = (datetime.strptime(best["outbound_date"],"%Y-%m-%d").date() - date.today()).days

    gf = (f"https://www.google.com/flights#flt={best['origin']}.{best['destination']}."
          f"{best['outbound_date']}*{best['destination']}.{best['origin']}."
          f"{best['return_date']};c:EUR;e:1;sd:1;t:r")
    sk = (f"https://www.skyscanner.es/transporte/vuelos/{best['origin'].lower()}/"
          f"{best['destination'].lower()}/{best['outbound_date'].replace('-','')[2:]}/"
          f"{best['return_date'].replace('-','')[2:]}/")

    return (
        header + "\n".join(lines) +
        f"\n\n📅 <b>Quedan {days_left} días</b> para la salida\n" +
        advice_line(days_left, "estable") +
        f"\n\n🔗 <a href='{gf}'>Google Flights ida y vuelta</a> · <a href='{sk}'>Skyscanner</a>"
    )

def advice_line(days_left, direction):
    if days_left <= 14:
        return "⚠️ Menos de 2 semanas — precio máximo pronto. Compra ya si te interesa."
    if days_left <= 21:
        return "⏰ 3 semanas — zona de riesgo, los precios suben rápido a partir de aquí."
    if days_left <= 45:
        if direction == "bajando":
            return "📉 Puede seguir bajando un poco, pero no esperes demasiado."
        if direction == "subiendo":
            return "📈 Está subiendo. Si el precio te encaja, compra pronto."
        return "⚖️ Zona razonable para comprar si el precio te encaja."
    if days_left <= 90:
        if direction == "bajando":
            return "📉 Más de 2 meses y bajando — puedes esperar unos días más."
        return "🕐 Más de 2 meses — monitoriza unos días más antes de decidir."
    return "🗓️ Mucha antelación aún — los precios suelen bajar acercándose a 2-3 meses."

# ── Monitor ───────────────────────────────────────────────────────────────────

def run_monitor():
    log.info("Comprobando vuelos...")
    alerts = fetchall("SELECT * FROM alerts WHERE enabled=1")
    name   = cfg("user_name") or "Viajero"
    total  = 0

    for alert in alerts:
        origins  = json.loads(alert.get("origins") or '["MAD"]')
        dests    = json.loads(alert["destinations"])
        adults   = alert["adults"]
        max_p    = float(alert["max_price"]) if alert.get("max_price") else None
        mode     = alert.get("search_mode","month")

        # Get dates to check this run
        if mode == "month" and alert.get("explore_month"):
            all_dates = month_dates(alert["explore_month"])
        else:
            raw = json.loads(alert.get("dates") or "[]")
            today = date.today()
            all_dates = sorted([d for d in raw
                if 0 <= (datetime.strptime(d,"%Y-%m-%d").date()-today).days <= 180])

        if not all_dates: continue

        # Rotate 3 dates per hour
        offset = datetime.utcnow().hour % max(1, len(all_dates))
        dates  = all_dates[offset:offset+3] or all_dates[:3]

        trip_type  = alert.get("trip_type", "one_way")
        dur_min    = int(alert.get("duration_min") or 5)
        dur_max    = int(alert.get("duration_max") or 5)

        # ── Round trip mode ───────────────────────────────────────────────
        if trip_type == "round_trip" and mode == "month" and alert.get("explore_month"):
            all_combos = round_trip_combos(alert["explore_month"], dur_min, dur_max)
            offset = datetime.utcnow().hour % max(1, len(all_combos))
            combos = all_combos[offset:offset+2] or all_combos[:2]  # 2 combos = 4 API calls
            rt_results = []
            for (dep_date, ret_date) in combos:
                for origin in origins:
                    for dest in dests:
                        rt = search_round_trip(origin, dest, dep_date, ret_date, adults, alert)
                        if rt:
                            # Save both legs to history
                            execute("""INSERT INTO price_history
                                (alert_id,origin,destination,dep_date,price,currency,
                                 airline,duration,stops,dep_time,arr_time)
                                VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                                (alert["id"],rt["origin"],rt["destination"],rt["outbound_date"],
                                 rt["outbound_price"],rt["currency"],rt["outbound_airline"],
                                 rt["outbound_duration"],rt["outbound_stops"],
                                 rt["outbound_dep"],rt["outbound_arr"]))
                            if max_p is None or rt["total_price"] <= max_p * 2:
                                rt_results.append(rt)
                        time.sleep(0.3)
            if not rt_results:
                log.info(f"  {alert['name']}: sin resultados ida y vuelta")
                continue
            rt_results.sort(key=lambda x: x["total_price"])
            top5_rt = rt_results[:5]
            best_rt = top5_rt[0]
            log.info(f"  {alert['name']} IDA+VUELTA: {best_rt['origin']}→{best_rt['destination']} "
                     f"{best_rt['outbound_date']}→{best_rt['return_date']} €{best_rt['total_price']:.0f}")
            # Cooldown check
            last = fetchone("SELECT price,sent_at FROM notifications WHERE alert_id=? ORDER BY sent_at DESC LIMIT 1", (alert["id"],))
            if last:
                try:
                    hrs = (datetime.utcnow()-datetime.fromisoformat(last["sent_at"])).total_seconds()/3600
                    last_price = float(last["price"]) if last["price"] else 0
                    drop_pct = (last_price - best_rt["total_price"]) / last_price * 100 if last_price > 0 else 0
                    if hrs < 12 and drop_pct < 8:
                        log.info(f"    RT Omitiendo — hace {hrs:.1f}h, variación {drop_pct:+.1f}%")
                        continue
                except: pass
            msg = build_roundtrip_message(name, alert, top5_rt, best_rt)
            ok  = notify_telegram(msg)
            execute("INSERT INTO notifications(alert_id,origin,destination,dep_date,price,reason,channel) VALUES(?,?,?,?,?,?,?)",
                (alert["id"],best_rt["origin"],best_rt["destination"],best_rt["outbound_date"],
                 best_rt["total_price"],"round_trip top5","telegram" if ok else ""))
            if ok: total += 1
            continue  # skip one_way logic below

        # ── One way mode ──────────────────────────────────────────────────
        results = []
        for dep_date in dates:
            for origin in origins:
                for dest in dests:
                    f = search_one(origin, dest, dep_date, adults, alert)
                    if f:
                        execute("""INSERT INTO price_history
                            (alert_id,origin,destination,dep_date,price,currency,
                             airline,duration,stops,dep_time,arr_time)
                            VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                            (alert["id"],f["origin"],f["destination"],f["dep_date"],
                             f["price"],f["currency"],f["airline"],f["duration"],
                             f["stops"],f["dep_time"],f["arr_time"]))
                        if max_p is None or f["price"] <= max_p:
                            results.append(f)
                    time.sleep(0.3)

        if not results:
            log.info(f"  {alert['name']}: sin resultados dentro del presupuesto")
            continue

        results.sort(key=lambda x: x["price"])
        top5 = results[:5]
        best = top5[0]

        log.info(f"  {alert['name']}: mejor {best['origin']}→{best['destination']} {best['dep_date']} €{best['price']:.0f}")

        # Cooldown: skip if sent recently and price hasn't dropped much
        last = fetchone("""SELECT price,sent_at FROM notifications
            WHERE alert_id=? ORDER BY sent_at DESC LIMIT 1""", (alert["id"],))
        if last:
            try:
                hrs = (datetime.utcnow()-datetime.fromisoformat(last["sent_at"])).total_seconds()/3600
                last_p = float(last["price"]) if last["price"] else 0
                # drop_pct > 0 = price went down (good news), < 0 = went up
                drop_pct = (last_p - best["price"]) / last_p * 100 if last_p > 0 else 0
                if hrs < 12 and drop_pct < 8:
                    log.info(f"    Omitiendo — hace {hrs:.1f}h, variación {drop_pct:+.1f}% (umbral bajada 8%)")
                    continue
                log.info(f"    Notificando — hace {hrs:.1f}h, precio cambió {drop_pct:+.1f}%")
            except Exception as e:
                log.debug(f"Cooldown error: {e}")

        msg = build_message(name, alert, top5, best)
        ok  = notify_telegram(msg)
        execute("""INSERT INTO notifications
            (alert_id,origin,destination,dep_date,price,reason,channel)
            VALUES(?,?,?,?,?,?,?)""",
            (alert["id"],best["origin"],best["destination"],best["dep_date"],
             best["price"],"ranking top5","telegram" if ok else ""))
        if ok: total += 1

    log.info(f"Listo: {total} alerta(s) enviada(s)")

# ── Scheduler ─────────────────────────────────────────────────────────────────

_running = False
def start_scheduler():
    global _running
    if _running: return
    _running = True
    interval = int(cfg("check_interval") or 60)
    schedule.every(interval).minutes.do(run_monitor)
    threading.Thread(
        target=lambda:[schedule.run_pending() or time.sleep(30) for _ in iter(int,1)],
        daemon=True).start()
    log.info(f"Scheduler: cada {interval} min")

# ── API routes ────────────────────────────────────────────────────────────────

@app.route("/")
def index(): return render_template("index.html")

@app.route("/api/settings", methods=["GET"])
def get_settings():
    return jsonify({r["key"]:r["value"] for r in fetchall("SELECT key,value FROM settings")})

@app.route("/api/settings", methods=["POST"])
def save_settings():
    for k,v in request.json.items(): set_cfg(k,str(v))
    global _running; schedule.clear(); _running=False; start_scheduler()
    return jsonify({"ok":True})

@app.route("/api/alerts", methods=["GET"])
def get_alerts():
    rows = fetchall("SELECT * FROM alerts ORDER BY created_at DESC")
    for r in rows:
        r["origins"]      = json.loads(r.get("origins") or '["MAD"]')
        r["destinations"] = json.loads(r["destinations"])
        r["dates"]        = json.loads(r.get("dates") or "[]")
    return jsonify(rows)

@app.route("/api/alerts", methods=["POST"])
def create_alert():
    d = request.json
    execute("""INSERT INTO alerts(name,origins,destinations,search_mode,dates,
        explore_month,adults,max_stops,dep_from,dep_to,arr_from,arr_to,max_price,
        trip_type,duration_min,duration_max,enabled)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)""",
        (d["name"],
         json.dumps([x.upper() for x in d.get("origins",["MAD"])]),
         json.dumps([x.upper() for x in d["destinations"]]),
         d.get("search_mode","month"),
         json.dumps(d.get("dates",[])),
         d.get("explore_month",""),
         int(d.get("adults",1)), int(d.get("max_stops",1)),
         d.get("dep_from","06:00"), d.get("dep_to","22:00"),
         d.get("arr_from","06:00"), d.get("arr_to","23:59"),
         float(d["max_price"]) if d.get("max_price") else None,
         d.get("trip_type","one_way"),
         int(d.get("duration_min",5)), int(d.get("duration_max",5))))
    return jsonify({"ok":True})

@app.route("/api/alerts/<int:aid>", methods=["PUT"])
def update_alert(aid):
    d = request.json
    if list(d.keys()) == ["enabled"]:
        execute("UPDATE alerts SET enabled=? WHERE id=?",(d["enabled"],aid))
    else:
        execute("""UPDATE alerts SET name=?,origins=?,destinations=?,search_mode=?,
            dates=?,explore_month=?,adults=?,max_stops=?,dep_from=?,dep_to=?,
            arr_from=?,arr_to=?,max_price=?,trip_type=?,duration_min=?,duration_max=?
            WHERE id=?""",
            (d["name"],
             json.dumps([x.upper() for x in d.get("origins",["MAD"])]),
             json.dumps([x.upper() for x in d["destinations"]]),
             d.get("search_mode","month"),
             json.dumps(d.get("dates",[])),
             d.get("explore_month",""),
             int(d.get("adults",1)), int(d.get("max_stops",1)),
             d.get("dep_from","06:00"), d.get("dep_to","22:00"),
             d.get("arr_from","06:00"), d.get("arr_to","23:59"),
             float(d["max_price"]) if d.get("max_price") else None,
             d.get("trip_type","one_way"),
             int(d.get("duration_min",5)), int(d.get("duration_max",5)), aid))
    return jsonify({"ok":True})

@app.route("/api/alerts/<int:aid>", methods=["DELETE"])
def delete_alert(aid):
    execute("DELETE FROM alerts WHERE id=?",(aid,))
    return jsonify({"ok":True})

@app.route("/api/check-now", methods=["POST"])
def check_now():
    threading.Thread(target=run_monitor, daemon=True).start()
    return jsonify({"ok":True})

@app.route("/api/test-telegram", methods=["POST"])
def test_telegram():
    name = cfg("user_name") or "Viajero"
    ok = notify_telegram(
        f"✅ <b>Bot conectado, {name}!</b>\n\n"
        f"FlightBot está activo y monitorizando vuelos.\n"
        f"Pronto recibirás alertas con análisis completo de precios. ✈️")
    return jsonify({"ok":ok,"msg":"Telegram OK ✓" if ok else "Error — revisa token y chat_id"})

@app.route("/api/stats", methods=["GET"])
def get_stats():
    return jsonify({
        "total_checks":  (fetchone("SELECT COUNT(*) n FROM price_history") or {"n":0})["n"],
        "total_notifs":  (fetchone("SELECT COUNT(*) n FROM notifications") or {"n":0})["n"],
        "active_alerts": (fetchone("SELECT COUNT(*) n FROM alerts WHERE enabled=1") or {"n":0})["n"],
        "recent_notifs": fetchall("""SELECT n.*,a.name alert_name FROM notifications n
            LEFT JOIN alerts a ON a.id=n.alert_id ORDER BY sent_at DESC LIMIT 10"""),
        "recent_prices": fetchall("""
            SELECT p.origin, p.destination, p.dep_date,
                MIN(p.price) min_price, MAX(p.price) max_price, AVG(p.price) avg_price,
                COUNT(*) n, MAX(p.checked_at) last_checked,
                (SELECT dep_time FROM price_history p2
                 WHERE p2.origin=p.origin AND p2.destination=p.destination
                 AND p2.dep_date=p.dep_date AND p2.price=MIN(p.price)
                 ORDER BY p2.checked_at DESC LIMIT 1) dep_time,
                (SELECT arr_time FROM price_history p2
                 WHERE p2.origin=p.origin AND p2.destination=p.destination
                 AND p2.dep_date=p.dep_date AND p2.price=MIN(p.price)
                 ORDER BY p2.checked_at DESC LIMIT 1) arr_time
            FROM price_history p
            GROUP BY p.origin, p.destination, p.dep_date
            ORDER BY last_checked DESC LIMIT 20"""),
    })

# ── Start ─────────────────────────────────────────────────────────────────────

init_db()
start_scheduler()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--port",type=int,default=int(os.environ.get("PORT",8081)))
    args = parser.parse_args()
    print(f"\n✈️  FlightBot → http://localhost:{args.port}\n")
    app.run(host="0.0.0.0", port=args.port, debug=False)

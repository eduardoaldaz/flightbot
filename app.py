"""
FlightBot — Multi-user flight alert app
"""
import json, threading, logging, time, schedule, requests, argparse, os, calendar, secrets
from datetime import datetime, date, timedelta
from pathlib import Path
from statistics import mean
from flask import (Flask, request, jsonify, render_template,
                   redirect, url_for, session, abort)
from authlib.integrations.flask_client import OAuth

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")
if DATABASE_URL:
    import pg8000.dbapi
    from urllib.parse import urlparse as pg_urlparse
else:
    import sqlite3
    DB = Path("bot.db")

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", secrets.token_hex(32))

# ── OAuth ─────────────────────────────────────────────────────────────────────
oauth = OAuth(app)
google = oauth.register(
    name="google",
    client_id=os.environ.get("GOOGLE_CLIENT_ID",""),
    client_secret=os.environ.get("GOOGLE_CLIENT_SECRET",""),
    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
    client_kwargs={"scope": "openid email profile"},
)

# ── DB ────────────────────────────────────────────────────────────────────────
def get_conn():
    if DATABASE_URL:
        u = pg_urlparse(DATABASE_URL)
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

        cur.execute("""CREATE TABLE IF NOT EXISTS users(
            id SERIAL PRIMARY KEY,
            google_id TEXT UNIQUE,
            email TEXT UNIQUE,
            name TEXT,
            avatar TEXT,
            plan TEXT DEFAULT 'free',
            serpapi_key TEXT DEFAULT '',
            telegram_token TEXT DEFAULT '',
            telegram_chat_id TEXT DEFAULT '',
            currency TEXT DEFAULT 'EUR',
            check_interval INTEGER DEFAULT 60,
            created_at TEXT DEFAULT to_char(NOW(),'YYYY-MM-DD HH24:MI:SS')
        )""" if DATABASE_URL else """CREATE TABLE IF NOT EXISTS users(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            google_id TEXT UNIQUE,
            email TEXT UNIQUE,
            name TEXT,
            avatar TEXT,
            plan TEXT DEFAULT 'free',
            serpapi_key TEXT DEFAULT '',
            telegram_token TEXT DEFAULT '',
            telegram_chat_id TEXT DEFAULT '',
            currency TEXT DEFAULT 'EUR',
            check_interval INTEGER DEFAULT 60,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")

        cur.execute(f"""CREATE TABLE IF NOT EXISTS alerts(
            id {pk}, user_id INTEGER NOT NULL,
            name TEXT, origins TEXT, destinations TEXT,
            search_mode TEXT DEFAULT 'month',
            trip_type TEXT DEFAULT 'one_way',
            duration_min INTEGER DEFAULT 5,
            duration_max INTEGER DEFAULT 7,
            dates TEXT DEFAULT '[]',
            explore_month TEXT DEFAULT '',
            adults INTEGER DEFAULT 1,
            max_stops INTEGER DEFAULT 1,
            dep_from TEXT DEFAULT '06:00', dep_to TEXT DEFAULT '22:00',
            arr_from TEXT DEFAULT '06:00', arr_to TEXT DEFAULT '23:59',
            max_price REAL, enabled INTEGER DEFAULT 1, created_at {ts})""")

        cur.execute(f"""CREATE TABLE IF NOT EXISTS price_history(
            id {pk}, user_id INTEGER, alert_id INTEGER,
            origin TEXT, destination TEXT, dep_date TEXT,
            price REAL, currency TEXT, airline TEXT,
            duration TEXT, stops INTEGER, dep_time TEXT, arr_time TEXT,
            checked_at {ts})""")

        cur.execute(f"""CREATE TABLE IF NOT EXISTS notifications(
            id {pk}, user_id INTEGER, alert_id INTEGER,
            origin TEXT, destination TEXT,
            dep_date TEXT, price REAL, reason TEXT, channel TEXT,
            sent_at {ts})""")

        cur.execute(f"CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT)")
        for k,v in [("admin_email",""),("max_free_alerts","3")]:
            cur.execute(ins,(k,v))

        # Migrations for existing deployments
        migrations = [
            "ALTER TABLE alerts ADD COLUMN user_id INTEGER",
            "ALTER TABLE alerts ADD COLUMN trip_type TEXT DEFAULT 'one_way'",
            "ALTER TABLE alerts ADD COLUMN duration_min INTEGER DEFAULT 5",
            "ALTER TABLE alerts ADD COLUMN duration_max INTEGER DEFAULT 7",
            "ALTER TABLE price_history ADD COLUMN user_id INTEGER",
            "ALTER TABLE notifications ADD COLUMN user_id INTEGER",
        ]
        conn.commit()
        for m in migrations:
            try: cur.execute(m); conn.commit()
            except: conn.rollback()

        log.info("DB lista")
    finally:
        conn.close()

# ── Auth helpers ──────────────────────────────────────────────────────────────
def current_user():
    uid = session.get("user_id")
    if not uid: return None
    return fetchone("SELECT * FROM users WHERE id=?", (uid,))

def require_login(f):
    from functools import wraps
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("landing"))
        return f(*args, **kwargs)
    return wrapper

def is_admin(user):
    admin_email = fetchone("SELECT value FROM settings WHERE key='admin_email'")
    if not admin_email: return False
    return user and user.get("email") == admin_email.get("value","")

# ── User config helpers ───────────────────────────────────────────────────────
def ucfg(user, key):
    """
    Get config value for a user.
    - serpapi_key: user's own key if set, else global server key
    - telegram_*: always from user's own config (each user has their own bot)
    """
    val = user.get(key, "") or ""

    if key == "serpapi_key":
        # User's own key takes priority (assigned by admin for Pro users)
        if val: return val
        # Fall back to global server key
        return os.environ.get("SERPAPI_KEY", "")

    if key == "telegram_token":
        return val or os.environ.get("TELEGRAM_TOKEN", "")

    if key == "telegram_chat_id":
        return val or os.environ.get("TELEGRAM_CHAT_ID", "")

    return val

# ── Cities ────────────────────────────────────────────────────────────────────
CITIES = {
    "LIS":"Lisboa","OPO":"Porto","FAO":"Faro","MAD":"Madrid","BCN":"Barcelona",
    "AGP":"Malaga","VLC":"Valencia","BIO":"Bilbao","SVQ":"Sevilla","PMI":"Palma",
    "ALC":"Alicante","SCQ":"Santiago","ZAZ":"Zaragoza","LPA":"Gran Canaria",
    "TFS":"Tenerife Sur","TFN":"Tenerife Norte","ACE":"Lanzarote","FUE":"Fuerteventura",
    "IBZ":"Ibiza","MAH":"Menorca","IST":"Estambul","SAW":"Estambul Sabiha",
    "ATH":"Atenas","HER":"Creta","RHO":"Rodas","AYT":"Antalya",
    "CDG":"Paris","LHR":"Londres","LGW":"Gatwick","STN":"Stansted",
    "AMS":"Amsterdam","BRU":"Bruselas","FCO":"Roma","MXP":"Milan",
    "VCE":"Venecia","FRA":"Frankfurt","MUC":"Munich","BER":"Berlin",
    "VIE":"Viena","ZRH":"Zurich","DUB":"Dublin","PRG":"Praga",
    "BUD":"Budapest","WAW":"Varsovia","CPH":"Copenhague","ARN":"Estocolmo",
    "DXB":"Dubai","DOH":"Doha","CMN":"Casablanca","CAI":"El Cairo",
    "JFK":"Nueva York","MIA":"Miami","LAX":"Los Angeles","BOG":"Bogota",
    "EZE":"Buenos Aires","GRU":"Sao Paulo","SCL":"Santiago Chile",
    "BKK":"Bangkok","SIN":"Singapur","NRT":"Tokio","HKG":"Hong Kong",
}
def city(c): return CITIES.get(c, c)

# ── Notifications ─────────────────────────────────────────────────────────────
def notify_telegram_user(user, msg):
    token    = ucfg(user, "telegram_token")
    chat_id  = ucfg(user, "telegram_chat_id")
    if not token or not chat_id: return False
    try:
        r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id":chat_id,"text":msg,"parse_mode":"HTML"}, timeout=10)
        return r.status_code == 200
    except: return False

# ── Flight search ─────────────────────────────────────────────────────────────
def search_one(origin, dest, dep_date, adults, alert, user):
    key = ucfg(user, "serpapi_key")
    if not key: return None
    try:
        r = requests.get("https://serpapi.com/search", params={
            "engine":"google_flights","departure_id":origin,"arrival_id":dest,
            "outbound_date":dep_date,"adults":adults,
            "currency":user.get("currency","EUR"),"hl":"es",
            "stops":int(alert.get("max_stops",1)),"type":"2","api_key":key}, timeout=15)
        data = r.json()
    except Exception as e:
        log.error(f"SerpApi: {e}"); return None
    if "error" in data:
        log.error(f"SerpApi: {data['error']}"); return None

    dep_from  = alert.get("dep_from","06:00")
    dep_to    = alert.get("dep_to","22:00")
    arr_from  = alert.get("arr_from","06:00")
    arr_to    = alert.get("arr_to","23:59")
    max_stops = int(alert.get("max_stops",1))
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
                        "price":float(price),"currency":user.get("currency","EUR"),
                        "airline":legs[0].get("airline","?"),
                        "duration":f"{dur_m//60}h{dur_m%60:02d}m" if dur_m else "?",
                        "stops":stops,"dep_time":dep_t,"arr_time":arr_t,
                    }
            except: continue
    return best

def search_round_trip(origin, dest, out_date, ret_date, adults, alert, user):
    outbound = search_one(origin, dest, out_date, adults, alert, user)
    if not outbound: return None
    time.sleep(0.3)
    ret_alert = dict(alert)
    ret_alert["dep_from"]="06:00"; ret_alert["dep_to"]="23:00"
    ret_alert["arr_from"]="06:00"; ret_alert["arr_to"]="23:59"
    inbound = search_one(dest, origin, ret_date, adults, ret_alert, user)
    if not inbound: return None
    return {
        "origin":origin,"destination":dest,
        "outbound_date":out_date,"return_date":ret_date,
        "outbound_price":outbound["price"],"inbound_price":inbound["price"],
        "total_price":outbound["price"]+inbound["price"],
        "currency":outbound["currency"],
        "outbound_airline":outbound["airline"],"inbound_airline":inbound["airline"],
        "outbound_dep":outbound["dep_time"],"outbound_arr":outbound["arr_time"],
        "inbound_dep":inbound["dep_time"],"inbound_arr":inbound["arr_time"],
        "outbound_duration":outbound["duration"],"inbound_duration":inbound["duration"],
        "outbound_stops":outbound["stops"],"inbound_stops":inbound["stops"],
    }

def month_dates(ym):
    y,m = map(int,ym.split("-"))
    days = calendar.monthrange(y,m)[1]
    today = date.today()
    return [date(y,m,d).strftime("%Y-%m-%d") for d in range(1,days+1) if date(y,m,d)>=today]

def round_trip_combos(month_str, dur_min, dur_max):
    dates = month_dates(month_str)
    combos = []
    for dep in dates:
        dep_dt = datetime.strptime(dep,"%Y-%m-%d").date()
        for dur in range(dur_min, dur_max+1):
            ret_str = (dep_dt+timedelta(days=dur)).strftime("%Y-%m-%d")
            combos.append((dep,ret_str))
    return combos

# ── Intelligence ──────────────────────────────────────────────────────────────
def get_price_trend(alert_id, origin, dest, dep_date):
    rows = fetchall(f"""SELECT price,checked_at FROM price_history
        WHERE alert_id=? AND origin=? AND destination=? AND dep_date=?
        AND checked_at>{ago(7)} ORDER BY checked_at ASC""",
        (alert_id,origin,dest,dep_date))
    if len(rows)<2: return None
    prices = [r["price"] for r in rows]
    first,last = prices[0],prices[-1]
    pct = (last-first)/first*100 if first>0 else 0
    return {
        "prices":prices[-5:],
        "direction":"bajando" if pct<-2 else "subiendo" if pct>2 else "estable",
        "days":len(set(d["checked_at"][:10] for d in rows)),
    }

def get_month_context(alert_id, month_str):
    if not month_str: return None
    rows = fetchall(f"""SELECT origin,destination,dep_date,MIN(price) min_price
        FROM price_history WHERE alert_id=? AND dep_date LIKE ?
        GROUP BY origin,destination,dep_date ORDER BY min_price ASC LIMIT 8""",
        (alert_id,f"{month_str}%"))
    if not rows: return None
    try:
        y,m = map(int,month_str.split("-"))
        total = sum(1 for d in range(1,calendar.monthrange(y,m)[1]+1) if date(y,m,d)>=date.today())
        scanned = len(set(r["dep_date"] for r in rows))
    except:
        total,scanned = 0,len(rows)
    return {"top":rows[:5],"scanned":scanned,"total":total}

def get_volatility(alert_id, origin, dest):
    rows = fetchall(f"""SELECT price FROM price_history
        WHERE alert_id=? AND origin=? AND destination=? AND checked_at>{ago(7)}
        ORDER BY checked_at ASC""", (alert_id,origin,dest))
    if len(rows)<4: return None
    changes = sum(1 for i in range(1,len(rows)) if abs(rows[i]["price"]-rows[i-1]["price"])>5)
    rate = changes/len(rows)
    return "ALTA" if rate>0.3 else "MEDIA" if rate>0.1 else "BAJA"

def advice_line(days_left, direction):
    if days_left<=14: return "⚠️ Menos de 2 semanas — compra ya si te interesa."
    if days_left<=21: return "⏰ 3 semanas — los precios suben rápido a partir de aquí."
    if days_left<=45:
        if direction=="bajando": return "📉 Puede seguir bajando un poco, pero no esperes demasiado."
        if direction=="subiendo": return "📈 Está subiendo — compra pronto si el precio te encaja."
        return "⚖️ Zona razonable para comprar."
    if days_left<=90:
        if direction=="bajando": return "📉 Más de 2 meses y bajando — espera unos días más."
        return "🕐 Más de 2 meses — monitoriza antes de decidir."
    return "🗓️ Mucha antelación — espera, los precios suelen bajar acercándose a 2-3 meses."

def cross_month_insight(alert_id, current_month, best_price):
    rows = fetchall(f"""SELECT dep_date,MIN(price) min_price FROM price_history
        WHERE alert_id=? AND dep_date NOT LIKE ? AND checked_at>{ago(60)}
        GROUP BY dep_date ORDER BY min_price ASC LIMIT 3""",
        (alert_id,f"{current_month}%"))
    if not rows: return None
    cheaper = [r for r in rows if r["min_price"]<best_price-20]
    if not cheaper: return None
    b = cheaper[0]
    month_name = datetime.strptime(b["dep_date"][:7]+"-01","%Y-%m-%d").strftime("%B")
    return f"💡 {month_name} tiene vuelos desde €{b['min_price']:.0f} (ahorras €{best_price-b['min_price']:.0f})"

def build_message(user, alert, top5, best):
    name  = user.get("name","Viajero").split()[0]
    mode  = alert.get("search_mode","month")
    month = alert.get("explore_month","")
    days_left = (datetime.strptime(best["dep_date"],"%Y-%m-%d").date()-date.today()).days
    try:
        date_s = datetime.strptime(best["dep_date"],"%Y-%m-%d").strftime("%A %d de %B").capitalize()
    except:
        date_s = best["dep_date"]

    medal=["🥇","🥈","🥉","4️⃣","5️⃣"]
    stops_s=lambda f:"directo" if f["stops"]==0 else f"{f['stops']} esc."

    if mode=="month" and month:
        try: month_name=datetime.strptime(month+"-01","%Y-%m-%d").strftime("%B %Y").capitalize()
        except: month_name=month
        header=f"✈️ <b>Mejores vuelos — {month_name}</b>\n<b>{alert['name']}</b>\n\n"
    else:
        header=f"✈️ <b>Vuelo barato encontrado</b>\n<b>{alert['name']}</b>\n\n"

    ranking=""
    for i,f in enumerate(top5):
        ds=f["dep_date"][5:].replace("-","/")
        ranking+=f"{medal[i]} <b>{city(f['origin'])} → {city(f['destination'])}</b> | {ds} | <b>€{f['price']:.0f}</b> | {f['dep_time']} | {stops_s(f)} | {f['airline']}\n"

    trend=get_price_trend(alert["id"],best["origin"],best["destination"],best["dep_date"])
    trend_block=""
    if trend and trend["days"]>=2:
        prices_str=" → ".join(f"€{p:.0f}" for p in trend["prices"])
        emoji="📉" if trend["direction"]=="bajando" else "📈" if trend["direction"]=="subiendo" else "➡️"
        trend_block=f"\n{emoji} <b>Tendencia</b>: {prices_str}\n"

    ctx=get_month_context(alert["id"],month) if month else None
    context_block=""
    if ctx and ctx["scanned"]>1:
        pct=int(ctx["scanned"]/ctx["total"]*100) if ctx["total"]>0 else 0
        context_block=f"\n📊 <b>Contexto del mes</b> ({ctx['scanned']}/{ctx['total']} días — {pct}%):\n"
        for r in ctx["top"][:4]:
            ds=r["dep_date"][5:].replace("-","/")
            flag=" 🟢" if r["dep_date"]==best["dep_date"] else ""
            context_block+=f"  {ds} {city(r['origin'])}→{city(r['destination'])} €{r['min_price']:.0f}{flag}\n"

    days_block=f"\n📅 <b>Quedan {days_left} días</b>\n"
    direction=trend["direction"] if trend else "estable"
    days_block+=advice_line(days_left,direction)+"\n"

    vol=get_volatility(alert["id"],best["origin"],best["destination"])
    vol_block=""
    if vol:
        vol_emoji="🔴" if vol=="ALTA" else "🟡" if vol=="MEDIA" else "🟢"
        vol_block=f"\n{vol_emoji} Volatilidad: {vol}"
        if vol=="ALTA": vol_block+=" — precio cambia frecuentemente"

    cross=cross_month_insight(alert["id"],month,best["price"]) if month else None
    cross_block=f"\n{cross}" if cross else ""

    gf=f"https://www.google.com/flights#flt={best['origin']}.{best['destination']}.{best['dep_date']};c:{user.get('currency','EUR')};e:1;sd:1;t:f"
    sk=f"https://www.skyscanner.es/transporte/vuelos/{best['origin'].lower()}/{best['destination'].lower()}/{best['dep_date'].replace('-','')[2:]}/"
    links=f"\n🔗 <a href='{gf}'>Google Flights</a> · <a href='{sk}'>Skyscanner</a>"

    return header+ranking+trend_block+context_block+days_block+vol_block+cross_block+links

def build_roundtrip_message(user, alert, top5_rt, best):
    name=user.get("name","Viajero").split()[0]
    try:
        out_d=datetime.strptime(best["outbound_date"],"%Y-%m-%d").strftime("%d/%m")
        ret_d=datetime.strptime(best["return_date"],"%Y-%m-%d").strftime("%d/%m")
        dur=(datetime.strptime(best["return_date"],"%Y-%m-%d")-datetime.strptime(best["outbound_date"],"%Y-%m-%d")).days
    except:
        out_d=best["outbound_date"]; ret_d=best["return_date"]; dur=0
    try: month_name=datetime.strptime(alert.get("explore_month","2026-01")+"-01","%Y-%m-%d").strftime("%B %Y").capitalize()
    except: month_name=alert.get("explore_month","")

    medal=["🥇","🥈","🥉","4️⃣","5️⃣"]
    header=f"✈️ <b>Ida y vuelta — {month_name}</b>\n<b>{alert['name']}</b>\n\n"
    lines=[]
    for i,rt in enumerate(top5_rt):
        try:
            od=datetime.strptime(rt["outbound_date"],"%Y-%m-%d").strftime("%d/%m")
            rd=datetime.strptime(rt["return_date"],"%Y-%m-%d").strftime("%d/%m")
            d=(datetime.strptime(rt["return_date"],"%Y-%m-%d")-datetime.strptime(rt["outbound_date"],"%Y-%m-%d")).days
        except: od=rt["outbound_date"]; rd=rt["return_date"]; d=0
        out_s="directo" if rt["outbound_stops"]==0 else f"{rt['outbound_stops']}esc."
        ret_s="directo" if rt["inbound_stops"]==0 else f"{rt['inbound_stops']}esc."
        lines.append(f"{medal[i]} <b>{city(rt['origin'])}↔{city(rt['destination'])}</b> | Ida: {od} ({out_s}) · Vuelta: {rd} ({ret_s}) | <b>€{rt['total_price']:.0f} total</b> | {d} días")

    days_left=(datetime.strptime(best["outbound_date"],"%Y-%m-%d").date()-date.today()).days
    gf=(f"https://www.google.com/flights#flt={best['origin']}.{best['destination']}."
        f"{best['outbound_date']}*{best['destination']}.{best['origin']}."
        f"{best['return_date']};c:{user.get('currency','EUR')};e:1;sd:1;t:r")
    sk=(f"https://www.skyscanner.es/transporte/vuelos/{best['origin'].lower()}/"
        f"{best['destination'].lower()}/{best['outbound_date'].replace('-','')[2:]}/"
        f"{best['return_date'].replace('-','')[2:]}/")

    return (header+"\n".join(lines)+
        f"\n\n📅 <b>Quedan {days_left} días</b>\n"+advice_line(days_left,"estable")+
        f"\n\n🔗 <a href='{gf}'>Google Flights I+V</a> · <a href='{sk}'>Skyscanner</a>")

# ── Monitor ───────────────────────────────────────────────────────────────────
def run_monitor():
    log.info("Comprobando vuelos...")
    alerts = fetchall("SELECT * FROM alerts WHERE enabled=1")
    total  = 0

    for alert in alerts:
        uid = alert.get("user_id")
        if not uid: continue
        user = fetchone("SELECT * FROM users WHERE id=?", (uid,))
        if not user: continue

        origins   = json.loads(alert.get("origins") or '["MAD"]')
        dests     = json.loads(alert["destinations"])
        adults    = alert["adults"]
        max_p     = float(alert["max_price"]) if alert.get("max_price") else None
        mode      = alert.get("search_mode","month")
        trip_type = alert.get("trip_type","one_way")
        dur_min   = int(alert.get("duration_min") or 5)
        dur_max   = int(alert.get("duration_max") or 7)

        if mode=="month" and alert.get("explore_month"):
            all_dates = month_dates(alert["explore_month"])
        else:
            raw = json.loads(alert.get("dates") or "[]")
            today = date.today()
            all_dates = sorted([d for d in raw if 0<=(datetime.strptime(d,"%Y-%m-%d").date()-today).days<=180])

        if not all_dates: continue

        # ── Round trip ────────────────────────────────────────────────────
        if trip_type=="round_trip" and mode=="month" and alert.get("explore_month"):
            all_combos = round_trip_combos(alert["explore_month"],dur_min,dur_max)
            offset = datetime.utcnow().hour % max(1,len(all_combos))
            combos = all_combos[offset:offset+2] or all_combos[:2]
            rt_results=[]
            for (dep_date,ret_date) in combos:
                for origin in origins:
                    for dest in dests:
                        rt=search_round_trip(origin,dest,dep_date,ret_date,adults,alert,user)
                        if rt:
                            execute("""INSERT INTO price_history
                                (user_id,alert_id,origin,destination,dep_date,price,currency,
                                 airline,duration,stops,dep_time,arr_time)
                                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                                (uid,alert["id"],rt["origin"],rt["destination"],rt["outbound_date"],
                                 rt["outbound_price"],rt["currency"],rt["outbound_airline"],
                                 rt["outbound_duration"],rt["outbound_stops"],
                                 rt["outbound_dep"],rt["outbound_arr"]))
                            if max_p is None or rt["total_price"]<=max_p*2:
                                rt_results.append(rt)
                        time.sleep(0.3)
            if not rt_results: continue
            rt_results.sort(key=lambda x:x["total_price"])
            best_rt=rt_results[0]
            last=fetchone("SELECT price,sent_at FROM notifications WHERE alert_id=? ORDER BY sent_at DESC LIMIT 1",(alert["id"],))
            if last:
                try:
                    hrs=(datetime.utcnow()-datetime.fromisoformat(last["sent_at"])).total_seconds()/3600
                    drop_pct=(float(last["price"])-best_rt["total_price"])/float(last["price"])*100 if last["price"] else 0
                    if hrs<12 and drop_pct<8: continue
                except: pass
            msg=build_roundtrip_message(user,alert,rt_results[:5],best_rt)
            ok=notify_telegram_user(user,msg)
            execute("INSERT INTO notifications(user_id,alert_id,origin,destination,dep_date,price,reason,channel) VALUES(?,?,?,?,?,?,?,?)",
                (uid,alert["id"],best_rt["origin"],best_rt["destination"],best_rt["outbound_date"],best_rt["total_price"],"rt top5","telegram" if ok else ""))
            if ok: total+=1
            continue

        # ── One way ───────────────────────────────────────────────────────
        offset = datetime.utcnow().hour % max(1,len(all_dates))
        dates  = all_dates[offset:offset+3] or all_dates[:3]
        results=[]
        for dep_date in dates:
            for origin in origins:
                for dest in dests:
                    f=search_one(origin,dest,dep_date,adults,alert,user)
                    if f:
                        execute("""INSERT INTO price_history
                            (user_id,alert_id,origin,destination,dep_date,price,currency,
                             airline,duration,stops,dep_time,arr_time)
                            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                            (uid,alert["id"],f["origin"],f["destination"],f["dep_date"],
                             f["price"],f["currency"],f["airline"],f["duration"],
                             f["stops"],f["dep_time"],f["arr_time"]))
                        if max_p is None or f["price"]<=max_p:
                            results.append(f)
                    time.sleep(0.3)
        if not results: continue
        results.sort(key=lambda x:x["price"])
        top5=results[:5]; best=top5[0]
        log.info(f"  {alert['name']}: {best['origin']}→{best['destination']} {best['dep_date']} €{best['price']:.0f}")

        last=fetchone("SELECT price,sent_at FROM notifications WHERE alert_id=? ORDER BY sent_at DESC LIMIT 1",(alert["id"],))
        if last:
            try:
                hrs=(datetime.utcnow()-datetime.fromisoformat(last["sent_at"])).total_seconds()/3600
                last_p=float(last["price"]) if last["price"] else 0
                drop_pct=(last_p-best["price"])/last_p*100 if last_p>0 else 0
                if hrs<12 and drop_pct<8:
                    log.info(f"    Omitiendo — hace {hrs:.1f}h, variación {drop_pct:+.1f}%")
                    continue
            except: pass

        msg=build_message(user,alert,top5,best)
        ok=notify_telegram_user(user,msg)
        execute("INSERT INTO notifications(user_id,alert_id,origin,destination,dep_date,price,reason,channel) VALUES(?,?,?,?,?,?,?,?)",
            (uid,alert["id"],best["origin"],best["destination"],best["dep_date"],best["price"],"top5","telegram" if ok else ""))
        if ok: total+=1

    log.info(f"Listo: {total} alerta(s) enviada(s)")

_running=False
def start_scheduler():
    global _running
    if _running: return
    _running=True
    schedule.every(60).minutes.do(run_monitor)
    threading.Thread(target=lambda:[schedule.run_pending() or time.sleep(30) for _ in iter(int,1)],daemon=True).start()
    log.info("Scheduler iniciado")

# ── Auth routes ───────────────────────────────────────────────────────────────
@app.route("/")
def landing():
    user = current_user()
    if user: return redirect(url_for("dashboard"))
    return render_template("landing.html")

@app.route("/login")
def login():
    redirect_uri = url_for("auth_callback", _external=True)
    return google.authorize_redirect(redirect_uri)

@app.route("/auth/callback")
def auth_callback():
    try:
        token = google.authorize_access_token()
        info  = token.get("userinfo") or google.userinfo()
        google_id = info["sub"]
        email     = info["email"]
        name      = info.get("name","")
        avatar    = info.get("picture","")

        user = fetchone("SELECT * FROM users WHERE google_id=?", (google_id,))
        if not user:
            execute("INSERT INTO users(google_id,email,name,avatar) VALUES(?,?,?,?)",
                    (google_id,email,name,avatar))
            user = fetchone("SELECT * FROM users WHERE google_id=?", (google_id,))
        else:
            execute("UPDATE users SET name=?,avatar=? WHERE google_id=?",(name,avatar,google_id))

        session["user_id"] = user["id"]
        return redirect(url_for("dashboard"))
    except Exception as e:
        log.error(f"Auth error: {e}")
        return redirect(url_for("landing"))

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("landing"))

# ── App routes ────────────────────────────────────────────────────────────────
@app.route("/app")
@require_login
def dashboard():
    return render_template("app.html")

# ── API ───────────────────────────────────────────────────────────────────────
@app.route("/api/me", methods=["GET"])
@require_login
def get_me():
    user = current_user()
    alerts_count = fetchone("SELECT COUNT(*) n FROM alerts WHERE user_id=?",(user["id"],))
    return jsonify({
        "id":user["id"],"name":user["name"],"email":user["email"],
        "avatar":user.get("avatar",""),"plan":user.get("plan","free"),
        "currency":user.get("currency","EUR"),
        "has_serpapi": bool(ucfg(user,"serpapi_key")),
        "has_telegram": bool(ucfg(user,"telegram_token") and ucfg(user,"telegram_chat_id")),
        "alerts_count": alerts_count["n"] if alerts_count else 0,
        "max_alerts": 999 if user.get("plan")=="pro" else 3,
    })

@app.route("/api/me", methods=["POST"])
@require_login
def update_me():
    user = current_user()
    d = request.json
    execute("""UPDATE users SET serpapi_key=?,telegram_token=?,telegram_chat_id=?,
        currency=?,check_interval=? WHERE id=?""",
        (d.get("serpapi_key",""),d.get("telegram_token",""),
         d.get("telegram_chat_id",""),d.get("currency","EUR"),
         int(d.get("check_interval",60)),user["id"]))
    return jsonify({"ok":True})

@app.route("/api/alerts", methods=["GET"])
@require_login
def get_alerts():
    user = current_user()
    rows = fetchall("SELECT * FROM alerts WHERE user_id=? ORDER BY created_at DESC",(user["id"],))
    for r in rows:
        r["origins"]      = json.loads(r.get("origins") or '["MAD"]')
        r["destinations"] = json.loads(r["destinations"])
        r["dates"]        = json.loads(r.get("dates") or "[]")
    return jsonify(rows)

@app.route("/api/alerts", methods=["POST"])
@require_login
def create_alert():
    user = current_user()
    max_alerts = 999 if user.get("plan")=="pro" else 3
    count = fetchone("SELECT COUNT(*) n FROM alerts WHERE user_id=?",(user["id"],))
    if count and count["n"] >= max_alerts:
        return jsonify({"ok":False,"error":f"Plan gratuito: máximo {max_alerts} alertas. Actualiza a Pro para ilimitadas."}),403
    d = request.json
    execute("""INSERT INTO alerts(user_id,name,origins,destinations,search_mode,trip_type,
        duration_min,duration_max,dates,explore_month,adults,max_stops,
        dep_from,dep_to,arr_from,arr_to,max_price,enabled)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)""",
        (user["id"],d["name"],
         json.dumps([x.upper() for x in d.get("origins",["MAD"])]),
         json.dumps([x.upper() for x in d["destinations"]]),
         d.get("search_mode","month"),d.get("trip_type","one_way"),
         int(d.get("duration_min",5)),int(d.get("duration_max",7)),
         json.dumps(d.get("dates",[])),d.get("explore_month",""),
         int(d.get("adults",1)),int(d.get("max_stops",1)),
         d.get("dep_from","06:00"),d.get("dep_to","22:00"),
         d.get("arr_from","06:00"),d.get("arr_to","23:59"),
         float(d["max_price"]) if d.get("max_price") else None))
    return jsonify({"ok":True})

@app.route("/api/alerts/<int:aid>", methods=["PUT"])
@require_login
def update_alert(aid):
    user = current_user()
    d = request.json
    if list(d.keys())==["enabled"]:
        execute("UPDATE alerts SET enabled=? WHERE id=? AND user_id=?",(d["enabled"],aid,user["id"]))
    else:
        execute("""UPDATE alerts SET name=?,origins=?,destinations=?,search_mode=?,trip_type=?,
            duration_min=?,duration_max=?,dates=?,explore_month=?,adults=?,max_stops=?,
            dep_from=?,dep_to=?,arr_from=?,arr_to=?,max_price=? WHERE id=? AND user_id=?""",
            (d["name"],json.dumps([x.upper() for x in d.get("origins",["MAD"])]),
             json.dumps([x.upper() for x in d["destinations"]]),
             d.get("search_mode","month"),d.get("trip_type","one_way"),
             int(d.get("duration_min",5)),int(d.get("duration_max",7)),
             json.dumps(d.get("dates",[])),d.get("explore_month",""),
             int(d.get("adults",1)),int(d.get("max_stops",1)),
             d.get("dep_from","06:00"),d.get("dep_to","22:00"),
             d.get("arr_from","06:00"),d.get("arr_to","23:59"),
             float(d["max_price"]) if d.get("max_price") else None,
             aid,user["id"]))
    return jsonify({"ok":True})

@app.route("/api/alerts/<int:aid>", methods=["DELETE"])
@require_login
def delete_alert(aid):
    user = current_user()
    execute("DELETE FROM alerts WHERE id=? AND user_id=?",(aid,user["id"]))
    return jsonify({"ok":True})

@app.route("/api/stats", methods=["GET"])
@require_login
def get_stats():
    user = current_user()
    uid  = user["id"]
    return jsonify({
        "total_checks":  (fetchone("SELECT COUNT(*) n FROM price_history WHERE user_id=?",(uid,)) or {"n":0})["n"],
        "total_notifs":  (fetchone("SELECT COUNT(*) n FROM notifications WHERE user_id=?",(uid,)) or {"n":0})["n"],
        "active_alerts": (fetchone("SELECT COUNT(*) n FROM alerts WHERE user_id=? AND enabled=1",(uid,)) or {"n":0})["n"],
        "recent_notifs": fetchall("""SELECT n.*,a.name alert_name FROM notifications n
            LEFT JOIN alerts a ON a.id=n.alert_id
            WHERE n.user_id=? ORDER BY sent_at DESC LIMIT 10""",(uid,)),
        "recent_prices": fetchall(f"""SELECT origin,destination,dep_date,
            MIN(price) min_price,MAX(price) max_price,AVG(price) avg_price,
            COUNT(*) n,MAX(checked_at) last_checked,
            (SELECT dep_time FROM price_history p2
             WHERE p2.origin=p.origin AND p2.destination=p.destination
             AND p2.dep_date=p.dep_date AND p2.user_id={uid}
             ORDER BY p2.checked_at DESC LIMIT 1) dep_time,
            (SELECT arr_time FROM price_history p2
             WHERE p2.origin=p.origin AND p2.destination=p.destination
             AND p2.dep_date=p.dep_date AND p2.user_id={uid}
             ORDER BY p2.checked_at DESC LIMIT 1) arr_time
            FROM price_history p WHERE user_id={uid}
            GROUP BY origin,destination,dep_date
            ORDER BY last_checked DESC LIMIT 20"""),
    })

@app.route("/api/check-now", methods=["POST"])
@require_login
def check_now():
    threading.Thread(target=run_monitor,daemon=True).start()
    return jsonify({"ok":True})

@app.route("/api/test-telegram", methods=["POST"])
@require_login
def test_telegram():
    user = current_user()
    name = user.get("name","Viajero").split()[0]
    ok = notify_telegram_user(user,
        f"✅ <b>Bot conectado, {name}!</b>\n\nFlightBot funcionando correctamente. ✈️")
    return jsonify({"ok":ok,"msg":"Telegram OK ✓" if ok else "Error — revisa token y chat_id"})

# ── Admin ─────────────────────────────────────────────────────────────────────
@app.route("/admin")
@require_login
def admin():
    user = current_user()
    if not is_admin(user): abort(403)
    return render_template("admin.html")

@app.route("/api/admin/stats")
@require_login
def admin_stats():
    user = current_user()
    if not is_admin(user): abort(403)
    return jsonify({
        "users":    fetchall("SELECT id,name,email,plan,serpapi_key,created_at FROM users ORDER BY created_at DESC"),
        "alerts":   fetchone("SELECT COUNT(*) n FROM alerts")["n"],
        "checks":   fetchone("SELECT COUNT(*) n FROM price_history")["n"],
        "notifs":   fetchone("SELECT COUNT(*) n FROM notifications")["n"],
        "global_serpapi": bool(os.environ.get("SERPAPI_KEY","")),
    })

@app.route("/api/admin/set-plan", methods=["POST"])
@require_login
def admin_set_plan():
    user = current_user()
    if not is_admin(user): abort(403)
    d = request.json
    execute("UPDATE users SET plan=? WHERE id=?",(d["plan"],d["user_id"]))
    return jsonify({"ok":True})

@app.route("/api/admin/set-serpapi", methods=["POST"])
@require_login
def admin_set_serpapi():
    user = current_user()
    if not is_admin(user): abort(403)
    d = request.json
    execute("UPDATE users SET serpapi_key=? WHERE id=?",(d.get("key",""),d["user_id"]))
    return jsonify({"ok":True})

# ── Start ─────────────────────────────────────────────────────────────────────
init_db()
start_scheduler()

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--port",type=int,default=int(os.environ.get("PORT",8081)))
    args=parser.parse_args()
    app.run(host="0.0.0.0",port=args.port,debug=False)

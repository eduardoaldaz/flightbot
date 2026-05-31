import json, threading, logging, time, schedule, requests, argparse, os, calendar
from datetime import datetime, date
from pathlib import Path
from statistics import mean
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
            search_mode TEXT DEFAULT 'dates',
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
        conn.commit()
        log.info("DB lista")
    finally:
        conn.close()

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

CITIES = {
    "LIS":"Lisboa","OPO":"Porto","FAO":"Faro","MAD":"Madrid","BCN":"Barcelona",
    "AGP":"Malaga","VLC":"Valencia","BIO":"Bilbao","SVQ":"Sevilla","PMI":"Palma",
    "ALC":"Alicante","SCQ":"Santiago","ZAZ":"Zaragoza","SDR":"Santander",
    "LPA":"Gran Canaria","TFS":"Tenerife Sur","TFN":"Tenerife Norte",
    "ACE":"Lanzarote","FUE":"Fuerteventura","IST":"Estambul","SAW":"Estambul Sabiha",
    "ATH":"Atenas","CDG":"Paris","LHR":"Londres","AMS":"Amsterdam","FCO":"Roma",
    "MXP":"Milan","FRA":"Frankfurt","MUC":"Munich","BER":"Berlin","DXB":"Dubai",
    "JFK":"Nueva York","MIA":"Miami","BOG":"Bogota","EZE":"Buenos Aires",
}
def city(c): return CITIES.get(c,c)

def notify_telegram(msg):
    token,chat_id=cfg("telegram_token"),cfg("telegram_chat_id")
    if not token or not chat_id: return False
    try:
        r=requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id":chat_id,"text":msg,"parse_mode":"HTML"},timeout=10)
        return r.status_code==200
    except: return False

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

def month_dates(ym):
    y,m = map(int, ym.split("-"))
    days = calendar.monthrange(y,m)[1]
    today = date.today()
    dates = []
    for d in range(1, days+1):
        dt = date(y,m,d)
        if dt >= today:
            dates.append(dt.strftime("%Y-%m-%d"))
    return dates

def run_monitor():
    log.info("Comprobando vuelos...")
    alerts = fetchall("SELECT * FROM alerts WHERE enabled=1")
    name  = cfg("user_name") or "Viajero"
    total = 0

    for alert in alerts:
        origins  = json.loads(alert.get("origins") or '["MAD"]')
        dests    = json.loads(alert["destinations"])
        adults   = alert["adults"]
        max_p    = float(alert["max_price"]) if alert.get("max_price") else None
        mode     = alert.get("search_mode","dates")

        if mode == "month" and alert.get("explore_month"):
            dates = month_dates(alert["explore_month"])
            # For month mode: rotate to check ~3 dates per hour
            offset = datetime.utcnow().hour % max(1,len(dates))
            dates = dates[offset:offset+3] or dates[:3]
        else:
            all_dates = json.loads(alert.get("dates") or "[]")
            today = date.today()
            dates = sorted([d for d in all_dates
                if 0<=(datetime.strptime(d,"%Y-%m-%d").date()-today).days<=180])
            offset = datetime.utcnow().hour % max(1,len(dates)) if dates else 0
            dates = dates[offset:offset+3] or dates[:3]

        if not dates: continue

        # Search all origin x destination x date combinations
        results = []
        for dep_date in dates:
            for origin in origins:
                for dest in dests:
                    f = search_one(origin, dest, dep_date, adults, alert)
                    if f:
                        execute("""INSERT INTO price_history
                            (alert_id,origin,destination,dep_date,price,currency,airline,duration,stops,dep_time,arr_time)
                            VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                            (alert["id"],f["origin"],f["destination"],f["dep_date"],
                             f["price"],f["currency"],f["airline"],f["duration"],
                             f["stops"],f["dep_time"],f["arr_time"]))
                        if max_p is None or f["price"] <= max_p:
                            results.append(f)
                    time.sleep(0.3)

        if not results: continue
        results.sort(key=lambda x: x["price"])
        top5 = results[:5]
        best = top5[0]

        # Check if already sent similar alert recently
        last = fetchone("""SELECT price,sent_at FROM notifications
            WHERE alert_id=? ORDER BY sent_at DESC LIMIT 1""", (alert["id"],))
        if last:
            try:
                hrs = (datetime.utcnow()-datetime.fromisoformat(last["sent_at"])).total_seconds()/3600
                drp = (last["price"]-best["price"])/last["price"]*100
                if hrs<12 and drp<10: continue
            except: pass

        # Build message
        date_str = lambda d: datetime.strptime(d,"%Y-%m-%d").strftime("%d/%m").strip()
        medal = ["🥇","🥈","🥉","4️⃣","5️⃣"]
        lines = []
        for i,f in enumerate(top5):
            stops = "directo" if f["stops"]==0 else f"{f['stops']} esc."
            lines.append(f"{medal[i]} {city(f['origin'])}→{city(f['destination'])} | {date_str(f['dep_date'])} | <b>€{f['price']:.0f}</b> | {f['dep_time']} | {stops} | {f['airline']}")

        gf = f"https://www.google.com/travel/flights?q=vuelos+{best['origin']}+{best['destination']}+{best['dep_date']}"

        if mode == "month":
            month_name = datetime.strptime(alert["explore_month"]+"-01","%Y-%m-%d").strftime("%B %Y").capitalize()
            header = f"✈️ <b>Mejores vuelos en {month_name}, {name}</b>\n<b>{alert['name']}</b>\n\n"
        else:
            header = f"✈️ <b>Vuelos baratos encontrados, {name}</b>\n<b>{alert['name']}</b>\n\n"

        msg = header + "\n".join(lines) + f"\n\n<a href='{gf}'>Ver en Google Flights</a>"
        ok = notify_telegram(msg)
        execute("""INSERT INTO notifications(alert_id,origin,destination,dep_date,price,reason,channel)
            VALUES(?,?,?,?,?,?,?)""",
            (alert["id"],best["origin"],best["destination"],best["dep_date"],
             best["price"],"ranking top5","telegram" if ok else ""))
        total += 1
        log.info(f"  Alerta '{alert['name']}': mejor €{best['price']:.0f} {best['origin']}→{best['destination']}")

    log.info(f"Listo: {total} alerta(s) enviada(s)")

_running=False
def start_scheduler():
    global _running
    if _running: return
    _running=True
    interval=int(cfg("check_interval") or 60)
    schedule.every(interval).minutes.do(run_monitor)
    threading.Thread(target=lambda:[schedule.run_pending() or time.sleep(30) for _ in iter(int,1)],daemon=True).start()
    log.info(f"Scheduler: cada {interval} min")

@app.route("/")
def index(): return render_template("index.html")

@app.route("/api/settings",methods=["GET"])
def get_settings():
    return jsonify({r["key"]:r["value"] for r in fetchall("SELECT key,value FROM settings")})

@app.route("/api/settings",methods=["POST"])
def save_settings():
    for k,v in request.json.items(): set_cfg(k,str(v))
    global _running; schedule.clear(); _running=False; start_scheduler()
    return jsonify({"ok":True})

@app.route("/api/alerts",methods=["GET"])
def get_alerts():
    rows = fetchall("SELECT * FROM alerts ORDER BY created_at DESC")
    for r in rows:
        r["origins"]      = json.loads(r.get("origins") or '["MAD"]')
        r["destinations"] = json.loads(r["destinations"])
        if r.get("dates"): r["dates"] = json.loads(r["dates"])
        else: r["dates"] = []
    return jsonify(rows)

@app.route("/api/alerts",methods=["POST"])
def create_alert():
    d = request.json
    execute("""INSERT INTO alerts(name,origins,destinations,search_mode,dates,explore_month,
        adults,max_stops,dep_from,dep_to,arr_from,arr_to,max_price,enabled)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,1)""",
        (d["name"],
         json.dumps([x.upper() for x in d.get("origins",["MAD"])]),
         json.dumps([x.upper() for x in d["destinations"]]),
         d.get("search_mode","dates"),
         json.dumps(d.get("dates",[])),
         d.get("explore_month",""),
         int(d.get("adults",1)),int(d.get("max_stops",1)),
         d.get("dep_from","06:00"),d.get("dep_to","22:00"),
         d.get("arr_from","06:00"),d.get("arr_to","23:59"),
         float(d["max_price"]) if d.get("max_price") else None))
    return jsonify({"ok":True})

@app.route("/api/alerts/<int:aid>",methods=["PUT"])
def update_alert(aid):
    d = request.json
    if list(d.keys())==["enabled"]:
        execute("UPDATE alerts SET enabled=? WHERE id=?",(d["enabled"],aid))
    else:
        execute("""UPDATE alerts SET name=?,origins=?,destinations=?,search_mode=?,
            dates=?,explore_month=?,adults=?,max_stops=?,dep_from=?,dep_to=?,
            arr_from=?,arr_to=?,max_price=? WHERE id=?""",
            (d["name"],
             json.dumps([x.upper() for x in d.get("origins",["MAD"])]),
             json.dumps([x.upper() for x in d["destinations"]]),
             d.get("search_mode","dates"),
             json.dumps(d.get("dates",[])),
             d.get("explore_month",""),
             int(d.get("adults",1)),int(d.get("max_stops",1)),
             d.get("dep_from","06:00"),d.get("dep_to","22:00"),
             d.get("arr_from","06:00"),d.get("arr_to","23:59"),
             float(d["max_price"]) if d.get("max_price") else None,aid))
    return jsonify({"ok":True})

@app.route("/api/alerts/<int:aid>",methods=["DELETE"])
def delete_alert(aid):
    execute("DELETE FROM alerts WHERE id=?",(aid,)); return jsonify({"ok":True})

@app.route("/api/check-now",methods=["POST"])
def check_now():
    threading.Thread(target=run_monitor,daemon=True).start()
    return jsonify({"ok":True})

@app.route("/api/test-telegram",methods=["POST"])
def test_telegram():
    name=cfg("user_name") or "Viajero"
    ok=notify_telegram(f"✅ Bot conectado, <b>{name}</b>! FlightBot funcionando correctamente. ✈️")
    return jsonify({"ok":ok,"msg":"Telegram OK" if ok else "Error - revisa token y chat_id"})

@app.route("/api/stats",methods=["GET"])
def get_stats():
    return jsonify({
        "total_checks":(fetchone("SELECT COUNT(*) n FROM price_history") or {"n":0})["n"],
        "total_notifs":(fetchone("SELECT COUNT(*) n FROM notifications") or {"n":0})["n"],
        "active_alerts":(fetchone("SELECT COUNT(*) n FROM alerts WHERE enabled=1") or {"n":0})["n"],
        "recent_notifs":fetchall("SELECT n.*,a.name alert_name FROM notifications n LEFT JOIN alerts a ON a.id=n.alert_id ORDER BY sent_at DESC LIMIT 10"),
        "recent_prices":fetchall("""SELECT origin,destination,dep_date,
            MIN(price) min_price,MAX(price) max_price,AVG(price) avg_price,
            COUNT(*) n,MAX(checked_at) last_checked
            FROM price_history GROUP BY origin,destination,dep_date
            ORDER BY last_checked DESC LIMIT 20"""),
    })

init_db()
start_scheduler()

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--port",type=int,default=int(os.environ.get("PORT",8081)))
    args=parser.parse_args()
    print(f"\nFlightBot -> http://localhost:{args.port}\n")
    app.run(host="0.0.0.0",port=args.port,debug=False)

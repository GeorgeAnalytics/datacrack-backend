""" main.py — DataCrack Backend API
FastAPI + scraper Google News RSS + Claude + PostgreSQL
Deploy: Railway
"""
import os, json, hashlib, time, asyncio
from datetime import datetime, timezone
from urllib.parse import quote
from contextlib import asynccontextmanager

import feedparser
import anthropic
import psycopg2
import psycopg2.extras
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware

# ── CONFIG ─────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
DATABASE_URL      = os.environ.get("DATABASE_URL", "")
MAX_ARTICLES      = 6
MAX_RETRIES       = 3
RETRY_WAIT        = 5
FRONTEND_URL      = os.environ.get("FRONTEND_URL", "*")

# ── CACHÉ EN MEMORIA ───────────────────────────────────────────
cache = {
    "data": None,
    "actualizando_noticias": False,
    "actualizando_personas": False,
    "ultimo_update": None,
    "error": None,
}

# ── POSTGRESQL ─────────────────────────────────────────────────
def get_conn():
    url = DATABASE_URL.replace("postgresql://", "postgres://", 1)
    return psycopg2.connect(url)

def init_db():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS cache_noticias (
                id INTEGER PRIMARY KEY DEFAULT 1,
                data JSONB,
                ultimo_update TIMESTAMP WITH TIME ZONE,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS alertas_historial (
                id SERIAL PRIMARY KEY,
                texto TEXT NOT NULL,
                topic VARCHAR(50),
                fecha TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error init_db: {e}")

def guardar_cache_db():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO cache_noticias (id, data, ultimo_update)
            VALUES (1, %s, %s)
            ON CONFLICT (id) DO UPDATE
            SET data = EXCLUDED.data,
                ultimo_update = EXCLUDED.ultimo_update,
                updated_at = NOW()
        """, (json.dumps(cache["data"], ensure_ascii=False), cache["ultimo_update"]))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error guardar_cache_db: {e}")

def cargar_cache_db():
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT data, ultimo_update FROM cache_noticias WHERE id = 1")
        row = cur.fetchone()
        if row:
            cache["data"]          = row["data"]
            cache["ultimo_update"] = row["ultimo_update"].isoformat() if row["ultimo_update"] else None
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error cargar_cache_db: {e}")

def guardar_alerta_db(texto, topic):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT texto FROM alertas_historial
            WHERE topic = %s
            ORDER BY fecha DESC LIMIT 1
        """, (topic,))
        row = cur.fetchone()
        if not row or row[0] != texto:
            cur.execute("INSERT INTO alertas_historial (texto, topic) VALUES (%s, %s)", (texto, topic))
            cur.execute("""
                DELETE FROM alertas_historial
                WHERE id NOT IN (
                    SELECT id FROM alertas_historial ORDER BY fecha DESC LIMIT 50
                )
            """)
            conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error guardar_alerta_db: {e}")

def cargar_alertas_db():
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT texto, topic, fecha FROM alertas_historial ORDER BY fecha DESC LIMIT 50")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [{"texto": r["texto"], "topic": r["topic"], "fecha": r["fecha"].isoformat()} for r in rows]
    except Exception as e:
        print(f"Error cargar_alertas_db: {e}")
        return []

# ── PERSONAS ────────────────────────────────────────────────────
PERSONAS = [
    {"id": "bruno",   "nombre": "Bruno Blancas",      "partido": "Morena", "cargo": "Político"},
    {"id": "ra",      "nombre": "Ra Aguilar",          "partido": "Morena", "cargo": "Político"},
    {"id": "chuyita", "nombre": "Chuyita López",       "partido": "Morena", "cargo": "Política"},
    {"id": "flor",    "nombre": "Flor Michel López",   "partido": "Morena", "cargo": "Política"},
    {"id": "diego",   "nombre": "Diego Franco",        "partido": "MC",     "cargo": "Político"},
    {"id": "lupita",  "nombre": "Guadalupe Guerrero",  "partido": "MC",     "cargo": "Política"},
    {"id": "ricardo", "nombre": "Ricardo René",        "partido": "MC",     "cargo": "Político"},
    {"id": "luis",    "nombre": "Luis Munguia",        "partido": "PVEM",   "cargo": "Político"},
    {"id": "yussara", "nombre": "Yussara Canales",     "partido": "PVEM",   "cargo": "Política"},
]

# ── FEEDS ────────────────────────────────────────────────────────
FEEDS = {
    "vallarta": [
        "https://news.google.com/rss/search?q=Puerto+Vallarta&hl=es-419&gl=MX&ceid=MX:es-419",
        "https://news.google.com/rss/search?q=Puerto+Vallarta+politica+municipio&hl=es-419&gl=MX&ceid=MX:es-419",
        "https://news.google.com/rss/search?q=Puerto+Vallarta+gobierno+Jalisco&hl=es-419&gl=MX&ceid=MX:es-419",
    ],
    "morena": [
        "https://news.google.com/rss/search?q=Morena+partido+Mexico&hl=es-419&gl=MX&ceid=MX:es-419",
        "https://news.google.com/rss/search?q=Morena+politica+Mexico&hl=es-419&gl=MX&ceid=MX:es-419",
        "https://news.google.com/rss/search?q=Morena+Jalisco+Puerto+Vallarta&hl=es-419&gl=MX&ceid=MX:es-419",
    ],
}

# ── PROMPTS ──────────────────────────────────────────────────────
PROMPT_VALLARTA = """Eres un analista de noticias enfocado en el bienestar ciudadano de Puerto Vallarta, Jalisco.
Selecciona EXACTAMENTE {n} titulares. Debes devolver siempre {n} noticias aunque algunas tengan menor relevancia. Ordénalas de mayor a menor impacto.
Responde SOLO con JSON sin backticks:
{{"noticias":[{{"id":"string","titulo":"string max 90 chars","fuente":"string","url":"string","publicado":"string","resumen":"string max 120 chars","sentimiento":"pos|neg|neu","impacto":0,"tema":"seguridad|salud|corrupcion|economia|electoral|social|otro","entidades":["string"],"tags":["string"]}}],"resumen_general":"2 oraciones","alerta":"string o null"}}
Impacto: 80-100=afecta seguridad/salud/servicios, 60-79=decisión gobierno visible, 40-59=relevante indirecto, 0-39=informativa.
Sentimiento: pos=buenas noticias, neg=problemas, neu=neutral."""

PROMPT_MORENA = """Eres analista político especializado en la dinámica interna de Morena en Puerto Vallarta y Jalisco.
Selecciona EXACTAMENTE {n} titulares. Debes devolver siempre {n} noticias aunque algunas tengan menor relevancia. Ordénalas de mayor a menor impacto estratégico.
Responde SOLO con JSON sin backticks:
{{"noticias":[{{"id":"string","titulo":"string max 90 chars","fuente":"string","url":"string","publicado":"string","resumen":"string max 120 chars","sentimiento":"pos|neg|neu","impacto":0,"tema":"seguridad|salud|corrupcion|economia|electoral|social|otro","entidades":["string"],"tags":["string"]}}],"resumen_general":"2 oraciones sobre las tensiones, alianzas o movimientos internos dentro de Morena","alerta":"string o null"}}
Impacto: 80-100=conflicto interno o disputa de poder visible, 60-79=movimiento estratégico o posicionamiento de figuras clave, 40-59=relevante para entender la dinámica interna, 0-39=rutinario sin impacto interno.
Sentimiento: pos=figura interna se fortalece o consolida, neg=conflicto, escándalo o debilitamiento interno, neu=sin efecto en la dinámica interna."""

PROMPT_PERSONA = """Eres un analista político neutral que monitorea la presencia mediática de políticos mexicanos.
Recibirás titulares de noticias sobre {nombre} ({partido}). Analiza su actividad reciente.
Responde SOLO con JSON sin backticks:
{{"ultima_noticia":"string max 150 chars","tono":"pos|neg|neu","actividad":"alta|media|baja|sin_presencia","temas":["string"],"apariciones":0,"resumen":"string max 200 chars"}}
Actividad: alta=3+ noticias, media=1-2, baja=menciones indirectas, sin_presencia=no aparece.
Tono: pos=cobertura favorable, neg=cobertura negativa, neu=informativa neutral."""

# ── FEEDS RSS ────────────────────────────────────────────────────
def fetch_feed(topic):
    seen, articles = set(), []
    for url in FEEDS[topic]:
        try:
            feed = feedparser.parse(url)
            for e in feed.entries:
                key = hashlib.md5(e.title.encode()).hexdigest()
                if key in seen:
                    continue
                seen.add(key)
                published = ""
                if hasattr(e, "published_parsed") and e.published_parsed:
                    dt = datetime(*e.published_parsed[:6], tzinfo=timezone.utc)
                    published = dt.isoformat()
                articles.append({
                    "id":        key,
                    "titulo":    e.title[:80],
                    "fuente":    e.get("source", {}).get("title", "Google News")[:40],
                    "url":       e.get("link", ""),
                    "publicado": published,
                })
        except Exception:
            pass
    articles.sort(key=lambda x: x["publicado"], reverse=True)
    return articles[:MAX_ARTICLES]

def fetch_persona(nombre):
    q   = quote(f"{nombre} Jalisco")
    url = f"https://news.google.com/rss/search?q={q}&hl=es-419&gl=MX&ceid=MX:es-419"
    articles = []
    try:
        feed = feedparser.parse(url)
        for e in feed.entries[:10]:
            published = ""
            if hasattr(e, "published_parsed") and e.published_parsed:
                dt = datetime(*e.published_parsed[:6], tzinfo=timezone.utc)
                published = dt.isoformat()
            articles.append({
                "titulo":    e.title[:80],
                "fuente":    e.get("source", {}).get("title", "Google News")[:40],
                "url":       e.get("link", ""),
                "publicado": published,
            })
    except Exception:
        pass
    return articles

def claude_call(system, content, max_tokens=2500):
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY, timeout=60.0)
    for intento in range(1, MAX_RETRIES + 1):
        try:
            msg = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=max_tokens,
                system=system,
                messages=[{"role": "user", "content": content}]
            )
            raw = msg.content[0].text.strip().replace("```json", "").replace("```", "").strip()
            return json.loads(raw)
        except Exception as e:
            if intento < MAX_RETRIES:
                time.sleep(RETRY_WAIT)
            else:
                raise

# ── SCRAPE NOTICIAS (rápido ~30 seg) ─────────────────────────────
def run_scrape_noticias():
    if not ANTHROPIC_API_KEY:
        cache["error"] = "ANTHROPIC_API_KEY no configurada"
        return
    cache["actualizando_noticias"] = True
    cache["error"] = None
    data = cache["data"] or {}
    try:
        for topic in ["vallarta", "morena"]:
            articles = fetch_feed(topic)
            if not articles:
                data[topic] = {"noticias": [], "actualizado": datetime.now(timezone.utc).isoformat(), "resumen_general": None, "alerta": None}
                continue
            prompt = PROMPT_VALLARTA if topic == "vallarta" else PROMPT_MORENA
            slim   = [{"id": a["id"], "t": a["titulo"], "f": a["fuente"], "u": a["url"], "p": a["publicado"]} for a in articles]
            try:
                result = claude_call(prompt.format(n=MAX_ARTICLES), json.dumps(slim, ensure_ascii=False), max_tokens=5000)
                result["actualizado"] = datetime.now(timezone.utc).isoformat()
                if result.get("alerta"):
                    guardar_alerta_db(result["alerta"], topic)
                data[topic] = result
            except Exception:
                data[topic] = {
                    "noticias": [{**a, "resumen": a["titulo"], "sentimiento": "neu", "impacto": 50, "tema": "otro", "entidades": [], "tags": []} for a in articles],
                    "actualizado": datetime.now(timezone.utc).isoformat(),
                    "resumen_general": None,
                    "alerta": None,
                }
        cache["data"]          = data
        cache["ultimo_update"] = datetime.now(timezone.utc).isoformat()
        guardar_cache_db()
    except Exception as e:
        cache["error"] = str(e)
    finally:
        cache["actualizando_noticias"] = False

# ── SCRAPE PERSONAS (lento ~90 seg) ──────────────────────────────
def run_scrape_personas():
    if not ANTHROPIC_API_KEY:
        return
    cache["actualizando_personas"] = True
    data = cache["data"] or {}
    try:
        personas_data = asyncio.run(_scrape_personas())
        data["personas"] = personas_data
        cache["data"] = data
        guardar_cache_db()
    except Exception as e:
        cache["error"] = str(e)
    finally:
        cache["actualizando_personas"] = False

# ── SCRAPE COMPLETO (cron cada 6 hrs) ────────────────────────────
def run_scrape():
    run_scrape_noticias()
    run_scrape_personas()

async def _scrape_personas():
    loop = asyncio.get_event_loop()
    tasks = [loop.run_in_executor(None, _analizar_persona, p) for p in PERSONAS]
    results = await asyncio.gather(*tasks)
    return {p["id"]: r for p, r in zip(PERSONAS, results)}

def _analizar_persona(p):
    articles = fetch_persona(p["nombre"])
    try:
        prompt   = PROMPT_PERSONA.format(nombre=p["nombre"], partido=p["partido"])
        slim     = [{"t": a["titulo"], "f": a["fuente"], "p": a["publicado"]} for a in articles]
        analisis = claude_call(prompt, json.dumps(slim, ensure_ascii=False), max_tokens=500) if articles else {
            "ultima_noticia": "Sin noticias recientes",
            "tono": "neu", "actividad": "sin_presencia",
            "temas": [], "apariciones": 0,
            "resumen": f"No se encontraron noticias recientes de {p['nombre']}."
        }
        return {**p, **analisis, "noticias_recientes": articles[:3], "actualizado": datetime.now(timezone.utc).isoformat()}
    except Exception:
        return {**p, "ultima_noticia": "Error al analizar", "tono": "neu", "actividad": "sin_presencia",
                "temas": [], "apariciones": 0, "resumen": "No disponible",
                "noticias_recientes": [], "actualizado": datetime.now(timezone.utc).isoformat()}

# ── SCHEDULER ─────────────────────────────────────────────────────
scheduler = BackgroundScheduler()

@asynccontextmanager
async def lifespan(app):
    init_db()
    cargar_cache_db()
    scheduler.add_job(run_scrape, "interval", hours=6, id="scrape_periodico")
    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(title="DataCrack API", version="3.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_URL, "https://datacrack.mx", "http://datacrack.mx",
                   "https://www.datacrack.mx", "http://www.datacrack.mx",
                   "https://melodic-marzipan-eee891.netlify.app"],
    allow_credentials=True, allow_methods=["GET", "POST"], allow_headers=["*"],
)

# ── ENDPOINTS ─────────────────────────────────────────────────────
@app.get("/")
def root():
    return {"status": "ok", "service": "DataCrack API v3 — PostgreSQL"}

@app.get("/noticias")
def get_noticias():
    if cache["data"] is None:
        return {"status": "sin_datos",
                "actualizando": cache["actualizando_noticias"],
                "mensaje": "No hay datos aún. Usa /actualizar para cargar por primera vez."}
    return {**cache["data"], "meta": {
        "actualizando_noticias": cache["actualizando_noticias"],
        "actualizando_personas": cache["actualizando_personas"],
        "ultimo_update": cache["ultimo_update"],
    }}

@app.get("/actualizar")
def actualizar(background_tasks: BackgroundTasks):
    if cache["actualizando_noticias"]:
        return {"status": "en_progreso", "mensaje": "Ya hay una actualización de noticias en curso."}
    background_tasks.add_task(run_scrape_noticias)
    return {"status": "iniciado", "mensaje": "Actualizando noticias. Tarda ~30 segundos."}

@app.get("/actualizar/personas")
def actualizar_personas(background_tasks: BackgroundTasks):
    if cache["actualizando_personas"]:
        return {"status": "en_progreso", "mensaje": "Ya hay una actualización de personas en curso."}
    background_tasks.add_task(run_scrape_personas)
    return {"status": "iniciado", "mensaje": "Actualizando personas. Tarda ~90 segundos."}

@app.get("/alertas")
def get_alertas():
    alertas = cargar_alertas_db()
    return {"alertas": alertas, "total": len(alertas)}

@app.get("/status")
def get_status():
    return {
        "actualizando": cache["actualizando_noticias"] or cache["actualizando_personas"],
        "actualizando_noticias": cache["actualizando_noticias"],
        "actualizando_personas": cache["actualizando_personas"],
        "ultimo_update": cache["ultimo_update"],
        "tiene_datos": cache["data"] is not None,
        "error": cache["error"],
    }

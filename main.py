""" main.py — DataCrack Backend API
FastAPI + scraper Google News RSS + Claude
Deploy: Railway
"""
import os, json, hashlib, time, asyncio, httpx
from datetime import datetime, timezone
from urllib.parse import quote
from contextlib import asynccontextmanager

import feedparser
import anthropic
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware

# ── CONFIG ─────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MAX_ARTICLES      = 8
MAX_RETRIES       = 3
RETRY_WAIT        = 5
FRONTEND_URL      = os.environ.get("FRONTEND_URL", "*")
CACHE_FILE        = "cache.json"
ALERTAS_FILE      = "alertas.json"

# ── CACHÉ EN MEMORIA ───────────────────────────────────────────
cache = {"data": None, "actualizando": False, "ultimo_update": None, "error": None}

# ── PERSISTENCIA ───────────────────────────────────────────────
def guardar_cache():
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump({"data": cache["data"], "ultimo_update": cache["ultimo_update"]}, f, ensure_ascii=False)
    except Exception:
        pass

def cargar_cache():
    try:
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE) as f:
                saved = json.load(f)
                cache["data"]         = saved.get("data")
                cache["ultimo_update"] = saved.get("ultimo_update")
    except Exception:
        pass

def cargar_alertas():
    try:
        if os.path.exists(ALERTAS_FILE):
            with open(ALERTAS_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return []

def guardar_alerta(texto, topic):
    alertas = cargar_alertas()
    nueva = {
        "texto": texto,
        "topic": topic,
        "fecha": datetime.now(timezone.utc).isoformat()
    }
    # Evitar duplicados seguidos
    if not alertas or alertas[-1]["texto"] != texto:
        alertas.append(nueva)
        # Guardar solo las últimas 50 alertas
        alertas = alertas[-50:]
        try:
            with open(ALERTAS_FILE, "w") as f:
                json.dump(alertas, f, ensure_ascii=False)
        except Exception:
            pass

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
Selecciona los {n} titulares que más impactan la vida cotidiana de los ciudadanos.
Responde SOLO con JSON sin backticks:
{{"noticias":[{{"id":"string","titulo":"string max 90 chars","fuente":"string","url":"string","publicado":"string","resumen":"string max 120 chars","sentimiento":"pos|neg|neu","impacto":0,"tema":"seguridad|salud|corrupcion|economia|electoral|social|otro","entidades":["string"],"tags":["string"]}}],"resumen_general":"2 oraciones","alerta":"string o null"}}
Impacto: 80-100=afecta seguridad/salud/servicios, 60-79=decisión gobierno visible, 40-59=relevante indirecto, 0-39=informativa.
Sentimiento: pos=buenas noticias, neg=problemas, neu=neutral."""

PROMPT_MORENA = """Eres analista político que monitorea Morena para el PVEM en Puerto Vallarta.
Selecciona los {n} titulares más relevantes estratégicamente.
Responde SOLO con JSON sin backticks:
{{"noticias":[{{"id":"string","titulo":"string max 90 chars","fuente":"string","url":"string","publicado":"string","resumen":"string max 120 chars","sentimiento":"pos|neg|neu","impacto":0,"tema":"seguridad|salud|corrupcion|economia|electoral|social|otro","entidades":["string"],"tags":["string"]}}],"resumen_general":"2 oraciones sobre Morena y qué significa para PVEM","alerta":"string o null"}}
Impacto para PVEM: 80-100=escándalo capitalizable, 60-79=riesgo u oportunidad, 40-59=relevante sin impacto inmediato, 0-39=rutinario.
Sentimiento: pos=Morena se debilita, neg=Morena se fortalece, neu=sin efecto."""

PROMPT_PERSONA = """Eres un analista político neutral que monitorea la presencia mediática de políticos mexicanos.
Recibirás titulares de noticias sobre {nombre} ({partido}). Analiza su actividad reciente.
Responde SOLO con JSON sin backticks:
{{"ultima_noticia":"string max 150 chars","tono":"pos|neg|neu","actividad":"alta|media|baja|sin_presencia","temas":["string"],"apariciones":0,"resumen":"string max 200 chars"}}
Actividad: alta=3+ noticias, media=1-2, baja=menciones indirectas, sin_presencia=no aparece.
Tono: pos=cobertura favorable, neg=cobertura negativa, neu=informativa neutral."""

# ── MINIATURA (og:image) ─────────────────────────────────────────
async def fetch_thumbnail(url: str) -> str:
    try:
        async with httpx.AsyncClient(timeout=5.0, follow_redirects=True) as client:
            r = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            html = r.text
            # Buscar og:image
            for tag in ['og:image', 'twitter:image']:
                idx = html.find(f'property="{tag}"')
                if idx == -1:
                    idx = html.find(f"property='{tag}'")
                if idx == -1:
                    idx = html.find(f'name="{tag}"')
                if idx != -1:
                    content_idx = html.find('content="', idx)
                    if content_idx != -1:
                        start = content_idx + 9
                        end = html.find('"', start)
                        return html[start:end]
    except Exception:
        pass
    return ""

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

# ── SCRAPE COMPLETO ───────────────────────────────────────────────
def run_scrape():
    if not ANTHROPIC_API_KEY:
        cache["error"] = "ANTHROPIC_API_KEY no configurada"
        cache["actualizando"] = False
        return
    cache["actualizando"] = True
    cache["error"] = None
    data = {}
    try:
        # Vallarta y Morena
        for topic in ["vallarta", "morena"]:
            articles = fetch_feed(topic)
            if not articles:
                data[topic] = {"noticias": [], "actualizado": datetime.now(timezone.utc).isoformat(), "resumen_general": None, "alerta": None}
                continue
            prompt = PROMPT_VALLARTA if topic == "vallarta" else PROMPT_MORENA
            slim   = [{"id": a["id"], "t": a["titulo"], "f": a["fuente"], "u": a["url"], "p": a["publicado"]} for a in articles]
            try:
                result = claude_call(prompt.format(n=MAX_ARTICLES), json.dumps(slim, ensure_ascii=False), max_tokens=6000)
                result["actualizado"] = datetime.now(timezone.utc).isoformat()
                # Guardar alerta en historial si existe
                if result.get("alerta"):
                    guardar_alerta(result["alerta"], topic)
                # Agregar miniaturas en paralelo
                urls = [n.get("url", "") for n in result.get("noticias", [])]
                thumbs = asyncio.run(_fetch_thumbnails(urls))
                for i, noticia in enumerate(result.get("noticias", [])):
                    noticia["imagen"] = thumbs[i]
                data[topic] = result
            except Exception:
                data[topic] = {
                    "noticias": [{**a, "resumen": a["titulo"], "sentimiento": "neu", "impacto": 50, "tema": "otro", "entidades": [], "tags": [], "imagen": ""} for a in articles],
                    "actualizado": datetime.now(timezone.utc).isoformat(),
                    "resumen_general": None,
                    "alerta": None,
                }

        # Personas en paralelo
        personas_data = asyncio.run(_scrape_personas())
        data["personas"] = personas_data

        cache["data"]         = data
        cache["ultimo_update"] = datetime.now(timezone.utc).isoformat()
        guardar_cache()

    except Exception as e:
        cache["error"] = str(e)
    finally:
        cache["actualizando"] = False

async def _fetch_thumbnails(urls):
    tasks = [fetch_thumbnail(u) for u in urls]
    return await asyncio.gather(*tasks)

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
    cargar_cache()
    scheduler.add_job(run_scrape, "interval", hours=6, id="scrape_periodico")
    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(title="DataCrack API", version="2.0.0", lifespan=lifespan)
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
    return {"status": "ok", "service": "DataCrack API v2"}

@app.get("/noticias")
def get_noticias():
    if cache["data"] is None:
        return {"status": "sin_datos", "actualizando": cache["actualizando"],
                "mensaje": "No hay datos aún. Usa /actualizar para cargar por primera vez."}
    return {**cache["data"], "meta": {"actualizando": cache["actualizando"], "ultimo_update": cache["ultimo_update"]}}

@app.get("/actualizar")
def actualizar(background_tasks: BackgroundTasks):
    if cache["actualizando"]:
        return {"status": "en_progreso", "mensaje": "Ya hay una actualización en curso."}
    background_tasks.add_task(run_scrape)
    return {"status": "iniciado", "mensaje": "Actualización iniciada. Tarda 2-3 minutos."}

@app.get("/alertas")
def get_alertas():
    alertas = cargar_alertas()
    return {"alertas": list(reversed(alertas)), "total": len(alertas)}

@app.get("/status")
def get_status():
    return {"actualizando": cache["actualizando"], "ultimo_update": cache["ultimo_update"],
            "tiene_datos": cache["data"] is not None, "error": cache["error"]}

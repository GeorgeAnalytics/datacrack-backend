import os, feedparser, anthropic, json, hashlib, time
from datetime import datetime, timezone
from urllib.parse import quote
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MAX_ARTICLES = 8
MAX_RETRIES = 3
RETRY_WAIT = 5
FRONTEND_URL = os.environ.get("FRONTEND_URL", "*")

app = FastAPI(title="DataCrack API", version="1.0.0")
app.add_middleware(CORSMiddleware,
    allow_origins=[FRONTEND_URL, "https://datacrack.mx", "https://www.datacrack.mx",
                   "https://melodic-marzipan-eee891.netlify.app"],
    allow_credentials=True, allow_methods=["GET"], allow_headers=["*"])

PERSONAS = [
    {"id":"bruno",   "nombre":"Bruno Blancas",     "partido":"Morena","cargo":"Politico"},
    {"id":"ra",      "nombre":"Ra Aguilar",         "partido":"Morena","cargo":"Politico"},
    {"id":"chuyita", "nombre":"Chuyita Lopez",      "partido":"Morena","cargo":"Politica"},
    {"id":"flor",    "nombre":"Flor Michel Lopez",  "partido":"Morena","cargo":"Politica"},
    {"id":"diego",   "nombre":"Diego Franco",       "partido":"MC",   "cargo":"Politico"},
    {"id":"lupita",  "nombre":"Guadalupe Guerrero", "partido":"MC",   "cargo":"Politica"},
    {"id":"ricardo", "nombre":"Ricardo Rene",       "partido":"MC",   "cargo":"Politico"},
    {"id":"luis",    "nombre":"Luis Munguia",       "partido":"PVEM", "cargo":"Politico"},
    {"id":"yussara", "nombre":"Yussara Canales",    "partido":"PVEM", "cargo":"Politica"},
]

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

SCHEMA_NEWS = '{"noticias":[{"id":"str","titulo":"str","fuente":"str","url":"str","publicado":"str","resumen":"str","sentimiento":"pos|neg|neu","impacto":50,"tema":"seguridad|salud|corrupcion|economia|electoral|social|otro","entidades":[],"tags":[]}],"resumen_general":"2 oraciones","alerta":null}'
SCHEMA_PERSONA = '{"ultima_noticia":"str","tono":"pos|neg|neu","actividad":"alta|media|baja|sin_presencia","temas":[],"apariciones":0,"resumen":"str"}'

PROMPT_VALLARTA = "Eres analista de noticias de Puerto Vallarta. Selecciona los titulares mas relevantes para ciudadanos. Responde SOLO JSON sin backticks: " + SCHEMA_NEWS
PROMPT_MORENA = "Eres analista politico monitoreando Morena para PVEM. Selecciona los titulares mas relevantes estrategicamente. Responde SOLO JSON sin backticks: " + SCHEMA_NEWS

def persona_prompt(nombre, partido):
    return "Analiza presencia mediatica de " + nombre + " (" + partido + "). Responde SOLO JSON sin backticks: " + SCHEMA_PERSONA

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
                    "id": key, "titulo": e.title[:80],
                    "fuente": e.get("source", {}).get("title", "Google News")[:40],
                    "url": e.get("link", ""), "publicado": published
                })
        except Exception:
            pass
    articles.sort(key=lambda x: x["publicado"], reverse=True)
    return articles[:MAX_ARTICLES]

def fetch_persona(nombre):
    q = quote(nombre + " Jalisco")
    url = "https://news.google.com/rss/search?q=" + q + "&hl=es-419&gl=MX&ceid=MX:es-419"
    articles = []
    try:
        feed = feedparser.parse(url)
        for e in feed.entries[:10]:
            published = ""
            if hasattr(e, "published_parsed") and e.published_parsed:
                dt = datetime(*e.published_parsed[:6], tzinfo=timezone.utc)
                published = dt.isoformat()
            articles.append({
                "titulo": e.title[:80],
                "fuente": e.get("source", {}).get("title", "Google News")[:40],
                "url": e.get("link", ""), "publicado": published
            })
    except Exception:
        pass
    return articles

def claude_call(prompt, content, max_tokens=3000):
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY, timeout=60.0)
    for i in range(1, MAX_RETRIES + 1):
        try:
            msg = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt + "\n\nNoticias:\n" + content}]
            )
            raw = msg.content[0].text.strip()
            start = raw.find("{")
            end = raw.rfind("}") + 1
            if start >= 0 and end > start:
                raw = raw[start:end]
            return json.loads(raw)
        except Exception:
            if i < MAX_RETRIES:
                time.sleep(RETRY_WAIT)
            else:
                raise

@app.get("/")
def root():
    return {"status": "ok", "service": "DataCrack API"}

@app.get("/noticias")
def get_noticias():
    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=500, detail="ANTHROPIC_API_KEY no configurada")
    data = {}
    for topic in ["vallarta", "morena"]:
        articles = fetch_feed(topic)
        if not articles:
            data[topic] = {
                "noticias": [], "actualizado": datetime.now(timezone.utc).isoformat(),
                "resumen_general": None, "alerta": None
            }
            continue
        prompt = PROMPT_VALLARTA if topic == "vallarta" else PROMPT_MORENA
        slim = [{"id": a["id"], "t": a["titulo"], "f": a["fuente"], "u": a["url"], "p": a["publicado"]} for a in articles]
        try:
            result = claude_call(prompt, json.dumps(slim, ensure_ascii=False), max_tokens=6000)
            result["actualizado"] = datetime.now(timezone.utc).isoformat()
            data[topic] = result
        except Exception:
            data[topic] = {
                "noticias": [{**a, "resumen": a["titulo"], "sentimiento": "neu", "impacto": 50, "tema": "otro", "entidades": [], "tags": []} for a in articles],
                "actualizado": datetime.now(timezone.utc).isoformat(),
                "resumen_general": None, "alerta": None
            }
    personas_data = {}
    for p in PERSONAS:
        articles = fetch_persona(p["nombre"])
        try:
            slim = [{"t": a["titulo"], "f": a["fuente"], "p": a["publicado"]} for a in articles]
            if articles:
                analisis = claude_call(persona_prompt(p["nombre"], p["partido"]), json.dumps(slim, ensure_ascii=False), max_tokens=500)
            else:
                analisis = {
                    "ultima_noticia": "Sin noticias", "tono": "neu",
                    "actividad": "sin_presencia", "temas": [], "apariciones": 0, "resumen": "Sin datos."
                }
            personas_data[p["id"]] = {**p, **analisis, "noticias_recientes": articles[:3], "actualizado": datetime.now(timezone.utc).isoformat()}
        except Exception:
            personas_data[p["id"]] = {
                **p, "ultima_noticia": "Error", "tono": "neu", "actividad": "sin_presencia",
                "temas": [], "apariciones": 0, "resumen": "No disponible",
                "noticias_recientes": [], "actualizado": datetime.now(timezone.utc).isoformat()
            }
        time.sleep(1)
    data["personas"] = personas_data
    return data

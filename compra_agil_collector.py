#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Recolector Compra Ágil — Green Wolf SPA.

Usa la API pública del buscador de Mercado Público
(api.buscador.mercadopublico.cl), la misma que usa el sitio web oficial.
Ventaja: no requiere ticket (MP_TICKET) ni tiene cuota diaria.

Pipeline:
  1. Recolecta TODAS las regiones del país (por keywords o modo buscar_todo).
  2. Filtros duros baratos: cierre muy próximo/vencido, monto mínimo.
  3. Pre-filtro por texto: blacklist (sobre nombre y productos, no la
     descripción completa) + score heurístico 0-100 para priorizar.
  4. Enriquece con ficha + adjuntos SOLO los mejores candidatos (tope
     configurable) — el resto va al feed en versión liviana.
  5. Evaluación IA (Haiku) server-side con caché persistente eval_ia.json:
     cada código se evalúa UNA sola vez en la vida del proceso → mínimo
     consumo de tokens. Requiere secret ANTHROPIC_API_KEY; si no está,
     la app web evalúa como fallback.

Config en keywords.json (todas las claves son opcionales):
  {
    "palabras_clave": ["impresion 3d", ...],
    "buscar_todo": false,          # true = trae todo el país sin keywords
    "monto_min_clp": 100000,       # descarta montos menores (si se conocen)
    "horas_min_cierre": 24,        # descarta cierres a menos de N horas
    "max_detalle": 150,            # tope de fichas/adjuntos a descargar
    "max_eval_ia": 100,            # tope de evaluaciones IA nuevas por corrida
    "max_items_feed": 800,         # tope de items en oportunidades.json
    "rubros_bloqueados": []        # prefijos de categoría/UNSPSC a excluir
  }

Nota: son APIs del frontend oficial (no documentadas). Si algún día rotan las
claves públicas (BUSCADOR_API_KEY / ADJ_USER_KEY), se obtienen de nuevo
inspeccionando el JS de buscador.mercadopublico.cl.
"""

import os, re, sys, json, time, shutil, hashlib, unicodedata, datetime as dt
from urllib.parse import quote
try:
    from zoneinfo import ZoneInfo
    _TZ_CL = ZoneInfo("America/Santiago")
except Exception:          # sin tzdata: se asume horario de verano chileno
    _TZ_CL = None


def _ahora_chile():
    """Hora de Chile continental, sin zona (igual que las fechas de Mercado Público).
    El runner de GitHub está en UTC: comparar contra dt.datetime.now() desfasaba
    3-4 horas los cierres ("ya cerró", "cierra en menos de 24 h")."""
    if _TZ_CL is not None:
        return dt.datetime.now(_TZ_CL).replace(tzinfo=None)
    return dt.datetime.utcnow() - dt.timedelta(hours=3)
import requests

# API pública del buscador (la misma del sitio buscador.mercadopublico.cl)
BUSCADOR_BASE = "https://api.buscador.mercadopublico.cl"
BUSCADOR_API_KEY = "e93089e4-437c-4723-b343-4fa20045e3bc"  # clave pública del frontend

# Servicio público de adjuntos (el mismo del buscador)
ADJ_BASE = "https://adjunto.mercadopublico.cl/adjunto-compra-agil/v1/adjuntos-compra-agil"
ADJ_USER_KEY = "41186b85826e80d1a0d445a6ce67d1a3"  # clave pública del frontend
# El servicio de adjuntos responde 403 a clientes que no parecen navegador
# (probado desde GitHub Actions el 29-09-2026: curl y python-requests -> 403,
# User-Agent de Chrome -> 200, con la misma IP). Se usa en todas las llamadas a MP.
UA_NAVEGADOR = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/140.0 Safari/537.36")
_CAB_MP = {"User-Agent": UA_NAVEGADOR, "Origin": "https://buscador.mercadopublico.cl",
           "Referer": "https://buscador.mercadopublico.cl/"}

GH_REPO = os.environ.get("GITHUB_REPOSITORY", "Nyctric/compra-agil-feed")
GH_BRANCH = os.environ.get("GITHUB_REF_NAME", "master") or "master"
RAW_BASE = f"https://raw.githubusercontent.com/{GH_REPO}/{GH_BRANCH}"
ADJ_DIR = "adjuntos"
MAX_ADJ_MB = 25  # no descargar archivos más grandes que esto

# ---------- Configuración (keywords.json) ----------

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_kw_file = os.path.join(_BASE_DIR, "keywords.json")
_CFG = {}
if os.path.exists(_kw_file):
    with open(_kw_file, encoding="utf-8") as _f:
        _CFG = json.load(_f)

PALABRAS_CLAVE = _CFG.get("palabras_clave") or ["impresion 3d", "filamento", "prototipo", "plastico", "fabricacion"]
BUSCAR_TODO = bool(_CFG.get("buscar_todo", False))
MONTO_MIN_CLP = int(_CFG.get("monto_min_clp", 100000))
HORAS_MIN_CIERRE = int(_CFG.get("horas_min_cierre", 24))
MAX_DETALLE = int(_CFG.get("max_detalle", 150))
MAX_EVAL_IA = int(_CFG.get("max_eval_ia", 100))
MAX_ITEMS_FEED = int(_CFG.get("max_items_feed", 800))
RUBROS_BLOQUEADOS = [str(r) for r in (_CFG.get("rubros_bloqueados") or [])]
INCLUIR_LICITACIONES = bool(_CFG.get("incluir_licitaciones", True))
MAX_DETALLE_LIC = int(_CFG.get("max_detalle_licitaciones", 60))
VALOR_UTM_CLP = int(_CFG.get("valor_utm_clp", 69000))   # aprox., solo para filtrar/puntuar
VALOR_USD_CLP = int(_CFG.get("valor_usd_clp", 950))

# API oficial de Mercado Público (licitaciones) — requiere ticket, tiene cuota diaria
MP_TICKET = os.environ.get("MP_TICKET", "")
LIC_BASE = "https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json"
PAUSA_LIC_SEG = 1.3  # la API oficial limita consultas por segundo

# Resultados propios (ganadas/perdidas) + notificación Telegram
RESULT_FILE = os.environ.get("RESULT_FILE", "resultados.json")
RUTS_PROPIOS = [re.sub(r"[^0-9kK]", "", str(r)).lower()
                for r in (_CFG.get("ruts_propios") or ["77.387.704-1", "78.297.937-K"])]
NOMBRES_PROPIOS = [str(n).lower() for n in (_CFG.get("nombres_propios") or ["green wolf", "provectus"])]
TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "")
MAIL_USER = os.environ.get("MAIL_USER", "")            # cuenta Gmail que envía
MAIL_PASS = os.environ.get("MAIL_APP_PASSWORD", "")    # contraseña de aplicación de Gmail
MAIL_TO = os.environ.get("MAIL_TO", "") or MAIL_USER   # destinatario (por defecto, la misma)

# Historial de precios adjudicados (referencia de mercado para cotizar)
HIST_FILE = os.environ.get("HIST_FILE", "precios_historicos.json")
HISTORICO_ON = bool(_CFG.get("historico_precios", True))
HISTORICO_DIAS = int(_CFG.get("historico_dias", 365))          # ventana: 1 año
HIST_DIAS_POR_CORRIDA = int(_CFG.get("historico_dias_por_corrida", 30))  # backfill gradual
HIST_MAX_DETALLE = int(_CFG.get("max_detalle_historico", 40))  # tope de fichas por corrida (cuota)

ESTADOS = ["publicada"]
FETCH_DETALLE = True
OUTPUT_FILE = os.environ.get("OUTPUT_FILE", "oportunidades.json")
EVAL_CACHE_FILE = os.environ.get("EVAL_CACHE_FILE", "eval_ia.json")
PAUSA_SEG = 0.35
MAX_REINTENTOS = 3
MAX_PAGINAS = 20        # tope de seguridad por palabra clave
MAX_PAGINAS_TODO = int(_CFG.get("max_paginas_todo", 650))  # tope en modo buscar_todo; _paginar corta antes si pageCount es menor

ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
IA_MODELOS = ["claude-haiku-4-5", "claude-haiku-4-5-20251001", "claude-sonnet-4-5"]
IA_LOTE = 25            # licitaciones por llamada (más grande = menos overhead de prompt)
IA_CACHE_DIAS = 90      # conservar evaluaciones de códigos ya ausentes por N días

# Triage IA: lee TODOS los títulos que no calzaron con ninguna palabra y rescata
# los que podrían fabricarse en 3D. Cada título se evalúa una sola vez (caché).
TRIAGE_ON = bool(_CFG.get("triage_ia", True))
TRIAGE_LOTE = 150
MAX_TRIAGE = int(_CFG.get("max_triage", 5000))      # tope de títulos nuevos por corrida
TRIAGE_CACHE_FILE = os.environ.get("TRIAGE_CACHE_FILE", "triage_ia.json")
TRIAGE_CACHE_DIAS = 30

PERFIL_DEFAULT = ("Fabricación digital en Chile. Tecnologías: impresión 3D FDM (PLA, PETG, ABS, TPU) y resina "
    "(estándar, tough). Volumen máximo FDM 30x30x30 cm por pieza (piezas mayores se fabrican por secciones "
    "ensambladas). Postproceso: lijado, pintura, barniz UV. Productos típicos: prototipos, piezas funcionales, "
    "modelos anatómicos y fantomas, señalética y letreros 3D, repuestos plásticos, maquetas, trofeos y galvanos, "
    "llaveros y pines, material didáctico.")


def _perfil_texto():
    """El perfil lo edita el usuario en la app (Compra Ágil → Perfil de empresa) y
    llega aquí dentro de keywords.json. Si hay uno por empresa, se usan ambos."""
    p = _CFG.get("perfil_empresa")
    if isinstance(p, dict):
        partes = []
        for k, etiqueta in (("pv", "Provectus SpA"), ("gw", "Green Wolf SPA")):
            t = str(p.get(k) or "").strip()
            if t and t not in partes:
                partes.append(t)
        return "\n".join(partes) or PERFIL_DEFAULT
    return str(p or "").strip() or PERFIL_DEFAULT


PERFIL = _perfil_texto()

ESTADO_GLOSA = {2: "Publicada", 3: "Cerrada", 5: "Cancelada", 6: "Desierta"}
ESTADO_CODIGO = {2: "publicada", 3: "cerrada", 5: "cancelada", 6: "desierta"}
ESTADO_PARAM = {"publicada": 2, "cerrada": 3, "cancelada": 5, "desierta": 6}

REGION_NOMBRES = {
    1:"Tarapacá",2:"Antofagasta",3:"Atacama",4:"Coquimbo",5:"Valparaíso",
    6:"O'Higgins",7:"Maule",8:"Biobío",9:"Araucanía",10:"Los Lagos",
    11:"Aysén",12:"Magallanes y Antártica",13:"Metropolitana",14:"Los Ríos",
    15:"Arica y Parinacota",16:"Ñuble",
}
_REGION_POR_NOMBRE = {}
def _norm(s):
    s = unicodedata.normalize("NFD", str(s or "").lower())
    return "".join(c for c in s if unicodedata.category(c) != "Mn")
for _rid, _rn in REGION_NOMBRES.items():
    _REGION_POR_NOMBRE[_norm(_rn)] = _rid

# ---------- Pre-filtro por texto (mismas listas que la app) ----------

BLACKLIST = ["triptico","afiche","fotocopia","libro","imprenta","papel couche","licencia de software",
  "licencia","suscripcion","capacitacion","diplomado","curso de","alimento","colacion","vestuario","calzado",
  "arriendo de vehiculo","pasaje aereo","pasajes aereo","viatico","seguro de viaje","transporte escolar",
  "servicio de aseo","mantencion de aire acondicionado","mantencion preventiva","auditoria","asesoria juridica",
  "consultoria juridica","reparacion vehiculo","combustible","neumatico","catering","examen medico",
  "medicamento","farmaceutico","arriendo de carpa","musica","danza","teatro","software","peluqueria"]
# Vocabulario en dos niveles, medido contra el feed real (veredicto IA sí/no):
#   galvano 12/0, medalla 11/0, letrero 8/0, trofeo 3/0  -> FUERTES
#   repuesto 3/51, pieza 1/24, modelo 8/69, filamento 0/7 -> DEBILES
# Los DÉBILES siguen dejando entrar al feed (no se pierde nada), pero puntúan
# poco: ya no le quitan los cupos de ficha (150) y de IA (100) a lo relevante.
FUERTES = ["impresion 3d","impresora 3d","impreso en 3d","3d","maqueta","prototipo","modelado 3d",
  "fantoma","anatomic","trofeo","galvano","galardon","medalla","estatuilla","llavero","piocha",
  "pin","pines","pins","insignia","senaletic","letrero","letras corporeas","letra corporea",
  "placa conmemorativa","placa recordatoria","escultura","busto","replica","diorama","braille",
  "podotactil","material didactico","didactic","souvenir","merchandising","exhibidor","figura",
  "tipodonto","craneo","esqueleto","torso"]
DEBILES = ["repuesto","pieza","plastico","gabinete","carcasa","molde","resina","filamento","pla",
  "modelado","modelo","rotulo","placa","estuche","organizador","dispensador","atril","acrilico",
  "senalizacion","decoracion","juguete","rompecabezas","ajedrez","ortesis","ferula","maceta",
  "reconocimiento","premiacion","premio","premiar","copa","logo","soporte"]
WHITELIST_EXTRA = FUERTES + DEBILES
# Contexto que casi nunca termina en una pieza impresa. No descarta: resta
# prioridad, salvo que el título también tenga un término FUERTE.
RUIDO = ["vehicul","camioneta","camion","automovil","minibus","bus","motor","maquinaria",
  "retroexcavadora","motoniveladora","excavadora","mantencion","mantenimiento","limpieza","aseo",
  "arriendo","reparacion","instalacion","soporte tecnico","pieza de mano","toner","cartucho",
  "bateria","neumatico","sutura","farmac","insumos clinicos","ascensor","extintor","aeronave",
  "helicoptero","compresor","bomba","ecograf","autoclave","electrocardiograf"]

_BLACKLIST_N = [_norm(b) for b in BLACKLIST]
_FUERTES_N = [_norm(w) for w in FUERTES]
_DEBILES_N = [_norm(w) for w in DEBILES]
_RUIDO_N = [_norm(w) for w in RUIDO]
_WHITELIST_N = list(dict.fromkeys(_norm(w) for w in (WHITELIST_EXTRA + PALABRAS_CLAVE)))


def _kw_en_texto(kw_norm, texto_norm):
    """Match desde el INICIO de una palabra: 'senaletic' sí matchea 'senaletica',
    pero 'pieza' ya no matchea 'limpieza' ni 'resina' matchea 'desmopresina'.
    Las cortas (<5) exigen además borde final: 'pin' no matchea 'pintura'."""
    kw_norm = kw_norm.strip()
    if not kw_norm:
        return False
    pat = r"(?<![a-z0-9])" + re.escape(kw_norm)
    if len(kw_norm) < 5:
        pat += r"(?![a-z0-9])"
    return re.search(pat, texto_norm) is not None


def _tiene_fuerte(texto_norm):
    return any(_kw_en_texto(f, texto_norm) for f in _FUERTES_N)


def _hit_blacklist(texto_norm):
    # Un término fuerte gana: "maqueta de teatro", "organizador de medicamentos
    # impreso en 3d" o "atril para libro" no deben morir por la blacklist.
    if _tiene_fuerte(texto_norm):
        return None
    for b in _BLACKLIST_N:
        if _kw_en_texto(b, texto_norm):
            return b
    return None


def _peso_termino(t):
    t = _norm(t).strip()
    if t in _FUERTES_N:
        return 14
    if t in _DEBILES_N:
        return 4
    return 10   # keyword propia del usuario sin clasificar: neutra


def _matches_whitelist(texto_norm):
    return [w for w in _WHITELIST_N if _kw_en_texto(w, texto_norm)]


def _parse_fecha(s):
    if not s:
        return None
    s = str(s).strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M", "%d-%m-%Y"):
        try:
            return dt.datetime.strptime(s[:len(fmt) + 2].strip(), fmt)
        except ValueError:
            continue
    return None


def score_heuristico(reg):
    """0-100: prioriza qué candidatos merecen ficha + evaluación IA."""
    s = 0.0
    nombre_n = _norm(reg.get("nombre") or "")
    prods_n = _norm(" ".join((p.get("nombre") or "") for p in (reg.get("productos") or [])))
    texto = nombre_n + " " + prods_n
    # 1) coincidencias ponderadas: un término FUERTE vale 14, uno DÉBIL 4 (hasta 40)
    terminos = set(_matches_whitelist(texto)) | {_norm(k) for k in (reg.get("palabras_clave_match") or [])}
    pts = sum(_peso_termino(t) for t in terminos)
    if reg.get("triage"):
        pts += 12          # la IA lo marcó fabricable aunque ninguna palabra calzara
    s += min(40, pts)
    # 1b) ruido (vehículos, limpieza, mantención…) sin ningún término fuerte: -25
    if not reg.get("triage") and not _tiene_fuerte(texto) and any(_kw_en_texto(r, nombre_n) for r in _RUIDO_N):
        s -= 25
    # 2) monto en rango dulce para Compra Ágil (hasta 25)
    m = reg.get("monto_clp") or 0
    try: m = float(m)
    except (TypeError, ValueError): m = 0
    if 200_000 <= m <= 5_000_000: s += 25
    elif 100_000 <= m < 200_000 or 5_000_000 < m <= 8_000_000: s += 15
    elif m > 0: s += 5
    # 3) días hasta el cierre: 2-10 días es lo cómodo (hasta 20)
    fc = _parse_fecha(reg.get("fecha_cierre"))
    if fc:
        dias = (fc - _ahora_chile()).total_seconds() / 86400
        if 2 <= dias <= 10: s += 20
        elif 1 <= dias < 2 or 10 < dias <= 20: s += 10
    # 4) pocas ofertas recibidas = menos competencia (hasta 15; solo post-ficha)
    of = reg.get("total_ofertas")
    if of is not None:
        try:
            of = int(of)
            if of <= 2: s += 15
            elif of <= 5: s += 8
        except (TypeError, ValueError):
            pass
    return int(max(0, min(100, s)))


# ---------- HTTP ----------

def _get_buscador(params=None, intento=0):
    """GET a la API del buscador con reintentos."""
    url = f"{BUSCADOR_BASE}/compra-agil"
    while True:
        try:
            resp = requests.get(url, headers=dict(_CAB_MP, **{"x-api-key": BUSCADOR_API_KEY, "Accept": "application/json"}),
                                params=params, timeout=60)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            intento += 1
            if intento > MAX_REINTENTOS:
                print(f"  · Error de red: {e}", file=sys.stderr)
                return None
            time.sleep(5 * intento)
            continue
        if resp.status_code in (429,) or resp.status_code >= 500:
            intento += 1
            if intento > MAX_REINTENTOS:
                print(f"  · HTTP {resp.status_code} persistente — saltando", file=sys.stderr)
                return None
            time.sleep(2 ** intento)
            continue
        if resp.status_code != 200:
            print(f"  · HTTP {resp.status_code} — saltando", file=sys.stderr)
            return None
        try:
            data = resp.json()
        except ValueError:
            return None
        if data.get("success") != "OK":
            return None
        return data.get("payload")


def _paginar(params_base, max_paginas):
    items, pagina = [], 1
    while pagina <= max_paginas:
        params = dict(params_base); params["page_number"] = pagina
        payload = _get_buscador(params)
        time.sleep(PAUSA_SEG)
        if not payload:
            break
        items.extend(payload.get("resultados") or [])
        if pagina >= (payload.get("pageCount") or 1):
            break
        pagina += 1
    return items


_SIN_TILDE = str.maketrans("áéíóúÁÉÍÓÚ", "aeiouAEIOU")   # la ñ se conserva: "senaletica" no encuentra nada
_VOCALES = set("aeiouáéíóú")


def _plural(p):
    if p[-1] in _VOCALES:
        return p + "s"
    if p[-1] == "z":
        return p[:-1] + "ces"
    if p[-1] in "nlrdj":
        # galardón -> galardones (la tilde cae si ya no quedan vocales tras ella)
        for i in range(len(p) - 1, -1, -1):
            if p[i] in "áéíóú":
                if not any(c in _VOCALES for c in p[i + 1:]):
                    p = p[:i] + p[i].translate(_SIN_TILDE) + p[i + 1:]
                break
        return p + "es"
    return None


def _variantes(keyword):
    """La búsqueda de Mercado Público es LITERAL: 'anatomico' no encuentra
    'anatómico' ni 'anatómica', 'llavero' no encuentra 'llaveros'. Medido el
    29-09-2026: anatomico=0, anatómico=3, anatomica=1, llavero=2, llaveros=7.
    Se busca cada keyword con y sin tilde, en plural y, para adjetivos, en femenino."""
    base = str(keyword or "").strip().lower()
    if not base:
        return []
    formas = [base]
    if " " not in base and base.isalpha() and len(base) >= 3:
        for suf in ("ico", "ivo", "ado", "ido"):           # anatómico -> anatómica
            if base.endswith(suf):
                formas.append(base[:-1] + "a")
                break
        if not base.endswith("s"):
            formas += [pl for pl in (_plural(f) for f in list(formas)) if pl]
    out = []
    for f in formas:
        for v in (f, f.translate(_SIN_TILDE)):
            if v not in out:
                out.append(v)
    return out


def buscar_por_palabra(keyword):
    """Busca procesos por palabra clave (todas las regiones del país), con
    todas sus variantes. Devuelve la unión sin repetidos."""
    estado_id = ESTADO_PARAM.get((ESTADOS[0] if ESTADOS else "publicada"), 2)
    vistos, items = set(), []
    for v in _variantes(keyword):
        for it in _paginar({"keywords": v, "status": estado_id, "order_by": "recent"}, MAX_PAGINAS):
            cod = it.get("codigo")
            if cod and cod not in vistos:
                vistos.add(cod)
                items.append(it)
    return items


def _kw_en_titulo(keyword, texto_norm):
    return any(_kw_en_texto(_norm(v), texto_norm) for v in _variantes(keyword))


BARRIDO_MAX_FALLOS_SEGUIDOS = 5
_BARRIDO_ESTADO = {"paginas_fallidas": [], "cortado": False}


def _paginar_hasta(params_base, max_paginas, desde):
    """Como _paginar, pero la lista viene de lo más nuevo a lo más viejo
    (order_by=recent = fecha de publicación descendente, verificado 29-09-2026):
    se corta en la primera página cuyos procesos son TODOS anteriores a `desde`."""
    items, pagina, paginas = [], 1, 0
    fallos_seguidos = 0
    _BARRIDO_ESTADO["paginas_fallidas"] = []
    _BARRIDO_ESTADO["cortado"] = False
    while pagina <= max_paginas:
        params = dict(params_base); params["page_number"] = pagina
        payload = _get_buscador(params)
        time.sleep(PAUSA_SEG)
        if not payload:
            # una página caída (502/504) ya no corta el barrido: se salta y se sigue;
            # solo se abandona tras varias fallas seguidas (y queda marcado como cortado)
            _BARRIDO_ESTADO["paginas_fallidas"].append(pagina)
            fallos_seguidos += 1
            if fallos_seguidos >= BARRIDO_MAX_FALLOS_SEGUIDOS:
                _BARRIDO_ESTADO["cortado"] = True
                print(f"  · barrido: {fallos_seguidos} páginas seguidas fallidas — se detiene en la {pagina}",
                      file=sys.stderr)
                break
            time.sleep(10 * fallos_seguidos)
            pagina += 1
            continue
        fallos_seguidos = 0
        paginas += 1
        res = payload.get("resultados") or []
        items.extend(res)
        if desde is not None and res:
            fechas = [_parse_fecha(r.get("fecha_publicacion")) for r in res]
            if all(f is not None and f < desde for f in fechas):
                break
        if pagina >= (payload.get("pageCount") or 1):
            break
        pagina += 1
    return items, paginas


def buscar_todo(desde=None):
    """Trae lo publicado en el país, sin keywords. Con `desde`, solo hasta esa
    fecha de publicación (barrido incremental). Devuelve (items, páginas)."""
    estado_id = ESTADO_PARAM.get((ESTADOS[0] if ESTADOS else "publicada"), 2)
    items, pags = _paginar_hasta({"status": estado_id, "order_by": "recent"}, MAX_PAGINAS_TODO, desde)
    if not items:  # algunas variantes exigen el parámetro aunque sea vacío
        items, pags = _paginar_hasta({"keywords": "", "status": estado_id, "order_by": "recent"}, MAX_PAGINAS_TODO, desde)
    return items, pags


BARRIDO_INCREMENTAL = bool(_CFG.get("barrido_incremental", True))
BARRIDO_MARGEN_H = 3          # solapamiento con la corrida anterior
BARRIDO_MAX_DIAS = 5          # si la última corrida es más vieja, barrido completo


def decidir_barrido(prev_meta, triage_cache_vacio):
    """Devuelve (desde, motivo). desde=None => barrido completo.
    Lo ya barrido antes está en el feed (arrastre) o en el caché del triage, y las
    búsquedas por keyword miran todas las fechas: basta con revisar lo nuevo."""
    if not BARRIDO_INCREMENTAL:
        return None, "incremental desactivado en keywords.json"
    ini = _parse_fecha((prev_meta.get("barrido") or {}).get("inicio_cl"))
    if ini is None:
        return None, "sin registro de la corrida anterior"
    if triage_cache_vacio:
        return None, "caché del triage vacío (primera vez o perfil cambiado)"
    if (prev_meta.get("barrido") or {}).get("cortado"):
        return None, "el barrido anterior quedó cortado por fallas de la API"
    if "cortado" not in (prev_meta.get("barrido") or {}):
        # feeds de la versión anterior: una página caída cortaba el barrido sin avisar
        return None, "la corrida anterior no registró si el barrido terminó"
    if not (prev_meta.get("triage_ia") or {}).get("completo", False):
        return None, "el triage anterior quedó incompleto"
    if (_ahora_chile() - ini).days >= BARRIDO_MAX_DIAS:
        return None, f"la corrida anterior tiene más de {BARRIDO_MAX_DIAS} días"
    return ini - dt.timedelta(hours=BARRIDO_MARGEN_H), "incremental"


def traer_ficha(codigo):
    payload = _get_buscador({"action": "ficha", "code": codigo})
    time.sleep(PAUSA_SEG)
    return payload


# ---------- Licitaciones (API oficial, requiere MP_TICKET) ----------

def _get_oficial(params, intento=0):
    if not MP_TICKET:
        return None
    params = dict(params); params["ticket"] = MP_TICKET
    while True:
        try:
            r = requests.get(LIC_BASE, params=params, timeout=60)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            intento += 1
            if intento > MAX_REINTENTOS:
                print(f"  · licitaciones: error de red: {e}", file=sys.stderr)
                return None
            time.sleep(5 * intento)
            continue
        if r.status_code in (403, 429, 500, 503):  # cuota por segundo / transitorio
            intento += 1
            if intento > MAX_REINTENTOS:
                print(f"  · licitaciones: HTTP {r.status_code} persistente — saltando", file=sys.stderr)
                return None
            time.sleep(3 * intento)
            continue
        if r.status_code != 200:
            print(f"  · licitaciones: HTTP {r.status_code} — saltando", file=sys.stderr)
            return None
        try:
            return r.json()
        except ValueError:
            return None


def listar_licitaciones():
    data = _get_oficial({"estado": "activas"})
    time.sleep(PAUSA_LIC_SEG)
    return (data or {}).get("Listado") or []


def _monto_a_clp(monto, moneda):
    try:
        m = float(monto)
    except (TypeError, ValueError):
        return None
    if not m:
        return None
    moneda = (moneda or "CLP").upper()
    if moneda == "UTM":
        return int(m * VALOR_UTM_CLP)
    if moneda in ("USD", "DOLAR"):
        return int(m * VALOR_USD_CLP)
    return int(m)  # CLP u otra: se asume CLP


def normalizar_licitacion(item):
    codigo = item.get("CodigoExterno") or ""
    fc = (item.get("FechaCierre") or "").replace("T", " ")[:19] or None
    fpub = (item.get("FechaCreacion") or "").replace("T", " ")[:19] or None
    return {
        "codigo": codigo,
        "tipo": "licitacion",
        "nombre": (item.get("Nombre") or "").strip(),
        "estado": "publicada",
        "estado_glosa": "Publicada",
        "organismo": None, "rut_organismo": None, "unidad_compra": None,
        "region": None, "region_nombre": None,
        "monto_clp": None, "moneda": "CLP",
        "fecha_publicacion": fpub,
        "fecha_cierre": fc,
        "fecha_ultimo_cambio": None,
        "palabras_clave_match": [],
        "total_ofertas": None,
        "productos": [], "adjuntos": [], "descripcion": None,
        "direccion_entrega": None, "plazo_entrega_dias": None,
        "ficha_publica": f"https://www.mercadopublico.cl/Procurement/Modules/RFB/DetailsAcquisition.aspx?idlicitacion={quote(codigo)}",
        "url_detalle_api": f"{LIC_BASE}?codigo={quote(codigo)}",
    }


def enriquecer_licitacion(reg):
    data = _get_oficial({"codigo": reg["codigo"]})
    time.sleep(PAUSA_LIC_SEG)
    det = ((data or {}).get("Listado") or [None])[0]
    if not det:
        return
    if det.get("Descripcion"):
        reg["descripcion"] = det["Descripcion"]
    m = _monto_a_clp(det.get("MontoEstimado"), det.get("Moneda"))
    if m:
        reg["monto_clp"] = m
        reg["moneda"] = det.get("Moneda") or "CLP"
    if det.get("Tipo"):
        reg["tipo_licitacion"] = det.get("Tipo")
    comp = det.get("Comprador") or {}
    if comp.get("NombreOrganismo"):
        reg["organismo"] = comp["NombreOrganismo"]
    reg["unidad_compra"] = comp.get("NombreUnidad") or reg.get("unidad_compra")
    reg["rut_organismo"] = comp.get("RutUnidad") or reg.get("rut_organismo")
    rid, rnom = _region_desde_texto(comp.get("RegionUnidad"))
    if rid or rnom:
        reg["region"], reg["region_nombre"] = rid, rnom
    fechas = det.get("Fechas") or {}
    if fechas.get("FechaCierre"):
        reg["fecha_cierre"] = str(fechas["FechaCierre"]).replace("T", " ")[:19]
    prods = []
    for p in ((det.get("Items") or {}).get("Listado") or []):
        prod = {"nombre": p.get("NombreProducto"), "descripcion": p.get("Descripcion"),
                "cantidad": p.get("Cantidad"), "unidad": p.get("UnidadMedida")}
        if p.get("CodigoProducto"):
            prod["categoria"] = str(p["CodigoProducto"])
        prods.append(prod)
    if prods:
        reg["productos"] = prods


# ---------- Historial de precios adjudicados ----------

def cargar_historico():
    if os.path.exists(HIST_FILE):
        try:
            with open(HIST_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"actualizado": None, "procesados": [], "items": []}


def listar_adjudicadas_dia(ddmmyyyy):
    data = _get_oficial({"fecha": ddmmyyyy, "estado": "adjudicada"})
    time.sleep(PAUSA_LIC_SEG)
    return (data or {}).get("Listado") or []


def _es_propio(texto):
    """True si el texto de proveedor corresponde a una de nuestras empresas."""
    t = _norm(texto or "")
    trut = re.sub(r"[^0-9k]", "", t)
    if any(r and r in trut for r in RUTS_PROPIOS):
        return True
    return any(n and n in t for n in NOMBRES_PROPIOS)


def cargar_resultados():
    if os.path.exists(RESULT_FILE):
        try:
            with open(RESULT_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"actualizado": None, "items": []}


def guardar_resultados(res):
    res["actualizado"] = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with open(RESULT_FILE, "w", encoding="utf-8") as f:
        json.dump(res, f, ensure_ascii=False, indent=1)


def notificar_telegram(texto):
    if not TG_TOKEN or not TG_CHAT:
        return False
    try:
        r = requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                          json={"chat_id": TG_CHAT, "text": texto, "parse_mode": "HTML",
                                "disable_web_page_preview": True}, timeout=30)
        return r.status_code == 200
    except Exception as e:
        print(f"  · telegram: {e}", file=sys.stderr)
        return False


def notificar_correo(asunto, cuerpo_html):
    if not MAIL_USER or not MAIL_PASS or not MAIL_TO:
        return False
    try:
        import smtplib
        from email.mime.text import MIMEText
        msg = MIMEText(cuerpo_html, "html", "utf-8")
        msg["Subject"] = asunto
        msg["From"] = MAIL_USER
        msg["To"] = MAIL_TO
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=60) as s:
            s.login(MAIL_USER, MAIL_PASS)
            s.sendmail(MAIL_USER, [MAIL_TO], msg.as_string())
        return True
    except Exception as e:
        print(f"  · correo: {e}", file=sys.stderr)
        return False


def _num_clp(s):
    s = re.sub(r"[^\d,.\-]", "", s or "")
    if not s:
        return None
    s = s.replace(".", "").replace(",", ".")
    try:
        v = float(s)
        return v if v > 0 else None
    except ValueError:
        return None


def parsear_acta_ofertas(url):
    """Extrae TODAS las ofertas (ganadoras y perdedoras) del acta pública de
    adjudicación. Parsing tolerante: si la página cambia, devuelve [] y el
    histórico cae al dato de la API (solo ganador)."""
    if not url:
        return []
    try:
        r = requests.get(url.replace("http://", "https://"), timeout=60,
                         headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return []
        html = r.text
    except Exception:
        return []
    # posiciones de cada producto: "Clasificación ONU : 82151704 ..."
    prods = [(m.start(), m.group(1)) for m in
             re.finditer(r"Clasificaci[^<]{0,30}ONU[^0-9]{0,60}(\d{6,10})", html)]
    ofertas = []
    for m in re.finditer(r"lnkViewProvider[^>]*>(.*?)</a>", html, re.S):
        prov = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", m.group(1))).strip()
        if not prov:
            continue
        # producto al que pertenece: el último encabezado ONU antes de esta fila
        cod = ""
        for pos, c in prods:
            if pos < m.start():
                cod = c
            else:
                break
        ventana = html[m.end():m.end() + 3000]
        mu = re.search(r"\$\s*([\d\.\,]+)", ventana)
        unit = _num_clp(mu.group(1)) if mu else None
        if not unit:
            continue
        me = re.search(r"(No\s+Adjudicad[ao]|Adjudicad[ao]|Rechazad[ao]|Fuera de Bases)", ventana, re.I)
        estado = re.sub(r"\s+", " ", me.group(1)).strip().lower() if me else ""
        mq = re.search(r"\$\s*[\d\.\,]+[^0-9]{0,200}?>\s*([\d\.,]+)\s*<", ventana)
        cant = _num_clp(mq.group(1)) if mq else None
        ofertas.append({"c": cod, "pr": prov[:70], "u": unit, "q": cant, "e": estado})
    return ofertas


def _relevante_para_historico(nombre):
    n = _norm(nombre or "")
    if _hit_blacklist(n):
        return False
    if any(_kw_en_texto(_norm(kw), n) for kw in PALABRAS_CLAVE):
        return True
    return bool(_matches_whitelist(n))


def _extraer_ofertas_ficha_ca(det):
    """Busca recursivamente pares (proveedor, monto) en la ficha de una Compra
    Ágil cerrada — la API no está documentada, así que se exploran claves
    plausibles. Devuelve [{'pr','u','e'}]."""
    res = []
    def walk(o):
        if isinstance(o, dict):
            nombre = monto = None
            sel = False
            for k, v in o.items():
                kl = str(k).lower()
                if isinstance(v, str) and v.strip() and any(t in kl for t in ("proveedor", "razon_social", "nombre_empresa")):
                    if "rut" not in kl:
                        nombre = v.strip()
                if isinstance(v, (int, float)) and not isinstance(v, bool) and v > 0 and any(t in kl for t in ("monto", "total", "precio")):
                    monto = float(v)
                if ("selecc" in kl or "adjudic" in kl or "ganador" in kl) and v in (True, 1, "1", "si", "SI"):
                    sel = True
                if isinstance(v, str) and "selecc" in v.lower():
                    sel = True
            if nombre and monto:
                res.append({"pr": nombre[:60], "u": monto, "e": "adjudicada" if sel else ""})
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(det)
    # si la ficha muestra una sola oferta, es la seleccionada
    if len(res) == 1 and not res[0]["e"]:
        res[0]["e"] = "adjudicada"
    # si hay varias sin estado claro, quedan como "oferta" (rango rival, no ganadora)
    for o in res:
        if not o["e"]:
            o["e"] = "oferta"
    return res


def capturar_historico_ca(hist, vistos, resultados):
    """Precios de Compras Ágiles cerradas del rubro (buscador público, sin cuota).
    Solo procesos con 0 o 1 producto: permite calcular precio unitario sin ambigüedad."""
    ca_proc = set(hist.get("ca_procesadas") or [])
    estado_id = ESTADO_PARAM.get("cerrada", 3)
    if BUSCAR_TODO:
        cerradas = _paginar({"status": estado_id, "order_by": "recent"}, 30)
    else:
        cerradas = []
        for kw in PALABRAS_CLAVE:
            cerradas.extend(_paginar({"keywords": kw, "status": estado_id, "order_by": "recent"}, 5))
    vistos_cod, fichas, nuevas = set(), 0, 0
    for it in cerradas:
        cod = it.get("codigo")
        if not cod or cod in vistos_cod or cod in ca_proc:
            continue
        vistos_cod.add(cod)
        if not _relevante_para_historico(it.get("nombre")):
            continue
        if fichas >= HIST_MAX_DETALLE:
            break
        det = traer_ficha(cod)
        fichas += 1
        ca_proc.add(cod)
        if not det:
            continue
        prods = det.get("productos_solicitados") or []
        if len(prods) > 1:
            continue  # sin desglose por ítem no se puede derivar el unitario
        ofertas = _extraer_ofertas_ficha_ca(det)
        if not ofertas:
            continue
        cant = 1.0
        if prods:
            try:
                cant = max(1.0, float(prods[0].get("cantidad") or 1))
            except (TypeError, ValueError):
                pass
        nombre_p = (prods[0].get("nombre") if prods else it.get("nombre")) or ""
        inst = det.get("informacion_institucion") or {}
        fecha = str(det.get("fecha_cierre") or it.get("fecha_cierre") or dt.date.today().isoformat())[:10]
        for o in ofertas:
            unit = int(round(o["u"] / cant))
            if unit <= 0:
                continue
            if _es_propio(o["pr"]) and o["e"] == "adjudicada":
                resultados["items"].append({
                    "codigo": cod, "tipo": "compra_agil", "fecha": fecha,
                    "nombre": nombre_p[:120], "nuestra_oferta": int(o["u"]),
                    "resultado": "ganada", "ganador": o["pr"], "monto_ganador": int(o["u"]),
                })
            clave = (cod, "CA", o["pr"][:60], unit)
            if clave in vistos:
                continue
            vistos.add(clave)
            hist["items"].append({
                "l": cod, "i": "CA", "f": fecha, "p": nombre_p[:120],
                "d": (det.get("descripcion") or "")[:120], "c": "",
                "q": cant, "u": unit, "m": "CLP",
                "pr": o["pr"], "org": (inst.get("organismo_comprador") or it.get("organismo") or "")[:60],
                "of": det.get("total_ofertas_recibidas"), "e": o["e"],
            })
            nuevas += 1
    # recordar procesadas (tope para que el archivo no crezca sin límite)
    hist["ca_procesadas"] = (hist.get("ca_procesadas") or [])
    hist["ca_procesadas"] = [c for c in hist["ca_procesadas"] if c in ca_proc] + \
                            [c for c in ca_proc if c not in set(hist["ca_procesadas"])]
    hist["ca_procesadas"] = hist["ca_procesadas"][-5000:]
    print(f"Histórico CA: +{nuevas} ofertas ({fichas} fichas revisadas)")
    return nuevas


def actualizar_historico():
    """Recolecta precios unitarios adjudicados de licitaciones del rubro.
    Backfill gradual hacia atrás hasta HISTORICO_DIAS; cuota controlada."""
    hist = cargar_historico()
    resultados = cargar_resultados()
    res_previos = len(resultados["items"])
    codigos_res = {(r.get("codigo"), r.get("resultado")) for r in resultados["items"]}
    procesados = set(hist.get("procesados") or [])
    vistos = {(it.get("l"), it.get("i"), it.get("pr"), it.get("u")) for it in hist.get("items") or []}
    hoy = dt.date.today()
    candidatos = [(hoy - dt.timedelta(days=d)).isoformat() for d in range(1, HISTORICO_DIAS + 1)]
    pendientes = ([d for d in candidatos if d not in procesados][:HIST_DIAS_POR_CORRIDA]) if MP_TICKET else []
    detalles_usados, nuevos = 0, 0
    for dia in pendientes:
        if detalles_usados >= HIST_MAX_DETALLE:
            break
        f = dt.date.fromisoformat(dia)
        lst = listar_adjudicadas_dia(f.strftime("%d%m%Y"))
        relevantes = [it for it in lst
                      if it.get("CodigoExterno") and _relevante_para_historico(it.get("Nombre"))]
        if len(relevantes) > HIST_MAX_DETALLE - detalles_usados:
            break  # no alcanza la cuota para el día completo: se retoma en la próxima corrida
        for it in relevantes:
            data = _get_oficial({"codigo": it["CodigoExterno"]})
            time.sleep(PAUSA_LIC_SEG)
            detalles_usados += 1
            det = ((data or {}).get("Listado") or [None])[0]
            if not det:
                continue
            moneda = det.get("Moneda") or "CLP"
            n_of = ((det.get("Adjudicacion") or {}).get("NumeroOferentes"))
            org = ((det.get("Comprador") or {}).get("NombreOrganismo") or "")[:60]
            items_api = ((det.get("Items") or {}).get("Listado") or [])
            mapa_prod = {str(p.get("CodigoProducto") or ""): p for p in items_api}
            # 1) Acta pública: TODAS las ofertas (ganadoras y perdedoras) con su monto
            acta_url = ((det.get("Adjudicacion") or {}).get("UrlActa")) or ""
            filas_acta = parsear_acta_ofertas(acta_url)
            time.sleep(PAUSA_LIC_SEG)
            if acta_url and not filas_acta:
                print(f"  · acta sin ofertas parseables: {it['CodigoExterno']}", file=sys.stderr)
            if filas_acta:
                # ¿Ofertamos nosotros? → registrar resultado (ganada/perdida)
                propias = [o for o in filas_acta if _es_propio(o["pr"])]
                if propias:
                    ganadora = next((o for o in filas_acta if str(o.get("e", "")).startswith("adjudicad")), None)
                    for o in propias:
                        gane = str(o.get("e", "")).startswith("adjudicad")
                        resultados["items"].append({
                            "codigo": it["CodigoExterno"], "tipo": "licitacion", "fecha": dia,
                            "nombre": (it.get("Nombre") or "")[:120],
                            "nuestra_oferta": _monto_a_clp(o["u"], moneda),
                            "resultado": "ganada" if gane else "perdida",
                            "ganador": (ganadora or {}).get("pr", ""),
                            "monto_ganador": _monto_a_clp((ganadora or {}).get("u"), moneda),
                        })
                for idx, o in enumerate(filas_acta):
                    unit = _monto_a_clp(o["u"], moneda)
                    if not unit:
                        continue
                    clave = (it["CodigoExterno"], o["c"] or idx, o["pr"][:60], unit)
                    if clave in vistos:
                        continue
                    vistos.add(clave)
                    pin = mapa_prod.get(o["c"]) or (items_api[0] if len(items_api) == 1 else {})
                    hist["items"].append({
                        "l": it["CodigoExterno"], "i": o["c"] or idx,
                        "f": dia, "p": (pin.get("NombreProducto") or it.get("Nombre") or "")[:120],
                        "d": (pin.get("Descripcion") or "")[:120],
                        "c": o["c"] or str(pin.get("CodigoProducto") or ""),
                        "q": o.get("q") or pin.get("Cantidad"),
                        "u": unit, "m": moneda,
                        "pr": o["pr"][:60], "org": org, "of": n_of,
                        "e": o.get("e") or "",
                    })
                    nuevos += 1
            else:
                # 2) Fallback API: solo el precio ganador por ítem
                for p in items_api:
                    adj = p.get("Adjudicacion") or {}
                    unit = _monto_a_clp(adj.get("MontoUnitario"), moneda)
                    if not unit:
                        continue
                    clave = (it["CodigoExterno"], p.get("Correlativo"), (adj.get("NombreProveedor") or "")[:60], unit)
                    if clave in vistos:
                        continue
                    vistos.add(clave)
                    hist["items"].append({
                        "l": it["CodigoExterno"], "i": p.get("Correlativo"),
                        "f": dia, "p": (p.get("NombreProducto") or "")[:120],
                        "d": (p.get("Descripcion") or "")[:120],
                        "c": str(p.get("CodigoProducto") or ""),
                        "q": adj.get("Cantidad") or p.get("Cantidad"),
                        "u": unit, "m": moneda,
                        "pr": (adj.get("NombreProveedor") or "")[:60],
                        "org": org, "of": n_of,
                        "e": "adjudicada",
                    })
                    nuevos += 1
        procesados.add(dia)
    # Compra Ágil cerradas (buscador público, sin cuota del ticket)
    try:
        nuevos += capturar_historico_ca(hist, vistos, resultados)
    except Exception as e:
        print(f"  · histórico CA: {e}", file=sys.stderr)
    # poda: fuera de la ventana de un año
    limite = (hoy - dt.timedelta(days=HISTORICO_DIAS)).isoformat()
    hist["items"] = [x for x in hist["items"] if (x.get("f") or "") >= limite]
    hist["procesados"] = sorted(d for d in procesados if d >= limite)
    hist["actualizado"] = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    # dedup y guardado de resultados propios
    unicos, vistos_r = [], set()
    for r in resultados["items"]:
        k = (r.get("codigo"), r.get("resultado"), r.get("nuestra_oferta"))
        if k in vistos_r:
            continue
        vistos_r.add(k)
        unicos.append(r)
    resultados["items"] = unicos[-500:]
    guardar_resultados(resultados)
    hist["resultados_nuevos"] = len(resultados["items"]) - min(res_previos, len(resultados["items"]))
    hist["dias_cubiertos"] = len(hist["procesados"])
    hist["dias_pendientes"] = HISTORICO_DIAS - len(hist["procesados"])
    with open(HIST_FILE, "w", encoding="utf-8") as f:
        json.dump(hist, f, ensure_ascii=False)
    print(f"Histórico de precios: +{nuevos} registros ({len(hist['items'])} totales, "
          f"{hist['dias_cubiertos']}/{HISTORICO_DIAS} días cubiertos, {detalles_usados} fichas usadas)")
    return hist


# ---------- Adjuntos ----------

def _safe_filename(nombre):
    nombre = (nombre or "archivo").strip()
    nombre = nombre.replace("\\", "_").replace("/", "_")
    nombre = re.sub(r'[<>:"|?*\x00-\x1f]', "_", nombre)
    nombre = re.sub(r"\s+", " ", nombre).strip()
    return nombre[:150] or "archivo"


# Estado del servicio de adjuntos durante la corrida. El servicio de Mercado
# Público devuelve 504 con frecuencia; antes eso se tragaba en silencio y la
# corrida perdía ~30 s por oportunidad para terminar con cero adjuntos.
_ADJ_ESTADO = {"fallos_seguidos": 0, "apagado": False, "motivos": {}}
ADJ_TIMEOUT = 8          # antes 30: si tarda más, no sirve
ADJ_MAX_FALLOS = 5       # cortacircuitos tras N fallos consecutivos


def listar_adjuntos_publico(codigo):
    if _ADJ_ESTADO["apagado"]:
        return []

    def _fallo(motivo):
        _ADJ_ESTADO["fallos_seguidos"] += 1
        _ADJ_ESTADO["motivos"][motivo] = _ADJ_ESTADO["motivos"].get(motivo, 0) + 1
        print(f"  · adjuntos {codigo}: {motivo}", file=sys.stderr)
        if _ADJ_ESTADO["fallos_seguidos"] >= ADJ_MAX_FALLOS:
            _ADJ_ESTADO["apagado"] = True
            print(f"  · adjuntos: {ADJ_MAX_FALLOS} fallos seguidos — se omiten "
                  f"los adjuntos en el resto de la corrida", file=sys.stderr)
        return []

    try:
        r = requests.get(f"{ADJ_BASE}/listar/{quote(codigo)}",
                         headers=dict(_CAB_MP, user_key=ADJ_USER_KEY), timeout=ADJ_TIMEOUT)
        if r.status_code != 200:
            return _fallo(f"HTTP {r.status_code}")
        data = r.json()
        if data.get("success") != "OK":
            return _fallo(f"success={data.get('success')!r}")
        files = (data.get("payload") or {}).get("files") or []
        _ADJ_ESTADO["fallos_seguidos"] = 0   # respuesta buena: se reinicia el contador
        return files
    except Exception as e:
        return _fallo(type(e).__name__)


def resumen_adjuntos():
    """Una línea al final de la corrida con el estado del servicio de adjuntos."""
    m = _ADJ_ESTADO["motivos"]
    if not m:
        return ""
    detalle = ", ".join(f"{k}×{v}" for k, v in sorted(m.items(), key=lambda x: -x[1]))
    apagado = " (servicio omitido tras el cortacircuitos)" if _ADJ_ESTADO["apagado"] else ""
    return f"Adjuntos: sin resultados — {detalle}{apagado}"


def descargar_adjunto(guid, destino):
    try:
        with requests.get(f"{ADJ_BASE}/descargar/{guid}",
                          headers=dict(_CAB_MP, user_key=ADJ_USER_KEY),
                          timeout=120, stream=True) as r:
            if r.status_code != 200:
                return False
            cl = r.headers.get("Content-Length")
            if cl and int(cl) > MAX_ADJ_MB * 1024 * 1024:
                print(f"  · adjunto {guid} supera {MAX_ADJ_MB} MB — omitido", file=sys.stderr)
                return False
            tot = 0
            with open(destino, "wb") as f:
                for chunk in r.iter_content(chunk_size=65536):
                    tot += len(chunk)
                    if tot > MAX_ADJ_MB * 1024 * 1024:
                        f.close(); os.remove(destino)
                        print(f"  · adjunto {guid} supera {MAX_ADJ_MB} MB — omitido", file=sys.stderr)
                        return False
                    f.write(chunk)
            return tot > 0
    except Exception as e:
        print(f"  · descarga adjunto {guid}: {e}", file=sys.stderr)
        if os.path.exists(destino):
            try: os.remove(destino)
            except OSError: pass
        return False


def procesar_adjuntos(registro):
    codigo = registro["codigo"]
    files = listar_adjuntos_publico(codigo)
    time.sleep(PAUSA_SEG)
    adjuntos = []
    if files:
        carpeta = os.path.join(ADJ_DIR, codigo)
        os.makedirs(carpeta, exist_ok=True)
        for f in files:
            guid = f.get("id") or ""
            nombre = _safe_filename(f.get("nombreArchivo"))
            destino = os.path.join(carpeta, nombre)
            if guid and (os.path.exists(destino) or descargar_adjunto(guid, destino)):
                url = f"{RAW_BASE}/{ADJ_DIR}/{quote(codigo)}/{quote(nombre)}"
            else:
                url = registro["ficha_publica"]  # fallback: descargar desde la ficha
            ext = nombre.rsplit(".", 1)[-1].lower() if "." in nombre else ""
            adjuntos.append({"id": guid, "nombre": f.get("nombreArchivo") or nombre,
                             "url": url, "tipo": ext})
            time.sleep(PAUSA_SEG)
    registro["adjuntos"] = adjuntos


def limpiar_adjuntos_viejos(codigos_vigentes):
    if not os.path.isdir(ADJ_DIR):
        return
    for d in os.listdir(ADJ_DIR):
        ruta = os.path.join(ADJ_DIR, d)
        if os.path.isdir(ruta) and d not in codigos_vigentes:
            shutil.rmtree(ruta, ignore_errors=True)
            print(f"  · limpieza: adjuntos/{d} eliminado")


# ---------- Normalización ----------

def _region_desde_texto(v):
    """Región a partir de un nombre en texto libre ('Región del Biobío')."""
    if not isinstance(v, str) or not v.strip():
        return None, None
    vn = _norm(v).replace("region", "").replace("del ", "").replace("de ", "").strip()
    for nom_n, rid in _REGION_POR_NOMBRE.items():
        if nom_n in vn or vn in nom_n:
            return rid, REGION_NOMBRES[rid]
    return None, v.strip()  # nombre desconocido: se muestra tal cual


def _extraer_region(obj):
    """Busca la región en varios campos posibles (API no documentada)."""
    if not isinstance(obj, dict):
        return None, None
    for k in ("id_region", "region_id", "idRegion", "id_region_unidad", "id_region_compradora"):
        v = obj.get(k)
        if v is not None:
            try:
                rid = int(v)
                if rid in REGION_NOMBRES:
                    return rid, REGION_NOMBRES[rid]
            except (TypeError, ValueError):
                pass
    for k in ("region", "region_nombre", "nombre_region", "region_unidad", "region_compradora"):
        v = obj.get(k)
        if isinstance(v, str) and v.strip():
            return _region_desde_texto(v)
        if isinstance(v, int) and v in REGION_NOMBRES:
            return v, REGION_NOMBRES[v]
    return None, None


def _extraer_categoria(prod):
    """Captura código de categoría/rubro (UNSPSC) si la API lo trae."""
    for k in ("id_categoria", "codigo_categoria", "id_producto", "codigo_producto",
              "categoria_id", "onu", "codigo_onu", "unspsc"):
        v = prod.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return None


def normalizar(item, palabras_match):
    codigo = item.get("codigo") or ""
    id_estado = item.get("id_estado")
    rid, rnom = _extraer_region(item)
    return {
        "codigo": codigo,
        "tipo": "compra_agil",
        "nombre": (item.get("nombre") or "").strip(),
        "estado": ESTADO_CODIGO.get(id_estado, str(item.get("estado") or "").lower()),
        "estado_glosa": ESTADO_GLOSA.get(id_estado, item.get("estado")),
        "organismo": item.get("organismo"),
        "rut_organismo": None,          # se completa con la ficha
        "unidad_compra": item.get("unidad"),
        "region": rid,
        "region_nombre": rnom,
        "monto_clp": item.get("monto_disponible_CLP") or item.get("monto_disponible"),
        "moneda": item.get("moneda") or "CLP",
        "fecha_publicacion": item.get("fecha_publicacion"),
        "fecha_cierre": item.get("fecha_cierre"),
        "fecha_ultimo_cambio": item.get("fecha_cambio"),
        "palabras_clave_match": sorted(palabras_match),
        "total_ofertas": None,          # se completa con la ficha
        "productos": [], "adjuntos": [], "descripcion": None,
        "direccion_entrega": None, "plazo_entrega_dias": None,
        "ficha_publica": f"https://buscador.mercadopublico.cl/ficha?code={quote(codigo)}",
        "url_detalle_api": f"{BUSCADOR_BASE}/compra-agil?action=ficha&code={quote(codigo)}",
    }


def enriquecer_con_detalle(registro):
    det = traer_ficha(registro["codigo"])
    if det:
        prods = []
        for p in (det.get("productos_solicitados") or []):
            prod = {"nombre": p.get("nombre"), "descripcion": p.get("descripcion"),
                    "cantidad": p.get("cantidad"), "unidad": p.get("unidad_medida")}
            cat = _extraer_categoria(p)
            if cat:
                prod["categoria"] = cat
            prods.append(prod)
        registro["productos"] = prods
        if det.get("descripcion"):
            registro["descripcion"] = det["descripcion"]
        registro["direccion_entrega"] = det.get("direccion_entrega")
        registro["plazo_entrega_dias"] = det.get("plazo_entrega")
        if det.get("total_ofertas_recibidas") is not None:
            registro["total_ofertas"] = det.get("total_ofertas_recibidas")
        if det.get("presupuesto_estimado") and not registro.get("monto_clp"):
            registro["monto_clp"] = det.get("presupuesto_estimado")
        inst = det.get("informacion_institucion") or {}
        if inst.get("organismo_comprador"):
            registro["organismo"] = inst["organismo_comprador"]
        registro["rut_organismo"] = inst.get("rut_organismo_comprador")
        if inst.get("division"):
            registro["unidad_compra"] = inst["division"]
        if registro.get("region") is None:
            rid, rnom = _extraer_region(inst)
            if rid or rnom:
                registro["region"], registro["region_nombre"] = rid, rnom
    # Adjuntos: servicio público (GUIDs + descarga real al repo)
    procesar_adjuntos(registro)


# ---------- Filtros ----------

def cerrada_ya(reg):
    """True si el proceso ya cerró (única razón para sacarlo del feed)."""
    fc = _parse_fecha(reg.get("fecha_cierre"))
    return fc is not None and fc <= _ahora_chile()


def filtro_duro(reg):
    """Filtros baratos que no requieren ficha ni IA. Devuelve razón o None."""
    fc = _parse_fecha(reg.get("fecha_cierre"))
    if fc is not None:
        horas = (fc - _ahora_chile()).total_seconds() / 3600
        if horas < HORAS_MIN_CIERRE:
            return f"cierre a menos de {HORAS_MIN_CIERRE}h"
    m = reg.get("monto_clp")
    if m is not None:
        try:
            if float(m) < MONTO_MIN_CLP:
                return f"monto bajo el mínimo ({MONTO_MIN_CLP:,} CLP)"
        except (TypeError, ValueError):
            pass
    return None


def prefiltro_texto(reg, con_detalle=False):
    """Blacklist sobre nombre y nombres de productos (NO la descripción
    completa, para no matar oportunidades por menciones incidentales).
    Devuelve (pasa, razon)."""
    nombre_n = _norm(reg.get("nombre") or "")
    hit = _hit_blacklist(nombre_n)
    if hit:
        return False, f"blacklist: '{hit}' en el nombre"
    if con_detalle:
        for p in reg.get("productos") or []:
            pn = _norm(p.get("nombre") or "")
            hit = _hit_blacklist(pn)
            if hit:
                return False, f"blacklist: '{hit}' en producto"
            cat = str(p.get("categoria") or "")
            for rb in RUBROS_BLOQUEADOS:
                if rb and cat.startswith(rb):
                    return False, f"rubro bloqueado {rb}"
    if BUSCAR_TODO:
        # sin keywords: exigir al menos una señal en nombre o productos para candidato IA
        texto = nombre_n
        if con_detalle:
            texto += " " + _norm(" ".join((p.get("nombre") or "") + " " + (p.get("descripcion") or "")
                                          for p in (reg.get("productos") or [])))
        if not reg.get("palabras_clave_match") and not reg.get("triage") and not _matches_whitelist(texto):
            return False, "sin coincidencia con keywords/whitelist"
    return True, ""


# ---------- Evaluación IA (Haiku, caché persistente) ----------

def cargar_cache_ia():
    if os.path.exists(EVAL_CACHE_FILE):
        try:
            with open(EVAL_CACHE_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def guardar_cache_ia(cache, codigos_vigentes):
    ahora = time.time()
    limite = IA_CACHE_DIAS * 86400
    limpio = {c: e for c, e in cache.items()
              if c in codigos_vigentes or (ahora - (e.get("t", 0) / 1000.0)) < limite}
    with open(EVAL_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(limpio, f, ensure_ascii=False)
    return limpio


def _llamar_anthropic(prompt, max_tokens):
    last_err = ""
    for modelo in IA_MODELOS:
        try:
            r = requests.post("https://api.anthropic.com/v1/messages",
                headers={"Content-Type": "application/json", "x-api-key": ANTHROPIC_KEY,
                         "anthropic-version": "2023-06-01"},
                json={"model": modelo, "max_tokens": max_tokens,
                      "messages": [{"role": "user", "content": prompt}]},
                timeout=120)
        except requests.exceptions.RequestException as e:
            last_err = str(e); continue
        if r.status_code == 200:
            data = r.json()
            return (data.get("content") or [{}])[0].get("text") or ""
        last_err = f"API {r.status_code} ({modelo})"
        if r.status_code not in (400, 404):
            break  # solo probar otro modelo si este no existe
    raise RuntimeError(last_err or "API sin respuesta")


PROMPT_EVAL = ("Perfil de capacidades:\n" + PERFIL.replace("{", "(").replace("}", ")") + "\n\n"
    "Green Wolf SPA y Provectus SpA (Chile) fabrican con impresión 3D FDM y resina: prototipos, "
    "piezas plásticas funcionales, modelos anatómicos y fantomas, señalética y letreros 3D, "
    "señalética inclusiva (braille, podotáctil), repuestos plásticos, maquetas, trofeos, galvanos "
    "y medallas, llaveros y pines, material didáctico, exhibidores y organizadores. Evalúa cada oportunidad de Mercado Público (t=CA: Compra Ágil, "
    "t=LIC: licitación formal, exige más papeleo y garantías): ¿lo pedido PUEDE fabricarse "
    "con impresión 3D y es buen negocio (monto, plazo, cantidad producible)? NO viable: "
    "imprenta de papel, software/licencias, servicios profesionales, químicos, textiles, "
    "electrónica terminada, alimentos.\n"
    "Responde SOLO un arreglo JSON, una entrada por licitación ({n} en total):\n"
    '[{{"c":"código","v":true,"s":0,"r":"razón, máx 10 palabras"}}]\n'
    "s = atractivo 0-100.\nLicitaciones:\n{datos}")


def _compactar_para_ia(reg):
    d = {"c": reg["codigo"], "n": (reg.get("nombre") or "")[:120]}
    if reg.get("tipo") == "licitacion":
        d["t"] = "LIC"
    if reg.get("descripcion"):
        d["d"] = reg["descripcion"][:200]
    prods = reg.get("productos") or []
    if prods:
        d["p"] = "; ".join(f"{p.get('cantidad') or 1}x {(p.get('nombre') or '')[:60]}" for p in prods[:8])[:300]
    if reg.get("monto_clp"): d["m"] = reg["monto_clp"]
    if reg.get("fecha_cierre"): d["fc"] = str(reg["fecha_cierre"])[:16]
    if reg.get("plazo_entrega_dias"): d["pe"] = reg["plazo_entrega_dias"]
    return d


def evaluar_ia(candidatos, cache):
    """Evalúa con Haiku SOLO los códigos sin caché. Devuelve (cache, nuevos, errores)."""
    pendientes = [r for r in candidatos if r["codigo"] not in cache][:MAX_EVAL_IA]
    if not pendientes:
        return cache, 0, 0
    print(f"Evaluación IA: {len(pendientes)} códigos nuevos (caché: {len(cache)})")
    nuevos, errores = 0, 0
    for i in range(0, len(pendientes), IA_LOTE):
        lote = pendientes[i:i + IA_LOTE]
        datos = [_compactar_para_ia(r) for r in lote]
        prompt = PROMPT_EVAL.format(n=len(datos), datos=json.dumps(datos, ensure_ascii=False))
        try:
            txt = _llamar_anthropic(prompt, max_tokens=90 * len(datos) + 200)
            txt = txt.replace("```json", "").replace("```", "").strip()
            ini, fin = txt.find("["), txt.rfind("]")
            if ini == -1 or fin <= ini:
                raise ValueError("respuesta sin JSON")
            for e in json.loads(txt[ini:fin + 1]):
                cod = e.get("c") or e.get("codigo")
                if not cod: continue
                cache[cod] = {"v": bool(e.get("v", e.get("viable"))),
                              "s": max(0, min(100, int(e.get("s", e.get("score", 0)) or 0))),
                              "r": str(e.get("r", e.get("razon", "")))[:150],
                              "t": int(time.time() * 1000)}
                nuevos += 1
        except Exception as ex:
            errores += 1
            print(f"  · lote IA {i // IA_LOTE + 1}: {ex}", file=sys.stderr)
        time.sleep(1)
    return cache, nuevos, errores


# ---------- Triage IA sobre todo el barrido ----------

PROMPT_TRIAGE = (
    "Eres el filtro de oportunidades de compras públicas de una empresa chilena de fabricación digital.\n"
    "LO QUE PUEDE FABRICAR:\n{perfil}\n\n"
    "LO QUE NO PUEDE: piezas de metal, textiles y vestuario, electrónica terminada, dispositivos médicos "
    "implantables o con registro sanitario, químicos, alimentos, impresión en papel, señalética vial "
    "reflectante certificada, servicios (limpieza, mantención, transporte, capacitación, arriendo).\n"
    "DESCARTA SIEMPRE (no son para esta empresa aunque parezcan objetos): muebles (camas, sillas, mesas, "
    "escritorios, estantes, cajoneras, casilleros, carros, pizarras); impresión gran formato, gigantografías, "
    "vinilos, pendones, lienzos y lonas; ferretería y cerrajería (cerraduras, bisagras, candados, tornillos); "
    "máquinas y equipos (impresoras, computadores, electrodomésticos, herramientas) y sus insumos (tóner, "
    "tinta, repuestos de equipos); EPP y seguridad industrial; construcción y obras (puertas, ventanas, "
    "cierres, techumbres, policarbonato, planchas, pintura); aseo, basureros y contenedores; útiles de "
    "oficina y papelería genéricos.\n\n"
    "Tarea: de la lista de títulos, devuelve SOLO los que PODRÍAN resolverse total o parcialmente con "
    "piezas impresas en 3D por esta empresa. Sé inclusivo con modelos, maquetas, réplicas, simuladores y "
    "fantomas de entrenamiento, soportes, carcasas, organizadores, exhibidores, trofeos, galvanos, "
    "medallas, llaveros, placas y letreros, material didáctico, juguetes y juegos terapéuticos, timbres y "
    "piezas plásticas a medida. Incluye un proceso solo si una parte importante de lo pedido son piezas que "
    "se imprimen en 3D; si lo pedido es mayormente un producto industrial de catálogo, descártalo.\n"
    "Para cada uno indica en t la palabra o frase del título que lo delata, copiada tal como aparece.\n"
    'Responde SOLO un arreglo JSON: [{{"c":"código","t":"palabra"}}]. Si ninguno califica: [].\n'
    "Títulos (código|título):\n{datos}")


PERFIL_HASH = hashlib.sha1(PERFIL.encode("utf-8")).hexdigest()[:12]
_AVISO_PERFIL = []


def cargar_cache_triage():
    """El veredicto depende del perfil: si el perfil cambió, el caché no sirve
    (y se hace un barrido completo para re-evaluar todo con el perfil nuevo).
    Un caché sin marca de perfil (versión anterior) se adopta tal cual."""
    if os.path.exists(TRIAGE_CACHE_FILE):
        try:
            with open(TRIAGE_CACHE_FILE, encoding="utf-8") as f:
                data = json.load(f) or {}
            marca = data.pop("_perfil", None)
            if marca is not None and marca != PERFIL_HASH:
                if not _AVISO_PERFIL:
                    _AVISO_PERFIL.append(1)
                    print("Triage IA: el perfil de empresa cambió — se re-evalúa todo")
                return {}
            return data
        except Exception:
            pass
    return {}


def guardar_cache_triage(cache):
    limite = (time.time() - TRIAGE_CACHE_DIAS * 86400) * 1000
    cache = {k: v for k, v in cache.items() if (v.get("ts") or 0) >= limite}
    with open(TRIAGE_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(dict(cache, _perfil=PERFIL_HASH), f, ensure_ascii=False, separators=(",", ":"))
    return cache


def triage_ia(regs):
    """Devuelve (rescatados, stats). Marca reg["triage"] con la palabra que lo delató.
    Solo se consultan títulos nuevos; los ya vistos salen del caché sin costo."""
    stats = {"revisados": 0, "nuevos": 0, "rescatados": 0, "errores": 0, "completo": True}
    if not TRIAGE_ON or not ANTHROPIC_KEY:
        stats["completo"] = False      # sin triage no se puede barrer solo lo nuevo
        return [], stats
    if not regs:
        return [], stats
    cache = cargar_cache_triage()
    stats["revisados"] = len(regs)
    todos_pend = [r for r in regs if r["codigo"] not in cache]
    pendientes = todos_pend[:MAX_TRIAGE]
    if len(todos_pend) > MAX_TRIAGE:
        stats["completo"] = False
    ahora = int(time.time() * 1000)
    print(f"Triage IA: {len(regs)} títulos sin coincidencia, {len(pendientes)} nuevos a evaluar")
    for i in range(0, len(pendientes), TRIAGE_LOTE):
        lote = pendientes[i:i + TRIAGE_LOTE]
        lineas = "\n".join(f"{r['codigo']}|{(r.get('nombre') or '')[:110]}" for r in lote)
        prompt = PROMPT_TRIAGE.format(perfil=PERFIL, datos=lineas)
        try:
            txt = _llamar_anthropic(prompt, max_tokens=2500)
            txt = txt.replace("```json", "").replace("```", "").strip()
            ini, fin = txt.find("["), txt.rfind("]")
            if ini == -1 or fin < ini:
                raise ValueError("respuesta sin JSON")
            positivos = {}
            for e in json.loads(txt[ini:fin + 1]):
                if isinstance(e, dict) and e.get("c"):
                    positivos[str(e["c"]).strip()] = str(e.get("t") or "")[:60]
            for r in lote:        # solo se cachea un lote que respondió bien
                cod = r["codigo"]
                cache[cod] = {"v": 1, "t": positivos[cod], "ts": ahora} if cod in positivos else {"v": 0, "ts": ahora}
            stats["nuevos"] += len(lote)
        except Exception as ex:
            stats["errores"] += 1
            print(f"  · triage lote {i // TRIAGE_LOTE + 1}: {ex}", file=sys.stderr)
        time.sleep(1)
    if stats["errores"]:
        stats["completo"] = False
    guardar_cache_triage(cache)
    rescatados = []
    for r in regs:
        ev = cache.get(r["codigo"])
        if ev and ev.get("v"):
            r["triage"] = ev.get("t") or "IA"
            rescatados.append(r)
    stats["rescatados"] = len(rescatados)
    return rescatados, stats


def sugerir_keywords(rescatados):
    """Palabras que el triage usó para rescatar y que ninguna keyword/whitelist cubre:
    son candidatas a keyword (la búsqueda por keyword mira también DENTRO de los productos)."""
    conteo, ejemplos = {}, {}
    for r in rescatados:
        t = str(r.get("triage") or "").strip().lower()
        tn = _norm(t)
        if not tn or len(tn) < 4:
            continue
        if _matches_whitelist(tn) or any(_kw_en_titulo(k, tn) for k in PALABRAS_CLAVE):
            continue
        conteo[t] = conteo.get(t, 0) + 1
        ejemplos.setdefault(t, [])
        if len(ejemplos[t]) < 3:
            ejemplos[t].append({"c": r["codigo"], "n": (r.get("nombre") or "")[:90]})
    orden = sorted(conteo.items(), key=lambda kv: -kv[1])[:25]
    return [{"t": t, "n": n, "ej": ejemplos[t]} for t, n in orden]


# ---------- Main ----------

def _fecha_orden(reg):
    fc = reg.get("fecha_cierre")
    return (fc is None, fc or "")


def main():
    modo = "buscar_todo (todo el país, sin keywords)" if BUSCAR_TODO else f"{len(PALABRAS_CLAVE)} keywords (todas las regiones)"
    print(f"Buscando Compra Ágil — modo: {modo}, estados={ESTADOS}")

    # 1) Recolección
    por_codigo, matches = {}, {}
    prev_meta, prev_abiertos = {}, {}
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, encoding="utf-8") as f:
                _prev = json.load(f)
            prev_meta = {k: v for k, v in _prev.items() if k != "items"}
            prev_abiertos = {it["codigo"]: it for it in (_prev.get("items") or []) if it.get("codigo")}
        except Exception:
            prev_meta, prev_abiertos = {}, {}
    barrido = {"modo": None, "inicio_cl": _ahora_chile().strftime("%Y-%m-%d %H:%M"), "desde": None,
               "paginas": 0, "motivo": ""}
    if BUSCAR_TODO:
        desde, motivo = decidir_barrido(prev_meta, not cargar_cache_triage())
        barrido.update(modo="incremental" if desde else "completo", motivo=motivo,
                       desde=desde.strftime("%Y-%m-%d %H:%M") if desde else None)
        t0 = time.time()
        items, barrido["paginas"] = buscar_todo(desde)
        barrido["minutos"] = round((time.time() - t0) / 60, 1)
        barrido["cortado"] = bool(_BARRIDO_ESTADO["cortado"])
        barrido["paginas_fallidas"] = list(_BARRIDO_ESTADO["paginas_fallidas"])
        if barrido["paginas_fallidas"]:
            print(f"  · barrido: páginas saltadas por falla de la API: {barrido['paginas_fallidas']}"
                  f"{' (CORTADO)' if barrido['cortado'] else ''}")
        print(f"  · barrido {barrido['modo']} ({motivo}{', desde ' + barrido['desde'] if desde else ''}): "
              f"{len(items)} resultados en {barrido['paginas']} páginas, {barrido['minutos']} min")
        for it in items:
            cod = it.get("codigo")
            if cod and cod not in por_codigo:
                por_codigo[cod] = it
        # ADEMÁS búsqueda dirigida por keyword: garantiza que lo relevante entre
        # aunque haya quedado fuera de la ventana de paginación del "todo"
        for kw in PALABRAS_CLAVE:
            extra = buscar_por_palabra(kw)
            n_nuevos = 0
            for it in extra:
                cod = it.get("codigo")
                if not cod:
                    continue
                matches.setdefault(cod, set()).add(kw)
                if cod not in por_codigo:
                    por_codigo[cod] = it
                    n_nuevos += 1
            if n_nuevos:
                print(f"  · '{kw}': +{n_nuevos} que el barrido general no alcanzó")
        # igualmente marcamos matches por keyword contra el nombre (sirve al score)
        for cod, it in por_codigo.items():
            nom = _norm(it.get("nombre") or "")
            for kw in PALABRAS_CLAVE:
                if _kw_en_titulo(kw, nom):
                    matches.setdefault(cod, set()).add(kw)
    else:
        for kw in PALABRAS_CLAVE:
            items = buscar_por_palabra(kw)
            print(f"  · '{kw}': {len(items)} resultados")
            for it in items:
                cod = it.get("codigo")
                if not cod: continue
                matches.setdefault(cod, set()).add(kw)
                if cod not in por_codigo:
                    por_codigo[cod] = it

    # Feed anterior: red de seguridad para no perder procesos aún abiertos
    prev_items = {}
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, encoding="utf-8") as f:
                for it in (json.load(f).get("items") or []):
                    if it.get("codigo"):
                        prev_items[it["codigo"]] = it
        except Exception:
            pass

    # 2) Normalizar + pre-filtro. IMPORTANTE: solo se descarta lo cerrado, la
    #    blacklist y lo sin match; el filtro duro (monto/cierre próximo) MARCA
    #    pero no elimina — así nada visible desaparece mientras siga abierto.
    registros, descartados = [], {"cerradas": 0, "blacklist": 0, "sin_match": 0}
    sin_match = []
    _DETALLE = ("productos", "descripcion", "direccion_entrega", "plazo_entrega_dias", "total_ofertas",
                "rut_organismo", "adjuntos", "organismo", "unidad_compra", "region", "region_nombre")
    for cod, it in por_codigo.items():
        reg = normalizar(it, matches.get(cod, set()))
        prev = prev_abiertos.get(cod)
        if prev and prev.get("productos"):
            # ya se bajó su ficha en una corrida anterior: se reutiliza (cada ficha son ~20 s)
            for k in _DETALLE:
                if prev.get(k) not in (None, [], ""):
                    reg[k] = prev[k]
        if cerrada_ya(reg):
            descartados["cerradas"] += 1
            continue
        pasa, razon = prefiltro_texto(reg, con_detalle=False)
        if not pasa:
            if razon.startswith("blacklist"):
                descartados["blacklist"] += 1
            else:
                sin_match.append(reg)
            continue
        razon_dura = filtro_duro(reg)
        reg["prefiltro"] = {"pasa": razon_dura is None, "razon": razon_dura or ""}
        reg["score_heuristico"] = score_heuristico(reg)
        registros.append(reg)

    # 2b) Triage IA: de lo que no calzó con ninguna palabra, rescatar lo fabricable.
    #     Solo lo que igual podría cotizarse (cierre y monto OK): no gastar en lo que no sirve.
    triables = [r for r in sin_match if filtro_duro(r) is None]
    rescatados, triage_stats = triage_ia(triables)
    for reg in rescatados:
        reg["prefiltro"] = {"pasa": True, "razon": ""}
        reg["score_heuristico"] = score_heuristico(reg)
        registros.append(reg)
    descartados["sin_match"] = len(sin_match) - len(rescatados)
    descartados["rescatados_triage"] = len(rescatados)
    if rescatados:
        print(f"Triage IA: rescatados {len(rescatados)} procesos que ninguna palabra atrapaba")

    # 2c) Barrido incremental: los candidatos de corridas anteriores que siguen
    #     abiertos y aún no tienen veredicto IA vuelven a competir por los cupos
    #     (con barrido completo lo hacían solos, porque se volvían a recolectar).
    if barrido.get("modo") == "incremental":
        ya = {r["codigo"] for r in registros}
        evaluados = cargar_cache_ia()
        reincorporados = 0
        for cod, it in prev_abiertos.items():
            if cod in ya or cod in evaluados or it.get("tipo") != "compra_agil" or cerrada_ya(it):
                continue
            razon_dura = filtro_duro(it)
            it["prefiltro"] = {"pasa": razon_dura is None, "razon": razon_dura or ""}
            it["score_heuristico"] = score_heuristico(it)
            it.pop("recuperado_corrida_anterior", None)
            registros.append(it)
            reincorporados += 1
        if reincorporados:
            print(f"  · {reincorporados} candidatos de corridas anteriores sin evaluar vuelven a competir")

    # 3) Priorizar por score y enriquecer SOLO los mejores que pasan todo
    registros.sort(key=lambda r: -(r.get("score_heuristico") or 0))
    a_enriquecer = ([r for r in registros if r["prefiltro"]["pasa"]][:MAX_DETALLE]) if FETCH_DETALLE else []
    print(f"Candidatos tras filtros: {len(registros)} (descartados: {descartados}). "
          f"Enriqueciendo top {len(a_enriquecer)} con ficha + adjuntos…")
    for i, reg in enumerate(a_enriquecer, 1):
        if not reg.get("productos"):      # ya enriquecido en una corrida anterior: no repetir la ficha
            enriquecer_con_detalle(reg)
        # re-chequeo con productos/categorías ya conocidos + monto real
        pasa, razon = prefiltro_texto(reg, con_detalle=True)
        if pasa:
            razon_dura = filtro_duro(reg)
            if razon_dura:
                pasa, razon = False, razon_dura
        reg["prefiltro"] = {"pasa": pasa, "razon": razon}
        reg["score_heuristico"] = score_heuristico(reg)  # ahora con total_ofertas
        if i % 10 == 0:
            print(f"  · {i}/{len(a_enriquecer)} procesados")

    # 3b) Licitaciones públicas (API oficial, cuota del ticket)
    lic_stats = {"activas": 0, "candidatas": 0, "incluidas": 0}
    lic_enriquecidas = []
    if INCLUIR_LICITACIONES and MP_TICKET:
        lst = listar_licitaciones()
        lic_stats["activas"] = len(lst)
        print(f"Licitaciones activas en el país: {len(lst)}")
        lics, vistos = [], set()
        for it in lst:
            cod = it.get("CodigoExterno")
            if not cod or cod in vistos:
                continue
            vistos.add(cod)
            reg = normalizar_licitacion(it)
            if cerrada_ya(reg):
                descartados["cerradas"] += 1
                continue
            nombre_n = _norm(reg["nombre"])
            if _hit_blacklist(nombre_n):
                descartados["blacklist"] += 1
                continue
            # el listado no permite búsqueda por keyword → exigir señal en el nombre
            reg["palabras_clave_match"] = sorted({kw for kw in PALABRAS_CLAVE
                                                  if _kw_en_texto(_norm(kw), nombre_n)})
            if not reg["palabras_clave_match"] and not _matches_whitelist(nombre_n):
                descartados["sin_match"] += 1
                continue
            razon_dura = filtro_duro(reg)
            reg["prefiltro"] = {"pasa": razon_dura is None, "razon": razon_dura or ""}
            reg["score_heuristico"] = score_heuristico(reg)
            lics.append(reg)
        lics.sort(key=lambda r: -(r.get("score_heuristico") or 0))
        candidatas = [r for r in lics if r["prefiltro"]["pasa"]][:MAX_DETALLE_LIC]  # cuota del ticket
        lic_stats["candidatas"] = len(candidatas)
        print(f"Licitaciones abiertas relevantes: {len(lics)} — detalle para top {len(candidatas)} (cuota MP_TICKET)…")
        for i, reg in enumerate(candidatas, 1):
            enriquecer_licitacion(reg)
            if filtro_duro(reg):  # re-chequeo: ahora se conoce el monto real
                reg["prefiltro"] = {"pasa": False, "razon": filtro_duro(reg)}
            else:
                pasa, razon = prefiltro_texto(reg, con_detalle=True)
                reg["prefiltro"] = {"pasa": pasa, "razon": razon}
            reg["score_heuristico"] = score_heuristico(reg)
            if i % 10 == 0:
                print(f"  · {i}/{len(candidatas)} procesadas")
        lic_enriquecidas = [r for r in candidatas if r["prefiltro"]["pasa"]]
        lic_stats["incluidas"] = len(lic_enriquecidas)
        registros.extend(lics)  # TODAS las abiertas relevantes van al feed, con o sin detalle
    elif INCLUIR_LICITACIONES:
        print("Sin MP_TICKET: se omiten licitaciones (solo Compra Ágil).")

    # 3c) Historial de precios adjudicados (referencia para cotizar −10%)
    hist_info = None
    if HISTORICO_ON:
        try:
            h = actualizar_historico()
            hist_info = {"items": len(h.get("items") or []), "dias_cubiertos": h.get("dias_cubiertos"),
                         "dias_pendientes": h.get("dias_pendientes")}
        except Exception as e:
            print(f"  · histórico de precios: {e}", file=sys.stderr)

    # 4) Evaluación IA con caché persistente (solo códigos nuevos que pasan todo)
    cache = cargar_cache_ia()
    ia_nuevos = ia_errores = 0
    pre_eval = set(cache.keys())
    if ANTHROPIC_KEY:
        candidatos_ia = sorted([r for r in a_enriquecer if r["prefiltro"]["pasa"]] + lic_enriquecidas,
                               key=lambda r: -(r.get("score_heuristico") or 0))
        cache, ia_nuevos, ia_errores = evaluar_ia(candidatos_ia, cache)
    else:
        print("Sin ANTHROPIC_API_KEY: la evaluación IA queda para la app (fallback).")

    # 4a. Sugerencias de keywords: solo de los rescatados que la evaluación IA
    #     completa (con la ficha) confirmó como viables — el triage solo ve títulos.
    sugerencias = sugerir_keywords([r for r in rescatados
                                    if (cache.get(r["codigo"]) or {}).get("v")])

    # 4b) Arrastre: procesos del feed anterior que siguen abiertos pero no
    #     aparecieron en esta corrida (hipo de la API, cambio de scores, etc.)
    codigos_nuevos = {r["codigo"] for r in registros}
    recuperados = 0
    for cod, it in prev_items.items():
        if cod in codigos_nuevos:
            continue
        if cerrada_ya(it):
            continue
        it["recuperado_corrida_anterior"] = True
        registros.append(it)
        recuperados += 1
    if recuperados:
        print(f"Arrastrados del feed anterior (aún abiertos): {recuperados}")

    # 5) Adjuntar evaluación al feed + recorte final
    for reg in registros:
        ev = cache.get(reg["codigo"])
        if ev:
            reg["ia"] = ev
    registros.sort(key=lambda r: (-(r.get("ia", {}).get("v") and 1 or 0),
                                  -(r.get("ia", {}).get("s") or 0),
                                  -(r.get("score_heuristico") or 0)))
    if len(registros) > MAX_ITEMS_FEED:
        registros = registros[:MAX_ITEMS_FEED]

    cache = guardar_cache_ia(cache, {r["codigo"] for r in registros})
    limpiar_adjuntos_viejos({r["codigo"] for r in registros})
    n_adj = sum(len(r.get("adjuntos") or []) for r in registros)
    n_viables = sum(1 for r in registros if r.get("ia", {}).get("v"))
    n_lic = sum(1 for r in registros if r.get("tipo") == "licitacion")
    salida = {
        "generado": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total": len(registros),
        "palabras_clave": PALABRAS_CLAVE,
        "buscar_todo": BUSCAR_TODO,
        "regiones": [],  # vacío = todo el país
        "estados": ESTADOS,
        "config": {"monto_min_clp": MONTO_MIN_CLP, "horas_min_cierre": HORAS_MIN_CIERRE,
                   "max_detalle": MAX_DETALLE, "max_eval_ia": MAX_EVAL_IA},
        "recolectados_total": len(por_codigo),
        "descartados": descartados,
        "recuperados_feed_anterior": recuperados,
        "triage_ia": dict(triage_stats, habilitado=TRIAGE_ON),
        "barrido": barrido,
        "sugerencias_keywords": sugerencias,
        "licitaciones": dict(lic_stats, habilitadas=INCLUIR_LICITACIONES, con_ticket=bool(MP_TICKET)),
        "historico_precios": hist_info,
        "eval_ia": {"evaluados_total": len(cache), "nuevos_esta_corrida": ia_nuevos,
                    "errores": ia_errores, "server_side": bool(ANTHROPIC_KEY)},
        "items": registros,
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(salida, f, ensure_ascii=False, indent=2)
    print(f"OK: {len(registros)} oportunidades ({n_lic} licitaciones, {n_adj} adjuntos, "
          f"{n_viables} viables IA, {ia_nuevos} evaluaciones nuevas) → {OUTPUT_FILE}")
    _res_adj = resumen_adjuntos()
    if _res_adj:
        print(_res_adj)

    # 6) Avisos (Telegram y/o correo, según secrets configurados)
    if (TG_TOKEN and TG_CHAT) or (MAIL_USER and MAIL_PASS):
        nuevos_codigos = set(cache.keys()) - pre_eval
        viables = [r for r in registros if r.get("ia", {}).get("v")]
        viables.sort(key=lambda r: -(r.get("ia", {}).get("s") or 0))

        def _link(r):
            url = r.get("ficha_publica") or ""
            nom = (r.get("nombre") or "")[:65]
            return f'<a href="{url}">{nom}</a>' if url else nom

        def _monto(r):
            m = r.get("monto_clp")
            try:
                return "$" + f"{int(float(m)):,}".replace(",", ".") if m else "s/m"
            except (TypeError, ValueError):
                return "s/m"

        def _dias(r):
            fc = _parse_fecha(r.get("fecha_cierre"))
            if not fc:
                return None
            return (fc - _ahora_chile()).total_seconds() / 86400

        hoy_txt = dt.date.today().strftime("%d-%m-%Y")
        lineas = [f"🦊 <b>Mercado Público — {hoy_txt}</b>",
                  f"Revisados {len(por_codigo):,} procesos + {lic_stats['activas']:,} licitaciones → "
                  f"{len(registros)} oportunidades, ⭐ {len(viables)} viables".replace(",", ".")]

        nuevas = [r for r in viables if r["codigo"] in nuevos_codigos]
        if nuevas:
            lineas.append("")
            lineas.append(f"🆕 <b>Nuevas viables hoy ({len(nuevas)}):</b>")
            for r in nuevas[:30]:
                s = r.get("ia", {}).get("s", 0)
                lineas.append(f"• [{s}] {_link(r)} — {_monto(r)}")

        urgentes = [r for r in viables if r["codigo"] not in nuevos_codigos
                    and (_dias(r) is not None and _dias(r) <= 2)]
        if urgentes:
            lineas.append("")
            lineas.append(f"⏰ <b>Viables que cierran en ≤48h ({len(urgentes)}):</b>")
            for r in urgentes[:20]:
                d = _dias(r)
                cuando = "HOY" if d < 1 else "mañana"
                lineas.append(f"• {_link(r)} — {_monto(r)} · cierra {cuando}")

        if not nuevas and not urgentes and viables:
            lineas.append("")
            lineas.append("<b>Top viables vigentes:</b>")
            for r in viables[:20]:
                s = r.get("ia", {}).get("s", 0)
                lineas.append(f"• [{s}] {_link(r)} — {_monto(r)} · cierra {str(r.get('fecha_cierre') or '')[:10]}")

        try:
            ayer = (dt.date.today() - dt.timedelta(days=1)).isoformat()
            res_items = [x for x in (cargar_resultados().get("items") or [])
                         if (x.get("fecha") or "") >= ayer]
            if res_items:
                lineas.append("")
                lineas.append("<b>Resultados de tus ofertas:</b>")
                for x in res_items[:15]:
                    if x["resultado"] == "ganada":
                        lineas.append(f"🎉 GANASTE: {x.get('nombre', '')[:60]}")
                    else:
                        lineas.append(f"❌ Perdiste: {x.get('nombre', '')[:60]} — ganó {x.get('ganador', '?')[:35]}")
        except Exception:
            pass

        if TG_TOKEN and TG_CHAT:
            txt_tg = "\n".join(lineas)
            if len(txt_tg) > 3900:  # límite de Telegram: 4096 caracteres
                txt_tg = txt_tg[:3900] + "\n…(lista completa en el correo y la app)"
            ok_tg = notificar_telegram(txt_tg)
            print(f"Telegram: {'enviado' if ok_tg else 'falló'}")
        if MAIL_USER and MAIL_PASS:
            asunto = f"🦊 Mercado Público: {len(nuevas)} nuevas, {len(viables)} viables · {hoy_txt}"
            ok_mail = notificar_correo(asunto, "<br>".join(lineas))
        print(f"Correo: {'enviado' if ok_mail else 'falló'}")


if __name__ == "__main__":
    main()

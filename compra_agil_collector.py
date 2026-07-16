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

import os, re, sys, json, time, shutil, unicodedata, datetime as dt
from urllib.parse import quote
import requests

# API pública del buscador (la misma del sitio buscador.mercadopublico.cl)
BUSCADOR_BASE = "https://api.buscador.mercadopublico.cl"
BUSCADOR_API_KEY = "e93089e4-437c-4723-b343-4fa20045e3bc"  # clave pública del frontend

# Servicio público de adjuntos (el mismo del buscador)
ADJ_BASE = "https://adjunto.mercadopublico.cl/adjunto-compra-agil/v1/adjuntos-compra-agil"
ADJ_USER_KEY = "41186b85826e80d1a0d445a6ce67d1a3"  # clave pública del frontend

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
MAX_PAGINAS_TODO = int(_CFG.get("max_paginas_todo", 400))  # tope en modo buscar_todo (todo el país)

ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
IA_MODELOS = ["claude-haiku-4-5", "claude-haiku-4-5-20251001", "claude-sonnet-4-5"]
IA_LOTE = 25            # licitaciones por llamada (más grande = menos overhead de prompt)
IA_CACHE_DIAS = 90      # conservar evaluaciones de códigos ya ausentes por N días

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
WHITELIST_EXTRA = ["impresion 3d","letrero","senaletic","rotulo","prototipo","plastico","modelado",
  "escultura","trofeo","medalla","placa conmemorativa","gabinete","carcasa","molde","maqueta",
  "filamento","resina","pla ","fantoma","modelo anatomico","repuesto","pieza"]

_BLACKLIST_N = [_norm(b) for b in BLACKLIST]
_WHITELIST_N = [_norm(w) for w in (WHITELIST_EXTRA + PALABRAS_CLAVE)]


def _hit_blacklist(texto_norm):
    for b in _BLACKLIST_N:
        if b in texto_norm:
            return b
    return None


def _kw_en_texto(kw_norm, texto_norm):
    """Palabras cortas (<5 chars) exigen borde de palabra: 'pin' no matchea 'pintura'."""
    kw_norm = kw_norm.strip()
    if not kw_norm:
        return False
    if len(kw_norm) < 5:
        return re.search(r"(?<![a-z0-9])" + re.escape(kw_norm) + r"(?![a-z0-9])", texto_norm) is not None
    return kw_norm in texto_norm


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
    texto = _norm((reg.get("nombre") or "") + " " + " ".join(reg.get("palabras_clave_match") or []))
    # 1) coincidencias con keywords/whitelist (hasta 40)
    n_match = len(set(_matches_whitelist(texto)) | set(reg.get("palabras_clave_match") or []))
    s += min(40, n_match * 14)
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
        dias = (fc - dt.datetime.now()).total_seconds() / 86400
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
            resp = requests.get(url, headers={"x-api-key": BUSCADOR_API_KEY, "Accept": "application/json"},
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


def buscar_por_palabra(keyword):
    """Busca procesos por palabra clave (todas las regiones del país)."""
    estado_id = ESTADO_PARAM.get((ESTADOS[0] if ESTADOS else "publicada"), 2)
    return _paginar({"keywords": keyword, "status": estado_id, "order_by": "recent"}, MAX_PAGINAS)


def buscar_todo():
    """Trae todo lo publicado en el país, sin keywords."""
    estado_id = ESTADO_PARAM.get((ESTADOS[0] if ESTADOS else "publicada"), 2)
    items = _paginar({"status": estado_id, "order_by": "recent"}, MAX_PAGINAS_TODO)
    if not items:  # algunas variantes exigen el parámetro aunque sea vacío
        items = _paginar({"keywords": "", "status": estado_id, "order_by": "recent"}, MAX_PAGINAS_TODO)
    return items


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
    for m in re.finditer(r"lnkViewProvider[^>]*>\s*([^<]+?)\s*<", html):
        prov = re.sub(r"\s+", " ", m.group(1)).strip()
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


def capturar_historico_ca(hist, vistos):
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
            if filas_acta:
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
        nuevos += capturar_historico_ca(hist, vistos)
    except Exception as e:
        print(f"  · histórico CA: {e}", file=sys.stderr)
    # poda: fuera de la ventana de un año
    limite = (hoy - dt.timedelta(days=HISTORICO_DIAS)).isoformat()
    hist["items"] = [x for x in hist["items"] if (x.get("f") or "") >= limite]
    hist["procesados"] = sorted(d for d in procesados if d >= limite)
    hist["actualizado"] = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
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


def listar_adjuntos_publico(codigo):
    try:
        r = requests.get(f"{ADJ_BASE}/listar/{quote(codigo)}",
                         headers={"user_key": ADJ_USER_KEY}, timeout=30)
        if r.status_code != 200:
            return []
        data = r.json()
        if data.get("success") != "OK":
            return []
        return (data.get("payload") or {}).get("files") or []
    except Exception as e:
        print(f"  · listar adjuntos {codigo}: {e}", file=sys.stderr)
        return []


def descargar_adjunto(guid, destino):
    try:
        with requests.get(f"{ADJ_BASE}/descargar/{guid}",
                          headers={"user_key": ADJ_USER_KEY},
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
    return fc is not None and fc <= dt.datetime.now()


def filtro_duro(reg):
    """Filtros baratos que no requieren ficha ni IA. Devuelve razón o None."""
    fc = _parse_fecha(reg.get("fecha_cierre"))
    if fc is not None:
        horas = (fc - dt.datetime.now()).total_seconds() / 3600
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
        if not reg.get("palabras_clave_match") and not _matches_whitelist(texto):
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


PROMPT_EVAL = ("Green Wolf SPA (Chile) fabrica con impresión 3D FDM y resina: prototipos, "
    "piezas plásticas funcionales, modelos anatómicos, señalética y letreros 3D, repuestos "
    "plásticos, maquetas. Evalúa cada oportunidad de Mercado Público (t=CA: Compra Ágil, "
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


# ---------- Main ----------

def _fecha_orden(reg):
    fc = reg.get("fecha_cierre")
    return (fc is None, fc or "")


def main():
    modo = "buscar_todo (todo el país, sin keywords)" if BUSCAR_TODO else f"{len(PALABRAS_CLAVE)} keywords (todas las regiones)"
    print(f"Buscando Compra Ágil — modo: {modo}, estados={ESTADOS}")

    # 1) Recolección
    por_codigo, matches = {}, {}
    if BUSCAR_TODO:
        items = buscar_todo()
        print(f"  · buscar_todo: {len(items)} resultados")
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
                if _kw_en_texto(_norm(kw), nom):
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
    for cod, it in por_codigo.items():
        reg = normalizar(it, matches.get(cod, set()))
        if cerrada_ya(reg):
            descartados["cerradas"] += 1
            continue
        pasa, razon = prefiltro_texto(reg, con_detalle=False)
        if not pasa:
            descartados["blacklist" if razon.startswith("blacklist") else "sin_match"] += 1
            continue
        razon_dura = filtro_duro(reg)
        reg["prefiltro"] = {"pasa": razon_dura is None, "razon": razon_dura or ""}
        reg["score_heuristico"] = score_heuristico(reg)
        registros.append(reg)

    # 3) Priorizar por score y enriquecer SOLO los mejores que pasan todo
    registros.sort(key=lambda r: -(r.get("score_heuristico") or 0))
    a_enriquecer = ([r for r in registros if r["prefiltro"]["pasa"]][:MAX_DETALLE]) if FETCH_DETALLE else []
    print(f"Candidatos tras filtros: {len(registros)} (descartados: {descartados}). "
          f"Enriqueciendo top {len(a_enriquecer)} con ficha + adjuntos…")
    for i, reg in enumerate(a_enriquecer, 1):
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
    if ANTHROPIC_KEY:
        candidatos_ia = sorted([r for r in a_enriquecer if r["prefiltro"]["pasa"]] + lic_enriquecidas,
                               key=lambda r: -(r.get("score_heuristico") or 0))
        cache, ia_nuevos, ia_errores = evaluar_ia(candidatos_ia, cache)
    else:
        print("Sin ANTHROPIC_API_KEY: la evaluación IA queda para la app (fallback).")

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
        "descartados": descartados,
        "recuperados_feed_anterior": recuperados,
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


if __name__ == "__main__":
    main()

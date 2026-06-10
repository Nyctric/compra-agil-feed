#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Recolector Compra Ágil — Green Wolf SPA.

Usa la API pública del buscador de Mercado Público
(api.buscador.mercadopublico.cl), la misma que usa el sitio web oficial.
Ventaja: no requiere ticket (MP_TICKET) ni tiene cuota diaria.

Además descarga los archivos adjuntos de cada proceso (servicio público de
adjuntos) y los guarda en el repo bajo adjuntos/{codigo}/, de modo que la app
pueda enlazarlos directamente vía raw.githubusercontent.com.

Nota: son APIs del frontend oficial (no documentadas). Si algún día rotan las
claves públicas (BUSCADOR_API_KEY / ADJ_USER_KEY), se obtienen de nuevo
inspeccionando el JS de buscador.mercadopublico.cl.
"""

import os, re, sys, json, time, shutil, datetime as dt
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

_kw_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "keywords.json")
if os.path.exists(_kw_file):
    with open(_kw_file, encoding="utf-8") as _f:
        PALABRAS_CLAVE = json.load(_f).get("palabras_clave", [])
else:
    PALABRAS_CLAVE = ["impresion 3d", "filamento", "prototipo", "plastico", "fabricacion"]

REGIONES = [13]
ESTADOS = ["publicada"]
FETCH_DETALLE = True
OUTPUT_FILE = os.environ.get("OUTPUT_FILE", "oportunidades.json")
PAUSA_SEG = 0.35
MAX_REINTENTOS = 3
MAX_PAGINAS = 20  # tope de seguridad por palabra clave

ESTADO_GLOSA = {2: "Publicada", 3: "Cerrada", 5: "Cancelada", 6: "Desierta"}
ESTADO_CODIGO = {2: "publicada", 3: "cerrada", 5: "cancelada", 6: "desierta"}
ESTADO_PARAM = {"publicada": 2, "cerrada": 3, "cancelada": 5, "desierta": 6}

REGION_NOMBRES = {
    1:"Tarapacá",2:"Antofagasta",3:"Atacama",4:"Coquimbo",5:"Valparaíso",
    6:"O'Higgins",7:"Maule",8:"Biobío",9:"Araucanía",10:"Los Lagos",
    11:"Aysén",12:"Magallanes y Antártica",13:"Metropolitana",14:"Los Ríos",
    15:"Arica y Parinacota",16:"Ñuble",
}


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


def buscar_por_palabra(keyword):
    """Busca procesos por palabra clave usando el buscador público."""
    items, pagina = [], 1
    estado_id = ESTADO_PARAM.get((ESTADOS[0] if ESTADOS else "publicada"), 2)
    while pagina <= MAX_PAGINAS:
        params = {"keywords": keyword, "status": estado_id, "order_by": "recent", "page_number": pagina}
        if REGIONES and len(REGIONES) == 1:
            params["region"] = REGIONES[0]
        payload = _get_buscador(params)
        time.sleep(PAUSA_SEG)
        if not payload:
            break
        items.extend(payload.get("resultados") or [])
        if pagina >= (payload.get("pageCount") or 1):
            break
        pagina += 1
    return items


def traer_ficha(codigo):
    payload = _get_buscador({"action": "ficha", "code": codigo})
    time.sleep(PAUSA_SEG)
    return payload


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


# ---------- Normalización (mismo esquema de feed que antes) ----------

def normalizar(item, palabras_match):
    codigo = item.get("codigo") or ""
    id_estado = item.get("id_estado")
    region = REGIONES[0] if len(REGIONES) == 1 else None
    return {
        "codigo": codigo,
        "nombre": (item.get("nombre") or "").strip(),
        "estado": ESTADO_CODIGO.get(id_estado, str(item.get("estado") or "").lower()),
        "estado_glosa": ESTADO_GLOSA.get(id_estado, item.get("estado")),
        "organismo": item.get("organismo"),
        "rut_organismo": None,          # se completa con la ficha
        "unidad_compra": item.get("unidad"),
        "region": region,
        "region_nombre": REGION_NOMBRES.get(region),
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
        registro["productos"] = [
            {"nombre": p.get("nombre"), "descripcion": p.get("descripcion"),
             "cantidad": p.get("cantidad"), "unidad": p.get("unidad_medida")}
            for p in (det.get("productos_solicitados") or [])
        ]
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
    # Adjuntos: servicio público (GUIDs + descarga real al repo)
    procesar_adjuntos(registro)


def _fecha_orden(reg):
    fc = reg.get("fecha_cierre")
    return (fc is None, fc or "")


def main():
    print(f"Buscando Compra Ágil (buscador público) — {len(PALABRAS_CLAVE)} palabras, regiones={REGIONES}, estados={ESTADOS}")
    por_codigo, matches = {}, {}
    for kw in PALABRAS_CLAVE:
        items = buscar_por_palabra(kw)
        print(f"  · '{kw}': {len(items)} resultados")
        for it in items:
            cod = it.get("codigo")
            if not cod: continue
            matches.setdefault(cod, set()).add(kw)
            if cod not in por_codigo:
                por_codigo[cod] = it
    registros = []
    total = len(por_codigo)
    print(f"Procesando {total} procesos únicos (ficha una a una)…")
    for i, (cod, it) in enumerate(por_codigo.items(), 1):
        reg = normalizar(it, matches.get(cod, set()))
        if FETCH_DETALLE:
            enriquecer_con_detalle(reg)
        registros.append(reg)
        if i % 10 == 0:
            print(f"  · {i}/{total} procesados")
    registros.sort(key=_fecha_orden)
    limpiar_adjuntos_viejos({r["codigo"] for r in registros})
    n_adj = sum(len(r.get("adjuntos") or []) for r in registros)
    salida = {
        "generado": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total": len(registros),
        "palabras_clave": PALABRAS_CLAVE,
        "regiones": REGIONES,
        "estados": ESTADOS,
        "items": registros,
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(salida, f, ensure_ascii=False, indent=2)
    print(f"OK: {len(registros)} oportunidades ({n_adj} adjuntos) → {OUTPUT_FILE}")


if __name__ == "__main__":
    main()

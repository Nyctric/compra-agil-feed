#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Recolector Compra Ágil — Green Wolf SPA."""

import os, sys, json, time, datetime as dt
from urllib.parse import quote
import requests

BASE_URL = "https://api2.mercadopublico.cl"
TICKET = os.environ.get("MP_TICKET", "").strip()

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

REGION_NOMBRES = {
    1:"Tarapacá",2:"Antofagasta",3:"Atacama",4:"Coquimbo",5:"Valparaíso",
    6:"O'Higgins",7:"Maule",8:"Biobío",9:"Araucanía",10:"Los Lagos",
    11:"Aysén",12:"Magallanes y Antártica",13:"Metropolitana",14:"Los Ríos",
    15:"Arica y Parinacota",16:"Ñuble",
}

class CuotaAgotada(Exception):
    pass

def _headers():
    return {"ticket": TICKET, "Accept": "application/json"}

def _get(path, params=None):
    url = f"{BASE_URL}{path}"
    intento = 0
    while True:
        try:
            resp = requests.get(url, headers=_headers(), params=params, timeout=60)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            intento += 1
            if intento > MAX_REINTENTOS:
                print(f"  · Error de red en {path}: {e}", file=sys.stderr)
                return None
            print(f"  · Error de red, reintento {intento}...", file=sys.stderr)
            time.sleep(5 * intento)
            continue

        if resp.status_code == 429:
            raise CuotaAgotada(f"Cuota diaria agotada (429). Retry-After={resp.headers.get('Retry-After')}.")
        if resp.status_code in (400, 404):
            print(f"  · {resp.status_code} para {path} — saltando", file=sys.stderr)
            return None
        if resp.status_code in (401, 403):
            raise SystemExit(f"ERROR {resp.status_code}: ticket inválido.")
        if resp.status_code >= 500:
            intento += 1
            if intento > MAX_REINTENTOS:
                resp.raise_for_status()
            time.sleep(2 ** intento)
            continue
        resp.raise_for_status()
        data = resp.json()
        if data.get("success") != "OK":
            return None
        return data.get("payload")

def buscar_por_palabra(keyword):
    items, pagina = [], 1
    region_param = ",".join(str(r) for r in REGIONES) if REGIONES else None
    estado_param = ",".join(ESTADOS) if ESTADOS else None
    while True:
        params = {"q": keyword, "tamano_pagina": 50, "numero_pagina": pagina, "ordenar_por": "FechaPublicacion"}
        if region_param: params["region"] = region_param
        if estado_param: params["estado"] = estado_param
        payload = _get("/v2/compra-agil", params=params)
        time.sleep(PAUSA_SEG)
        if not payload:
            break
        items.extend(payload.get("items", []))
        pag = payload.get("paginacion", {}) or {}
        if pagina >= (pag.get("total_paginas", 1) or 1):
            break
        pagina += 1
    return items

def traer_detalle(codigo):
    payload = _get(f"/v2/compra-agil/{quote(codigo)}")
    time.sleep(PAUSA_SEG)
    return payload

def normalizar(item, palabras_match):
    estado = item.get("estado", {}) or {}
    fechas = item.get("fechas", {}) or {}
    montos = item.get("montos", {}) or {}
    inst = item.get("institucion", {}) or {}
    region = inst.get("region")
    return {
        "codigo": item.get("codigo"),
        "nombre": item.get("nombre"),
        "estado": estado.get("codigo"),
        "estado_glosa": estado.get("glosa"),
        "organismo": inst.get("organismo_comprador"),
        "rut_organismo": inst.get("rut"),
        "unidad_compra": inst.get("unidad_compra"),
        "region": region,
        "region_nombre": REGION_NOMBRES.get(region) or inst.get("nombre_region"),
        "monto_clp": montos.get("monto_disponible_clp") or montos.get("monto_disponible"),
        "moneda": montos.get("moneda") or "CLP",
        "fecha_publicacion": fechas.get("fecha_publicacion"),
        "fecha_cierre": fechas.get("fecha_cierre"),
        "fecha_ultimo_cambio": fechas.get("fecha_ultimo_cambio"),
        "palabras_clave_match": sorted(palabras_match),
        "total_ofertas": (item.get("resumen", {}) or {}).get("total_ofertas_recibidas"),
        "productos": [], "adjuntos": [], "descripcion": None,
        "direccion_entrega": None, "plazo_entrega_dias": None,
        "url_detalle_api": (item.get("links", {}) or {}).get("detalle") or f"/v2/compra-agil/{item.get('codigo')}",
    }

def enriquecer_con_detalle(registro):
    det = traer_detalle(registro["codigo"])
    if not det:
        return
    registro["productos"] = [{"nombre": p.get("nombre"), "descripcion": p.get("descripcion"), "cantidad": p.get("cantidad"), "unidad": p.get("unidad_medida")} for p in (det.get("productos_solicitados") or [])]
    if det.get("descripcion"):
        registro["descripcion"] = det["descripcion"]
    entrega = det.get("entrega", {}) or {}
    registro["direccion_entrega"] = entrega.get("direccion_entrega")
    registro["plazo_entrega_dias"] = entrega.get("plazo_entrega_dias")
    adjuntos = []
    for campo in ("documentos", "adjuntos", "archivos", "anexos", "files", "attachments"):
        for a in (det.get(campo) or []):
            if isinstance(a, dict):
                doc_id = a.get("id") or ""
                nombre = a.get("nombre") or a.get("name") or "Archivo"
                url = a.get("url") or a.get("link") or ""
                if not url and doc_id:
                    url = f"https://compra-agil.mercadopublico.cl/documentos/{doc_id}"
                adjuntos.append({"id": doc_id, "nombre": nombre, "url": url, "tipo": a.get("tipo") or a.get("type") or ""})
    registro["adjuntos"] = adjuntos
    if not hasattr(enriquecer_con_detalle, "_logged"):
        enriquecer_con_detalle._logged = True
        print(f"  [DEBUG] Campos en detalle: {sorted(det.keys())}", file=sys.stderr)

def _fecha_orden(reg):
    fc = reg.get("fecha_cierre")
    return (fc is None, fc or "")

def main():
    if not TICKET:
        raise SystemExit("ERROR: falta MP_TICKET.")
    print(f"Buscando Compra Ágil — {len(PALABRAS_CLAVE)} palabras, regiones={REGIONES}, estados={ESTADOS}")
    por_codigo, matches = {}, {}
    for kw in PALABRAS_CLAVE:
        try:
            items = buscar_por_palabra(kw)
        except CuotaAgotada as e:
            print(f"DETENIDO en búsqueda: {e}", file=sys.stderr)
            break
        print(f"  · '{kw}': {len(items)} resultados")
        for it in items:
            cod = it.get("codigo")
            if not cod: continue
            matches.setdefault(cod, set()).add(kw)
            if cod not in por_codigo:
                por_codigo[cod] = it
    registros = []
    total = len(por_codigo)
    print(f"Procesando {total} procesos únicos (detalle uno a uno)…")
    for i, (cod, it) in enumerate(por_codigo.items(), 1):
        reg = normalizar(it, matches.get(cod, set()))
        if FETCH_DETALLE:
            try:
                enriquecer_con_detalle(reg)
            except CuotaAgotada as e:
                print(f"DETENIDO en detalle #{i}/{total}: {e}", file=sys.stderr)
                registros.append(reg)
                break
        registros.append(reg)
        if i % 10 == 0:
            print(f"  · {i}/{total} procesados")
    registros.sort(key=_fecha_orden)
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
    print(f"OK: {len(registros)} oportunidades → {OUTPUT_FILE}")

if __name__ == "__main__":
    main()

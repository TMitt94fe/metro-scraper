#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Metro News Scraper – Enriched v2
- IRJ, Railway Gazette (Metro), UrbanRail.net
- Holt Listenseiten + (wo möglich) Artikelseiten
- Füllt Stadt, Land, Projekttyp, Status über mehrsprachige Heuristiken
- Robust gegen leere Felder, dedupliziert auf (Link, Titel)

Benötigt:
  pip install -U requests beautifulsoup4 pandas python-dateutil lxml
"""

from __future__ import annotations
import os, re, time
from datetime import datetime
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
import pandas as pd

# ----------------------- Konfiguration -----------------------

OUTPUT_CSV = "metro_news.csv"
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")

SOURCES = [
    # Achtung: IRJ/RailwayGazette ändern gelegentlich Struktur; wir parsen defensiv.
    {
        "name": "IRJ Metro",
        "base": "https://www.railjournal.com",
        "url":  "https://www.railjournal.com/tag/metro/",
        "list_selector": "article",
        "title_selector": "h2 a, h3 a, .entry-title a",
        "link_selector":  "h2 a, h3 a, .entry-title a",
        "date_selector":  "time, .jeg_meta_date, .post-date",
    },
    {
        "name": "Railway Gazette Metro",
        "base": "https://www.railwaygazette.com",
        "url":  "https://www.railwaygazette.com/metro",
        "list_selector": "article, .listingResult, .article",
        "title_selector": "h3 a, h2 a, .headline a, a.link",
        "link_selector":  "h3 a, h2 a, .headline a, a.link",
        "date_selector":  "time, .date, time[datetime]",
    },
    {
        "name": "UrbanRail News",
        "base": "https://www.urbanrail.net",
        "url":  "https://www.urbanrail.net/news.htm",
        "list_selector": "p, li",
        "title_selector": "a",
        "link_selector":  "a",
        "date_selector":  None,  # UrbanRail listet News meist ohne Zeitstempel
    },
]

# Projekttypen (de/en); grobe Heuristiken
TYPE_MAP = {
    "metro": ["metro", "u-bahn", "subway", "underground", "ube", "u‑bahn"],
    "lrt":   ["light rail", "lrv", "tram", "tramway", "straßenbahn", "streetcar"],
    "s-bahn":["s-bahn", "sbahn", "commuter rail", "régional express"],
    "monorail": ["monorail"],
    "brt":   ["brt", "bus rapid transit"],
}
# Infrastruktur vs. Flotte
TYPE_INFRA = [
    "extension","extend","verlängerung","neubaustrecke","tunnel","tunneling","station",
    "bahnhof","signalling","cbtc","electrification","upgrade","depot","viaduct",
    "civil works","construction","bau","inbetriebnahme","commissioning","opening","eröffnung",
    "interchange","platform","route","alignment","double track"
]
TYPE_FLEET = [
    "fleet","rolling stock","fahrzeuge","fahrzeug","zug","züge","train","trains",
    "wagen","cars","carriages","emu","dmu","multiple unit","bogie","bogies","order",
    "bestellung","delivery","lieferung","supplier","manufacturer","trainset"
]

# Status (de/en)
STATUS_KEYS = {
    "Planung": ["plan","rfp","tender","procure","studie","study","design","feasibility","proposed","approval","genehmigung"],
    "Bau": ["build","construction","works start","groundbreaking","civil works","installation","im bau","baubeginn"],
    "Eröffnung": ["open","opening","commission","enter service","in operation","service starts","eröffnung","betrieb"],
    "Verzögerung": ["delay","postpone","halted","suspended","overrun","verzögerung","verschoben"],
    "Finanzierung": ["funding","finance","loan","grant","budget","ppp","finanzierung","förderung"],
}

# Stadt -> Land (Auszug Europa + ein paar Global), gerne später erweitern
CITY_TO_COUNTRY = {
    "Berlin":"Germany","Hamburg":"Germany","München":"Germany","Munich":"Germany","Frankfurt":"Germany",
    "Köln":"Germany","Cologne":"Germany","Stuttgart":"Germany","Leipzig":"Germany","Nürnberg":"Germany",
    "Wien":"Austria","Vienna":"Austria","Graz":"Austria","Linz":"Austria",
    "Zürich":"Switzerland","Zurich":"Switzerland","Genf":"Switzerland","Geneva":"Switzerland","Basel":"Switzerland",
    "Paris":"France","Lyon":"France","Marseille":"France","Lille":"France","Rennes":"France","Toulouse":"France",
    "Madrid":"Spain","Barcelona":"Spain","Valencia":"Spain","Sevilla":"Spain","Seville":"Spain","Bilbao":"Spain",
    "London":"United Kingdom","Birmingham":"United Kingdom","Manchester":"United Kingdom","Glasgow":"United Kingdom","Edinburgh":"United Kingdom",
    "Roma":"Italy","Rome":"Italy","Milano":"Italy","Milan":"Italy","Napoli":"Italy","Naples":"Italy","Torino":"Italy","Turin":"Italy",
    "Warszawa":"Poland","Warsaw":"Poland","Kraków":"Poland","Krakow":"Poland","Gdańsk":"Poland","Gdansk":"Poland",
    "Praha":"Czech Republic","Prague":"Czech Republic","Brno":"Czech Republic","Ostrava":"Czech Republic",
    "Budapest":"Hungary","Debrecen":"Hungary","Szeged":"Hungary",
    "Lisboa":"Portugal","Lisbon":"Portugal","Porto":"Portugal",
    "Oslo":"Norway","Stockholm":"Sweden","Göteborg":"Sweden","Gothenburg":"Sweden","Malmö":"Sweden",
    "København":"Denmark","Copenhagen":"Denmark","Aarhus":"Denmark",
    "Helsinki":"Finland","Tampere":"Finland",
    "Athens":"Greece","Thessaloniki":"Greece",
    "Istanbul":"Turkey","Ankara":"Turkey","İzmir":"Turkey","Izmir":"Turkey",
    "Doha":"Qatar","Dubai":"UAE","Abu Dhabi":"UAE","Riyadh":"Saudi Arabia","Jeddah":"Saudi Arabia",
    "Cairo":"Egypt","Casablanca":"Morocco","Algiers":"Algeria",
    "New York":"USA","Chicago":"USA","Boston":"USA","Los Angeles":"USA","Washington":"USA","Miami":"USA","San Francisco":"USA",
    "Toronto":"Canada","Montréal":"Canada","Montreal":"Canada","Vancouver":"Canada",
    "Mexico City":"Mexico","Guadalajara":"Mexico","Monterrey":"Mexico",
    "Santiago":"Chile","Buenos Aires":"Argentina","Lima":"Peru",
    "São Paulo":"Brazil","Sao Paulo":"Brazil","Rio de Janeiro":"Brazil",
    "Jakarta":"Indonesia","Manila":"Philippines","Bangkok":"Thailand","Singapore":"Singapore","Hong Kong":"China",
    "Shanghai":"China","Beijing":"China","Shenzhen":"China","Guangzhou":"China","Wuhan":"China","Chengdu":"China",
    "Tokyo":"Japan","Osaka":"Japan","Nagoya":"Japan","Yokohama":"Japan","Fukuoka":"Japan",
    "Seoul":"South Korea","Busan":"South Korea","Incheon":"South Korea",
    "Taipei":"Taiwan","Kaohsiung":"Taiwan",
    "Sydney":"Australia","Melbourne":"Australia","Perth":"Australia","Brisbane":"Australia",
    "Auckland":"New Zealand","Wellington":"New Zealand",
}

# UrbanRail-URL-Hinweise (robert-schwandl.de enthält viele Länderkürzel im Pfad)
URL_CITY_HINTS = {
    "/berlin/":("Berlin","Germany"),
    "/muenchen":("München","Germany"),
    "/pra":("Praha","Czech Republic"),
    "/warschau":("Warszawa","Poland"),
    "/london":("London","United Kingdom"),
    "/par":("Paris","France"),
    "/mad":("Madrid","Spain"),
    "/bar":("Barcelona","Spain"),
}

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"})

# ----------------------- Hilfsfunktionen -----------------------

def clean(text: str | None) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()

def http_get(url: str) -> str:
    r = SESSION.get(url, timeout=25)
    r.raise_for_status()
    return r.text

def pick(sel_list: str, scope: BeautifulSoup):
    for sel in [s.strip() for s in sel_list.split(",") if s.strip()]:
        el = scope.select_one(sel)
        if el:
            return el
    return None

def find_city_country(text: str, url: str = "") -> tuple[str,str]:
    t = text or ""
    # 1) Explizit aus Text
    for city, country in CITY_TO_COUNTRY.items():
        if re.search(rf"\b{re.escape(city)}\b", t, flags=re.IGNORECASE):
            return city, country
    # 2) URL-Hinweise (UrbanRail / robert-schwandl.de)
    for key,(c,co) in URL_CITY_HINTS.items():
        if key in url.lower():
            return c, co
    # 3) Fallback leer
    return "", ""

def detect_modes(text: str) -> list[str]:
    t = (text or "").lower()
    modes = []
    for m, keys in TYPE_MAP.items():
        if any(k in t for k in keys):
            modes.append(m)
    return modes

def detect_type_bucket(text: str) -> str:
    t = (text or "").lower()
    has_infra = any(k in t for k in TYPE_INFRA)
    has_fleet = any(k in t for k in TYPE_FLEET)
    if has_infra and has_fleet: return "gemischt"
    if has_infra: return "infrastruktur"
    if has_fleet: return "flotte"
    return ""

def detect_status(text: str) -> str:
    t = (text or "").lower()
    for status, keys in STATUS_KEYS.items():
        if any(k in t for k in keys):
            return status
    return ""

def parse_article_page(url: str) -> dict:
    """Versucht Publikationsdatum + Details aus Artikelseite zu lesen."""
    try:
        html = http_get(url)
    except Exception:
        return {}
    soup = BeautifulSoup(html, "lxml")
    # generische Suche
    time_el = soup.select_one("time[datetime]") or soup.select_one("time")
    date_txt = clean(time_el.get("datetime") or time_el.get_text()) if time_el else ""
    # kurzer Textauszug
    p = soup.select_one("article p") or soup.select_one("p")
    details = clean(p.get_text()) if p else ""
    return {"pubdate": date_txt, "details": details}

# ----------------------- Scraper -----------------------

def parse_list(cfg: dict) -> list[dict]:
    items = []
    try:
        html = http_get(cfg["url"])
    except Exception as e:
        print(f"[WARN] Quelle fehlgeschlagen: {cfg['name']} – {e}")
        return items

    soup = BeautifulSoup(html, "lxml")
    for block in soup.select(cfg["list_selector"]):
        title_el = pick(cfg["title_selector"], block)
        link_el  = pick(cfg["link_selector"], block)
        date_el  = pick(cfg["date_selector"],  block) if cfg.get("date_selector") else None

        title = clean(title_el.get_text()) if title_el else ""
        link  = urljoin(cfg["base"], link_el.get("href")) if link_el and link_el.has_attr("href") else ""
        date  = clean(date_el.get("datetime") or date_el.get_text()) if date_el else ""

        if not title and not link:
            continue

        # Artikelseite optional nachziehen (UrbanRail oft nicht nötig)
        details, pubdate = "", ""
        if link and (cfg["name"] != "UrbanRail News"):
            meta = parse_article_page(link)
            details = meta.get("details","")
            pubdate = meta.get("pubdate","")

        # Anreicherungen
        text_for_nlp = " ".join([title, details])
        city, country = find_city_country(text_for_nlp, link)
        bucket = detect_type_bucket(text_for_nlp)
        status = detect_status(text_for_nlp)
        modes  = detect_modes(text_for_nlp)
        # Projekttyp feiner, wenn Modus erkennbar
        proj_type = bucket
        if not proj_type and modes:
            # z.B. "metro" zählt eher als Infrastruktur-kategorie
            proj_type = ",".join(modes)

        items.append({
            "Datum": pubdate or date or datetime.utcnow().date().isoformat(),
            "Quelle": cfg["name"],
            "Titel": title,
            "Link": link or cfg["url"],
            "Land": country,
            "Stadt": city,
            "Projekttyp": proj_type,
            "Status": status,
            "Details": details,
        })
    return items

def write_csv(rows: list[dict], path: str):
    cols = ["Datum","Quelle","Titel","Link","Land","Stadt","Projekttyp","Status","Details"]
    df_new = pd.DataFrame(rows, columns=cols)
    if os.path.exists(path):
        try:
            df_old = pd.read_csv(path)
            df = pd.concat([df_old, df_new], ignore_index=True)
        except Exception:
            df = df_new
    else:
        df = df_new
    # Dedupe
    df = df.drop_duplicates(subset=["Link","Titel"], keep="first")
    # Sort: neu oben
    if "Datum" in df.columns:
        df["Datum_sort"] = pd.to_datetime(df["Datum"], errors="coerce")
        df = df.sort_values("Datum_sort", ascending=False).drop(columns=["Datum_sort"])
    df.to_csv(path, index=False, encoding="utf-8-sig")

def main():
    all_rows = []
    for cfg in SOURCES:
        batch = parse_list(cfg)
        all_rows.extend(batch)
        time.sleep(1)  # nicht zu aggressiv
    if not all_rows:
        print("Keine Artikel gefunden – prüfe Quellen.")
        return
    write_csv(all_rows, OUTPUT_CSV)
    print(f"{len(all_rows)} Artikel verarbeitet. Datei aktualisiert: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()

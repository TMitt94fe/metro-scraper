
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Metro News Scraper – Enriched (Cities, Countries, Project Type, Status)
Free, no API keys. Sources: IRJ (Metro), Railway Gazette (Metro), UrbanRail.net (News)

Outputs/updates metro_news.csv with columns:
Datum | Quelle | Titel | Link | Land | Stadt | Projekttyp | Status | Details

Install once:
  pip install -U requests beautifulsoup4 pandas python-dateutil lxml

Run:
  python metro_news_scraper_enriched.py
"""
import os
import re
import sys
import time
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import pandas as pd

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"
OUTPUT_CSV = "metro_news.csv"

SOURCES = [
    # IRJ Metro tag page (articles list)
    {
        "name": "IRJ Metro",
        "base": "https://www.railjournal.com",
        "url": "https://www.railjournal.com/tag/metro/",
        "list_sel": "article",
        "title_sel": "h2 a, h3 a, .entry-title a",
        "link_sel": "h2 a, h3 a, .entry-title a",
        "date_sel": "time, .jeg_meta_date, .post-date",
        "summary_sel": ".entry-content p, .jeg_post_excerpt, p",
    },
    # Railway Gazette Metro page
    {
        "name": "Railway Gazette Metro",
        "base": "https://www.railwaygazette.com",
        "url": "https://www.railwaygazette.com/metro",
        "list_sel": "article, .listingResult, .article",
        "title_sel": "h3 a, h2 a, .headline a, a.link",
        "link_sel": "h3 a, h2 a, .headline a, a.link",
        "date_sel": "time, .date, time[datetime]",
        "summary_sel": "p, .standfirst",
    },
    # UrbanRail News page (plain HTML list)
    {
        "name": "UrbanRail News",
        "base": "https://www.urbanrail.net",
        "url": "https://www.urbanrail.net/news.htm",
        "list_sel": "p, li",
        "title_sel": "a",
        "link_sel": "a",
        "date_sel": None,
        "summary_sel": None,
    },
]

# Project type keywords (lowercase)
KT_INFRA = [
    "extension","extend","line","branch","tunnel","station","stations","signalling","cbtc","electrification",
    "commission","opening","inaugur","double track","upgrade","viaduct","depot","interchange","route",
    "alignment","platform","metro line","construction","build","civil works","contract award","phase"
]
KT_FLEET = [
    "fleet","rolling stock","train","trains","metro car","cars","carriages","tram","lrvs","lrv",
    "light rail vehicle","monorail","trainset","emu","dmu","multiple unit","bogie","bogies","order",
    "contract","tender","delivery","deliveries","supplier","manufacturer","wagon","coach"
]
KT_STATUS = {
    "Planung": ["plan","tender","procure","propos","feasibility","study","design","rfp"],
    "Bau": ["construction","build","works start","groundbreaking","contract awarded","civil works","installation"],
    "Eröffnung": ["open","opening","commission","enter service","inaugur","start service","launched","commence operations"],
    "Verzögerung": ["delay","postpone","late","overrun","halted","suspended","slipped"],
    "Finanzierung": ["funding","finance","loan","grant","budget","ppp","approval","approved","bond"],
}

# City-to-country heuristics (extend as needed)
CITY_HINTS = {
    "Berlin": "Germany","Hamburg":"Germany","Munich":"Germany","München":"Germany","Frankfurt":"Germany","Cologne":"Germany","Köln":"Germany","Stuttgart":"Germany","Düsseldorf":"Germany","Leipzig":"Germany","Nuremberg":"Germany","Nürnberg":"Germany",
    "Vienna":"Austria","Wien":"Austria","Graz":"Austria","Linz":"Austria",
    "Zurich":"Switzerland","Zürich":"Switzerland","Geneva":"Switzerland","Genève":"Switzerland","Basel":"Switzerland",
    "Paris":"France","Lyon":"France","Marseille":"France","Lille":"France","Toulouse":"France","Rennes":"France",
    "Madrid":"Spain","Barcelona":"Spain","Valencia":"Spain","Seville":"Spain","Sevilla":"Spain","Bilbao":"Spain","Malaga":"Spain",
    "London":"United Kingdom","Birmingham":"United Kingdom","Manchester":"United Kingdom","Glasgow":"United Kingdom","Edinburgh":"United Kingdom","Liverpool":"United Kingdom","Leeds":"United Kingdom","Sheffield":"United Kingdom","Newcastle":"United Kingdom",
    "Rome":"Italy","Milano":"Italy","Milan":"Italy","Naples":"Italy","Torino":"Italy","Turin":"Italy","Bologna":"Italy","Genova":"Italy","Genoa":"Italy",
    "Warsaw":"Poland","Krakow":"Poland","Kraków":"Poland","Gdansk":"Poland","Gdańsk":"Poland","Wroclaw":"Poland","Wrocław":"Poland",
    "Prague":"Czech Republic","Praha":"Czech Republic","Brno":"Czech Republic","Ostrava":"Czech Republic",
    "Budapest":"Hungary","Debrecen":"Hungary","Szeged":"Hungary",
    "Lisbon":"Portugal","Porto":"Portugal",
    "Oslo":"Norway","Stockholm":"Sweden","Gothenburg":"Sweden","Göteborg":"Sweden","Malmö":"Sweden",
    "Copenhagen":"Denmark","København":"Denmark","Aarhus":"Denmark",
    "Helsinki":"Finland","Tampere":"Finland",
    "Athens":"Greece","Thessaloniki":"Greece",
    "Istanbul":"Turkey","Ankara":"Turkey","Izmir":"Turkey",
    "Doha":"Qatar","Dubai":"UAE","Abu Dhabi":"UAE","Riyadh":"Saudi Arabia","Jeddah":"Saudi Arabia",
    "Cairo":"Egypt","Casablanca":"Morocco","Algiers":"Algeria",
    "New York":"USA","Los Angeles":"USA","Chicago":"USA","Washington":"USA","Boston":"USA","Philadelphia":"USA","Miami":"USA","San Francisco":"USA","Seattle":"USA","Dallas":"USA","Houston":"USA","Atlanta":"USA",
    "Toronto":"Canada","Montreal":"Canada","Vancouver":"Canada",
    "Mexico City":"Mexico","Guadalajara":"Mexico","Monterrey":"Mexico",
    "Santiago":"Chile","Buenos Aires":"Argentina","Lima":"Peru","Bogotá":"Colombia","Medellín":"Colombia",
    "São Paulo":"Brazil","Sao Paulo":"Brazil","Rio de Janeiro":"Brazil","Brasília":"Brazil","Brasilia":"Brazil",
    "Jakarta":"Indonesia","Manila":"Philippines","Singapore":"Singapore","Bangkok":"Thailand","Hong Kong":"China",
    "Shanghai":"China","Beijing":"China","Shenzhen":"China","Guangzhou":"China","Wuhan":"China","Chengdu":"China",
    "Tokyo":"Japan","Osaka":"Japan","Nagoya":"Japan","Yokohama":"Japan","Fukuoka":"Japan",
    "Seoul":"South Korea","Busan":"South Korea","Incheon":"South Korea",
    "Taipei":"Taiwan","Kaohsiung":"Taiwan","Hsinchu":"Taiwan",
    "Sydney":"Australia","Melbourne":"Australia","Perth":"Australia","Brisbane":"Australia",
    "Auckland":"New Zealand","Wellington":"New Zealand"
}

def http_get(url):
    headers = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"}
    r = requests.get(url, headers=headers, timeout=25)
    r.raise_for_status()
    return r.text

def clean(s):
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip()

def classify_type(text):
    t = (text or "").lower()
    infra = any(k in t for k in KT_INFRA)
    fleet = any(k in t for k in KT_FLEET)
    if infra and fleet:
        return "gemischt"
    if infra:
        return "infrastruktur"
    if fleet:
        return "flotte"
    return ""

def classify_status(text):
    t = (text or "").lower()
    for status, keys in KT_STATUS.items():
        if any(k in t for k in keys):
            return status
    return ""

def guess_city_country(text):
    txt = text or ""
    for city, country in CITY_HINTS.items():
        if re.search(rf"\b{re.escape(city)}\b", txt, re.IGNORECASE):
            return city, country
    return "", ""

def parse_list_page(cfg):
    html = http_get(cfg["url"])
    soup = BeautifulSoup(html, "lxml")
    items = []

    for block in soup.select(cfg["list_sel"]):
        # title
        title_el = None
        for sel in cfg["title_sel"].split(","):
            el = block.select_one(sel.strip())
            if el and clean(el.get_text()):
                title_el = el
                break
        title = clean(title_el.get_text()) if title_el else ""
        # link
        link_el = None
        for sel in cfg["link_sel"].split(","):
            el = block.select_one(sel.strip())
            if el and el.get("href"):
                link_el = el
                break
        link = urljoin(cfg["base"], link_el["href"]) if link_el and link_el.get("href") else ""
        # date
        date_txt = ""
        if cfg["date_sel"]:
            d = block.select_one(cfg["date_sel"])
            if d:
                date_txt = clean(d.get("datetime") or d.get_text())
        # summary
        summary = ""
        if cfg["summary_sel"]:
            sm = block.select_one(cfg["summary_sel"])
            if sm:
                summary = clean(sm.get_text())

        if not title and not link:
            continue

        items.append({
            "Datum": date_txt or datetime.utcnow().date().isoformat(),
            "Quelle": cfg["name"],
            "Titel": title,
            "Link": link,
            "Details": summary
        })
    return items

def enrich(rows):
    enriched = []
    for r in rows:
        text = " ".join([r.get("Titel",""), r.get("Details","")])
        city, country = guess_city_country(text)
        ptype = classify_type(text)
        status = classify_status(text)
        enriched.append({
            "Datum": r.get("Datum"),
            "Quelle": r.get("Quelle"),
            "Titel": r.get("Titel"),
            "Link": r.get("Link"),
            "Land": country,
            "Stadt": city,
            "Projekttyp": ptype,
            "Status": status,
            "Details": r.get("Details","")
        })
    return enriched

def dedupe_and_write(path, rows):
    cols = ["Datum","Quelle","Titel","Link","Land","Stadt","Projekttyp","Status","Details"]
    df_new = pd.DataFrame(rows, columns=cols)
    if os.path.exists(path):
        try:
            df_old = pd.read_csv(path)
            df = pd.concat([df_old, df_new], ignore_index=True)
            df = df.drop_duplicates(subset=["Link","Titel"], keep="first")
        except Exception:
            df = df_new
    else:
        df = df_new
    df.to_csv(path, index=False, encoding="utf-8-sig")

def main():
    all_rows = []
    for cfg in SOURCES:
        try:
            items = parse_list_page(cfg)
            all_rows.extend(items)
            time.sleep(1)
        except Exception as e:
            print(f"[WARN] Quelle fehlgeschlagen: {cfg['name']} – {e}", file=sys.stderr)
    if not all_rows:
        print("Keine Artikel gefunden – prüfe Quellen/Selektoren.")
        return
    enriched = enrich(all_rows)
    dedupe_and_write(OUTPUT_CSV, enriched)
    print(f"{len(enriched)} Artikel verarbeitet. Datei aktualisiert: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()

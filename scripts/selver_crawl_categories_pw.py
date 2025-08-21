#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Selver category crawler → CSV (staging_selver_products)

Features:
- Collect PDP links from listings and remember which listing page each link came from.
- Two modes:
  1) Direct mode (default): open PDPs directly, with click-through fallback if needed.
  2) CLICK mode (CLICK_PRODUCTS=1): literally click each product card on listings,
     open PDP, extract, then go back (no URL harvesting required).
- CSV columns (downstream-safe): ext_id, name, ean_raw, sku_raw, size_text,
  price, currency, category_path, category_leaf.

Noise & speed fixes:
- Canonical URLs (no /e-selver/) and pagination via ?page=N.
- Tight router (default ON): aborts non-Selver scripts/xhr/fetch/fonts/stylesheet/websocket/manifest/eventsource.
- No 'Upgrade-Insecure-Requests' header.
- Block service workers (context + router kill known SW iframes).
- Wider 3P blocklist.
- Console is quiet by default (opt-in with LOG_CONSOLE).

Run:
  OUTPUT_CSV=data/selver.csv python scripts/selver_crawl_categories_pw.py

Env toggles:
  CLICK_PRODUCTS=0 (default) | 1
  ALLOWLIST_ONLY=1 (default) | 0
  PAGE_LIMIT=0 (no cap)
  USE_ROUTER=1 (default) | 0
  LOG_CONSOLE=0 (default) | warn | all
  REQ_DELAY=0.6 (seconds between steps)
"""

from __future__ import annotations
import os, re, csv, time, json
from typing import Dict, Set, Tuple, List, Optional
from urllib.parse import urljoin, urlparse, urlsplit, urlunsplit, parse_qs, urlencode
from playwright.sync_api import sync_playwright

# ---------------------------------------------------------------------------
# Config
BASE = "https://www.selver.ee"

OUTPUT = os.getenv("OUTPUT_CSV", "data/selver.csv")
REQ_DELAY = float(os.getenv("REQ_DELAY", "0.6"))
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "0"))  # 0 = no limit
CATEGORIES_FILE = os.getenv("CATEGORIES_FILE", "data/selver_categories.txt")

USE_ROUTER = int(os.getenv("USE_ROUTER", "1")) == 1
CLICK_PRODUCTS = int(os.getenv("CLICK_PRODUCTS", "0")) == 1
LOG_CONSOLE = (os.getenv("LOG_CONSOLE", "0") or "0").lower()  # 0|off, warn, all

# Strict allowlist of FOOD roots/leaves (canonical, no /e-selver/)
STRICT_ALLOWLIST = [
    "/puu-ja-koogiviljad",
    "/liha-ja-kalatooted",
    "/piimatooted-munad-void",
    "/juustud",
    "/leivad-saiad-kondiitritooted",
    "/valmistoidud",
    "/kuivained-hommikusoogid-hoidised",
    "/maitseained-ja-puljongid",
    "/maitseained-ja-puljongid/k

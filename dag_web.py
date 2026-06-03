"""
Daily scrape of competitor pricing pages -> JSON -> web.raw_provider_responses,
followed by a dbt run of the web staging models that read from that table.

Providers covered:
  - Cherry Servers (dedicated + virtual, scraped from window.PLANS_PRICING_DATA)
  - Latitude       (servers + VMs, scraped from embedded Next.js data)
  - OVH            (baremetal / eco / cloud / vps catalogs across all subsidiaries,
                    fetched directly from the public order API)
  - Hetzner        (dedicated rootserver matrices for ax/ex/rx/sx/gpu, scraped
                    from matrix-<line> pages and joined with the public
                    website-price-api product endpoint)

Each scrape is independent (failure in one provider doesn't block the others).
Within OVH, each (range x subsidiary) fetch is also independent so a single 404
or transient failure doesn't poison the rest. Within Hetzner, each matrix line
is also independent for the same reason. The dbt task waits for all
extractions to finish (success or failure) and runs against whatever fresh data
made it in.
"""

import json
import logging
import re
import time
import urllib.parse
from datetime import datetime, timedelta
from html.parser import HTMLParser

import requests

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

POSTGRES_CONN_ID = "alexandria_postgres"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT = 30  # seconds

DBT_PROJECT_DIR = "/opt/airflow/dbt/"
DBT_PROFILES_DIR = "/opt/airflow/scripts/"
DBT_MODEL = "staging.web+"

default_args = {
    "email": ["mantas.levinas@cherryservers.com", "egle.sakalauskaite@cherryservers.com"],
    "email_on_failure": True
}

OVH_ENDPOINTS = [
    {
        "host": "https://api.ovh.com/1.0",
        # "subsidiaries": ["DE", "FR", "GB", "PL", "IE", "ES", "IT"],
        "subsidiaries": ["DE"],
        "ranges": {
            "baremetal": "/order/catalog/public/baremetalServers",
            "eco":       "/order/catalog/public/eco",
            "cloud":     "/order/catalog/public/cloud",
            "vps":       "/order/catalog/public/vps",
        },
    },
    # {
    #     "host": "https://api.us.ovhcloud.com/1.0",
    #     "subsidiaries": ["US"],
    #     "ranges": {
    #         "baremetal": "/order/catalog/public/baremetalServers",
    #         "eco":       "/order/catalog/public/eco",
    #         "cloud":     "/order/catalog/public/cloud",
    #         "vps":       "/order/catalog/public/vps",
    #     },
    # },
]

HETZNER_MATRIX_BASE = "https://www.hetzner.com/dedicated-rootserver"
HETZNER_PRICE_API_BASE = "https://website-price-api.hetzner.com/api/v1/products"
HETZNER_LINES = ["ex", "ax", "rx", "sx", "gpu"]
HETZNER_INTER_REQUEST_DELAY_SECS = 0.25

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fetch_html(url):
    """GET a URL with a real browser UA. Raises on non-2xx."""
    response = requests.get(
        url,
        headers={"User-Agent": USER_AGENT, "Accept": "text/html,*/*"},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    if len(response.text) < 1000:
        raise ValueError(
            "Response from {} is only {} chars; likely blocked.".format(
                url, len(response.text)
            )
        )
    return response.text


def _fetch_ovh_catalog(url):
    """GET an OVH public catalog endpoint and return parsed JSON."""
    response = requests.get(
        url,
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict) or "plans" not in payload:
        raise ValueError(
            "OVH: unexpected payload shape from {} (keys={})".format(
                url,
                list(payload.keys()) if isinstance(payload, dict) else type(payload).__name__,
            )
        )
    return payload


def _extract_cherry_groups(html):
    """Pull the plansGroups array out of Cherry's window.PLANS_PRICING_DATA blob."""
    marker = "window.PLANS_PRICING_DATA = "
    start = html.find(marker)
    if start < 0:
        raise ValueError("Cherry: 'window.PLANS_PRICING_DATA' marker not found in HTML")
    start += len(marker)

    depth = 0
    in_string = False
    escape = False
    end = -1
    for i in range(start, len(html)):
        c = html[i]
        if escape:
            escape = False
            continue
        if c == "\\":
            escape = True
            continue
        if c == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break

    if end < 0:
        raise ValueError("Cherry: could not find end of JSON object")

    data = json.loads(html[start:end])
    groups = data.get("plansGroups", [])
    if not groups:
        raise ValueError("Cherry: plansGroups is empty - page structure may have changed")
    return groups


def _extract_latitude_array(html, marker, search_from=0):
    """Pull a JSON array out of Latitude's embedded Next.js data.
    Returns a tuple of (parsed_list, position_after_array)."""
    start = html.find(marker, search_from)
    if start < 0:
        return [], 0
    start += len(marker) - 1  # position at the opening '['

    depth = 0
    in_string = False
    escape = False
    for i in range(start, len(html)):
        c = html[i]
        if escape:
            escape = False
            continue
        if c == "\\":
            escape = True
            continue
        if c == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if c == "[":
            depth += 1
        elif c == "]":
            depth -= 1
            if depth == 0:
                return json.loads(html[start:i + 1]), i + 1
    return [], 0


def _extract_latitude_plans(html):
    """Combine servers + virtualMachines arrays into one list."""
    unescaped = html.replace('\\"', '"')
    servers, pos = _extract_latitude_array(unescaped, '"servers":[')
    vms, _ = _extract_latitude_array(unescaped, '"virtualMachines":[', pos)
    plans = servers + vms
    if not plans:
        raise ValueError("Latitude: extracted 0 plans - page structure may have changed")
    return plans


# ---------------------------------------------------------------------------
# Hetzner: matrix HTML parsing + price API
# ---------------------------------------------------------------------------

_TRADEMARK_PATTERN = re.compile(r"[\u2122\u00ae\u00a9]")
_HTML_VOID_TAGS = {
    "area", "base", "br", "col", "embed", "hr", "img", "input",
    "link", "meta", "param", "source", "track", "wbr",
}


class _Node:
    """One element node in the lightweight DOM we build for Hetzner pages."""
    __slots__ = ("tag", "attrs", "_items")

    def __init__(self, tag, attrs):
        self.tag = tag
        self.attrs = dict(attrs)
        self._items = []

    @property
    def children(self):
        return [x for x in self._items if isinstance(x, _Node)]

    def get(self, key, default=""):
        return self.attrs.get(key, default)

    def has_class(self, name):
        return name in (self.attrs.get("class", "") or "").split()

    def text(self):
        """Concatenate all descendant text with whitespace collapsed,
        preserving document order so nested fragments stay in place."""
        out = []
        self._collect_text(out)
        return " ".join(" ".join(out).split())

    def _collect_text(self, out):
        for item in self._items:
            if isinstance(item, str):
                if item.strip():
                    out.append(item)
            else:
                item._collect_text(out)

    def find_all(self, predicate):
        out = []
        for c in self.children:
            if predicate(c):
                out.append(c)
            out.extend(c.find_all(predicate))
        return out

    def find(self, predicate):
        for c in self.children:
            if predicate(c):
                return c
            r = c.find(predicate)
            if r is not None:
                return r
        return None


class _TreeBuilder(HTMLParser):
    """Build a _Node tree from HTML."""
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.root = _Node("__root__", {})
        self.stack = [self.root]

    def handle_starttag(self, tag, attrs):
        node = _Node(tag, attrs)
        self.stack[-1]._items.append(node)
        if tag not in _HTML_VOID_TAGS:
            self.stack.append(node)

    def handle_startendtag(self, tag, attrs):
        node = _Node(tag, attrs)
        self.stack[-1]._items.append(node)

    def handle_endtag(self, tag):
        for i in range(len(self.stack) - 1, 0, -1):
            if self.stack[i].tag == tag:
                del self.stack[i:]
                return

    def handle_data(self, data):
        if data:
            self.stack[-1]._items.append(data)


def _parse_html(html):
    p = _TreeBuilder()
    p.feed(html)
    p.close()
    return p.root


def _hetzner_clean(text):
    """Strip trademark symbols and collapse whitespace."""
    if not text:
        return ""
    return re.sub(r"\s+", " ", _TRADEMARK_PATTERN.sub("", text)).strip()


def _hetzner_expand_row(tr, n_columns):
    """Read a <tr>, honouring colspan, returning exactly n_columns values."""
    cells = [c for c in tr.children if c.tag in ("th", "td")]
    if not cells:
        return [""] * n_columns

    value_cells = cells[1:]
    expanded = []
    for c in value_cells:
        text = c.text()
        try:
            span = int(c.get("colspan", "1"))
        except (TypeError, ValueError):
            span = 1
        span = max(1, span)
        expanded.extend([text] * span)

    if len(expanded) < n_columns:
        expanded.extend([""] * (n_columns - len(expanded)))
    elif len(expanded) > n_columns:
        expanded = expanded[:n_columns]
    return expanded


def _hetzner_parse_cores_count(detail):
    """Extract the integer core count from a Hetzner CPU-Details cell.
    Returns an int, or None if the count is unparseable."""
    if not detail:
        return None
    word_to_int = {
        "single": 1, "dual": 2, "quad": 4, "hexa": 6, "octa": 8,
        "deca": 10, "dodeca": 12,
    }
    m = re.match(r"\s*([A-Za-z]+)\s*-\s*Core\b", detail, re.I)
    if m and m.group(1).lower() in word_to_int:
        return word_to_int[m.group(1).lower()]

    m = re.match(r"\s*(\d+)\s*[-\s]*Core\b", detail, re.I)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    return None


def _hetzner_parse_cpu_arch(detail):
    """Extract the microarchitecture from a Hetzner CPU-Details cell."""
    if not detail:
        return None
    m = re.search(r"\(([^)]+)\)", detail)
    if not m:
        return None
    arch = m.group(1).strip()
    if arch.upper() in ("AMD-V", "INTEL-VT", "VT-X", "VT-D"):
        return None
    return arch or None


def _hetzner_enrich_from_hardware_table(root, cards):
    """For non-GPU lines: pull CPU/RAM/storage/bandwidth/traffic from the
    Hardware table and assign by column position."""
    if not cards:
        return cards
    n = len(cards)

    cpu_row = [""] * n
    cpu_detail_row = [""] * n
    ram_row = [""] * n
    nvme_row = [""] * n
    sata_ssd_row = [""] * n
    hdd_row = [""] * n
    bandwidth_row = [""] * n
    traffic_row = [""] * n

    seen = {
        "cpu": False, "cpu_detail": False, "ram": False,
        "nvme": False, "ssd": False, "hdd": False,
        "bandwidth": False, "traffic": False,
    }

    tables = root.find_all(lambda n_: n_.tag == "table")
    for table in tables:
        for tr in table.find_all(lambda n_: n_.tag == "tr"):
            cells = [c for c in tr.children if c.tag in ("th", "td")]
            if not cells:
                continue
            label = cells[0].text().lower()

            if (not seen["cpu"]
                    and label.startswith("cpu") and "detail" not in label):
                cpu_row = _hetzner_expand_row(tr, n)
                seen["cpu"] = True
            elif (not seen["cpu_detail"]
                    and "cpu" in label and "detail" in label):
                cpu_detail_row = _hetzner_expand_row(tr, n)
                seen["cpu_detail"] = True
            elif not seen["ram"] and label.startswith("ram"):
                ram_row = _hetzner_expand_row(tr, n)
                seen["ram"] = True
            elif not seen["nvme"] and "nvme" in label:
                nvme_row = _hetzner_expand_row(tr, n)
                seen["nvme"] = True
            elif not seen["ssd"] and ("sata ssd" in label
                                       or label.startswith("ssd ")
                                       or label == "ssd"):
                sata_ssd_row = _hetzner_expand_row(tr, n)
                seen["ssd"] = True
            elif not seen["hdd"] and "hdd" in label and "ssd" not in label:
                hdd_row = _hetzner_expand_row(tr, n)
                seen["hdd"] = True
            elif (not seen["bandwidth"]
                    and ("guaranteed bandwidth" in label
                         or label == "connection")):
                bandwidth_row = _hetzner_expand_row(tr, n)
                seen["bandwidth"] = True
            elif not seen["traffic"] and label.startswith("traffic"):
                traffic_row = _hetzner_expand_row(tr, n)
                seen["traffic"] = True

    for i, card in enumerate(cards):
        card["cpu_model"] = _hetzner_clean(cpu_row[i]) or None
        card["cores_count"] = _hetzner_parse_cores_count(cpu_detail_row[i])
        card["cpu_arch"] = _hetzner_parse_cpu_arch(cpu_detail_row[i])
        card["cpu_detail_raw"] = _hetzner_clean(cpu_detail_row[i]) or None

        ram = _hetzner_clean(ram_row[i])
        ram = re.sub(r"\s*Upgradeable.*$", "", ram, flags=re.I).strip()
        card["ram"] = ram or None

        storage_parts = []
        for row, label in [(nvme_row, "NVMe"), (sata_ssd_row, "SSD"), (hdd_row, "HDD")]:
            v = row[i].strip()
            if not v or v == "-":
                continue
            v = re.split(r"\boptionally\b", v, flags=re.I)[0].strip()
            v = re.sub(r"\s+", " ", v)
            if not v or not re.search(r"\d", v):
                continue
            storage_parts.append("{} {}".format(v, label))
        card["storage"] = ", ".join(storage_parts) or None

        bw = _hetzner_clean(
            re.sub(r"\s*Upgradeable.*$", "", bandwidth_row[i], flags=re.I)
        )
        card["bandwidth"] = bw or None
        card["traffic"] = _hetzner_clean(traffic_row[i]) or None

    return cards


def _hetzner_enrich_from_gpu_grid(root, cards):
    """For matrix-gpu (no <table> elements): pull specs from per-card
    gpu-information-grid divs. GPU rows have no listed storage on Hetzner's
    page, so storage stays empty for these.
    """
    by_name = {c["plan_name"]: c for c in cards}

    grids = root.find_all(
        lambda n_: n_.tag == "div" and n_.has_class("gpu-information-grid")
    )
    for grid in grids:
        name_el = grid.find(lambda n_: n_.has_class("grid-header-name"))
        if not name_el:
            continue
        name = _hetzner_clean(name_el.text())
        card = by_name.get(name)
        if not card:
            continue

        def cell(klass):
            el = grid.find(lambda n_: n_.tag == "div" and n_.has_class(klass))
            return _hetzner_clean(el.text()) if el else ""

        cpu_model = cell("item-a")
        cpu_cores = cell("item-b")
        ram = cell("item-c")
        gpu = cell("item-d")
        tflops = cell("item-e")
        vram = cell("item-f")

        card["cpu_model"] = cpu_model or None

        cores = None
        if cpu_cores:
            m_pe = re.match(
                r"(\d+)\s+Performance\s+Cores\s+(\d+)\s+Efficient\s+Cores",
                cpu_cores, re.I,
            )
            if m_pe:
                cores = int(m_pe.group(1)) + int(m_pe.group(2))
            else:
                m_plain = re.match(r"\s*(\d+)\s*[-\s]*Cores?\b", cpu_cores, re.I)
                if m_plain:
                    cores = int(m_plain.group(1))
                else:
                    m_lead = re.match(r"\s*(\d+)\b", cpu_cores)
                    if m_lead:
                        cores = int(m_lead.group(1))
        card["cores_count"] = cores
        card["cpu_cores_raw"] = cpu_cores or None

        card["gpu"] = gpu or None
        card["gpu_vram"] = vram or None
        card["gpu_tflops"] = tflops or None

        card["ram"] = ram or None
    return cards


def _hetzner_parse_matrix(html, line):
    """Extract a list of plan dicts (no prices yet) from one matrix-<line> page.

    Each dict has: plan_name, plan_id, cpu_model, cpu_count, cores_count,
    cpu_arch, cpu_detail_raw, frequency, ram, storage, gpu, gpu_vram,
    gpu_tflops, cpu_cores_raw, bandwidth, traffic, monthly_key, hourly_key.
    Fields not present on the page are None. Prices are looked up via the
    price API in a separate step.
    """
    root = _parse_html(html)
    cards = []

    product_cards = root.find_all(
        lambda n_: (
            n_.tag == "div"
            and n_.has_class("product")
            and n_.has_class("product-type-HetznerProduct")
        )
    )
    for card_div in product_cards:
        name_el = card_div.find(lambda n_: n_.has_class("product-text-name"))
        if not name_el:
            continue
        name = _hetzner_clean(name_el.text())
        if not name:
            continue

        monthly_el = card_div.find(
            lambda n_: n_.tag == "ho-split-price-container"
        )
        monthly_key = monthly_el.get("product-key", "") if monthly_el else ""

        hourly_el = card_div.find(
            lambda n_: (n_.tag == "ho-price-container"
                        and n_.get("price-type", "") == "hourly")
        )
        hourly_key = hourly_el.get("product-key", "") if hourly_el else ""

        # Same bundle key applies to both cycles when only one is present.
        if not monthly_key and hourly_key:
            monthly_key = hourly_key
        if not hourly_key and monthly_key:
            hourly_key = monthly_key

        if not monthly_key:
            continue

        cards.append({
            "line": line,
            "plan_name": name,
            "plan_id": monthly_key,
            "cpu_count": None,
            "cpu_model": None,
            "cores_count": None,
            "cpu_arch": None,
            "cpu_detail_raw": None,
            "frequency": None,
            "ram": None,
            "storage": None,
            "gpu": None,
            "gpu_vram": None,
            "gpu_tflops": None,
            "cpu_cores_raw": None,
            "bandwidth": None,
            "traffic": None,
            "monthly_key": monthly_key,
            "hourly_key": hourly_key,
        })

    has_table = root.find(lambda n_: n_.tag == "table") is not None
    if has_table:
        cards = _hetzner_enrich_from_hardware_table(root, cards)
    else:
        cards = _hetzner_enrich_from_gpu_grid(root, cards)
    return cards


def _hetzner_fetch_prices(session, product_key):
    """Fetch one product's pricing from website-price-api.hetzner.com.
    Returns the parsed JSON (dict)"""
    log = logging.getLogger("airflow.task")
    encoded = urllib.parse.quote(product_key, safe="_")
    url = "{}/{}".format(HETZNER_PRICE_API_BASE, encoded)
    try:
        r = session.get(url, timeout=REQUEST_TIMEOUT)
    except requests.RequestException as exc:
        log.warning("Hetzner price fetch failed for %s: %s", product_key, exc)
        return None
    if r.status_code != 200:
        log.warning(
            "Hetzner price API returned HTTP %s for %s",
            r.status_code, product_key,
        )
        return None
    try:
        return r.json()
    except ValueError:
        log.warning("Hetzner price API returned non-JSON for %s", product_key)
        return None


def _scrape_hetzner_line(session, line):
    """End-to-end scrape for one matrix line. Returns a list of plan dicts
    (each enriched with monthly_prices / hourly_prices objects from the API)."""
    log = logging.getLogger("airflow.task")
    url = "{}/matrix-{}/".format(HETZNER_MATRIX_BASE, line)
    log.info("Hetzner: fetching %s", url)
    html = _fetch_html(url)
    cards = _hetzner_parse_matrix(html, line)
    if not cards:
        log.info("Hetzner matrix-%s: no plan cards found", line)
        return []

    keys = set()
    for c in cards:
        if c["monthly_key"]:
            keys.add(c["monthly_key"])
        if c["hourly_key"]:
            keys.add(c["hourly_key"])

    price_cache = {}
    for i, key in enumerate(sorted(keys)):
        price_cache[key] = _hetzner_fetch_prices(session, key)
        if i < len(keys) - 1:
            time.sleep(HETZNER_INTER_REQUEST_DELAY_SECS)

    for c in cards:
        c["monthly_prices"] = price_cache.get(c["monthly_key"])
        c["hourly_prices"] = price_cache.get(c["hourly_key"])

    log.info("Hetzner matrix-%s: parsed %d plans", line, len(cards))
    return cards


def _upsert_raw(provider, payload, source_url):
    """Write payload to web.raw_provider_responses, replacing any existing row for today.

    Returns a row count for logging: number of plans for OVH catalogs (dict
    payloads), or list length / 1 for the other providers.
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql = """
        INSERT INTO web.raw_provider_responses (provider, extracted_date, source_url, payload)
        VALUES (%s, CURRENT_DATE, %s, %s::jsonb)
        ON CONFLICT (source_url, extracted_date) DO UPDATE
            SET payload    = EXCLUDED.payload,
                source_url = EXCLUDED.source_url,
                fetched_at = now()
    """
    hook.run(sql, parameters=(provider, source_url, json.dumps(payload)))

    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict) and isinstance(payload.get("plans"), list):
        return len(payload["plans"])
    return 1


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def extract_cherry_dedicated():
    url = "https://www.cherryservers.com/pricing/dedicated-servers"
    html = _fetch_html(url)
    groups = _extract_cherry_groups(html)
    return _upsert_raw("Cherry Servers", groups, url)


def extract_cherry_virtual():
    url = "https://www.cherryservers.com/pricing/virtual-servers"
    html = _fetch_html(url)
    groups = _extract_cherry_groups(html)
    return _upsert_raw("Cherry Servers", groups, url)


def extract_latitude():
    url = "https://www.latitude.sh/pricing"
    html = _fetch_html(url)
    plans = _extract_latitude_plans(html)
    return _upsert_raw("Latitude", plans, url)


def extract_ovh():
    """Fetch every (range x subsidiary) catalog from OVH's public order API.

    Each fetch is independent: a 404 or transient failure on one endpoint is
    logged but does not abort the others. The task only fails if every fetch
    failed (i.e. nothing landed in web.raw_provider_responses for OVH today).
    """
    log = logging.getLogger("airflow.task")
    ok, failed = 0, 0

    for endpoint in OVH_ENDPOINTS:
        host = endpoint["host"]
        for range_name, path in endpoint["ranges"].items():
            for sub in endpoint["subsidiaries"]:
                url = "{}{}?ovhSubsidiary={}".format(host, path, sub)
                try:
                    payload = _fetch_ovh_catalog(url)
                    plan_count = _upsert_raw("OVH", payload, url)
                    log.info(
                        "OVH ok: range=%s subsidiary=%s plans=%d",
                        range_name, sub, plan_count,
                    )
                    ok += 1
                except Exception as exc:
                    log.warning(
                        "OVH fetch failed: range=%s subsidiary=%s url=%s err=%s",
                        range_name, sub, url, exc,
                    )
                    failed += 1

    if ok == 0:
        raise RuntimeError(
            "OVH: every catalog fetch failed ({} attempts)".format(failed)
        )
    log.info("OVH summary: %d ok, %d failed", ok, failed)
    return {"ok": ok, "failed": failed}


def extract_hetzner():
    """Scrape each matrix-<line> page, join with the price API, and upsert.

    Each line is fetched and upserted independently so a single bad page
    (e.g. RX has historically been listed-but-empty) doesn't block the rest.
    The task only fails if every line failed.
    """
    log = logging.getLogger("airflow.task")
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })

    ok, failed = 0, 0
    for line in HETZNER_LINES:
        url = "{}/matrix-{}/".format(HETZNER_MATRIX_BASE, line)
        try:
            plans = _scrape_hetzner_line(session, line)
            if not plans:
                _upsert_raw("Hetzner", [], url)
                log.info("Hetzner ok (empty): line=%s url=%s", line, url)
                ok += 1
                continue
            plan_count = _upsert_raw("Hetzner", plans, url)
            log.info("Hetzner ok: line=%s plans=%d", line, plan_count)
            ok += 1
        except Exception as exc:
            log.warning(
                "Hetzner fetch failed: line=%s url=%s err=%s",
                line, url, exc,
            )
            failed += 1

    if ok == 0:
        raise RuntimeError(
            "Hetzner: every line fetch failed ({} attempts)".format(failed)
        )
    log.info("Hetzner summary: %d ok, %d failed", ok, failed)
    return {"ok": ok, "failed": failed}


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id='competitor_pricing_scrape',
    description='Daily scrape of Cherry, Latitude, OVH and Hetzner pricing data, then dbt run of web staging models',
    schedule_interval='@weekly',
    start_date=datetime(2026, 4, 28),
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
) as dag:

    cherry_dedicated_task = PythonOperator(
        task_id='extract_cherry_dedicated',
        python_callable=extract_cherry_dedicated,
        execution_timeout=timedelta(minutes=10),
        pool='daily_jobs'
    )

    cherry_virtual_task = PythonOperator(
        task_id='extract_cherry_virtual',
        python_callable=extract_cherry_virtual,
        execution_timeout=timedelta(minutes=10),
        pool='daily_jobs'
    )

    latitude_task = PythonOperator(
        task_id='extract_latitude',
        python_callable=extract_latitude,
        execution_timeout=timedelta(minutes=10),
        pool='daily_jobs'
    )

    ovh_task = PythonOperator(
        task_id='extract_ovh',
        python_callable=extract_ovh,
        # ~32 sequential HTTP calls at up to 30s each; give plenty of headroom.
        execution_timeout=timedelta(minutes=30),
        pool='daily_jobs'
    )

    hetzner_task = PythonOperator(
        task_id='extract_hetzner',
        python_callable=extract_hetzner,
        execution_timeout=timedelta(minutes=15),
        pool='daily_jobs'
    )

    dbt_run_web_staging = BashOperator(
        task_id='dbt_run_web_staging',
        bash_command=(
            "dbt run --select {models} "
            "--project-dir {project_dir} "
            "--profiles-dir {profiles_dir}"
        ).format(
            models=DBT_MODEL,
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROFILES_DIR,
        ),
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=30),
        pool='daily_jobs'
    )

    [
        cherry_dedicated_task,
        cherry_virtual_task,
        latitude_task,
        ovh_task,
        hetzner_task,
    ] >> dbt_run_web_staging

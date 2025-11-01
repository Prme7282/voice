import os
import json
import time
import requests
from urllib.parse import urlencode, quote_plus
from flask import Flask, render_template, request, jsonify, redirect, url_for

app = Flask(__name__, static_folder="static", template_folder="templates")

# --- Config ---
API_BASE = "https://api.data.gov.in/resource/ee03643a-ee4c-48c2-ac30-9f2ff26ab722"
API_KEY = os.environ.get("MGNREGA_API_KEY")
DATA_DIR = "data"
CACHE_FILE = os.path.join(DATA_DIR, "cache.json")
DISTRICT_FILE = os.path.join(DATA_DIR, "district.json")
# API requires limit < 1000; use 999
API_PAGE_LIMIT = 999
CACHE_TTL_SECONDS = 24 * 3600  # treat cache as fresh for 24 hours

os.makedirs(DATA_DIR, exist_ok=True)

# create cache file if missing
if not os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump({}, f)

# ensure district file exists (user supplied file should overwrite this if present)
if not os.path.exists(DISTRICT_FILE):
    with open(DISTRICT_FILE, "w", encoding="utf-8") as f:
        json.dump({"states": []}, f, indent=2)


def read_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def write_json_atomic(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)


def load_cache():
    c = read_json(CACHE_FILE)
    if c is None:
        return {}
    return c


def save_cache(cache):
    write_json_atomic(CACHE_FILE, cache)


def load_districts():
    d = read_json(DISTRICT_FILE)
    if d is None:
        return {"states": []}
    return d


def save_districts(district_data):
    write_json_atomic(DISTRICT_FILE, district_data)


def build_api_url(params: dict) -> str:
    # build url string for debugging / logs as user requested earlier
    q = urlencode(params, doseq=True, quote_via=quote_plus)
    return f"{API_BASE}?{q}"


def fetch_state_year_all(state_name: str, fin_year: str):
    """
    Fetch all pages for given state_name and fin_year.
    Return list of raw records (list of dicts).
    Update district.json if new district names found.
    Cache will be updated by caller.
    """
    state_u = state_name.strip().upper()
    fin_u = fin_year.strip()

    all_records = []
    offset = 0

    while True:
        params = {
            "api-key": API_KEY,
            "format": "json",
            "limit": API_PAGE_LIMIT,
            "offset": offset,
            "filters[state_name]": state_u,
            "filters[fin_year]": fin_u,
        }
        url = build_api_url(params)
        print("Hitting API:", url)

        try:
            resp = requests.get(API_BASE, params=params, timeout=20)
        except Exception as e:
            print("API request failed:", e)
            break

        if resp.status_code != 200:
            print("API returned non-200:", resp.status_code, resp.text[:400])
            break

        j = resp.json()
        records = j.get("records", [])
        count = int(j.get("count", len(records)))
        total = int(j.get("total", len(records)))

        all_records.extend(records)

        # update district.json with any new district names
        try:
            district_data = load_districts()
            # find or create state entry (match ignoring case)
            sd = None
            for s in district_data.get("states", []):
                if s.get("state", "").strip().upper() == state_u:
                    sd = s
                    break
            if sd is None:
                sd = {"state": state_name, "districts": []}
                district_data.setdefault("states", []).append(sd)

            changed = False
            for r in records:
                # API field is 'district_name' (based on samples)
                dname = (r.get("district_name") or r.get("district") or "").strip()
                if dname and dname not in sd["districts"]:
                    sd["districts"].append(dname)
                    changed = True
            if changed:
                # sort district names for nicer UI
                sd["districts"] = sorted(sd["districts"], key=lambda x: x.upper())
                save_districts(district_data)
        except Exception as e:
            print("update district.json failed:", e)

        # determine stopping condition
        # total from API tells us how many records exist
        if len(all_records) >= total:
            break
        if count == 0:
            break

        offset += count
        # polite wait to avoid hitting rate limits
        time.sleep(0.2)

    return all_records


def build_cache_for_state_year(state_name: str, fin_year: str):
    """Fetch and persist cache grouped by district_name for quick lookups."""
    key = f"{state_name.strip().upper()}||{fin_year.strip()}"
    cache = load_cache()

    # if present and fresh, return existing
    if key in cache:
        meta = cache[key].get("_meta", {})
        fetched_at = meta.get("fetched_at", 0)
        if time.time() - fetched_at < CACHE_TTL_SECONDS:
            return cache[key]

    records = fetch_state_year_all(state_name, fin_year)
    # group records by district_name (uppercased key for matching)
    grouped = {}
    for r in records:
        d = (r.get("district_name") or r.get("district") or "").strip()
        if not d:
            continue
        d_u = d.upper()
        grouped.setdefault(d_u, []).append(r)

    # store compact metadata
    cache[key] = {
        "_meta": {
            "state": state_name.strip(),
            "fin_year": fin_year.strip(),
            "fetched_at": int(time.time()),
            "total_records": len(records)
        },
        "by_district": grouped
    }
    save_cache(cache)
    return cache[key]


@app.route("/")
def index():
    # Pass district.json (full object) to page
    district_data = load_districts()
    states = [s.get("state") for s in district_data.get("states", [])]
    # pass states list and full district data
    return render_template("index.html", states=states, district_data=district_data)


@app.route("/district", methods=["POST"])
def district_page():
    state = request.form.get("state_name", "").strip()
    fin_year = request.form.get("fin_year", "").strip()
    district = request.form.get("district", "").strip()

    if not state or not fin_year or not district:
        return redirect(url_for("index"))

    # ensure cache exists for state-year (fetch if missing)
    key = f"{state.strip().upper()}||{fin_year.strip()}"
    cache = load_cache()
    if key not in cache:
        # fetch & build cache (this may take a few seconds)
        build_cache_for_state_year(state, fin_year)
        cache = load_cache()

    entry = cache.get(key)
    found = None
    if entry:
        # matching by uppercase district name
        found = entry.get("by_district", {}).get(district.strip().upper())

    # If not found in cache (maybe district naming mismatch), try to fetch again and perform fuzzy match
    if not found:
        # fetch fresh
        entry = build_cache_for_state_year(state, fin_year)
        # try exact again
        found = entry.get("by_district", {}).get(district.strip().upper())
        if not found:
            # try case-insensitive contains matching
            for dkey, recs in entry.get("by_district", {}).items():
                if district.strip().upper() in dkey:
                    found = recs
                    break

    # Prepare simple summary for low-literacy users (choose latest month record for headline)
    summary = None
    if found:
        # sort by month info if present (we'll try to present latest by 'month' field + fin_year)
        # We will rely on the returned records order; otherwise sort by month name mapping if required.
        latest = found[0]
        # Try find record with the latest 'month' (if month present)
        try:
            # If month values are strings like 'Jan', 'Feb', map to numbers
            month_order = {"Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,"Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}
            def month_idx(rec):
                m = (rec.get("month") or "").strip()[:3].title()
                return month_order.get(m, 0)
            latest = sorted(found, key=month_idx, reverse=True)[0]
        except Exception:
            latest = found[0]

        # sensible fields for quick display (fall back to keys that might exist)
        summary = {
            "month": latest.get("month") or latest.get("Month") or "",
            "fin_year": latest.get("fin_year") or "",
            "total_households": latest.get("Total_Households_Worked") or latest.get("Total Households Worked") or "",
            "total_individuals": latest.get("Total_Individuals_Worked") or latest.get("Total Individuals Worked") or "",
            "persondays": latest.get("Persondays_of_Central_Liability_so_far") or latest.get("Persondays_of_Central_Liability_so_far") or "",
            "wages": latest.get("Wages") or latest.get("Wages(in lakhs?)") or "",
            "avg_days_per_hh": latest.get("Average_days_of_employment_provided_per_Household") or ""
        }

    return render_template("district.html",
                           state=state,
                           fin_year=fin_year,
                           district=district,
                           records=found,
                           summary=summary)


@app.route("/api/preview")
def api_preview():
    """
    Debug endpoint to show the actual API url that would be hit for given state/fin_year/offset/limit.
    Example: /api/preview?state=ANDHRA%20PRADESH&fin_year=2024-2025&limit=999&offset=0
    """
    state = request.args.get("state", "")
    fin_year = request.args.get("fin_year", "")
    limit = int(request.args.get("limit", API_PAGE_LIMIT))
    offset = int(request.args.get("offset", 0))
    params = {
        "api-key": API_KEY,
        "format": "json",
        "limit": limit,
        "offset": offset,
        "filters[state_name]": state,
        "filters[fin_year]": fin_year
    }
    return jsonify({"url": build_api_url(params), "params": params})


if __name__ == "__main__":
    # ensure files exist
    if not os.path.exists(DISTRICT_FILE):
        write_json_atomic(DISTRICT_FILE, {"states": []})
    if not os.path.exists(CACHE_FILE):
        write_json_atomic(CACHE_FILE, {})

    # Run
    app.run(host="0.0.0.0", port=8080, debug=True)

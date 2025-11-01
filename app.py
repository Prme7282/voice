import os
import time
import json
import requests
from urllib.parse import urlencode, quote_plus
from flask import Flask, render_template, request, jsonify, redirect, url_for
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

load_dotenv()
app = Flask(__name__, static_folder="static", template_folder="templates")

# MongoDB setup
mongo_uri = os.getenv("MONGO_URI")
db_name = os.getenv("DB_NAME")

client = MongoClient(mongo_uri)
db = client[db_name]
cache_collection = db["cache"]
district_collection = db["districts"]

# Config
API_BASE = "https://api.data.gov.in/resource/ee03643a-ee4c-48c2-ac30-9f2ff26ab722"
API_KEY = os.getenv("MGNREGA_API_KEY")
API_PAGE_LIMIT = 10000
CACHE_TTL_SECONDS = 24 * 3600  # 24h

# -------------------------------------------
# Utility: API URL builder
# -------------------------------------------
def build_api_url(params: dict) -> str:
    q = urlencode(params, doseq=True, quote_via=quote_plus)
    return f"{API_BASE}?{q}"


# -------------------------------------------
# District Helpers
# -------------------------------------------
def load_districts():
    """Load all states and their districts from MongoDB."""
    docs = list(district_collection.find({}, {"_id": 0}))
    if not docs:
        return {"states": []}

    if "states" in docs[0]:
        return {"states": docs[0]["states"]}
    return {"states": docs}


def save_districts(state_name, districts):
    """Upsert district data for a state."""
    district_collection.update_one(
        {"state": state_name.strip()},
        {"$set": {"state": state_name.strip(), "districts": sorted(districts)}},
        upsert=True
    )


# -------------------------------------------
# Cache Helpers
# -------------------------------------------
def save_cache(state, fin_year, grouped, total_records):
    """Save or update each district's cache as a separate document."""
    operations = []
    for district, records in grouped.items():
        operations.append(UpdateOne(
            {"state": state.upper(), "fin_year": fin_year, "district": district},
            {"$set": {
                "records": records,
                "fetched_at": int(time.time())
            }},
            upsert=True
        ))
    if operations:
        cache_collection.bulk_write(operations)
    print(f"✅ Saved {len(operations)} district caches for {state} {fin_year}")


def get_cache(state, fin_year):
    """Return grouped cache by district from MongoDB."""
    docs = list(cache_collection.find(
        {"state": state.upper(), "fin_year": fin_year},
        {"_id": 0, "district": 1, "records": 1, "fetched_at": 1}
    ))

    if not docs:
        return None

    grouped = {doc["district"]: doc["records"] for doc in docs}
    total_records = sum(len(doc["records"]) for doc in docs)

    fetched_at = docs[0].get("fetched_at", 0)
    if time.time() - fetched_at < CACHE_TTL_SECONDS:
        return {"by_district": grouped, "total_records": total_records}
    return None


# -------------------------------------------
# Fetch State-Year Data (fetch all districts at once)
# -------------------------------------------
def fetch_state_year_all(state_name, fin_year):
    """Fetch all district records for a given state & year."""
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
        print("🔗 Hitting API:", url)

        try:
            resp = requests.get(API_BASE, params=params, timeout=20)
            resp.raise_for_status()
        except Exception as e:
            print("❌ API request failed:", e)
            break

        j = resp.json()
        records = j.get("records", [])
        if not records:
            break

        all_records.extend(records)
        offset += len(records)

        total = int(j.get("total", len(all_records)))
        if len(all_records) >= total:
            break

        time.sleep(0.2)

    # Extract & save district list
    try:
        districts = list({
            (r.get("district_name") or r.get("district") or "").strip()
            for r in all_records if (r.get("district_name") or r.get("district"))
        })
        if districts:
            save_districts(state_name, districts)
    except Exception as e:
        print("⚠️ District update failed:", e)

    return all_records


# -------------------------------------------
# Build Cache (fetch and store all districts for the state)
# -------------------------------------------
def build_cache_for_state_year(state_name, fin_year):
    """Build cache for all districts in a state for a specific year."""
    existing = get_cache(state_name, fin_year)
    if existing:
        return existing

    print(f"⚙️ Building cache for state={state_name}, year={fin_year} ...")
    records = fetch_state_year_all(state_name, fin_year)

    grouped = {}
    for r in records:
        d = (r.get("district_name") or r.get("district") or "").strip()
        if not d:
            continue
        d_u = d.upper()
        grouped.setdefault(d_u, []).append(r)

    save_cache(state_name, fin_year, grouped, len(records))
    return get_cache(state_name, fin_year)


# -------------------------------------------
# Routes
# -------------------------------------------
@app.route("/")
def index():
    district_data = load_districts()
    states = [s.get("state") for s in district_data.get("states", [])]
    return render_template("index.html", states=states, district_data=district_data)

@app.route("/ping")
def ping():
    return "OK", 200

@app.route("/district", methods=["POST"])
def district_page():
    state = request.form.get("state_name", "").strip()
    fin_year = request.form.get("fin_year", "").strip()
    district = request.form.get("district", "").strip()

    if not state or not fin_year or not district:
        return redirect(url_for("index"))

    entry = build_cache_for_state_year(state, fin_year)
    found = entry.get("by_district", {}).get(district.strip().upper(), [])

    # Save for debugging
    with open("a.json", "w", encoding="utf-8") as f:
        json.dump(found, f, ensure_ascii=False, indent=4)

    if not found:
        for dkey, recs in entry.get("by_district", {}).items():
            if district.strip().upper() in dkey:
                found = recs
                break

    # --- Compute monthly averages ---
    month_order = {"Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,
                   "Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}
    monthly = {}

    for r in found:
        month = (r.get("month") or "").strip().title()[:3]
        if not month:
            continue
        mkey = month
        try:
            hh = float(r.get("Total_Households_Worked") or 0)
            ind = float(r.get("Total_Individuals_Worked") or 0)
            pers = float(r.get("Persondays_of_Central_Liability_so_far") or 0)
            wages = float(r.get("Wages") or 0)
            avg_days = float(r.get("Average_days_of_employment_provided_per_Household") or 0)
        except ValueError:
            continue

        monthly.setdefault(mkey, {"hh": [], "ind": [], "pers": [], "wages": [], "avg_days": []})
        monthly[mkey]["hh"].append(hh)
        monthly[mkey]["ind"].append(ind)
        monthly[mkey]["pers"].append(pers)
        monthly[mkey]["wages"].append(wages)
        monthly[mkey]["avg_days"].append(avg_days)

    averaged = []
    for m, vals in sorted(monthly.items(), key=lambda x: month_order.get(x[0], 0)):
        averaged.append({
            "month": m,
            "Total_Households_Worked": round(sum(vals["hh"]) / len(vals["hh"]), 2),
            "Total_Individuals_Worked": round(sum(vals["ind"]) / len(vals["ind"]), 2),
            "Persondays_of_Central_Liability_so_far": round(sum(vals["pers"]) / len(vals["pers"]), 2),
            "Wages": round(sum(vals["wages"]) / len(vals["wages"]), 2),
            "Average_days_of_employment_provided_per_Household": round(sum(vals["avg_days"]) / len(vals["avg_days"]), 2)
        })

    # --- Build latest summary ---
    summary = averaged[-1] if averaged else None
    if summary:
        summary["fin_year"] = fin_year

    return render_template("district.html",
                           state=state,
                           fin_year=fin_year,
                           district=district,
                           records=averaged,
                           summary=summary)



@app.route("/api/preview")
def api_preview():
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
    app.run(host="0.0.0.0", port=8080, debug=True)

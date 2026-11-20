"""Create the Metabase dashboard through the Metabase API.

Sets up the admin user (first run only), connects the warehouse, then creates the
questions and the dashboard. If the dashboard already exists it does nothing,
unless run with --recreate.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

MB_URL = os.environ.get("METABASE_URL", "http://localhost:3000")
EMAIL = os.environ.get("METABASE_ADMIN_EMAIL", "admin@citibike.local")
PASSWORD = os.environ.get("METABASE_ADMIN_PASSWORD", "Citibike-Admin-2025")
DB_NAME = "Citi Bike warehouse"
DASHBOARD_NAME = "Citi Bike trips and NYC weather"

INTRO = (
    "## Citi Bike trips and NYC weather\n"
    "Citi Bike trip data joined with daily weather for Central Park (Open-Meteo). "
    "Data is loaded monthly by the Airflow pipeline and modelled with dbt."
)

# name, description, display, sql, visualization settings, (col, row, width, height)
CARDS = [
    (
        "Total trips", "All trips in the loaded months.", "scalar",
        'select sum(trips) as "Total trips" from marts.mart_daily_rides',
        {}, (0, 2, 8, 3),
    ),
    (
        "Average trip length (minutes)", "Average trip duration across all trips.", "scalar",
        'select round(sum(avg_duration_min * trips) / sum(trips), 1) as "Minutes" from marts.mart_rider_mix',
        {}, (8, 2, 8, 3),
    ),
    (
        "E-bike share of trips (%)", "Share of trips made on electric bikes.", "scalar",
        "select round(100.0 * sum(trips) filter (where bike_type = 'electric_bike') / sum(trips), 1) "
        'as "Percent" from marts.mart_rider_mix',
        {}, (16, 2, 8, 3),
    ),
    (
        "Daily trips and temperature", "Trips per day (bars) and the mean temperature that day (line).", "combo",
        'select ride_date as "Date", trips as "Trips", temp_mean_c as "Mean temperature (°C)" '
        "from marts.mart_daily_rides order by ride_date",
        {
            "graph.dimensions": ["Date"],
            "graph.metrics": ["Trips", "Mean temperature (°C)"],
            "series_settings": {
                "Trips": {"display": "bar"},
                "Mean temperature (°C)": {"display": "line", "axis": "right"},
            },
            "graph.x_axis.title_text": "Date",
            "graph.y_axis.title_text": "Trips",
        },
        (0, 5, 24, 8),
    ),
    (
        "Trips by rider type and bike type", "Members vs casual riders, split by classic and electric bikes.", "bar",
        "select rider_type as \"Rider type\", replace(bike_type, '_', ' ') as \"Bike type\", trips as \"Trips\" "
        "from marts.mart_rider_mix order by 1, 2",
        {
            "graph.dimensions": ["Rider type", "Bike type"],
            "graph.metrics": ["Trips"],
            "stackable.stack_type": "stacked",
            "graph.x_axis.title_text": "Rider type",
            "graph.y_axis.title_text": "Trips",
        },
        (0, 13, 12, 8),
    ),
    (
        "Average trips per day by weather", "Days grouped by mean temperature, wet days (1 mm or more) vs dry days.", "bar",
        "select temp_band || ' °C' as \"Mean temperature\", day_type as \"Day\", avg_trips_per_day as \"Average trips per day\" "
        "from marts.mart_weather_impact order by temp_band_order, day_type",
        {
            "graph.dimensions": ["Mean temperature", "Day"],
            "graph.metrics": ["Average trips per day"],
            "graph.x_axis.title_text": "Mean temperature of the day",
            "graph.y_axis.title_text": "Average trips per day",
        },
        (12, 13, 12, 8),
    ),
    (
        "Trips by hour of day", "Average trips started in each hour, weekdays vs weekends.", "line",
        'select hour as "Hour of day", day_type as "Day type", avg_trips as "Average trips" '
        "from marts.mart_hourly_usage order by hour",
        {
            "graph.dimensions": ["Hour of day", "Day type"],
            "graph.metrics": ["Average trips"],
            "graph.x_axis.title_text": "Hour of day",
            "graph.y_axis.title_text": "Average trips started",
        },
        (0, 21, 12, 8),
    ),
    (
        "Top 10 start stations", "Stations where the most trips started.", "row",
        'select station_name as "Station", trips_started as "Trips started" '
        "from marts.mart_station_usage order by trips_started desc limit 10",
        {
            "graph.dimensions": ["Station"],
            "graph.metrics": ["Trips started"],
        },
        (12, 21, 12, 8),
    ),
]

session = None


def call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(MB_URL + path, data=data, method=method)
    request.add_header("Content-Type", "application/json")
    if session:
        request.add_header("X-Metabase-Session", session)
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            text = response.read().decode()
            return json.loads(text) if text else None
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{method} {path} -> {e.code}: {e.read().decode()[:500]}") from None


def as_list(result):
    # some endpoints return a list, newer ones return {"data": [...]}
    return result["data"] if isinstance(result, dict) else result


def wait_for_metabase():
    for _ in range(120):
        try:
            with urllib.request.urlopen(MB_URL + "/api/health", timeout=10) as response:
                if json.loads(response.read()).get("status") == "ok":
                    return
        except Exception:
            pass
        print("waiting for Metabase...")
        time.sleep(5)
    raise RuntimeError("Metabase did not start in 10 minutes")


def log_in():
    global session
    props = call("GET", "/api/session/properties")
    if props.get("setup-token") and not props.get("has-user-setup"):
        result = call("POST", "/api/setup", {
            "token": props["setup-token"],
            "user": {"email": EMAIL, "password": PASSWORD, "first_name": "Admin", "last_name": "Citibike"},
            "prefs": {"site_name": "Citi Bike pipeline"},
        })
        print("created the Metabase admin user")
    else:
        result = call("POST", "/api/session", {"username": EMAIL, "password": PASSWORD})
    session = result["id"]


def get_database():
    for db in as_list(call("GET", "/api/database")):
        if db["name"] == DB_NAME:
            return db["id"]

    db = call("POST", "/api/database", {
        "engine": "postgres",
        "name": DB_NAME,
        "details": {
            "host": os.environ.get("WAREHOUSE_HOST", "warehouse"),
            "port": int(os.environ.get("WAREHOUSE_PORT", "5432")),
            "dbname": os.environ.get("WAREHOUSE_DB", "warehouse"),
            "user": os.environ.get("WAREHOUSE_USER", "dw"),
            "password": os.environ.get("WAREHOUSE_PASSWORD", "dw"),
            "ssl": False,
            "schema-filters-type": "inclusion",
            "schema-filters-patterns": "core,marts",
        },
    })
    print(f"connected the warehouse to Metabase (database id {db['id']})")
    return db["id"]


def create_card(db_id, name, description, display, sql, settings):
    card = {
        "name": name,
        "description": description,
        "display": display,
        "visualization_settings": settings,
        "collection_id": None,
        "dataset_query": {"database": db_id, "type": "native", "native": {"query": sql}},
    }
    try:
        return call("POST", "/api/card", card)["id"]
    except RuntimeError as e:
        if "-> 400" not in str(e):
            raise
        # newer Metabase versions may want the new query format
        card["dataset_query"] = {
            "lib/type": "mbql/query",
            "database": db_id,
            "stages": [{"lib/type": "mbql.stage/native", "native": sql}],
        }
        return call("POST", "/api/card", card)["id"]


def main(recreate):
    wait_for_metabase()
    log_in()
    db_id = get_database()

    for dashboard in as_list(call("GET", "/api/dashboard")):
        if dashboard["name"] == DASHBOARD_NAME and not dashboard.get("archived"):
            if not recreate:
                print(f"dashboard already exists: http://localhost:3000/dashboard/{dashboard['id']}")
                return
            call("PUT", f"/api/dashboard/{dashboard['id']}", {"archived": True})
            print("archived the old dashboard")

    dashcards = [{
        "id": -1,
        "card_id": None,
        "row": 0, "col": 0, "size_x": 24, "size_y": 2,
        "parameter_mappings": [],
        "visualization_settings": {
            "virtual_card": {"name": None, "display": "text", "visualization_settings": {},
                             "dataset_query": {}, "archived": False},
            "text": INTRO,
        },
    }]
    for i, (name, description, display, sql, settings, (col, row, width, height)) in enumerate(CARDS):
        card_id = create_card(db_id, name, description, display, sql, settings)
        dashcards.append({
            "id": -(i + 2),
            "card_id": card_id,
            "row": row, "col": col, "size_x": width, "size_y": height,
            "parameter_mappings": [],
            "visualization_settings": {},
        })
        print(f"created question: {name}")

    dashboard_id = call("POST", "/api/dashboard", {
        "name": DASHBOARD_NAME,
        "description": "Citi Bike trips and NYC daily weather, loaded by the Airflow pipeline.",
    })["id"]
    call("PUT", f"/api/dashboard/{dashboard_id}", {"dashcards": dashcards, "width": "full"})
    print(f"dashboard ready: http://localhost:3000/dashboard/{dashboard_id}")


if __name__ == "__main__":
    main(recreate="--recreate" in sys.argv)

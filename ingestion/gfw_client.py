import requests
import os

# ─────────────────────────────────────────────
# WHY replace GFW?
# The GFW Data API (data-api.globalforestwatch.org) requires a key
# from their own developer portal — separate from Resource Watch keys.
# Getting one requires manual approval.
#
# Replacements used here:
#   • Overpass API (OpenStreetMap) → protected areas, free, no key
#   • Biome polygon lookup         → primary/tropical forest, offline
# ─────────────────────────────────────────────

# Rough bounding boxes of major tropical/primary forest biomes
# Source: WWF Biomes (simplified)
# Each entry: (min_lat, max_lat, min_lon, max_lon)
TROPICAL_FOREST_ZONES = [
    (-10, 10,   -80, -35),   # Amazon basin
    (-5,  5,     8,  30),    # Congo basin
    (-10, 10,   95, 141),    # SE Asia / Borneo / Papua
    (5,   20,   72,  100),   # South Asia forests
    (-30, 5,    10,  42),    # East/Central Africa
]

def _is_tropical_forest(lat: float, lon: float) -> bool:
    """
    Returns True if the coordinate falls inside a known
    tropical/primary forest biome zone.
    Fast, offline, no API needed.
    """
    for min_lat, max_lat, min_lon, max_lon in TROPICAL_FOREST_ZONES:
        if min_lat <= lat <= max_lat and min_lon <= lon <= max_lon:
            return True
    return False

def _query_protected_area(lat: float, lon: float, radius_km: float = 5.0) -> bool:
    radius_m = int(radius_km * 1000)
    overpass_url = "https://overpass-api.de/api/interpreter"

    query = f"""
    [out:json][timeout:15];
    (
      way["boundary"="protected_area"](around:{radius_m},{lat},{lon});
      relation["boundary"="protected_area"](around:{radius_m},{lat},{lon});
      way["leisure"="nature_reserve"](around:{radius_m},{lat},{lon});
      relation["leisure"="nature_reserve"](around:{radius_m},{lat},{lon});
    );
    out count;
    """

    try:
        resp = requests.post(overpass_url, data={"data": query}, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        elements = data.get("elements", [])
        if not elements:
            return False  # ← was crashing here before, returning None

        # Overpass returns total as a STRING, not int
        total = int(elements[0].get("tags", {}).get("total", "0"))
        return total > 0

    except requests.exceptions.Timeout:
        print(f"⚠️ Overpass timeout for ({lat}, {lon}) — marking as False")
        return False  # ← was returning None before
    except Exception as e:
        print(f"⚠️ Overpass query failed for ({lat}, {lon}): {e}")
        return False  # ← was returning None before


def get_forest_coverage(lat: float, lon: float, radius_km: float = 5.0) -> dict:
    """
    Returns forest/protected area data for a fire hotspot coordinate.

    Parameters:
        lat       → latitude of the fire hotspot
        lon       → longitude of the fire hotspot
        radius_km → area radius to check (default 5km)

    Returns dict with:
        forest_pct      → None (not available without GFW — kept for schema compatibility)
        primary_forest  → True if inside a known tropical forest biome
        protected_area  → True if inside an OSM-designated protected area
    """
    primary_forest = _is_tropical_forest(lat, lon)
    protected_area = _query_protected_area(lat, lon, radius_km)

    result = {
        'forest_pct': None,       # schema placeholder — needs GFW dev portal key to fill
        'primary_forest': primary_forest,
        'protected_area': protected_area,
    }

    print(f"🌲 Forest data for ({lat}, {lon}): {result}")
    return result
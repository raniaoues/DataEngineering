import requests
import os
from dotenv import load_dotenv

load_dotenv()

GFW_API_KEY = os.getenv("GFW_API_KEY")

GFW_BASE_URL= "https://data-api.globalforestwatch.org"

def get_forest_coverage(lat: float, lon:float, radius_km: float = 5.0) -> dict:
    """
    Queries GFW API for forest coverage around a given coordinate.

    Parameters:
        lat       → latitude of the fire hotspot
        lon       → longitude of the fire hotspot
        radius_km → area radius to analyze around the point (default 5km)

    Returns a dict with:
        forest_pct      → % of area covered by trees
        primary_forest  → True if primary/old-growth forest detected
        protected_area  → True if location is in a protected zone
        tree_cover_loss → hectares of tree cover lost (if available)

    """

    # Step 1: Build a GeoJSON polygon around the fire point
    # WHY GeoJSON?
    # The GFW API works with geographic areas, not just single points.
    # We create a small square around our fire coordinate.
    offset = radius_km / 111.0
    # WHY divide by 111?
    # 1 degree of latitude ≈ 111 km
    # So radius_km / 111 gives us the degree offset for our bounding box

    geojson = {
        "type": "Polygon",
        "coordinates": [[
            [lon - offset, lat - offset],  # bottom-left
            [lon + offset, lat - offset],  # bottom-right
            [lon + offset, lat + offset],  # top-right
            [lon - offset, lat + offset],  # top-left
            [lon - offset, lat - offset],  # close the polygon (same as first)
        ]]
        # WHY close the polygon?
        # GeoJSON spec requires the first and last coordinate to be identical
        # to explicitly "close" the shape
    }

    tree_cover_url = f"{GFW_BASE_URL}/dataset/umd_tree_cover_density_2000/latest/query"
    headers = {
        "x-api-key": GFW_API_KEY,
        # WHY x-api-key header?
        # GFW authenticates via HTTP headers, not URL parameters
        "Content-Type": "application/json"
    }

    payload = {
        "geometry": geojson,
        "sql": "SELECT SUM(area__ha) as total_area, "
               "SUM(umd_tree_cover_density_2000__30pct * area__ha) / SUM(area__ha) "
               "as tree_cover_pct "
               "FROM umd_tree_cover_density_2000"
        # WHY SQL inside the API call?
        # GFW Data API supports SQL-like queries on its datasets
        # This calculates the weighted average tree cover % for our polygon
    }

    try:
        response = requests.post(
            tree_cover_url,
            headers=headers,
            json=payload,
            timeout=10
            # WHY timeout=10?
            # If GFW doesn't respond in 10 seconds, we stop waiting
            # and return default values — better than hanging forever
        )
        response.raise_for_status()
        data = response.json()
        rows = data.get("data", [])
        forest_pct = rows[0].get("tree_cover_pct", 0) if rows else 0

    except requests.exceptions.RequestException as e:
        print(f" GFW tree cover query failed: {e}")
        forest_pct = None

    # Step 3: Query primary forest status
    primary_forest_url = f"{GFW_BASE_URL}/dataset/umd_regional_primary_forest_2001/latest/query"

    primary_payload = {
        "geometry": geojson,
        "sql": "SELECT SUM(area__ha) as primary_forest_area "
               "FROM umd_regional_primary_forest_2001"
    }

    try:
        primary_response = requests.post(
            primary_forest_url,
            headers=headers,
            json=primary_payload,
            timeout=10
        )
        primary_response.raise_for_status()
        primary_data = primary_response.json()
        primary_rows = primary_data.get("data", [])
        primary_forest_area = primary_rows[0].get("primary_forest_area", 0) if primary_rows else 0
        primary_forest = primary_forest_area > 0
        # WHY > 0?
        # If ANY primary forest area is detected in our zone,
        # we flag it as primary forest = True
    
    except requests.exceptions.RequestException as e:
        print(f"⚠️ GFW primary forest query failed: {e}")
        primary_forest = None


    # Step 4: Query protected areas via WDPA dataset
    protected_url = f"{GFW_BASE_URL}/dataset/wdpa_protected_areas/latest/query"

    protected_payload = {
        "geometry": geojson,
        "sql": "SELECT COUNT(*) as protected_count "
               "FROM wdpa_protected_areas "
               "WHERE status = 'Designated'"
        # WHY filter by 'Designated'?
        # Protected areas can have status: Designated, Proposed, Inscribed
        # Only 'Designated' means it's officially and legally protected
    }
    
    try:
        protected_response = requests.post(
            protected_url,
            headers=headers,
            json=protected_payload,
            timeout=10
        )
        protected_response.raise_for_status()
        protected_data = protected_response.json()
        protected_rows = protected_data.get("data", [])
        protected_count = protected_rows[0].get("protected_count", 0) if protected_rows else 0
        protected_area = protected_count > 0

    except requests.exceptions.RequestException as e:
        print(f"GFW protected areas query failed: {e}")
        protected_area = None


    # Step 5: Return all results as a clean dict
    result = {
        'forest_pct': round(float(forest_pct), 2) if forest_pct is not None else None,
        'primary_forest': primary_forest,
        'protected_area': protected_area,
    }

    print(f"🌲 Forest data for ({lat}, {lon}): {result}")
    return result
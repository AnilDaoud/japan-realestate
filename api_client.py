"""
Python client for Japan Real Estate Analytics API.
Can be used by Streamlit app or external scripts.
"""

import requests
from typing import List, Dict, Any, Optional
from functools import lru_cache

class APIClient:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url.rstrip("/")

    def _request(self, method: str, endpoint: str, **kwargs) -> Any:
        """Make an HTTP request to the API."""
        url = f"{self.base_url}{endpoint}"
        response = requests.request(method, url, **kwargs)
        response.raise_for_status()
        return response.json()

    def get(self, endpoint: str, **kwargs) -> Any:
        """GET request."""
        return self._request("GET", endpoint, **kwargs)

    # Reference Data
    def get_prefectures(self) -> List[Dict]:
        """Get all prefectures."""
        return self.get("/prefectures")

    def get_municipalities(self, prefecture_code: str) -> List[Dict]:
        """Get municipalities for a prefecture."""
        return self.get(f"/municipalities?prefecture_code={prefecture_code}")

    def get_districts(self, municipality_codes: str) -> List[Dict]:
        """Get districts for municipalities (comma-separated)."""
        return self.get(f"/districts?municipality_codes={municipality_codes}")

    def get_property_types(self) -> List[str]:
        """Get all property types."""
        return self.get("/property-types")

    def get_structures(self) -> List[str]:
        """Get all structure types."""
        return self.get("/structures")

    def get_floor_plans(self) -> List[str]:
        """Get all floor plans."""
        return self.get("/floor-plans")

    def get_year_range(self) -> Dict:
        """Get min/max transaction years."""
        return self.get("/year-range")

    def get_building_year_range(self) -> Dict:
        """Get min/max building years."""
        return self.get("/building-year-range")

    # Statistics
    def get_stats(self) -> List[Dict]:
        """Get stats by year and quarter."""
        return self.get("/stats")

    def get_stats_summary(self) -> Dict:
        """Get overall statistics."""
        return self.get("/stats/summary")

    # Transactions
    def get_transactions(
        self,
        prefecture_code: Optional[str] = None,
        municipality_codes: Optional[str] = None,
        districts: Optional[str] = None,
        property_types: Optional[str] = None,
        year_min: Optional[int] = None,
        year_max: Optional[int] = None,
        price_min: Optional[float] = None,
        price_max: Optional[float] = None,
        area_min: Optional[float] = None,
        area_max: Optional[float] = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> Dict:
        """Query transactions with filters."""
        params = {}
        if prefecture_code:
            params["prefecture_code"] = prefecture_code
        if municipality_codes:
            params["municipality_codes"] = municipality_codes
        if districts:
            params["districts"] = districts
        if property_types:
            params["property_types"] = property_types
        if year_min:
            params["year_min"] = year_min
        if year_max:
            params["year_max"] = year_max
        if price_min:
            params["price_min"] = price_min
        if price_max:
            params["price_max"] = price_max
        if area_min:
            params["area_min"] = area_min
        if area_max:
            params["area_max"] = area_max
        params["limit"] = limit
        params["offset"] = offset

        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        return self.get(f"/transactions?{query_string}")

    # Price Analysis
    def get_price_trends(
        self,
        prefecture_code: Optional[str] = None,
        municipality_codes: Optional[str] = None,
        districts: Optional[str] = None,
        property_types: Optional[str] = None,
        frequency: str = "Quarterly",
    ) -> List[Dict]:
        """Get historical price trends."""
        params = {"frequency": frequency}
        if prefecture_code:
            params["prefecture_code"] = prefecture_code
        if municipality_codes:
            params["municipality_codes"] = municipality_codes
        if districts:
            params["districts"] = districts
        if property_types:
            params["property_types"] = property_types

        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        return self.get(f"/price-trends?{query_string}")

    def get_median_price(
        self,
        prefecture_code: Optional[str] = None,
        municipality_codes: Optional[str] = None,
        property_types: Optional[str] = None,
    ) -> Dict:
        """Get latest median price."""
        params = {}
        if prefecture_code:
            params["prefecture_code"] = prefecture_code
        if municipality_codes:
            params["municipality_codes"] = municipality_codes
        if property_types:
            params["property_types"] = property_types

        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        return self.get(f"/median-price?{query_string}")

    def get_price_by_district(
        self,
        prefecture_code: Optional[str] = None,
        municipality_codes: Optional[str] = None,
        property_types: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict]:
        """Get median prices by district."""
        params = {"limit": limit}
        if prefecture_code:
            params["prefecture_code"] = prefecture_code
        if municipality_codes:
            params["municipality_codes"] = municipality_codes
        if property_types:
            params["property_types"] = property_types

        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        return self.get(f"/price-by-district?{query_string}")

    def health_check(self) -> Dict:
        """Check API health."""
        return self.get("/health")

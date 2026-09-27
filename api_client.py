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
        return self.get("/municipalities", params={"prefecture_code": prefecture_code})

    def get_districts(self, municipality_codes: str) -> List[Dict]:
        """Get districts for municipalities (comma-separated)."""
        return self.get("/districts", params={"municipality_codes": municipality_codes})

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
        *,
        structures: Optional[str] = None,
        floor_plans: Optional[str] = None,
        building_year_min: Optional[int] = None,
        building_year_max: Optional[int] = None,
        unit_price_min: Optional[float] = None,
        unit_price_max: Optional[float] = None,
    ) -> Dict:
        """Query transactions with filters."""
        params: Dict[str, Any] = {}
        if prefecture_code:
            params["prefecture_code"] = prefecture_code
        if municipality_codes:
            params["municipality_codes"] = municipality_codes
        if districts:
            params["districts"] = districts
        if property_types:
            params["property_types"] = property_types
        if structures:
            params["structures"] = structures
        if floor_plans:
            params["floor_plans"] = floor_plans
        if year_min is not None:
            params["year_min"] = year_min
        if year_max is not None:
            params["year_max"] = year_max
        if price_min is not None:
            params["price_min"] = price_min
        if price_max is not None:
            params["price_max"] = price_max
        if area_min is not None:
            params["area_min"] = area_min
        if area_max is not None:
            params["area_max"] = area_max
        if building_year_min is not None:
            params["building_year_min"] = building_year_min
        if building_year_max is not None:
            params["building_year_max"] = building_year_max
        if unit_price_min is not None:
            params["unit_price_min"] = unit_price_min
        if unit_price_max is not None:
            params["unit_price_max"] = unit_price_max
        params["limit"] = limit
        params["offset"] = offset

        return self.get("/transactions", params=params)

    # Price Analysis
    def get_price_trends(
        self,
        prefecture_code: Optional[str] = None,
        municipality_codes: Optional[str] = None,
        districts: Optional[str] = None,
        property_types: Optional[str] = None,
        frequency: str = "Quarterly",
        *,
        structures: Optional[str] = None,
        floor_plans: Optional[str] = None,
        year_min: Optional[int] = None,
        year_max: Optional[int] = None,
        price_min: Optional[float] = None,
        price_max: Optional[float] = None,
        area_min: Optional[float] = None,
        area_max: Optional[float] = None,
        building_year_min: Optional[int] = None,
        building_year_max: Optional[int] = None,
        unit_price_min: Optional[float] = None,
        unit_price_max: Optional[float] = None,
    ) -> List[Dict]:
        """Get historical price trends."""
        params: Dict[str, Any] = {"frequency": frequency}
        if prefecture_code:
            params["prefecture_code"] = prefecture_code
        if municipality_codes:
            params["municipality_codes"] = municipality_codes
        if districts:
            params["districts"] = districts
        if property_types:
            params["property_types"] = property_types
        if structures:
            params["structures"] = structures
        if floor_plans:
            params["floor_plans"] = floor_plans
        if year_min is not None:
            params["year_min"] = year_min
        if year_max is not None:
            params["year_max"] = year_max
        if price_min is not None:
            params["price_min"] = price_min
        if price_max is not None:
            params["price_max"] = price_max
        if area_min is not None:
            params["area_min"] = area_min
        if area_max is not None:
            params["area_max"] = area_max
        if building_year_min is not None:
            params["building_year_min"] = building_year_min
        if building_year_max is not None:
            params["building_year_max"] = building_year_max
        if unit_price_min is not None:
            params["unit_price_min"] = unit_price_min
        if unit_price_max is not None:
            params["unit_price_max"] = unit_price_max

        return self.get("/price-trends", params=params)

    def get_median_price(
        self,
        prefecture_code: Optional[str] = None,
        municipality_codes: Optional[str] = None,
        property_types: Optional[str] = None,
        *,
        districts: Optional[str] = None,
        structures: Optional[str] = None,
        floor_plans: Optional[str] = None,
        year_min: Optional[int] = None,
        year_max: Optional[int] = None,
        price_min: Optional[float] = None,
        price_max: Optional[float] = None,
        area_min: Optional[float] = None,
        area_max: Optional[float] = None,
        building_year_min: Optional[int] = None,
        building_year_max: Optional[int] = None,
        unit_price_min: Optional[float] = None,
        unit_price_max: Optional[float] = None,
    ) -> Dict:
        """Get latest median price."""
        params: Dict[str, Any] = {}
        if prefecture_code:
            params["prefecture_code"] = prefecture_code
        if municipality_codes:
            params["municipality_codes"] = municipality_codes
        if districts:
            params["districts"] = districts
        if property_types:
            params["property_types"] = property_types
        if structures:
            params["structures"] = structures
        if floor_plans:
            params["floor_plans"] = floor_plans
        if year_min is not None:
            params["year_min"] = year_min
        if year_max is not None:
            params["year_max"] = year_max
        if price_min is not None:
            params["price_min"] = price_min
        if price_max is not None:
            params["price_max"] = price_max
        if area_min is not None:
            params["area_min"] = area_min
        if area_max is not None:
            params["area_max"] = area_max
        if building_year_min is not None:
            params["building_year_min"] = building_year_min
        if building_year_max is not None:
            params["building_year_max"] = building_year_max
        if unit_price_min is not None:
            params["unit_price_min"] = unit_price_min
        if unit_price_max is not None:
            params["unit_price_max"] = unit_price_max

        return self.get("/median-price", params=params)

    def get_price_by_district(
        self,
        prefecture_code: Optional[str] = None,
        municipality_codes: Optional[str] = None,
        property_types: Optional[str] = None,
        limit: int = 50,
        *,
        districts: Optional[str] = None,
        structures: Optional[str] = None,
        floor_plans: Optional[str] = None,
        year_min: Optional[int] = None,
        year_max: Optional[int] = None,
        price_min: Optional[float] = None,
        price_max: Optional[float] = None,
        area_min: Optional[float] = None,
        area_max: Optional[float] = None,
        building_year_min: Optional[int] = None,
        building_year_max: Optional[int] = None,
        unit_price_min: Optional[float] = None,
        unit_price_max: Optional[float] = None,
    ) -> List[Dict]:
        """Get median prices by district."""
        params: Dict[str, Any] = {"limit": limit}
        if prefecture_code:
            params["prefecture_code"] = prefecture_code
        if municipality_codes:
            params["municipality_codes"] = municipality_codes
        if districts:
            params["districts"] = districts
        if property_types:
            params["property_types"] = property_types
        if structures:
            params["structures"] = structures
        if floor_plans:
            params["floor_plans"] = floor_plans
        if year_min is not None:
            params["year_min"] = year_min
        if year_max is not None:
            params["year_max"] = year_max
        if price_min is not None:
            params["price_min"] = price_min
        if price_max is not None:
            params["price_max"] = price_max
        if area_min is not None:
            params["area_min"] = area_min
        if area_max is not None:
            params["area_max"] = area_max
        if building_year_min is not None:
            params["building_year_min"] = building_year_min
        if building_year_max is not None:
            params["building_year_max"] = building_year_max
        if unit_price_min is not None:
            params["unit_price_min"] = unit_price_min
        if unit_price_max is not None:
            params["unit_price_max"] = unit_price_max

        return self.get("/price-by-district", params=params)

    def health_check(self) -> Dict:
        """Check API health."""
        return self.get("/health")

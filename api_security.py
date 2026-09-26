"""
Security and monitoring for Japan Real Estate API.
Includes: API key auth, rate limiting, request logging, usage metrics.
"""

import time
import logging
from functools import wraps
from typing import Optional
from collections import defaultdict
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.security import APIKeyHeader

# =============================================================================
# LOGGING SETUP
# =============================================================================

# Configure detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler('/var/log/japan-realestate/api.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# =============================================================================
# API KEY MANAGEMENT
# =============================================================================

# In production, load these from environment or secure vault
VALID_API_KEYS = {
    "demo-key-123": {"name": "Demo User", "quota_per_hour": 1000},
    "claude-agent": {"name": "Claude Agent", "quota_per_hour": 5000},
    "streamlit-app": {"name": "Streamlit Dashboard", "quota_per_hour": 2000},
}

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

async def verify_api_key(api_key: Optional[str] = Depends(api_key_header)) -> str:
    """Verify API key from request header."""
    if not api_key:
        raise HTTPException(status_code=403, detail="API key required in X-API-Key header")

    if api_key not in VALID_API_KEYS:
        logger.warning(f"Invalid API key attempted: {api_key[:10]}...")
        raise HTTPException(status_code=401, detail="Invalid API key")

    return api_key

# =============================================================================
# RATE LIMITING
# =============================================================================

class RateLimiter:
    """Track requests per API key and enforce limits."""

    def __init__(self):
        self.requests = defaultdict(list)  # api_key -> [(timestamp, endpoint), ...]
        self.cleanup_interval = 3600  # Clean old entries every hour
        self.last_cleanup = time.time()

    def is_allowed(self, api_key: str, limit: int, window_seconds: int = 3600) -> bool:
        """Check if request is within rate limit."""
        now = time.time()
        cutoff = now - window_seconds

        # Clean old entries
        if now - self.last_cleanup > self.cleanup_interval:
            for key in self.requests:
                self.requests[key] = [(ts, ep) for ts, ep in self.requests[key] if ts > cutoff]
            self.last_cleanup = now

        # Count recent requests
        recent = [ts for ts, _ in self.requests[api_key] if ts > cutoff]

        if len(recent) >= limit:
            return False

        return True

    def record_request(self, api_key: str, endpoint: str):
        """Record a request."""
        self.requests[api_key].append((time.time(), endpoint))

rate_limiter = RateLimiter()

# =============================================================================
# REQUEST/RESPONSE LOGGING
# =============================================================================

async def log_request(request: Request, api_key: str, status_code: int, response_time_ms: float):
    """Log API request with details."""
    user_info = VALID_API_KEYS.get(api_key, {}).get("name", "Unknown")

    logger.info(
        f"REQUEST | "
        f"method={request.method} | "
        f"path={request.url.path} | "
        f"client_ip={request.client.host} | "
        f"api_key={api_key} ({user_info}) | "
        f"status={status_code} | "
        f"response_time={response_time_ms:.0f}ms"
    )

async def log_error(request: Request, api_key: str, error: str, status_code: int):
    """Log API errors."""
    user_info = VALID_API_KEYS.get(api_key, {}).get("name", "Unknown")

    logger.warning(
        f"ERROR | "
        f"method={request.method} | "
        f"path={request.url.path} | "
        f"client_ip={request.client.host} | "
        f"api_key={api_key} ({user_info}) | "
        f"status={status_code} | "
        f"error={error}"
    )

# =============================================================================
# MIDDLEWARE FOR RATE LIMITING & LOGGING
# =============================================================================

def add_security_middleware(app: FastAPI):
    """Add rate limiting and logging middleware to FastAPI app."""

    @app.middleware("http")
    async def rate_limit_and_log_middleware(request: Request, call_next):
        start_time = time.time()

        # Skip middleware for docs endpoints
        if request.url.path in ["/docs", "/redoc", "/openapi.json", "/health"]:
            return await call_next(request)

        # Get API key
        api_key = request.headers.get("X-API-Key")

        if not api_key:
            await log_error(request, "none", "Missing API key", 403)
            return {
                "detail": "API key required in X-API-Key header",
                "example": "curl -H 'X-API-Key: your-key' http://localhost:8000/..."
            }

        if api_key not in VALID_API_KEYS:
            await log_error(request, api_key, "Invalid API key", 401)
            return {"detail": "Invalid API key"}

        # Check rate limit
        key_config = VALID_API_KEYS[api_key]
        quota = key_config.get("quota_per_hour", 1000)

        if not rate_limiter.is_allowed(api_key, quota, window_seconds=3600):
            await log_error(request, api_key, f"Rate limit exceeded ({quota}/hour)", 429)
            return {"detail": f"Rate limit exceeded. Quota: {quota} requests/hour"}

        # Record request
        rate_limiter.record_request(api_key, request.url.path)

        # Call endpoint
        try:
            response = await call_next(request)
            process_time = (time.time() - start_time) * 1000

            await log_request(request, api_key, response.status_code, process_time)

            # Add custom headers
            response.headers["X-Process-Time"] = str(process_time)
            response.headers["X-RateLimit-Limit"] = str(quota)

            return response

        except Exception as e:
            await log_error(request, api_key, str(e), 500)
            raise

# =============================================================================
# USAGE METRICS & REPORTING
# =============================================================================

class UsageMetrics:
    """Track API usage for reporting."""

    def __init__(self):
        self.metrics = defaultdict(lambda: {
            "requests": 0,
            "errors": 0,
            "total_response_time": 0,
        })

    def record(self, api_key: str, success: bool, response_time: float):
        """Record a request."""
        self.metrics[api_key]["requests"] += 1
        if not success:
            self.metrics[api_key]["errors"] += 1
        self.metrics[api_key]["total_response_time"] += response_time

    def get_summary(self, api_key: Optional[str] = None) -> dict:
        """Get usage summary."""
        if api_key:
            m = self.metrics[api_key]
            return {
                "api_key": api_key,
                "user": VALID_API_KEYS.get(api_key, {}).get("name"),
                "requests": m["requests"],
                "errors": m["errors"],
                "avg_response_time_ms": (
                    m["total_response_time"] / m["requests"] if m["requests"] > 0 else 0
                ),
                "error_rate": (
                    m["errors"] / m["requests"] if m["requests"] > 0 else 0
                ),
            }

        # All keys
        summary = {}
        for key in self.metrics:
            summary[key] = self.get_summary(key)
        return summary

metrics = UsageMetrics()

# =============================================================================
# ADMIN ENDPOINTS (require special auth)
# =============================================================================

ADMIN_KEY = "admin-secret-key-change-in-production"

async def verify_admin(api_key: Optional[str] = Depends(api_key_header)) -> str:
    """Verify admin key."""
    if api_key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="Admin key required")
    return api_key

def add_admin_endpoints(app: FastAPI):
    """Add admin monitoring endpoints."""

    @app.get("/admin/metrics", dependencies=[Depends(verify_admin)])
    async def get_metrics(api_key: Optional[str] = None):
        """Get usage metrics for all or specific API key."""
        return metrics.get_summary(api_key)

    @app.get("/admin/status", dependencies=[Depends(verify_admin)])
    async def get_status():
        """Get API health and status."""
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "active_keys": len([k for k in VALID_API_KEYS if metrics.metrics[k]["requests"] > 0]),
            "total_requests": sum(m["requests"] for m in metrics.metrics.values()),
            "total_errors": sum(m["errors"] for m in metrics.metrics.values()),
        }

    @app.post("/admin/keys/add", dependencies=[Depends(verify_admin)])
    async def add_api_key(
        name: str,
        quota_per_hour: int = 1000
    ):
        """Add a new API key."""
        import secrets
        new_key = f"key_{secrets.token_urlsafe(16)}"
        VALID_API_KEYS[new_key] = {
            "name": name,
            "quota_per_hour": quota_per_hour,
            "created_at": datetime.now().isoformat(),
        }
        logger.info(f"New API key created: {name} ({new_key})")
        return {"api_key": new_key, "name": name, "quota_per_hour": quota_per_hour}

    @app.post("/admin/keys/revoke", dependencies=[Depends(verify_admin)])
    async def revoke_api_key(api_key: str):
        """Revoke an API key."""
        if api_key in VALID_API_KEYS:
            name = VALID_API_KEYS[api_key]["name"]
            del VALID_API_KEYS[api_key]
            logger.warning(f"API key revoked: {name} ({api_key})")
            return {"message": f"API key revoked: {name}"}
        raise HTTPException(status_code=404, detail="API key not found")

# =============================================================================
# NGINX CONFIGURATION HELPER
# =============================================================================

NGINX_CONFIG = """
# Add to your nginx configuration for rate limiting and security

# Rate limiting
limit_req_zone $binary_remote_addr zone=api_limit:10m rate=10r/s;
limit_req_zone $http_x_api_key zone=api_key_limit:10m rate=100r/s;

server {
    listen 443 ssl;
    server_name api.realestate.example.com;

    # SSL certificates
    ssl_certificate /etc/letsencrypt/live/api.realestate.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/api.realestate.example.com/privkey.pem;

    # Security headers
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-Frame-Options "DENY" always;
    add_header X-XSS-Protection "1; mode=block" always;
    add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;

    # Rate limiting per IP
    limit_req zone=api_limit burst=20 nodelay;

    # Rate limiting per API key
    limit_req zone=api_key_limit burst=200 nodelay;

    # Require API key header
    location / {
        if ($http_x_api_key = "") {
            return 403;
        }

        proxy_pass http://localhost:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # Timeouts
        proxy_connect_timeout 10s;
        proxy_send_timeout 30s;
        proxy_read_timeout 30s;
    }

    # Allow health check without key
    location /health {
        proxy_pass http://localhost:8000;
    }

    # Allow docs without key (optional)
    location /docs {
        proxy_pass http://localhost:8000;
    }
}
"""

print("NGINX Configuration helper:")
print(NGINX_CONFIG)

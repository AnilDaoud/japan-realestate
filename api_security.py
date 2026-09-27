"""
Rate limiting for Japan Real Estate API.
Public API - no authentication required, IP-based rate limiting only.
"""

import time
import os
import logging
import ipaddress
from collections import defaultdict
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

# =============================================================================
# LOGGING SETUP
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

# =============================================================================
# RATE LIMITING (IP-based, no authentication)
# =============================================================================

request_counts = defaultdict(list)  # ip -> [timestamp, timestamp, ...]

RATE_LIMIT_PER_MINUTE = 100  # 100 requests per minute per IP
RATE_LIMIT_WINDOW = 60  # 1 minute window (seconds)


def check_rate_limit(client_ip: str) -> tuple[bool, dict]:
    """
    Check if client IP is within rate limit.
    Returns: (is_allowed, headers)
    """
    now = time.time()
    cutoff = now - RATE_LIMIT_WINDOW

    # Clean old requests
    request_counts[client_ip] = [
        ts for ts in request_counts[client_ip] if ts > cutoff
    ]

    # Clean up empty IP keys to prevent unbounded memory growth
    for ip in [k for k, v in request_counts.items() if not v]:
        del request_counts[ip]

    # Count recent requests
    recent_count = len(request_counts[client_ip])

    # Check limit
    if recent_count >= RATE_LIMIT_PER_MINUTE:
        reset_time = min(request_counts[client_ip]) + RATE_LIMIT_WINDOW
        headers = {
            "X-RateLimit-Limit": str(RATE_LIMIT_PER_MINUTE),
            "X-RateLimit-Remaining": "0",
            "X-RateLimit-Reset": str(int(reset_time)),
        }
        return False, headers

    # Record this request
    request_counts[client_ip].append(now)

    # Return remaining
    headers = {
        "X-RateLimit-Limit": str(RATE_LIMIT_PER_MINUTE),
        "X-RateLimit-Remaining": str(RATE_LIMIT_PER_MINUTE - recent_count - 1),
        "X-RateLimit-Reset": str(int(cutoff + RATE_LIMIT_WINDOW)),
    }

    return True, headers


def add_rate_limit_middleware(app: FastAPI):
    """Add IP-based rate limiting middleware."""

    @app.middleware("http")
    async def rate_limit_middleware(request: Request, call_next):
        # Skip health check
        if request.url.path == "/health":
            return await call_next(request)

        # Get client IP. Only trust proxy headers from a known reverse proxy;
        # otherwise fall back to the direct connection address so spoofed
        # X-Forwarded-For / X-Real-IP headers cannot bypass rate limiting.
        # Set TRUSTED_PROXY_IPS (comma-separated) to the reverse proxy address(es).
        trusted_proxies = {
            ip.strip()
            for ip in os.environ.get("TRUSTED_PROXY_IPS", "").split(",")
            if ip.strip()
        }
        client_host = request.client.host if request.client else "unknown"

        if trusted_proxies and client_host in trusted_proxies:
            # The trusted proxy MUST overwrite/sanitize X-Forwarded-For (not
            # blindly append), otherwise a spoofed first token could still
            # evade rate limiting.
            def _valid_ip(value):
                # Accept both IPv4 and IPv6; reject anything that is not a
                # parseable address (e.g. spoofed hostnames or garbage).
                try:
                    ipaddress.ip_address(value)
                    return True
                except ValueError:
                    return False

            forwarded = request.headers.get("x-forwarded-for")
            if forwarded:
                first = forwarded.split(",")[0].strip()
                # Validate the first token is a plausible IP before trusting it.
                if first and _valid_ip(first):
                    client_ip = first
                else:
                    client_ip = client_host
            else:
                real_ip = request.headers.get("x-real-ip")
                client_ip = real_ip.strip() if real_ip and _valid_ip(real_ip.strip()) else client_host
        else:
            client_ip = client_host

        # Check rate limit
        is_allowed, headers = check_rate_limit(client_ip)

        if not is_allowed:
            logger.warning(f"Rate limit exceeded for {client_ip}")
            return JSONResponse(
                status_code=429,
                content={"detail": f"Rate limit exceeded: {RATE_LIMIT_PER_MINUTE} requests per minute per IP"},
                headers=headers,
            )

        # Call endpoint
        response = await call_next(request)

        # Add rate limit headers
        for key, value in headers.items():
            response.headers[key] = value

        return response

# API Security & Monitoring Guide

Since the API is exposed via nginx on the public internet, proper security and monitoring are critical. This guide covers authentication, rate limiting, logging, and abuse prevention.

## Quick Start

### 1. Enable Security (Production)

Set environment variables when deploying:

```bash
export REQUIRE_API_KEY=true
export CORS_ORIGINS="https://yourapp.com,https://agent.example.com"
export ADMIN_KEY="your-secure-admin-key"
```

Then restart:
```bash
docker compose restart api
```

### 2. Issue API Keys

Get an admin key first:
```bash
ADMIN_KEY="your-admin-key"

# Create a new API key for Claude agent
curl -X POST http://localhost:8000/admin/keys/add \
  -H "X-API-Key: $ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name": "Claude Agent", "quota_per_hour": 5000}'
```

Response:
```json
{
  "api_key": "key_abc123xyz...",
  "name": "Claude Agent",
  "quota_per_hour": 5000
}
```

### 3. Use the API Key

Every request must include the key:

```bash
curl -H "X-API-Key: key_abc123xyz..." http://localhost:8000/transactions?limit=10
```

Python client:
```python
from api_client import APIClient

client = APIClient("http://localhost:8000")
client.default_api_key = "key_abc123xyz..."
```

---

## Security Layers

### Layer 1: API Keys (Authentication)

**What it does**: Only known clients can access the API.

**Setup**:

```bash
# Pre-configured keys for demo
VALID_API_KEYS = {
    "demo-key-123": {"name": "Demo", "quota_per_hour": 1000},
    "claude-agent": {"name": "Claude", "quota_per_hour": 5000},
    "streamlit-app": {"name": "Dashboard", "quota_per_hour": 2000},
}
```

**In production**, load from environment or vault:

```python
# api_security.py - update VALID_API_KEYS
import os
import json

VALID_API_KEYS = json.loads(os.getenv("API_KEYS", "{}"))
```

**Required header on every request:**
```bash
X-API-Key: your-api-key
```

### Layer 2: Rate Limiting (Throttling)

**What it does**: Prevents abuse by limiting requests per user.

**Current limits (per API key, per hour)**:
- Demo: 1,000 requests
- Claude Agent: 5,000 requests
- Streamlit Dashboard: 2,000 requests

**Enforcement**:
```python
# api_security.py - RateLimiter class
if not rate_limiter.is_allowed(api_key, quota, window_seconds=3600):
    return HTTP 429 (Too Many Requests)
```

**To change limits**:
```bash
# Edit api_security.py VALID_API_KEYS dict, then restart
docker compose restart api
```

### Layer 3: nginx (Web Server)

**What it does**: IP-based rate limiting, SSL/TLS, request filtering.

**Configuration** (add to your nginx file):

```nginx
# Rate limit per IP: 10 requests/second
limit_req_zone $binary_remote_addr zone=api_limit:10m rate=10r/s;

# Rate limit per API key: 100 requests/second (for burst traffic)
limit_req_zone $http_x_api_key zone=api_key_limit:10m rate=100r/s;

server {
    listen 443 ssl;
    server_name api.yourdomain.com;

    # Security headers
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-Frame-Options "DENY" always;
    add_header X-XSS-Protection "1; mode=block" always;
    add_header Strict-Transport-Security "max-age=31536000" always;

    # Rate limiting
    limit_req zone=api_limit burst=20 nodelay;
    limit_req zone=api_key_limit burst=200 nodelay;

    # Require API key on all endpoints
    location / {
        if ($http_x_api_key = "") {
            return 403;
        }
        proxy_pass http://localhost:8000;
    }

    # Allow health check without key
    location /health {
        proxy_pass http://localhost:8000;
    }
}
```

### Layer 4: Request Validation

**What it does**: Validates input parameters to prevent injection attacks.

FastAPI + Pydantic automatically validates:
- Query parameter types
- String length limits
- Numeric ranges
- Enum values

Example:
```python
@app.get("/transactions")
def get_transactions(
    limit: int = Query(1000, le=10000),  # max 10,000
    offset: int = Query(0, ge=0),        # non-negative
):
    # limit and offset are validated before reaching this function
```

---

## Monitoring & Logging

### Log Files

**Location**: `/var/log/japan-realestate/api.log`

**Format**:
```
2026-09-26 12:34:56,789 | api | INFO | REQUEST | method=GET | path=/transactions | client_ip=192.168.1.1 | api_key=claude-agent (Claude Agent) | status=200 | response_time=42ms
```

**What's logged**:
- All requests (method, path, client IP, API key, status, response time)
- All errors (with details)
- API key creation/revocation (admin actions)
- Rate limit violations

**Monitor logs in real-time**:
```bash
tail -f /var/log/japan-realestate/api.log

# Filter for errors only
grep ERROR /var/log/japan-realestate/api.log

# Filter for rate limit violations
grep "Rate limit" /var/log/japan-realestate/api.log
```

### Usage Metrics

**Get metrics** (requires admin key):

```bash
ADMIN_KEY="your-admin-key"

# Overall metrics
curl -H "X-API-Key: $ADMIN_KEY" http://localhost:8000/admin/metrics

# Metrics for specific API key
curl -H "X-API-Key: $ADMIN_KEY" "http://localhost:8000/admin/metrics?api_key=claude-agent"
```

Response:
```json
{
  "claude-agent": {
    "user": "Claude Agent",
    "requests": 1523,
    "errors": 2,
    "avg_response_time_ms": 145.3,
    "error_rate": 0.0013
  }
}
```

### Health Check

```bash
# No API key required
curl http://localhost:8000/health
```

Response:
```json
{
  "status": "healthy",
  "timestamp": "2026-09-26T12:34:56.789Z"
}
```

### Status Dashboard

```bash
ADMIN_KEY="your-admin-key"
curl -H "X-API-Key: $ADMIN_KEY" http://localhost:8000/admin/status
```

Response:
```json
{
  "status": "healthy",
  "timestamp": "2026-09-26T12:34:56.789Z",
  "active_keys": 3,
  "total_requests": 5423,
  "total_errors": 15
}
```

---

## Abuse Prevention Checklist

### ✅ Do These

1. **Use strong API keys**
   ```bash
   # Generate with: python -c "import secrets; print(secrets.token_urlsafe(32))"
   ```

2. **Rotate keys regularly**
   ```bash
   # Every 30-90 days
   curl -X POST -H "X-API-Key: $ADMIN_KEY" http://localhost:8000/admin/keys/add \
     -d '{"name": "Claude Agent v2", "quota_per_hour": 5000}'
   
   # Revoke old key
   curl -X POST -H "X-API-Key: $ADMIN_KEY" http://localhost:8000/admin/keys/revoke \
     -d '{"api_key": "old-key-to-revoke"}'
   ```

3. **Monitor rate limits**
   ```bash
   # Alert if any key exceeds 80% of quota
   curl -H "X-API-Key: $ADMIN_KEY" http://localhost:8000/admin/metrics | \
     grep -E "requests.*8[0-9][0-9]|9[0-9][0-9]"
   ```

4. **Set appropriate quotas**
   - Claude Agent: 5,000/hour (internal use)
   - Public agents: 1,000/hour
   - Demo users: 100/hour

5. **Use HTTPS everywhere**
   - nginx must serve via SSL/TLS
   - API keys never sent over HTTP

6. **Log aggregation** (optional)
   - Send logs to Datadog, LogRocket, or ELK
   - Set up alerts for error spikes

### ❌ Don't Do These

1. **Don't expose API keys in code**
   ```bash
   # Bad
   curl http://localhost:8000/transactions?api_key=secret
   
   # Good
   curl -H "X-API-Key: secret" http://localhost:8000/transactions
   ```

2. **Don't use same key for multiple services**
   - Each client (Claude, Streamlit, etc.) gets its own key
   - Easier to revoke if compromised

3. **Don't disable CORS** (it's set to "*" by default in dev)
   ```bash
   # In production
   export CORS_ORIGINS="https://app.example.com,https://agent.example.com"
   ```

4. **Don't log sensitive data**
   - API keys are logged truncated: `key_abc123...`
   - Query parameters are logged (could contain PII)

5. **Don't have unlimited quotas**
   - Always set `quota_per_hour` for every key
   - Set lower for untrusted clients

---

## Responding to Abuse

### If a key is compromised:

```bash
ADMIN_KEY="your-admin-key"

# Immediately revoke
curl -X POST -H "X-API-Key: $ADMIN_KEY" \
  http://localhost:8000/admin/keys/revoke \
  -d '{"api_key": "compromised-key"}'

# Check usage before revoke
curl -H "X-API-Key: $ADMIN_KEY" \
  "http://localhost:8000/admin/metrics?api_key=compromised-key"

# Audit logs for suspicious activity
grep "compromised-key" /var/log/japan-realestate/api.log
```

### If rate limit is being exceeded:

```bash
# Check which key exceeded limit
grep "Rate limit" /var/log/japan-realestate/api.log | tail -20

# Lower quota or investigate usage
curl -H "X-API-Key: $ADMIN_KEY" http://localhost:8000/admin/metrics
```

### If seeing unexpected errors:

```bash
# Check error logs
grep ERROR /var/log/japan-realestate/api.log

# Get detailed metrics
curl -H "X-API-Key: $ADMIN_KEY" http://localhost:8000/admin/metrics
```

---

## Production Deployment Checklist

Before going live on public internet:

- [ ] Set `REQUIRE_API_KEY=true`
- [ ] Set `CORS_ORIGINS` to specific domains only
- [ ] Change `ADMIN_KEY` from default
- [ ] Generate strong API keys for all clients
- [ ] Set appropriate rate limits for each key
- [ ] Configure nginx with SSL/TLS
- [ ] Add nginx rate limiting rules
- [ ] Set up log rotation (see below)
- [ ] Configure monitoring/alerting
- [ ] Document API key rotation process
- [ ] Brief team on security procedures

### Log Rotation

Add to `/etc/logrotate.d/japan-realestate`:

```
/var/log/japan-realestate/api.log {
    daily
    rotate 30
    compress
    delaycompress
    notifempty
    missingok
    su www-data www-data
}
```

Then:
```bash
sudo logrotate -f /etc/logrotate.d/japan-realestate
```

---

## API Key Management Examples

### Create key for new Claude agent

```bash
curl -X POST http://localhost:8000/admin/keys/add \
  -H "X-API-Key: $ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Claude Agent - Production",
    "quota_per_hour": 10000
  }'
```

### Create key for public demo

```bash
curl -X POST http://localhost:8000/admin/keys/add \
  -H "X-API-Key: $ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Public Demo (limited)",
    "quota_per_hour": 100
  }'
```

### List all active keys (from logs)

```bash
grep "New API key created" /var/log/japan-realestate/api.log
```

### Revoke a key

```bash
curl -X POST http://localhost:8000/admin/keys/revoke \
  -H "X-API-Key: $ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"api_key": "key_to_revoke"}'
```

---

## Architecture Summary

```
Internet Users / AI Agents
    ↓
nginx (SSL/TLS, Rate Limit, Security Headers)
    ↓
FastAPI (API Key Auth, Rate Limit, Request Validation)
    ↓
PostgreSQL (Database)

With Logging:
    → /var/log/japan-realestate/api.log
    → /admin/metrics (metrics endpoint)
    → /admin/status (health endpoint)
```

---

## Support

For security issues, audit the logs and check metrics:

```bash
# Full diagnostic
echo "=== Recent errors ==="
tail -20 /var/log/japan-realestate/api.log | grep ERROR

echo "=== Usage metrics ==="
curl -H "X-API-Key: $ADMIN_KEY" http://localhost:8000/admin/metrics

echo "=== System status ==="
curl -H "X-API-Key: $ADMIN_KEY" http://localhost:8000/admin/status
```

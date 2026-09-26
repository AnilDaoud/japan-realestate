# Security Quick Reference

## Enable Security (one-time setup)

```bash
# Set environment variables
export REQUIRE_API_KEY=true
export ADMIN_KEY="super-secret-admin-key"
export CORS_ORIGINS="https://app.example.com"

# Restart API
docker compose restart api

# Check it's working
curl -i http://localhost:8000/health  # Should work (no key needed)
curl -i http://localhost:8000/transactions  # Should fail with 403
```

## Create API Keys

```bash
ADMIN_KEY="super-secret-admin-key"

# New key for Claude agent (5000 req/hr)
curl -X POST http://localhost:8000/admin/keys/add \
  -H "X-API-Key: $ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name": "Claude", "quota_per_hour": 5000}'

# New key for Streamlit dashboard (2000 req/hr)
curl -X POST http://localhost:8000/admin/keys/add \
  -H "X-API-Key: $ADMIN_KEY" \
  -d '{"name": "Streamlit", "quota_per_hour": 2000}'

# New key for public demo (100 req/hr)
curl -X POST http://localhost:8000/admin/keys/add \
  -H "X-API-Key: $ADMIN_KEY" \
  -d '{"name": "Public Demo", "quota_per_hour": 100}'
```

## Use API Key (in every request)

```bash
API_KEY="key_abc123xyz..."

# Command line
curl -H "X-API-Key: $API_KEY" http://api.example.com/transactions?limit=10

# Python
from api_client import APIClient
client = APIClient("http://api.example.com")
client.api_key = "key_abc123xyz..."
data = client.get_transactions()

# Browser (add to any request)
fetch("http://api.example.com/transactions", {
  headers: {"X-API-Key": "key_abc123xyz..."}
})
```

## Monitor Usage

```bash
ADMIN_KEY="super-secret-admin-key"

# All API keys
curl -H "X-API-Key: $ADMIN_KEY" http://localhost:8000/admin/metrics | jq

# Specific key
curl -H "X-API-Key: $ADMIN_KEY" \
  "http://localhost:8000/admin/metrics?api_key=claude-key" | jq

# System status
curl -H "X-API-Key: $ADMIN_KEY" http://localhost:8000/admin/status | jq
```

## View Logs

```bash
# Real-time
tail -f /var/log/japan-realestate/api.log

# Errors only
grep ERROR /var/log/japan-realestate/api.log

# Rate limit violations
grep "Rate limit" /var/log/japan-realestate/api.log

# Specific API key
grep "claude-key" /var/log/japan-realestate/api.log

# Last 50 requests
tail -50 /var/log/japan-realestate/api.log
```

## Manage Keys

```bash
ADMIN_KEY="super-secret-admin-key"

# Revoke a key (emergency)
curl -X POST http://localhost:8000/admin/keys/revoke \
  -H "X-API-Key: $ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"api_key": "key_to_revoke"}'

# Check key's usage before revoking
curl -H "X-API-Key: $ADMIN_KEY" \
  "http://localhost:8000/admin/metrics?api_key=key_to_check"
```

## nginx Configuration (add to your config)

```nginx
server {
    listen 443 ssl;
    server_name api.example.com;

    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;

    # Rate limiting
    limit_req_zone $http_x_api_key zone=api_limit:10m rate=100r/s;
    limit_req zone=api_limit burst=200 nodelay;

    # Security headers
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-Frame-Options "DENY" always;
    add_header Strict-Transport-Security "max-age=31536000" always;

    # Require API key
    location / {
        if ($http_x_api_key = "") {
            return 403;
        }
        proxy_pass http://localhost:8000;
    }

    # Exception: health check (no key needed)
    location /health {
        proxy_pass http://localhost:8000;
    }
}
```

## Security Checklist

### Before going public
- [ ] `REQUIRE_API_KEY=true`
- [ ] Generated strong ADMIN_KEY
- [ ] Created API keys for each client
- [ ] Set appropriate quotas (not unlimited)
- [ ] Configured nginx with SSL/TLS
- [ ] Added security headers in nginx
- [ ] Set `CORS_ORIGINS` to specific domains
- [ ] Logs directory writable by app
- [ ] Log rotation configured

### During operation
- [ ] Monitor logs daily: `tail -f /var/log/japan-realestate/api.log`
- [ ] Check metrics weekly: `/admin/metrics`
- [ ] Alert on error spikes
- [ ] Rotate keys every 30-90 days
- [ ] Review rate limit violations

### Response to abuse
- [ ] Check logs: `grep "Rate limit" /var/log/...`
- [ ] Review metrics: `/admin/metrics`
- [ ] Revoke compromised key: `/admin/keys/revoke`
- [ ] Lower quota if legitimate user is hitting limit

## Default Rate Limits

| Client | Requests/hour | Requests/second |
|--------|--------------|-----------------|
| Demo | 1,000 | ~0.3 |
| Claude Agent | 5,000 | ~1.4 |
| Streamlit | 2,000 | ~0.6 |
| nginx (per IP) | Unlimited | 10 |
| nginx (burst) | Unlimited | 20 |

## Common Issues

**403 Unauthorized**
```
→ Missing or invalid X-API-Key header
→ Fix: Add -H "X-API-Key: your-key" to request
```

**429 Too Many Requests**
```
→ Exceeded rate limit quota for your key
→ Fix: Wait 1 hour or contact admin to raise quota
```

**502 Bad Gateway**
```
→ API server down or unresponsive
→ Fix: Check docker: docker compose ps
→ Restart: docker compose restart api
```

**SSL certificate error**
```
→ nginx SSL misconfigured
→ Fix: Check paths in nginx config
→ Test: curl --insecure (to bypass SSL check)
```

## For Claude/Agent Integration

When giving agents access:

1. **Create a dedicated key** for that agent
2. **Set quota** appropriate to use case (5000/hr for heavy use)
3. **Document** the key and quota limit
4. **Monitor** the agent's usage for anomalies
5. **Rotate** the key every 90 days
6. **Revoke** immediately if compromised

Example for Claude integration:

```python
from api_client import APIClient

# Claude's dedicated key
CLAUDE_API_KEY = "key_claude_production_v1"

client = APIClient("https://api.example.com")
client.api_key = CLAUDE_API_KEY

# Now Claude can query
data = client.get_transactions(prefecture_code="13")
```

---

**For full details, see SECURITY.md**

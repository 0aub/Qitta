# Twitter Session Management

## 🔐 Security Notice

**IMPORTANT:** Session files contain sensitive authentication tokens. Never commit them to git!

The `.gitignore` file is configured to exclude:
- `backend/browser/sessions/twitter_session.json`
- All `twitter_session_backup_*.json` files

---

## 📋 Setup Instructions for New Users

### Step 1: Export Cookies from Browser

1. Login to [x.com](https://x.com) in your browser
2. Open Developer Tools (F12)
3. Go to **Application** tab → **Cookies** → `https://x.com`
4. Copy the following cookie values:
   - `auth_token` ⭐ **REQUIRED**
   - `ct0` ⭐ **REQUIRED**
   - `kdt` ⭐ **REQUIRED**
   - `twid` ⭐ **REQUIRED**
   - `guest_id`
   - `personalization_id`
   - Other cookies (optional but recommended)

### Step 2: Create Session File

**Option A: Using the Example Template**

1. Copy the example file:
   ```bash
   cp backend/browser/sessions/twitter_session.example.json backend/browser/sessions/twitter_session.json
   ```

2. Edit `backend/browser/sessions/twitter_session.json` and replace:
   - `YOUR_AUTH_TOKEN_HERE` → your actual `auth_token` value
   - `YOUR_CT0_TOKEN_HERE` → your actual `ct0` value
   - `YOUR_KDT_TOKEN_HERE` → your actual `kdt` value
   - `YOUR_TWID_HERE` → your actual `twid` value
   - Update other fields as needed

**Option B: Direct Docker Command**

```bash
docker exec backend-browser-1 bash -c 'cat > /sessions/twitter_session.json << '\''EOF'\''
{
  "cookies": {
    "auth_token": "YOUR_AUTH_TOKEN_HERE",
    "ct0": "YOUR_CT0_TOKEN_HERE",
    "kdt": "YOUR_KDT_TOKEN_HERE",
    "twid": "YOUR_TWID_HERE",
    "guest_id": "YOUR_GUEST_ID",
    "personalization_id": "YOUR_PERSONALIZATION_ID"
  },
  "captured_at": "2025-01-01T00:00:00.000000",
  "captured_at_timestamp": 1704067200.0,
  "expires_estimate": 1706745600.0,
  "username": "@YourTwitterHandle",
  "email": "your_email@example.com"
}
EOF
'
```

### Step 3: Verify Session

```bash
docker exec backend-browser-1 python3 -c "
import json, datetime
with open('/sessions/twitter_session.json') as f:
    s = json.load(f)
print(f'✅ Session loaded')
print(f'Username: {s[\"username\"]}')
print(f'Auth token: {s[\"cookies\"][\"auth_token\"][:20]}...')
"
```

---

## 📅 Session Lifespan

- **Valid for:** ~30 days from capture
- **Recommended refresh:** Every 2-3 weeks
- **Signs of expiration:**
  - Authentication failures
  - Rate limiting
  - Slower response times

---

## 🔄 Updating Sessions

When your session expires or degrades, follow the same steps above to create a fresh session file.

**Quick Update Script:**

```bash
# Export fresh cookies from your browser
# Then run this command with your new tokens:

docker exec backend-browser-1 python3 << 'EOF'
import json
import datetime

cookies = {
    "auth_token": "YOUR_NEW_AUTH_TOKEN",
    "ct0": "YOUR_NEW_CT0_TOKEN",
    "kdt": "YOUR_NEW_KDT_TOKEN",
    "twid": "YOUR_NEW_TWID",
    # Add other cookies...
}

now = datetime.datetime.now()
session_data = {
    "cookies": cookies,
    "captured_at": now.isoformat(),
    "captured_at_timestamp": now.timestamp(),
    "expires_estimate": (now + datetime.timedelta(days=30)).timestamp(),
    "username": "@YourHandle",
    "email": "your_email@example.com"
}

with open("/sessions/twitter_session.json", "w") as f:
    json.dump(session_data, f, indent=2)

print("✅ Session updated successfully!")
EOF
```

---

## 🧪 Testing Your Session

Test authentication only (safe, quick test):

```bash
curl -X POST http://localhost:8004/jobs/twitter \
  -H "Content-Type: application/json" \
  -d '{
    "task": "twitter",
    "username": "elonmusk",
    "scrape_posts": false,
    "scrape_level": 1,
    "use_session": true
  }'
```

Check the job status to verify authentication worked.

---

## 📁 File Structure

```
backend/browser/sessions/
├── README.md                          # This file
├── twitter_session.example.json      # Template (safe to commit)
├── twitter_session.json               # Your actual session (NEVER commit!)
└── twitter_session_backup_*.json     # Automatic backups (NEVER commit!)
```

---

## ⚠️ Security Best Practices

1. **Never share session files** - they contain your Twitter authentication
2. **Never commit to git** - already configured in `.gitignore`
3. **Refresh regularly** - don't use expired sessions
4. **Use separate accounts** - don't use your personal Twitter account for automation
5. **Monitor usage** - Twitter may rate-limit or suspend accounts used for scraping

---

## 🆘 Troubleshooting

### "Session file not found"
- Create the session file following Step 2 above
- Make sure you're inside the container: `docker exec backend-browser-1 ls /sessions/`

### "Authentication failed"
- Your session may be expired (>30 days old)
- Export fresh cookies and update the session file
- Make sure you copied all required cookies correctly

### "Invalid JSON format"
- Check for syntax errors in your JSON file
- Make sure all quotes are properly escaped
- Use the example template as reference

---

## 📖 Additional Resources

- **Session Update Guide:** `SESSION_UPDATE_GUIDE.md` (parent directory)
- **Main Documentation:** Project README

---

*Last Updated: October 21, 2025*

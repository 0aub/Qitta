# Twitter Session Update Guide

## 📅 Last Updated: October 21, 2025

---

## ✅ Session Successfully Updated

**New Session Details:**
- **Captured:** October 21, 2025 at 18:14:30 UTC
- **Valid Until:** ~November 20, 2025 (30 days)
- **Auth Token:** e76ad69e953d72b3dca8...
- **Status:** ✅ FRESH & ACTIVE

---

## 🔄 How to Update Session Cookies (For Future Use)

### Method 1: Direct JSON Update (Recommended)

1. **Export cookies from your browser:**
   - Login to x.com in Chrome/Firefox
   - Press F12 → Application → Cookies → x.com
   - Copy these important cookies:
     - `auth_token` (required)
     - `ct0` (required)
     - `kdt` (required)
     - `twid` (required)
     - Other cookies as available

2. **Update the session file:**
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
  "captured_at": "2025-XX-XXTXX:XX:XX.000000",
  "captured_at_timestamp": CURRENT_TIMESTAMP,
  "expires_estimate": TIMESTAMP_PLUS_30_DAYS,
  "username": "@YourTwitterHandle",
  "email": "your_email@example.com"
}
EOF
'
```

3. **Verify the update:**
```bash
docker exec backend-browser-1 python3 -c "
import json, datetime
with open('/sessions/twitter_session.json') as f:
    s = json.load(f)
age = (datetime.datetime.now() - datetime.datetime.fromisoformat(s['captured_at'])).total_seconds() / 60
print(f'Age: {age:.1f} minutes')
print(f'Auth token: {s[\"cookies\"][\"auth_token\"][:20]}...')
"
```

### Method 2: Using Browser Extension (Easy)

1. Install "Cookie Editor" or "EditThisCookie" browser extension
2. Export cookies from x.com as JSON
3. Parse and format into the structure above
4. Update `/sessions/twitter_session.json` in container

---

## 🧪 Testing Results with Fresh Session

### ✅ What Works:
- **Authentication:** Perfect (validates in ~8 seconds)
- **Browser Context:** Creates successfully
- **Session Loading:** 100% success rate
- **Navigation:** Fast (<1 second to profile pages)
- **Content Detection:** Loads correctly

### ❌ What Doesn't Work:
- **Profile Data Extraction:** Hangs indefinitely after navigation
- **Post Extraction:** Never reaches this phase (hangs before it)
- **Job Completion:** Times out after 5 minutes with no output

### 🔍 Test Results:

**Test 1 - Elon Musk Profile:**
```
✅ Session auth: SUCCESS (8s)
✅ Navigate to profile: SUCCESS (1s)
❌ Extract profile data: TIMEOUT (hung for 5 minutes)
Result: Failed - no data extracted
```

**Test 2 - Naval Profile (Level 1):**
```
✅ Session auth: SUCCESS (8s)
✅ Navigate to profile: SUCCESS (1s)
✅ Content loaded: SUCCESS (0s)
❌ Profile extraction: TIMEOUT (hung for 3+ minutes)
Result: Failed - no data extracted
```

---

## 🐛 Current System Issues

### Critical Bug: Profile Extraction Infinite Hang

**Location:** `src/tasks/twitter.py` (after line 6337)

**Symptoms:**
- Navigation completes successfully
- Content detection reports "CONTENT LOADED SUCCESSFULLY!"
- No further logs appear
- Process hangs indefinitely
- Job times out after 5 minutes with zero output

**What We Know:**
- Session is valid and authenticated ✅
- Navigation works perfectly ✅
- The hang occurs in the profile data extraction phase
- No timeout wrappers are being triggered
- No error messages are logged

**Likely Cause:**
The code after "Content loaded" is probably calling `page.inner_text()` or similar blocking operations on large page elements without proper timeouts.

**Previously Fixed Similar Issues:**
- Profile text extraction (line 8116) - added timeout wrapper
- Navigation (line 6315) - added timeout wrapper
- But there's likely another blocking call we haven't found yet

---

## 📊 Session Lifespan Data

Based on observations:

| Session Age | Authentication | Navigation | Extraction |
|------------|----------------|------------|------------|
| 0-7 days   | ✅ Perfect     | ✅ Fast    | ❌ Hangs*  |
| 8-14 days  | ✅ Works       | ✅ Works   | ❌ Hangs*  |
| 15-23 days | ✅ Works       | ⚠️ Slow   | ❌ Hangs*  |
| 24-30 days | ⚠️ Degraded   | ⚠️ Timeouts| ❌ Hangs*  |
| 30+ days   | ❌ Fails       | ❌ N/A     | ❌ N/A     |

\* The extraction hang is a code bug, not a session age issue

---

## 🎯 Recommendations

### For You (User):

1. **Update sessions every 2-3 weeks** for optimal performance
2. **Keep the update process simple** - use the bash command method shown above
3. **Don't expect extractions to work yet** - the profile extraction bug needs to be fixed first
4. **Monitor authentication only** - that's the only reliable test right now

### For Development:

1. **URGENT: Find and fix the extraction hang**
   - Add detailed logging after line 6337 in twitter.py
   - Wrap ALL Playwright operations in timeout wrappers
   - Test with small timeouts (5-10 seconds) to identify the hang point

2. **Medium Priority: Optimize extraction code**
   - Current approach loads too much at once
   - Need incremental extraction with checkpoints

3. **Low Priority: Automated session refresh**
   - Twitter blocks automated login (confirmed)
   - Manual update every 2-3 weeks is acceptable

---

## 📝 Session File Location

- **Main:** `/sessions/twitter_session.json` (inside container)
- **Backups:** `/sessions/twitter_session_backup_*.json`
- **Host Path:** `backend/browser/sessions/` (if volume mounted)

---

## 🔧 Quick Commands

**Check session status:**
```bash
docker exec backend-browser-1 python3 -c "
import json, datetime
with open('/sessions/twitter_session.json') as f:
    s = json.load(f)
captured = datetime.datetime.fromisoformat(s['captured_at'])
age_days = (datetime.datetime.now() - captured).days
print(f'Captured: {captured}')
print(f'Age: {age_days} days')
print(f'Valid: {age_days < 30}')
"
```

**List all sessions:**
```bash
docker exec backend-browser-1 ls -lh /sessions/
```

**Test authentication only:**
```bash
curl -X POST http://localhost:8004/jobs/twitter \
  -H "Content-Type: application/json" \
  -d '{"task": "twitter", "username": "naval", "scrape_posts": false, "scrape_level": 1, "use_session": true}'
```

---

*Generated: October 21, 2025*
*Status: Session updated successfully, extraction bug remains*

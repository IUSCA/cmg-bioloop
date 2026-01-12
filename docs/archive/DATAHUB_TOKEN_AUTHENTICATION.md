# Datahub Token-Based Authentication Implementation

## Overview

This document describes the implementation of token-based authentication for the `/datahub` endpoint, allowing external genome browsers (like WashU) to access session track data via URL-embedded tokens.

## Problem Statement

The WashU Epigenome Gateway browser needs to access the `/datahub` endpoint to fetch track configurations, but:
1. It makes cross-origin requests from `https://epigenomegateway.wustl.edu`
2. It cannot send custom `Authorization` headers in cross-origin contexts
3. The endpoint requires authentication for security

## Solution

Implement a token-in-query-parameter approach:
1. UI fetches a short-lived JWT token specifically for datahub access
2. Token is appended to the datahub URL as a query parameter: `?token=...`
3. The `/datahub` endpoint validates the token from the query parameter
4. Token is automatically refreshed every 5 minutes

## Implementation Details

### Backend (API)

#### 1. New Middleware: `authenticateWithQueryToken`

**File**: `api/src/middleware/auth.js`

```javascript
function authenticateWithQueryToken(req, res, next) {
  // Accepts token from query parameter OR Authorization header
  let token = req.query?.token;
  if (!token) {
    const authHeader = req.headers.authorization;
    if (!authHeader) {
      return next(createError.Unauthorized('Authentication failed. Token not found.'));
    }
    if (!authHeader.startsWith('Bearer ')) {
      return next(invalid_token_err);
    }
    token = authHeader.split(' ')[1];
  }

  // Validate the token
  const auth = authService.checkJWT(token);
  if (!auth) return next(invalid_token_err);

  req.user = auth.profile;
  req.token = auth;
  next();
}
```

**Purpose**: Validates JWT tokens from query parameters, allowing external applications to authenticate without custom headers.

#### 2. New Endpoint: `GET /sessions/:id/datahub-token`

**File**: `api/src/routes/sessions.js`

```javascript
router.get(
  '/:id/datahub-token',
  isPermittedTo('read'),
  [param('id').isInt().toInt()],
  asyncHandler(async (req, res) => {
    // Verify session exists and user has access
    const session = await prisma.genome_browser_session.findUnique({
      where: { id: sessionId },
      select: { id: true },
    });

    if (!session) {
      return res.status(404).json({ error: 'Session not found' });
    }

    // Generate short-lived JWT token (10 minutes)
    const token = authService.issueJWT({
      username: req.user.username,
      email: req.user.email,
      name: req.user.name,
      roles: req.user.roles,
      cas_id: req.user.cas_id,
      id: req.user.id,
    }, '10m');

    res.json({ token });
  }),
);
```

**Purpose**: Generates short-lived JWT tokens for datahub access.
**Token Expiration**: 10 minutes (sufficient since UI refreshes every 5 minutes)

#### 3. Updated Endpoint: `GET /sessions/:id/datahub`

**File**: `api/src/routes/sessions.js`

```javascript
// OPTIONS handler for CORS preflight
router.options('/:id/datahub', cors());

// GET endpoint with token authentication
router.get(
  '/:id/datahub',
  cors(), // Enable CORS for WashU browser
  authenticateWithQueryToken, // Accept token from query parameter
  isPermittedTo('read'), // Check user permissions
  [param('id').isInt().toInt()],
  asyncHandler(async (req, res) => {
    // ... existing datahub logic
  })
);
```

**Changes**:
- Added CORS middleware for cross-origin requests
- Added OPTIONS handler for CORS preflight requests
- Uses `authenticateWithQueryToken` instead of `authenticate`
- Maintains existing permission checks

### Frontend (UI)

#### 1. Session Service Enhancement

**File**: `ui/src/services/session.js`

```javascript
getDatahubToken(id) {
  return api.get(`/sessions/${id}/datahub-token`);
}
```

**Purpose**: Fetches datahub access tokens from the API.

#### 2. Session Detail Page Updates

**File**: `ui/src/pages/sessions/[id].vue`

**New State Variables**:
```javascript
const datahubToken = ref(null);
const tokenRefreshInterval = ref(null);
```

**Token Fetching Function**:
```javascript
const fetchDatahubToken = async () => {
  if (!session.value) return;

  try {
    const response = await sessionService.getDatahubToken(session.value.id);
    datahubToken.value = response.data.token;
  } catch (error) {
    console.error('Failed to fetch datahub token:', error);
    toast.error('Failed to load genome browser access token');
  }
};
```

**Token Refresh Setup**:
```javascript
const setupTokenRefresh = () => {
  // Clear any existing interval
  if (tokenRefreshInterval.value) {
    clearInterval(tokenRefreshInterval.value);
  }

  // Fetch initial token
  fetchDatahubToken();

  // Refresh token every 5 minutes (300000ms)
  tokenRefreshInterval.value = setInterval(
    () => {
      fetchDatahubToken();
    },
    5 * 60 * 1000
  );
};
```

**Token Cleanup**:
```javascript
const cleanupTokenRefresh = () => {
  if (tokenRefreshInterval.value) {
    clearInterval(tokenRefreshInterval.value);
    tokenRefreshInterval.value = null;
  }
};
```

**Updated Genome Browser URL**:
```javascript
const genomeBrowserUrl = computed(() => {
  if (!session.value || !datahubToken.value) return '';

  const genomeBrowserBaseUrl = config.genomeBrowserUrl;
  const protocol = window.location.protocol;
  const host = window.location.host;
  const apiBaseUrl = `${protocol}//${host}`;
  
  // Token embedded in URL as query parameter
  const sessionDataHubUrl = `${apiBaseUrl}/api/sessions/${session.value.id}/datahub?token=${datahubToken.value}`;

  const genome = session.value.genome || session.value.genome_value || '';

  return `${genomeBrowserBaseUrl}/?genome=${genome}&hub=${encodeURIComponent(sessionDataHubUrl)}`;
});
```

**Lifecycle Hooks**:
```javascript
onMounted(() => {
  loadSession();
  setupTokenRefresh(); // Start token refresh
});

onUnmounted(() => {
  cleanupTokenRefresh(); // Clean up interval
});
```

**Updated Open Browser Function**:
```javascript
const openInGenomeBrowser = () => {
  if (!session.value) return;

  if (!datahubToken.value) {
    toast.error('Genome browser access token not ready. Please wait a moment and try again.');
    return;
  }

  if (!genomeBrowserUrl.value) {
    toast.error('Unable to generate genome browser URL');
    return;
  }

  window.open(genomeBrowserUrl.value, '_blank');
};
```

**Button Disabled State**:
```html
<va-button
  :disabled="!datahubToken"
  @click="openInGenomeBrowser"
>
  Open in Genome Browser
</va-button>
```

## CORS Configuration

### API (/datahub endpoint)

**File**: `api/src/routes/sessions.js`

```javascript
const cors = require('cors');

// OPTIONS handler for preflight
router.options('/:id/datahub', cors());

// GET endpoint with CORS
router.get('/:id/datahub', cors(), ...);
```

### secure_download (/expose endpoint)

**File**: `secure_download/src/app.js`

```javascript
// Global CORS enabled
app.use(cors());
```

**File**: `secure_download/src/routes/files.js`

```javascript
// Additional CORS headers for file serving
res.set('Access-Control-Allow-Origin', '*');
res.set('Access-Control-Allow-Headers', 'Range, Authorization, Content-Type');
res.set('Access-Control-Expose-Headers', 'Content-Range, Accept-Ranges, Content-Length');
res.set('Access-Control-Allow-Methods', 'GET, OPTIONS');
```

## Security Considerations

1. **Short-lived Tokens**: Tokens expire after 10 minutes, limiting exposure window
2. **Automatic Refresh**: UI refreshes tokens every 5 minutes to maintain access
3. **Permission Checks**: `isPermittedTo('read')` ensures only authorized users can access datahub
4. **Session Validation**: Token generation validates session existence and access
5. **Token Scope**: Tokens contain full user profile, allowing proper permission checks at datahub endpoint

## Flow Diagram

```
User Opens Session Detail Page
    │
    ├──> onMounted()
    │       │
    │       ├──> setupTokenRefresh()
    │       │       │
    │       │       ├──> fetchDatahubToken()
    │       │       │       │
    │       │       │       └──> GET /sessions/:id/datahub-token
    │       │       │               └──> Returns 10-min JWT token
    │       │       │
    │       │       └──> setInterval (5 minutes)
    │       │               └──> fetchDatahubToken() (repeat)
    │       │
    │       └──> Button enabled (datahubToken available)
    │
    ├──> User clicks "Open in Genome Browser"
    │       │
    │       ├──> genomeBrowserUrl computed
    │       │       │
    │       │       └──> https://epigenomegateway.wustl.edu/browser
    │       │            ?genome=hg38
    │       │            &hub=https://localhost/api/sessions/5/datahub?token=xxx
    │       │
    │       └──> Opens in new tab
    │               │
    │               └──> WashU browser makes request to datahub URL
    │                       │
    │                       ├──> CORS preflight (OPTIONS)
    │                       │       └──> 200 OK
    │                       │
    │                       └──> GET /sessions/5/datahub?token=xxx
    │                               │
    │                               ├──> authenticateWithQueryToken
    │                               │       └──> Validates token from query
    │                               │
    │                               ├──> isPermittedTo('read')
    │                               │       └──> Checks user permissions
    │                               │
    │                               └──> Returns track configuration JSON
    │                                       │
    │                                       └──> WashU browser fetches track files
    │                                               │
    │                                               └──> GET /files/expose/...?token=xxx
    │                                                       └──> secure_download serves files
    │
    └──> onUnmounted()
            │
            └──> cleanupTokenRefresh()
                    └──> Clears interval
```

## Testing Checklist

- [x] API endpoint `/sessions/:id/datahub-token` generates valid JWT tokens
- [x] Tokens expire after 10 minutes
- [x] `/datahub` endpoint accepts tokens from query parameters
- [x] `/datahub` endpoint still validates user permissions
- [x] CORS headers allow cross-origin requests from WashU browser
- [x] UI fetches token on component mount
- [x] UI refreshes token every 5 minutes
- [x] UI cleans up interval on component unmount
- [x] "Open in Genome Browser" button is disabled when token not available
- [x] Proper error messages shown when token fetch fails
- [ ] End-to-end test: Open session in WashU browser and verify tracks load
- [ ] Verify token expiration handling (after 10 minutes without refresh)

## Related Files

### Backend
- `api/src/middleware/auth.js` - New `authenticateWithQueryToken` middleware
- `api/src/routes/sessions.js` - New `/datahub-token` endpoint, updated `/datahub` with CORS
- `secure_download/src/app.js` - Global CORS configuration
- `secure_download/src/routes/files.js` - File-specific CORS headers

### Frontend
- `ui/src/services/session.js` - New `getDatahubToken()` method
- `ui/src/pages/sessions/[id].vue` - Token fetching, refresh, and URL building

## OAuth Configuration

The implementation uses existing OAuth configuration:
- **Client ID**: `OAUTH_GENOME_BROWSER_CLIENT_ID`
- **Client Secret**: `OAUTH_GENOME_BROWSER_CLIENT_SECRET`
- **Scope Prefix**: `genome_browser_file:`

These are used by the file exposure token generation but not directly by datahub tokens, which are standard user JWTs.

## Future Enhancements

1. **Configurable Token Expiration**: Make token expiration and refresh interval configurable
2. **Token Revocation**: Implement token revocation for enhanced security
3. **Scope-specific Tokens**: Create dedicated token scopes for datahub vs file access
4. **Rate Limiting**: Add rate limiting to token generation endpoint
5. **Audit Logging**: Log token generation and usage for security auditing


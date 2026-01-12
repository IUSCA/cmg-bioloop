

# CMG-Bioloop: Embedded Genome Browser Architecture (IGV + WashU)

## Goal

Embed genome browsers **inside the Bioloop UI** (same page / same origin), instead of opening them in a new tab, in order to:

- Eliminate cross-origin and CORS issues
- Avoid passing auth tokens via URLs
- Reuse existing Bioloop authentication (JWT)
- Simplify file access for large genomics files
- Support both **IGV** and **WashU** with a single backend design

This document describes **what to implement** and **why**, targeted at an AI agent implementing the system.

**Notes**:

- **Current State**: Currently, the app only supports washU, not IGV.
- Even though this document mentions both WsshU and IGV, for now, we will keep the implementation requested here limited to the IGV browser, and not washU. In the rest of this doc, if any implementation changes are requested for washU, ignore them. Once we have tested this new approach for IVG, we will then try and implement something similar for WashU.

------

## High-Level Architecture

### Before (i.e. the approach we will discard)

- WashU / IGV opened in a **new tab**
- Genome browser running on **external origin**
- Required:
  - URL tokens
  - CORS headers
  - HTTPS / localhost hacks
  - Special handling for range requests

This approach will be **abandoned**.

------

### After (new approach)

- Genome browser is **embedded inside Bioloop UI**
- Genome browser JS runs **in the same page**
- All API + file requests are **same origin** (currently, the /files/expose API is not same origin, because it uses secure_download microservice. We will move these file exposure APIs to the core bioloop api)
- Authentication handled via **HttpOnly cookies**
- No CORS needed
- No token-in-URL needed

------

## Key Design Decisions

### 1. Embed genome browsers (not new tabs)

- IGV → use `igv.js`
- WashU → use WashU’s embeddable JS mode (not hub-in-new-tab)

Genome browsers are rendered into a `<div>` inside the existing session page.

------

### 2. For file exposire, Use cookie-based authentication

#### Why cookies

- Cookies are automatically sent by the browser
- No JS access required (HttpOnly)
- Works transparently for:
  - UI → API calls
  - Genome browser JS → file fetches

#### Cookie requirements

Auth cookie must be:

- `HttpOnly`
- `Secure`
- SameSite = `Lax` (or `Strict` for localhost)
- Set when user visits the /sessions/:id view

Example:

```
Set-Cookie: bioloop_auth=<JWT>;
  HttpOnly;
  Secure;
  SameSite=Lax;
  Path=/api/files/expose
```

- Cookies will be used for file exposure APIs only. Other APIs (including ones like /datahub) will continue using the usual auth middlewares that are used throughout the API endpoints.

------

### 3. No Authorization headers inside genome browsers

Genome browsers (IGV/WashU):

- Do **not** manually attach headers
- Rely on browser to send cookies automatically
  - The file exposure API (/files/expose) will authenticates requests using cookies, not headers.

**Note**: The UI will call the /datahub API using the Auth header which contains a JWT, not cookies. This is the standard practice thats followed in the rest of the app.

------

## API Endpoints (Core Bioloop API)

All endpoints are served from the **core Bioloop API**, behind the same origin:

```
https://localhost/api/...
```

### 1. Session DataHub endpoint

Returns track metadata for genome browsers.

```
GET /api/sessions/:id/datahub
```

Auth:

- Standard auth middleware used by APi endpoints in the rest of the app

Returns JSON array:

```
[
  {
    "type": "bigwig",
    "name": "Example Track",
    "url": "/api/files/expose/path/to/file.bw",
    "indexURL": null,
    "options": {
      "color": "#2669a3",
      "height": 100
    }
  }
]
```

Notes:

- URLs are **relative** and same-origin
- `indexURL` included only when applicable
- No tokens in URLs

------

### 2. File exposure endpoint

Serves actual genomic files (BIGWIG, BAM, VCF, etc).

```
GET /api/files/expose/:path
```

Responsibilities:

- Authenticate user via cookie
- Authorize access to dataset/session
- Support HTTP `Range` requests
- Stream file bytes

Must:

- Return `206 Partial Content` when `Range` header is present
- Not require any special CORS headers (same origin)

------

## Frontend: IGV Integration

### Where IGV lives

- Inside the **existing session view**:

- ```
  /sessions/:id
  ```

- Activated by a button (e.g. “View in IGV browser”). Replace current "Open in Genome Browser" button this.

- No separate UI route required

------

### IGV loading flow

On button 'View in IGV browser' click:

1. Call `GET /api/sessions/:id/datahub`
2. Transform response to IGV track config
3. Call:

```
igv.createBrowser(containerDiv, {
  genome: "hg38",
  tracks: tracksFromDatahub
});
```

Notes:

- IGV fetches files using `fetch()`
- Browser automatically sends auth cookies
- The /files/expose API validates cookie and streams files

------

**Notes**:

- We are only changing how Genome Browsers are made available (new tab vs in-app). Any other implementation details (for ex, having dataset_file.metadata.role, dataset_file.metadata.format etc) will not change.

------

## FOR LATER: WashU Integration (Embedded)

WashU can also be embedded using JS rather than “hub URL in new tab”.

Key differences vs IGV:

- WashU expects a **track JSON config**
- Still loads data via HTTP range requests
- Same-origin embedding removes CORS problems

Implementation strategy mirrors IGV:

1. Fetch `/api/sessions/:id/datahub`
2. Convert to WashU track format
3. Initialize WashU browser inside a container `<div>`

No token passing.
 No new window.
 No CORS configuration.

**Note**: Do NOT worry about washU right now. Just focus on IGV.

------

## Why This Works

### Authentication

- Cookie is set once at login
- Browser sends cookie automatically
- Genome browser JS doesn’t need to know about auth

### Security

- HttpOnly cookie → cannot be read by JS
- No credentials in URLs
- No external origins involved

### Simplicity

- One API
- One auth model
- One origin
- Same logic works for:
  - UI
  - IGV
  - WashU

------

## Explicitly NOT Needed

The following are **not** needed anymore for the purposes of sessions/tracks feature:

- `secure_download` service
- Token query params
- CORS headers for genome browsers
- Opening genome browsers in new tabs

------

## Summary

**This design embeds IGV and WashU directly inside Bioloop**, using:

- Same-origin API calls
- HttpOnly cookie authentication
- Core API file exposure
- Existing session + track models

This is the **cleanest**, **most secure**, and **least fragile** architecture for genome browser integration in Bioloop.


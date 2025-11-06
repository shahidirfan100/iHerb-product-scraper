# iHerb Product Scraper

Scrapes product data from iHerb.com using **Playwright** (real Firefox browser) to bypass bot detection. This Apify actor gathers structured product data from [iHerb](https://www.iherb.com/) using Crawlee's `PlaywrightCrawler` with anti-bot evasion techniques.

## Features

- **Flexible Input**: Start from keywords, categories, or any combination of `startUrl`, `url`, and `startUrls`
- **Market Support**: Configure market domain (`location`) and `Accept-Language` header to target localized storefronts
- **Dual Modes**: Detail mode (`collectDetails=true`) opens product pages for full data; listing mode pushes product URLs directly from category/search pages
- **Anti-Bot Protection**: Randomized desktop fingerprint, stealth techniques, session rotation, and proxy support
- **Data Extraction**: Extracts rich product data from Next.js `__NEXT_DATA__` payloads with HTML fallbacks
- **Smart Handling**: In-memory deduplication, configurable concurrency/retries, and automatic skip of bot challenges

## Input

All fields are optional unless noted. See `.actor/input_schema.json` for defaults.

- `startUrls` – Array of request sources (objects with `url` or `requestsFromUrl`) for bulk seeding
- `startUrl` / `url` – Convenient single URL aliases (product, category, or search)
- `keyword` / `category` – Build a search or category URL automatically when no explicit start URL is provided
- `location` – iHerb market prefix or full origin (e.g., `www`, `ru`, `https://au.iherb.com`). Defaults to `https://www.iherb.com`
- `language` – Custom `Accept-Language` header (empty string = random desktop locale)
- `collectDetails` – When `true` (default) download each product detail page; `false` emits listing URLs only
- `results_wanted` – Maximum number of products to save
- `max_pages` – Safety cap for category/search pagination
- `maxConcurrency` / `maxRequestRetries` – Tune throughput and resiliency
- `proxyConfiguration` – Standard Apify proxy options (residential proxies recommended)
- `cookies` / `cookiesJson` – Provide raw `Cookie` header text or structured cookies to replay consent or location preferences
- `dedupe` – Enable/disable in-memory deduplication of product URLs

## Output

When `collectDetails=true`, each dataset item includes comprehensive product data:

```json
{
  "product_title": "Vitamin D3 125 mcg",
  "brand": "Now Foods",
  "product_id": "12345",
  "price": "14.99",
  "currency": "USD",
  "availability": "InStock",
  "rating": 4.8,
  "reviews_count": 2451,
  "description_html": "<div>...</div>",
  "description_text": "High-potency vitamin D3 in softgels…",
  "images": ["https://s3.iherb.com/..."],
  "product_url": "https://www.iherb.com/pr/.../12345"
}
```

When `collectDetails=false`, the actor stores discovered listing URLs with available metadata without opening product pages.

## Installation & Usage

```bash
npm install
npm start
```

## Deploy to Apify

1. Push code to GitHub
2. Create new actor on Apify platform
3. Connect to your GitHub repository
4. Configure build with Node.js 20+
5. Run with proxy configuration

## Example Input

```json
{
  "keyword": "vitamin d3",
  "results_wanted": 20,
  "collectDetails": true,
  "proxyConfiguration": {
    "useApifyProxy": true,
    "apifyProxyGroups": ["RESIDENTIAL"]
  }
}
```

## Technical Details

- **Stack**: Apify SDK + Crawlee 3.x + Playwright
- **Browser**: Firefox (headless)
- **Data Source**: Next.js `__NEXT_DATA__` script tag
- **Concurrency**: Default concurrency of 1 (configurable)
- **Stealth**: Hides webdriver property, uses realistic user agents and fingerprints

## Notes

- Always run behind the Apify Proxy (residential or smartproxy groups recommended) for best stability
- The actor respects `robots.txt` and skips pages guarded by bots or CAPTCHAs while reporting counts in logs
- 403/429/503 responses trigger exponential backoff, session retirement, and retries
- If iHerb deploys major layout changes, adjust selectors in `src/main.js`, but JSON fallbacks cover most variations

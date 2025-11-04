# iHerb Product Scraper (Playwright)# iHerb Product Scraper



Scrapes product data from iHerb.com using **Playwright** (real Chromium browser) to bypass bot detection.This Apify actor gathers structured product data from [iHerb](https://www.iherb.com/) using Crawlee’s `CheerioCrawler` together with `got-scraping`. No headless browser is required, which keeps the run cost low while remaining production ready.



## Why Playwright?## Features

- Start from keywords, categories, or any combination of `startUrl`, `url`, and `startUrls` (including `requestsFromUrl` sources).

iHerb uses Next.js with client-side rendering and anti-bot protection. Playwright solves this by:- Configure market domain (`location`) and `Accept-Language` header to target localized storefronts.

- Using a real browser to bypass detection- Extracts rich product data by combining JSON-LD, Next.js `__NEXT_DATA__` payloads, and resilient HTML fallbacks.

- Executing JavaScript to access client-rendered content  - Detail mode (`collectDetails=true`) opens product pages; listing mode pushes product URLs directly from category/search pages.

- Extracting data from Next.js `__NEXT_DATA__` JSON- In-memory deduplication, configurable concurrency/retries, proxy support, and optional custom cookies.

- Randomized desktop fingerprint (HTTP/2 headers, client hints, referer chains) via `header-generator`, plus automatic skip & logging of bot/captcha/403 challenges with smart session rotation.

## Input

## Input

- `keyword` - Search term (e.g., "vitamin c")All fields are optional unless noted. See `.actor/input_schema.json` for defaults.

- `results_wanted` - Max products to scrape (default: 50)

- `max_pages` - Max category pages to crawl (default: 5)- `startUrls` – Array of request sources (objects with `url` or `requestsFromUrl`) for bulk seeding.

- `collectDetails` - If true, scrapes full product pages; if false, uses listing data (default: true)- `startUrl` / `url` – Convenient single URL aliases (product, category, or search).

- `proxyConfiguration` - Apify proxy settings (recommended: residential proxies)- `keyword` / `category` – Build a search or category URL automatically when no explicit start URL is provided.

- `location` – iHerb market prefix or full origin (e.g., `www`, `ru`, `https://au.iherb.com`). Defaults to `https://www.iherb.com`.

## Output- `language` – Custom `Accept-Language` header (empty string = random desktop locale).

- `collectDetails` – When `true` (default) download each product detail page; `false` emits listing URLs only.

Each product includes:- `results_wanted` – Maximum number of products to save.

- `product_url` - Product page URL- `max_pages` – Safety cap for category/search pagination.

- `product_id` - iHerb product ID- `maxConcurrency` / `maxRequestRetries` – Tune throughput and resiliency.

- `product_title` - Product name- `proxyConfiguration` – Standard Apify proxy options (residential proxies recommended).

- `brand` - Brand name- `cookies` / `cookiesJson` – Provide raw `Cookie` header text or structured cookies to replay consent or location preferences.

- `price` - Current price- `dedupe` – Enable/disable in-memory deduplication of product URLs.

- `currency` - Currency code

- `rating` - Average rating (1-5)## Output

- `reviews_count` - Number of reviewsWhen `collectDetails=true`, each dataset item includes comprehensive product data. Example:

- `description_html` - Product description (detail mode only)

```json

## Installation{

  "product_title": "Vitamin D3 125 mcg",

```bash  "brand": "Now Foods",

npm install  "product_id": "12345",

npm start  "sku": "NWF-01234",

```  "price": "14.99",

  "currency": "USD",

## Deploy to Apify  "availability": "InStock",

  "rating": 4.8,

1. Push code to GitHub  "reviews_count": 2451,

2. Create new actor on Apify platform  "description_html": "<div>...</div>",

3. Connect to your GitHub repository  "description_text": "High-potency vitamin D3 in softgels…",

4. Configure build with Node.js 20+  "images": ["https://s3.iherb.com/..."],

5. Run with proxy configuration  "categories": ["Supplements", "Vitamins"],

  "product_url": "https://www.iherb.com/pr/.../12345",

## Technical Details  "source_url": "https://www.iherb.com/pr/.../12345",

  "origin": "https://www.iherb.com",

- **Stack**: Apify SDK + Crawlee 3.x + Playwright  "accept_language": "en-US,en;q=0.9",

- **Browser**: Chromium (headless)  "price_note": "Price hidden on page",

- **Data Source**: Next.js `__NEXT_DATA__` script tag  "last_updated": "2025-01-16T12:34:56.789Z",

- **Concurrency**: 3 parallel browsers (configurable)  "_source": "iherb.com"

- **Stealth**: Hides webdriver property, uses realistic user agent}

```

## Example Input

When `collectDetails=false`, the actor stores the discovered listing URLs (with available metadata) without opening product pages.

```json

{## Notes

  "keyword": "vitamin d3",- Always run behind the Apify Proxy (residential or smartproxy groups recommended) for best stability.

  "results_wanted": 20,- The actor respects `robots.txt` and skips pages guarded by bots or CAPTCHAs while reporting the counts in the crawler summary.

  "collectDetails": true,- 403/429/503 responses trigger exponential backoff, session retirement, and a warm-up pass. Persistent blocking still points to proxy reputation—switch to a fresh residential pool, drop concurrency to 1–2, and consider supplying fresh cookies captured from a real browser session.

  "proxyConfiguration": {- If iHerb deploys major layout changes, adjust selectors in `src/main.js`, but the JSON-LD / Next.js fallbacks already cover most UI variations.

    "useApifyProxy": true,
    "apifyProxyGroups": ["RESIDENTIAL"]
  }
}
```

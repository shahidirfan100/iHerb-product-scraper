# iHerb Product Scraper

This Apify actor gathers structured product data from [iHerb](https://www.iherb.com/) using Crawlee’s `CheerioCrawler` together with `got-scraping`. No headless browser is required, which keeps the run cost low while remaining production ready.

## Features
- Start from keywords, categories, or any combination of `startUrl`, `url`, and `startUrls` (including `requestsFromUrl` sources).
- Configure market domain (`location`) and `Accept-Language` header to target localized storefronts.
- Extracts rich product data by combining JSON-LD, Next.js `__NEXT_DATA__`, and resilient HTML fallbacks.
- Detail mode (`collectDetails=true`) opens product pages; listing mode pushes product URLs directly from category/search pages.
- In-memory deduplication, configurable concurrency/retries, proxy support, and optional custom cookies.
- Randomized desktop headers via `header-generator`, plus automatic skip & logging of bot/captcha challenges.

## Input
All fields are optional unless noted. See `.actor/input_schema.json` for defaults.

- `startUrls` – Array of request sources (objects with `url` or `requestsFromUrl`) for bulk seeding.
- `startUrl` / `url` – Convenient single URL aliases (product, category, or search).
- `keyword` / `category` – Build a search or category URL automatically when no explicit start URL is provided.
- `location` – iHerb market prefix or full origin (e.g., `www`, `ru`, `https://au.iherb.com`). Defaults to `https://www.iherb.com`.
- `language` – Custom `Accept-Language` header (empty string = random desktop locale).
- `collectDetails` – When `true` (default) download each product detail page; `false` emits listing URLs only.
- `results_wanted` – Maximum number of products to save.
- `max_pages` – Safety cap for category/search pagination.
- `maxConcurrency` / `maxRequestRetries` – Tune throughput and resiliency.
- `proxyConfiguration` – Standard Apify proxy options (residential proxies recommended).
- `cookies` / `cookiesJson` – Provide raw `Cookie` header text or structured cookies to replay consent or location preferences.
- `dedupe` – Enable/disable in-memory deduplication of product URLs.

## Output
When `collectDetails=true`, each dataset item includes comprehensive product data. Example:

```json
{
  "product_title": "Vitamin D3 125 mcg",
  "brand": "Now Foods",
  "product_id": "12345",
  "sku": "NWF-01234",
  "price": "14.99",
  "currency": "USD",
  "availability": "InStock",
  "rating": 4.8,
  "reviews_count": 2451,
  "description_html": "<div>...</div>",
  "description_text": "High-potency vitamin D3 in softgels…",
  "images": ["https://s3.iherb.com/..."],
  "categories": ["Supplements", "Vitamins"],
  "product_url": "https://www.iherb.com/pr/.../12345",
  "source_url": "https://www.iherb.com/pr/.../12345",
  "origin": "https://www.iherb.com",
  "accept_language": "en-US,en;q=0.9",
  "price_note": "Price hidden on page",
  "last_updated": "2025-01-16T12:34:56.789Z",
  "_source": "iherb.com"
}
```

When `collectDetails=false`, the actor stores the discovered listing URLs (with available metadata) without opening product pages.

## Notes
- Always run behind the Apify Proxy (residential or smartproxy groups recommended) for best stability.
- The actor respects `robots.txt` and skips pages guarded by bots or CAPTCHAs while reporting the counts in the crawler summary.
- If iHerb deploys major layout changes, adjust selectors in `src/main.js`, but the JSON-LD / Next.js fallbacks already cover most UI variations.

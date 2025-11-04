// iHerb product scraper - CheerioCrawler implementation
import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';
import { gotScraping } from 'got-scraping';
import { load as cheerioLoad } from 'cheerio';
import { HeaderGenerator } from 'header-generator';

const DEFAULT_IHERB_ORIGIN = 'https://www.iherb.com';

const USER_AGENT_HEADERS = [
    { name: 'chrome', minVersion: 90 },
    { name: 'firefox', minVersion: 90 },
    { name: 'edge', minVersion: 90 },
];

const headerGenerator = new HeaderGenerator({
    browsers: USER_AGENT_HEADERS,
    devices: ['desktop'],
    operatingSystems: ['windows', 'macos'],
});

function buildCookieHeader(raw, jsonInput) {
    const merged = [];
    if (typeof raw === 'string' && raw.trim()) merged.push(raw.trim());
    if (typeof jsonInput === 'string' && jsonInput.trim()) {
        try {
            const parsed = JSON.parse(jsonInput);
            if (Array.isArray(parsed)) {
                for (const item of parsed) {
                    if (!item) continue;
                    const { name, value } = item;
                    if (name && typeof value !== 'undefined') merged.push(`${name}=${value}`);
                }
            } else if (parsed && typeof parsed === 'object') {
                for (const [name, value] of Object.entries(parsed)) {
                    if (name && typeof value !== 'undefined') merged.push(`${name}=${value}`);
                }
            }
        } catch (err) {
            log.warning(`Failed to parse cookiesJson: ${err.message}`);
        }
    }
    if (!merged.length) return null;
    return [...new Set(merged
        .flatMap((entry) => entry.split(/;\s*/))
        .map((entry) => entry.trim())
        .filter(Boolean))].join('; ');
}

function extractNextData($) {
    if (!$) return null;
    const script = $('script#__NEXT_DATA__').html() || $('script[id="__NEXT_DATA__"]').text();
    if (!script) return null;
    try {
        return JSON.parse(script);
    } catch (err) {
        log.debug(`Failed to parse __NEXT_DATA__: ${err.message}`);
        return null;
    }
}

function findProductNodeCandidate(root) {
    if (!root || typeof root !== 'object') return null;
    const stack = [root];
    const seen = new Set();
    while (stack.length) {
        const current = stack.pop();
        if (!current || typeof current !== 'object') continue;
        if (seen.has(current)) continue;
        seen.add(current);
        const entries = Object.entries(current);
        const keys = entries.map(([key]) => key.toLowerCase());
        const hasProductId = keys.some((key) => key.includes('productid') || key === 'id' || key.endsWith('id'));
        const hasName = keys.some((key) => key.includes('name') || key.includes('title'));
        if (hasProductId && hasName) return current;
        for (const [, value] of entries) {
            if (value && typeof value === 'object') stack.push(value);
        }
    }
    return null;
}

function extractProductFromNextData(nextData, toAbs, effectiveUrl) {
    if (!nextData) return {};
    const candidate = findProductNodeCandidate(nextData);
    if (!candidate) return {};
    const product = {};

    const idCandidate = candidate.productId ?? candidate.productID ?? candidate.ProductId ?? candidate.ProductID ?? candidate.id;
    if (idCandidate) product.product_id = String(idCandidate).trim();

    const nameCandidate = candidate.displayName ?? candidate.name ?? candidate.productName ?? candidate.title ?? candidate.productTitle;
    if (nameCandidate) product.product_title = String(nameCandidate).trim();

    const brandCandidate = candidate.brand?.name ?? candidate.brandName ?? candidate.brand;
    if (brandCandidate) product.brand = String(brandCandidate).trim();

    const skuCandidate = candidate.sku ?? candidate.SKU ?? candidate.code;
    if (skuCandidate) product.sku = String(skuCandidate).trim();

    const descriptionCandidate = candidate.description ?? candidate.shortDescription ?? candidate.longDescription ?? candidate.bodyHtml;
    if (descriptionCandidate) product.description_html = typeof descriptionCandidate === 'string' ? descriptionCandidate : null;

    const pricingNode = candidate.price ?? candidate.pricing ?? candidate.prices ?? candidate.priceInfo ?? candidate.listPrice;
    if (pricingNode) {
        const priceValue = pricingNode.value ?? pricingNode.current ?? pricingNode.price ?? pricingNode.amount ?? pricingNode.sale ?? pricingNode.list;
        if (priceValue !== undefined && priceValue !== null) product.price = String(priceValue).trim();
        const currencyCandidate = pricingNode.currency ?? pricingNode.currencyCode ?? pricingNode.isoCurrencyCode;
        if (currencyCandidate) product.currency = String(currencyCandidate).trim();
    }

    const inventoryNode = candidate.inventory ?? candidate.availability ?? candidate.stock ?? candidate.stockStatus;
    if (inventoryNode) {
        if (typeof inventoryNode === 'string') {
            product.availability = sanitizeAvailability(inventoryNode);
        } else if (inventoryNode.status) {
            product.availability = sanitizeAvailability(inventoryNode.status);
        } else if (inventoryNode.state) {
            product.availability = sanitizeAvailability(inventoryNode.state);
        }
    }

    const ratingNode = candidate.rating ?? candidate.ratings ?? candidate.reviewSummary ?? candidate.reviews;
    if (ratingNode) {
        const ratingValue = ratingNode.value ?? ratingNode.rating ?? ratingNode.average ?? ratingNode.avg ?? ratingNode.averageRating;
        if (ratingValue !== undefined && ratingValue !== null && !Number.isNaN(Number.parseFloat(ratingValue))) {
            product.rating = Number.parseFloat(ratingValue);
        }
        const reviewsCount = ratingNode.count ?? ratingNode.reviewCount ?? ratingNode.total ?? ratingNode.numberOfReviews;
        if (reviewsCount !== undefined && reviewsCount !== null && !Number.isNaN(Number.parseInt(reviewsCount, 10))) {
            product.reviews_count = Number.parseInt(reviewsCount, 10);
        }
    }

    const imagesCandidate = candidate.images ?? candidate.imageUrls ?? candidate.gallery ?? candidate.media?.images;
    if (imagesCandidate) {
        const list = Array.isArray(imagesCandidate) ? imagesCandidate : Object.values(imagesCandidate);
        const images = list
            .map((entry) => {
                if (!entry) return null;
                if (typeof entry === 'string') return entry;
                if (typeof entry === 'object') return entry.url ?? entry.href ?? entry.src ?? entry.image;
                return null;
            })
            .filter(Boolean)
            .map((src) => toAbs(src, effectiveUrl));
        if (images.length) product.images = [...new Set(images.filter(Boolean))];
    }

    const categoriesCandidate = candidate.categories ?? candidate.categoryBreadcrumbs ?? candidate.breadcrumbs ?? candidate.breadcrumb;
    if (categoriesCandidate) {
        const categoriesArray = Array.isArray(categoriesCandidate) ? categoriesCandidate : Object.values(categoriesCandidate);
        const categories = categoriesArray
            .map((entry) => {
                if (!entry) return null;
                if (typeof entry === 'string') return entry;
                if (typeof entry === 'object') return entry.name ?? entry.title ?? entry.text;
                return null;
            })
            .filter(Boolean)
            .map((text) => String(text).trim())
            .filter(Boolean);
        if (categories.length) product.categories = [...new Set(categories)];
    }

    if (!product.product_url && effectiveUrl) product.product_url = effectiveUrl;

    return product;
}

function extractListProductsFromNextData(nextData, toAbs, baseUrl) {
    if (!nextData) return [];
    const stack = [nextData];
    const seen = new Set();
    const results = new Map();

    const readText = (node, keys) => {
        for (const key of keys) {
            if (node[key]) return String(node[key]).trim();
        }
        return null;
    };

    while (stack.length) {
        const node = stack.pop();
        if (!node || typeof node !== 'object') continue;
        if (seen.has(node)) continue;
        seen.add(node);

        const keys = Object.keys(node);
        const hasProductId = keys.some((key) => /productid|product_id|itemid|sku|id/i.test(key));
        const hasUrl = keys.some((key) => /url|href|link/i.test(key));

        if (hasUrl) {
            const urlCandidate = node.url || node.href || node.link || node.productUrl || node.product_url;
            const absUrl = typeof urlCandidate === 'string' ? toAbs(urlCandidate, baseUrl) : null;
            if (absUrl && absUrl.includes('/pr/')) {
                const productId = readText(node, ['productId', 'productID', 'product_id', 'itemId', 'itemID', 'id']);
                const productTitle = readText(node, ['displayName', 'productName', 'name', 'title']);
                const brand = readText(node, ['brand', 'brandName', 'brandname', 'manufacturerName']);
                const price = readText(node, ['price', 'currentPrice', 'salePrice', 'listPrice']);
                const currency = readText(node, ['currency', 'currencyCode', 'currencySymbol']);

                const existing = results.get(absUrl) || {};
                results.set(absUrl, {
                    ...existing,
                    product_url: absUrl,
                    product_id: existing.product_id || productId,
                    product_title: existing.product_title || productTitle,
                    brand: existing.brand || brand,
                    price: existing.price || price,
                    currency: existing.currency || currency,
                });
            }
        }

        for (const value of Object.values(node)) {
            if (value && typeof value === 'object') stack.push(value);
        }
    }

    return [...results.values()];
}

function resolveBaseOrigin(loc) {
    if (typeof loc !== 'string') return DEFAULT_IHERB_ORIGIN;
    const trimmed = loc.trim();
    if (!trimmed) return DEFAULT_IHERB_ORIGIN;
    try {
        if (/^https?:\/\//i.test(trimmed)) return new URL(trimmed).origin;
        const sanitized = trimmed.replace(/^https?:\/\//i, '').replace(/[^a-z0-9.-]/gi, '').replace(/\.+$/g, '') || 'www';
        if (sanitized.toLowerCase().endsWith('iherb.com')) return `https://${sanitized}`;
        return `https://${sanitized}.iherb.com`;
    } catch {
        return DEFAULT_IHERB_ORIGIN;
    }
}

function detectLabelFromUrl(url) {
    if (!url) return 'CATEGORY';
    return url.includes('/pr/') ? 'PRODUCT' : 'CATEGORY';
}

function sanitizeAvailability(value) {
    if (!value) return null;
    const cleaned = String(value).trim();
    if (!cleaned) return null;
    const parts = cleaned.split(/[\/]/).filter(Boolean);
    return parts.length ? parts[parts.length - 1] : cleaned;
}

function isBotChallenge($) {
    const title = $('title').text().toLowerCase();
    if (/access denied|temporarily blocked|captcha|attention required|just a moment/i.test(title)) return true;
    const bodyText = $('body').text().toLowerCase();
    if (/captcha|human verification|blocked request/i.test(bodyText)) return true;
    if ($('form[action*="captcha"], #recaptcha, .g-recaptcha').length) return true;
    return false;
}

function derivePriceNote($) {
    const textSnippets = [
        $('.price, .price-current, .product-price, .pricing').text(),
        $('[data-test-id="price-message"]').text(),
    ];
    const combined = textSnippets.filter(Boolean).join(' ').toLowerCase();
    if (/see price in cart|price hidden|sign in to see price|add to cart to see price/.test(combined)) {
        return 'Price hidden on page';
    }
    return null;
}

function extractProductJsonLd($) {
    const scripts = $('script[type="application/ld+json"]');
    const candidates = [];
    scripts.each((_, el) => {
        try {
            const raw = $(el).contents().text().trim();
            if (!raw) return;
            const parsed = JSON.parse(raw);
            candidates.push(parsed);
        } catch {
            /* ignore invalid json-ld */
        }
    });

    const flatten = (node) => {
        if (!node) return [];
        if (Array.isArray(node)) return node.flatMap(flatten);
        if (typeof node === 'object') {
            const items = [node];
            if (node['@graph']) items.push(...flatten(node['@graph']));
            if (node.itemListElement) items.push(...flatten(node.itemListElement));
            if (node.mainEntity) items.push(...flatten(node.mainEntity));
            return items;
        }
        return [];
    };

    for (const entry of candidates.flatMap(flatten)) {
        const type = entry?.['@type'] || entry?.type;
        if (!type) continue;
        const types = Array.isArray(type) ? type : [type];
        if (types.map((t) => String(t).toLowerCase()).includes('product')) {
            return entry;
        }
    }
    return null;
}

function extractItemListProducts($, toAbs, baseUrl) {
    const scripts = $('script[type="application/ld+json"]');
    const products = [];
    const pushProduct = (entry) => {
        if (!entry) return;
        const item = entry.item || entry;
        if (!item || typeof item !== 'object') return;
        const type = item['@type'] || item.type;
        if (!type) return;
        const types = Array.isArray(type) ? type : [type];
        if (!types.some((t) => String(t).toLowerCase() === 'product')) return;
        const url = item.url ? toAbs(item.url, baseUrl) : null;
        products.push({
            product_title: item.name ? String(item.name).trim() : null,
            brand: typeof item.brand === 'string' ? item.brand : item.brand?.name ?? null,
            product_id: item.productID ? String(item.productID).trim() : null,
            price: item.offers?.price ? String(item.offers.price).trim() : null,
            currency: item.offers?.priceCurrency ? String(item.offers.priceCurrency).trim() : null,
            availability: item.offers?.availability ? sanitizeAvailability(item.offers.availability) : null,
            rating: item.aggregateRating?.ratingValue ? Number.parseFloat(item.aggregateRating.ratingValue) : null,
            reviews_count: item.aggregateRating?.reviewCount ? Number.parseInt(item.aggregateRating.reviewCount, 10) : null,
            product_url: url,
        });
    };

    scripts.each((_, el) => {
        try {
            const raw = $(el).contents().text().trim();
            if (!raw) return;
            const parsed = JSON.parse(raw);
            const queue = [parsed];
            while (queue.length) {
                const node = queue.shift();
                if (!node || typeof node !== 'object') continue;
                if (Array.isArray(node)) {
                    queue.push(...node);
                    continue;
                }
                if (node.itemListElement) {
                    const items = Array.isArray(node.itemListElement) ? node.itemListElement : [node.itemListElement];
                    for (const entry of items) pushProduct(entry);
                }
                for (const value of Object.values(node)) {
                    if (value && typeof value === 'object') queue.push(value);
                }
            }
        } catch {
            /* ignore malformed JSON-LD */
        }
    });

    return products.filter((item) => item.product_url);
}

async function ensureRobotsAllowed(origin, proxyConf) {
    const robotsUrl = new URL('/robots.txt', origin).href;
    try {
        const proxyUrl = proxyConf ? await proxyConf.newUrl() : undefined;
        const response = await gotScraping({ url: robotsUrl, proxyUrl, timeout: { request: 15000 } });
        const rules = parseRobotsRules(response.body);
        if (!rules) return;
        for (const pathPrefix of ['/c/', '/catalog/', '/pr/']) {
            if (!isPathAllowed(pathPrefix, rules)) {
                throw new Error(`robots.txt disallows path prefix ${pathPrefix} on ${origin}`);
            }
        }
    } catch (err) {
        if (err.message.includes('disallows path prefix')) {
            throw err;
        }
        log.warning(`Could not conclusively verify robots.txt at ${robotsUrl}: ${err.message}`);
    }
}

function parseRobotsRules(text) {
    if (!text) return null;
    const allow = [];
    const disallow = [];
    const lines = text.split(/\r?\n/);
    let starGroupActive = false;
    let lastDirectiveWasUserAgent = false;
    for (const rawLine of lines) {
        const line = rawLine.trim();
        if (!line || line.startsWith('#')) continue;
        const [directiveRaw, ...valueParts] = line.split(':');
        if (!directiveRaw || !valueParts.length) continue;
        const directive = directiveRaw.trim().toLowerCase();
        const value = valueParts.join(':').trim();
        if (directive === 'user-agent') {
            if (!lastDirectiveWasUserAgent) starGroupActive = false;
            lastDirectiveWasUserAgent = true;
            if (value && value.replace(/"/g, '').trim() === '*') starGroupActive = true;
            continue;
        }
        lastDirectiveWasUserAgent = false;
        if (!starGroupActive) continue;
        if (directive === 'allow') allow.push(value || '/');
        if (directive === 'disallow') disallow.push(value);
    }
    if (!allow.length && !disallow.length) return null;
    return { allow, disallow };
}

function isPathAllowed(path, rules) {
    if (!rules || !path) return true;
    const normalize = (segment) => {
        if (!segment) return '';
        if (segment === '/') return '/';
        const cleaned = segment.trim();
        if (!cleaned.startsWith('/')) return `/${cleaned}`;
        return cleaned;
    };
    const target = normalize(path);
    let longestAllow = -1;
    let longestDisallow = -1;
    for (const rule of rules.disallow || []) {
        const normalized = normalize(rule);
        if (!normalized) continue;
        if (target.startsWith(normalized) && normalized.length > longestDisallow) longestDisallow = normalized.length;
    }
    for (const rule of rules.allow || []) {
        const normalized = normalize(rule);
        if (!normalized) continue;
        if (target.startsWith(normalized) && normalized.length > longestAllow) longestAllow = normalized.length;
    }
    if (longestDisallow < 0) return true;
    if (longestAllow >= longestDisallow) return true;
    return false;
}

// Single-entrypoint main
await Actor.init();

async function main() {
    try {
        const input = (await Actor.getInput()) || {};
        const {
            keyword = '',
            category = '',
            location = '',
            language = '',
            results_wanted: RESULTS_WANTED_RAW = 100,
            max_pages: MAX_PAGES_RAW = 999,
            collectDetails = true,
            startUrl,
            startUrls,
            url,
            proxyConfiguration,
            cookies,
            cookiesJson,
            dedupe = true,
            maxConcurrency: MAX_CONCURRENCY_RAW,
            maxRequestRetries: MAX_REQUEST_RETRIES_RAW = 3,
        } = input;

        const RESULTS_WANTED = Number.isFinite(+RESULTS_WANTED_RAW) ? Math.max(1, +RESULTS_WANTED_RAW) : Number.MAX_SAFE_INTEGER;
        const MAX_PAGES = Number.isFinite(+MAX_PAGES_RAW) ? Math.max(1, +MAX_PAGES_RAW) : 999;
        const MAX_CONCURRENCY = Number.isFinite(+MAX_CONCURRENCY_RAW) && +MAX_CONCURRENCY_RAW > 0 ? Math.min(50, Math.max(1, +MAX_CONCURRENCY_RAW)) : 10;
        const MAX_REQUEST_RETRIES = Number.isFinite(+MAX_REQUEST_RETRIES_RAW) && +MAX_REQUEST_RETRIES_RAW >= 0 ? Math.min(10, Math.max(0, +MAX_REQUEST_RETRIES_RAW)) : 3;

        const baseOrigin = resolveBaseOrigin(location);
        const proxyConf = proxyConfiguration ? await Actor.createProxyConfiguration({ ...proxyConfiguration }) : undefined;
        await ensureRobotsAllowed(baseOrigin, proxyConf);

        log.info('iHerb product scraper started...');
        await Dataset.open('iherb-products');

        const toAbs = (href, base = baseOrigin) => {
            try { return new URL(href, base).href; } catch { return null; }
        };

        const cookieHeader = buildCookieHeader(cookies, cookiesJson);
        const acceptLanguageHeader = typeof language === 'string' && language.trim() ? language.trim() : null;

        const cleanText = (html) => {
            if (!html) return '';
            const $ = cheerioLoad(html);
            $('script, style, noscript, iframe').remove();
            return $.root().text().replace(/\s+/g, ' ').trim();
        };

        function normalizeStartEntry(entry) {
            if (!entry) return null;
            if (typeof entry === 'string') {
                const resolvedUrl = toAbs(entry, baseOrigin);
                if (!resolvedUrl) return null;
                const label = detectLabelFromUrl(resolvedUrl);
                const userData = { label };
                if (label === 'CATEGORY') userData.pageNo = 1;
                return { url: resolvedUrl, userData };
            }

            if (typeof entry === 'object') {
                const { url: candidateUrl, userData, ...rest } = entry;
                const resolvedUrl = toAbs(candidateUrl, baseOrigin);
                if (!resolvedUrl) return null;
                const mergedUserData = { ...(userData || {}) };
                if (!mergedUserData.label) mergedUserData.label = detectLabelFromUrl(resolvedUrl);
                if (mergedUserData.label === 'CATEGORY' && typeof mergedUserData.pageNo !== 'number') mergedUserData.pageNo = 1;
                return { ...rest, url: resolvedUrl, userData: mergedUserData };
            }

            return null;
        }

        function dedupeRequests(requests) {
            const seen = new Set();
            const output = [];
            for (const req of requests) {
                if (!req?.url) continue;
                if (seen.has(req.url)) continue;
                seen.add(req.url);
                output.push(req);
            }
            return output;
        }

        const buildStartUrl = (kw, cat, origin = baseOrigin) => {
            if (kw) {
                const u = new URL('/search', origin);
                u.searchParams.set('kw', String(kw).trim());
                return u.href;
            } else if (cat) {
                return new URL(`/c/${encodeURIComponent(String(cat).trim())}`, origin).href;
            } else {
                return new URL('/c/categories', origin).href;
            }
        };

        const initialEntries = [];
        const startUrlsSources = startUrls?.sources;
        if (Array.isArray(startUrlsSources) && startUrlsSources.length) initialEntries.push(...startUrlsSources);
        else if (Array.isArray(startUrls) && startUrls.length) initialEntries.push(...startUrls);
        if (startUrl) initialEntries.push(startUrl);
        if (url) initialEntries.push(url);

        async function expandRequestsFromUrl(entries) {
            const expanded = [];
            for (const entry of entries) {
                if (!entry || typeof entry !== 'object' || !entry.requestsFromUrl) {
                    expanded.push(entry);
                    continue;
                }
                const { requestsFromUrl, userData, ...rest } = entry;
                try {
                    const proxyUrl = proxyConf ? await proxyConf.newUrl() : undefined;
                    const response = await gotScraping({
                        url: requestsFromUrl,
                        proxyUrl,
                        timeout: { request: 30000 },
                    });
                    const body = response.body || '';
                    let sources;
                    try {
                        sources = JSON.parse(body);
                    } catch {
                        sources = body
                            .split(/\r?\n/)
                            .map((line) => line.trim())
                            .filter((line) => line && !line.startsWith('#'))
                            .map((line) => ({ url: line }));
                    }
                    const arr = Array.isArray(sources) ? sources : [sources];
                    for (const src of arr) {
                        if (!src) continue;
                        if (typeof src === 'string') {
                            expanded.push({ ...rest, url: src, userData });
                        } else {
                            const mergedUserData = { ...(userData || {}), ...(src.userData || {}) };
                            expanded.push({ ...rest, ...src, userData: mergedUserData });
                        }
                    }
                } catch (err) {
                    log.warning(`Failed to load requestsFromUrl (${requestsFromUrl}): ${err.message}`);
                }
            }
            return expanded;
        }

        const expandedEntries = await expandRequestsFromUrl(initialEntries);

        let normalizedInitial = expandedEntries
            .map((entry) => normalizeStartEntry(entry))
            .filter(Boolean);

        if (!normalizedInitial.length) normalizedInitial = [normalizeStartEntry(buildStartUrl(keyword, category))].filter(Boolean);

        const initialRequests = dedupeRequests(normalizedInitial);

        const requestQueue = await Actor.openRequestQueue();
        for (const req of initialRequests) {
            await requestQueue.addRequest(req);
        }

        let saved = 0;
        const seenProducts = new Set();
        const enqueuedProductUrls = new Set(initialRequests.filter((req) => req.userData?.label === 'PRODUCT').map((req) => req.url));
        const skipStats = { captcha: 0, blocked: 0 };

        function findProductLinks($, base) {
            const links = new Set();
            $('a[href*="/pr/"]').each((_, a) => {
                const href = $(a).attr('href');
                const abs = toAbs(href, base);
                if (abs) links.add(abs);
            });
            $('[data-product-url], [data-url]').each((_, el) => {
                const candidate = $(el).attr('data-product-url') || $(el).attr('data-url');
                if (!candidate) return;
                const abs = toAbs(candidate, base);
                if (abs && abs.includes('/pr/')) links.add(abs);
            });
            return [...links];
        }

        function findNextPage($, base) {
            const rel = $('a[rel="next"], link[rel="next"]').attr('href');
            if (rel) return toAbs(rel, base);
            const candidates = $('a, button')
                .filter((_, el) => {
                    const text = $(el).text().trim().toLowerCase();
                    if (!text) return false;
                    return ['next', '>', '>>', 'load more'].some((token) => text === token || text.includes(token));
                })
                .map((_, el) => $(el).attr('href') || $(el).attr('data-url'))
                .get()
                .filter(Boolean);
            for (const candidate of candidates) {
                const abs = toAbs(candidate, base);
                if (abs) return abs;
            }
            return null;
        }

        const crawler = new CheerioCrawler({
            proxyConfiguration: proxyConf,
            requestQueue,
            maxRequestRetries: MAX_REQUEST_RETRIES,
            useSessionPool: true,
            sessionPoolOptions: {
                maxPoolSize: 100,
                sessionOptions: {
                    maxErrorScore: 2,
                    maxUsageCount: 40,
                },
                blockedStatusCodes: [403, 429, 503],
            },
            persistCookiesPerSession: true,
            maxConcurrency: MAX_CONCURRENCY,
            requestHandlerTimeoutSecs: 90,
            navigationTimeoutSecs: 60,
            preNavigationHooks: [
                async ({ request, session }) => {
                    const headers = session?.userData?.headers || headerGenerator.getHeaders({
                        locales: acceptLanguageHeader ? [acceptLanguageHeader.split(',')[0]] : undefined,
                    });
                    if (session) session.userData.headers = headers;
                    request.headers = { ...(request.headers || {}), ...headers };
                    if (acceptLanguageHeader) request.headers['Accept-Language'] = acceptLanguageHeader;
                    if (cookieHeader && !request.headers.Cookie) request.headers.Cookie = cookieHeader;
                },
            ],
            throwOnBlocked: false,
            async requestHandler({ request, $, enqueueLinks, log: crawlerLog, response, session }) {
                const effectiveUrl = request.loadedUrl || request.url;
                const status = response?.statusCode;

                if (status === 403 || status === 429 || status === 503) {
                    crawlerLog.warning(`Blocked with status ${status} at ${effectiveUrl}; retiring session and retrying`);
                    if (session) session.markBad();
                    throw new Error(`Blocked with status ${status}`);
                }

                if (isBotChallenge($)) {
                    crawlerLog.warning(`Bot challenge detected at ${effectiveUrl}, skipping`);
                    skipStats.captcha++;
                    if (session) session.markBad();
                    return;
                }

                const label = request.userData?.label || detectLabelFromUrl(effectiveUrl);
                const pageNo = request.userData?.pageNo || 1;

                if (label === 'CATEGORY') {
                    const links = findProductLinks($, effectiveUrl);
                    const structuredItems = extractItemListProducts($, toAbs, effectiveUrl);
                    const nextData = extractNextData($);
                    const nextDataItems = extractListProductsFromNextData(nextData, toAbs, effectiveUrl);

                    const structuredCandidates = [...structuredItems, ...nextDataItems];
                    const structuredUrls = structuredCandidates.map((item) => item.product_url).filter(Boolean);
                    const allDiscovered = [...new Set([...links, ...structuredUrls])];
                    crawlerLog.info(`CATEGORY ${effectiveUrl} -> found ${links.length} anchor links, ${structuredUrls.length} structured links (Next.js: ${nextDataItems.length}), unique ${allDiscovered.length}`);

                    const remaining = RESULTS_WANTED - saved;
                    if (remaining <= 0) return;

                    const filteredLinks = dedupe
                        ? allDiscovered.filter((link) => {
                            if (enqueuedProductUrls.has(link)) return false;
                            enqueuedProductUrls.add(link);
                            return true;
                        })
                        : allDiscovered;

                    if (collectDetails) {
                        const toEnqueue = filteredLinks.slice(0, Math.max(0, remaining));
                        if (toEnqueue.length) {
                            await enqueueLinks({ urls: toEnqueue, userData: { label: 'PRODUCT' } });
                        }
                    } else {
                        const structuredByUrl = new Map(structuredCandidates.map((item) => [item.product_url, item]));
                        const toPush = filteredLinks.slice(0, Math.max(0, remaining)).map((productUrl) => {
                            const structured = structuredByUrl.get(productUrl);
                            return {
                                ...(structured || {}),
                                product_url: productUrl,
                                source_url: effectiveUrl,
                                _source: 'iherb.com',
                            };
                        });
                        if (toPush.length) {
                            await Dataset.pushData(toPush);
                            saved += toPush.length;
                        }
                    }

                    if (saved < RESULTS_WANTED && pageNo < MAX_PAGES) {
                        const next = findNextPage($, effectiveUrl);
                        if (next) {
                            await enqueueLinks({ urls: [next], userData: { label: 'CATEGORY', pageNo: pageNo + 1 } });
                        }
                    }
                    return;
                }


                if (label === 'PRODUCT') {
                    if (saved >= RESULTS_WANTED) return;
                    try {
        const product = {};
        product.product_url = effectiveUrl;
        product.source_url = effectiveUrl;
                        const match = effectiveUrl.match(/\/pr\/[^/]+\/(\d+)/);
                        if (match && match[1]) product.product_id = match[1];

                        const assign = (key, value) => {
                            if (value === undefined || value === null) return;
                            if (Array.isArray(value)) {
                                const existing = Array.isArray(product[key]) ? product[key] : [];
                                const merged = new Set([...existing, ...value.filter((item) => item !== undefined && item !== null)]);
                                if (merged.size) product[key] = [...merged];
                                return;
                            }
                            if (typeof value === 'string') {
                                const trimmed = value.trim();
                                if (!trimmed) return;
                                const current = product[key];
                                if (current === undefined || current === null || (typeof current === 'string' && !current.trim())) {
                                    product[key] = trimmed;
                                }
                                return;
                            }
                            if (typeof value === 'number') {
                                if (product[key] === undefined || product[key] === null) product[key] = value;
                                return;
                            }
                            if (typeof value === 'object') {
                                if (!Object.keys(value).length) return;
                                if (!product[key]) product[key] = value;
                                return;
                            }
                            if (product[key] === undefined || product[key] === null) product[key] = value;
                        };

                        const merge = (source) => {
                            if (!source || typeof source !== 'object') return;
                            for (const [key, value] of Object.entries(source)) assign(key, value);
                        };

                        const nextData = extractNextData($);
                        merge(extractProductFromNextData(nextData, toAbs, effectiveUrl));

                        const jsonLd = extractProductJsonLd($);
                        if (jsonLd) {
                            assign('product_title', jsonLd.name && String(jsonLd.name));
                            assign('brand', typeof jsonLd.brand === 'string' ? jsonLd.brand : jsonLd.brand?.name);
                            assign('sku', jsonLd.sku && String(jsonLd.sku));
                            if (jsonLd.productID) assign('product_id', String(jsonLd.productID));
                            assign('description_html', jsonLd.description && String(jsonLd.description));

                            const offer = Array.isArray(jsonLd.offers) ? jsonLd.offers[0] : jsonLd.offers;
                            if (offer) {
                                const currencyVal = offer.priceCurrency || offer.priceCurrencyCode;
                                if (offer.price !== undefined && offer.price !== null) assign('price', String(offer.price));
                                if (currencyVal) assign('currency', String(currencyVal));
                                if (offer.availability) assign('availability', sanitizeAvailability(offer.availability));
                                if (offer.sku) assign('sku', String(offer.sku));
                            }

                            const agg = jsonLd.aggregateRating;
                            if (agg) {
                                const aggRatingVal = agg.ratingValue !== undefined && agg.ratingValue !== null ? Number.parseFloat(agg.ratingValue) : null;
                                if (aggRatingVal !== null && !Number.isNaN(aggRatingVal)) assign('rating', aggRatingVal);
                                const aggReviewVal = agg.reviewCount !== undefined && agg.reviewCount !== null ? Number.parseInt(agg.reviewCount, 10) : null;
                                if (aggReviewVal !== null && !Number.isNaN(aggReviewVal)) assign('reviews_count', aggReviewVal);
                            }

                            if (jsonLd.image) {
                                const images = Array.isArray(jsonLd.image) ? jsonLd.image : [jsonLd.image];
                                assign('images', images.map((src) => toAbs(src, effectiveUrl)).filter(Boolean));
                            }
                            if (jsonLd.category) {
                                const categories = Array.isArray(jsonLd.category) ? jsonLd.category : [jsonLd.category];
                                assign('categories', categories.map((cat) => String(cat).trim()).filter(Boolean));
                            }
                        }

                        assign('product_title', $('h1').first().text());
                        assign('product_title', $('meta[property="og:title"]').attr('content'));
                        assign('brand', $('a[href*="/brand/"]').first().text());
                        assign('brand', $('[itemprop="brand"]').text());
                        const directPrice = $('[itemprop="price"]').attr('content');
                        if (directPrice) assign('price', directPrice);
                        const priceCandidate = $('.price, .price-current, .product-price').first().text();
                        const currencyAttr = $('[itemprop="priceCurrency"]').attr('content');
                        if (currencyAttr) assign('currency', String(currencyAttr));
                        if (priceCandidate && !/see price/i.test(priceCandidate)) assign('price', priceCandidate);
                        const availabilityRaw = $('[itemprop="availability"]').attr('content') || $('.availability, .stock-status').first().text();
                        if (availabilityRaw) assign('availability', sanitizeAvailability(availabilityRaw));
                        assign('sku', $('[itemprop="sku"]').attr('content'));

                        const descEl = $('#product, .product-description, .ProductOverview').first();
                        if (!product.description_html && descEl.length) assign('description_html', descEl.html());
                        if (!product.description_text && product.description_html) product.description_text = cleanText(product.description_html);
                        if (!product.description_text) assign('description_text', $('.product-overview, [data-test-id="product-details"]').first().text());

                        const ratingVal = parseFloat($('[itemprop="ratingValue"]').attr('content'));
                        if (!Number.isNaN(ratingVal)) assign('rating', ratingVal);
                        const reviewsVal = parseInt($('[itemprop="reviewCount"]').attr('content'), 10);
                        if (!Number.isNaN(reviewsVal)) assign('reviews_count', reviewsVal);

                        const imageSet = new Set(Array.isArray(product.images) ? product.images : []);
                        $('img').each((_, img) => {
                            const src = $(img).attr('src') || $(img).attr('data-src') || $(img).attr('data-lazy');
                            if (!src) return;
                            if (!/iherb\./i.test(src)) return;
                            const resolved = toAbs(src, effectiveUrl);
                            if (resolved) imageSet.add(resolved);
                        });
                        const ogImage = $('meta[property="og:image"]').attr('content');
                        if (ogImage) imageSet.add(toAbs(ogImage, effectiveUrl));
                        if (imageSet.size) product.images = [...imageSet];

                        const categorySet = new Set(Array.isArray(product.categories) ? product.categories : []);
                        $('.breadcrumb a, .breadcrumbs a').each((_, a) => {
                            const textVal = $(a).text().trim();
                            if (textVal) categorySet.add(textVal);
                        });
                        if (categorySet.size) product.categories = [...categorySet];

        if (!product.currency && product.price) {
            const currencyMeta = $('meta[itemprop="priceCurrency"], meta[name="currency"]').attr('content');
            if (currencyMeta) assign('currency', String(currencyMeta));
        }

                        if (!product.price) {
                            const note = derivePriceNote($);
                            if (note) assign('price_note', note);
                        }

        if (!product.product_title) {
            crawlerLog.warning(`PRODUCT ${effectiveUrl} missing title; skipping`);
            skipStats.blocked++;
            return;
        }

        if (!product.description_text && product.description_html) product.description_text = cleanText(product.description_html);
        if (!product.last_updated) product.last_updated = new Date().toISOString();
        if (!product._source) product._source = 'iherb.com';
        if (!product.origin) product.origin = baseOrigin;
        if (acceptLanguageHeader && !product.accept_language) product.accept_language = acceptLanguageHeader;
        if (product.currency && typeof product.currency === 'string') product.currency = product.currency.toUpperCase();

                        const uniqueKey = product.product_id || product.product_url;
                        if (dedupe && uniqueKey && seenProducts.has(uniqueKey)) {
                            crawlerLog.debug(`PRODUCT ${effectiveUrl} already processed, skipping duplicate`);
                            return;
                        }
                        if (uniqueKey) seenProducts.add(uniqueKey);

                        await Dataset.pushData(product);
                        saved++;
                    } catch (err) {
                        crawlerLog.error(`PRODUCT ${request.url} failed: ${err.message}`);
                    }
                    return;
                }
            },
            async failedRequestHandler({ request, log: crawlerLog, session, error }) {
                if (session) session.retire();
                crawlerLog.error(`Request ${request.url} failed after ${request.retryCount} retries: ${error?.message || error}`);
            },
        });

        await crawler.run();
        log.info(`iHerb product scraper finished. Saved ${saved} products`);
        if (skipStats.captcha) {
            log.warning(`Skipped ${skipStats.captcha} pages due to bot challenges`);
        }
        if (skipStats.blocked) {
            log.warning(`Skipped ${skipStats.blocked} products due to missing mandatory data`);
        }
        if (!saved) {
            log.warning('No products were saved. Verify start URLs/keywords and consider providing cookies or residential proxy to bypass geo or bot restrictions.');
        }
    } finally {
        await Actor.exit();
    }
}

main().catch(err => { console.error(err); process.exit(1); });

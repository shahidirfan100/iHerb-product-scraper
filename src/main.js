import { Actor, log } from 'apify';
import { Dataset, PlaywrightCrawler } from 'crawlee';

/**
 * Normalise origin/location input into a usable base URL.
 */
const normaliseOrigin = (location) => {
    if (!location) return 'https://www.iherb.com';
    if (/^https?:\/\//i.test(location)) {
        return location.replace(/\/+$/, '');
    }
    const cleaned = location.replace(/^https?:\/\//i, '').replace(/\/+$/, '');
    if (cleaned.includes('.')) {
        return `https://${cleaned}`;
    }
    return `https://${cleaned}.iherb.com`;
};

const buildProductUrl = (origin, slug, partNumber) => {
    if (!partNumber) return null;
    if (/^https?:\/\//i.test(slug)) return slug;
    const safeSlug = (slug || '').replace(/^\//, '');
    return `${origin}/pr/${safeSlug || 'product'}/${partNumber}`;
};

const asPositiveInteger = (value, fallback) => {
    if (value === undefined || value === null) return fallback;
    const num = Number(value);
    return Number.isFinite(num) && num > 0 ? Math.floor(num) : fallback;
};

/**
 * Parse cookies either from raw header string or JSON payload so we can replay them in Playwright.
 */
const parseCookies = (rawCookieHeader, cookiesJson, origin) => {
    const cookies = [];

    if (rawCookieHeader && typeof rawCookieHeader === 'string') {
        const pairs = rawCookieHeader.split(';').map((chunk) => chunk.trim()).filter(Boolean);
        for (const pair of pairs) {
            const [name, ...rest] = pair.split('=');
            if (!name || rest.length === 0) continue;
            cookies.push({
                name: name.trim(),
                value: rest.join('=').trim(),
            });
        }
    }

    if (cookiesJson && typeof cookiesJson === 'string') {
        try {
            const parsed = JSON.parse(cookiesJson);
            if (Array.isArray(parsed)) {
                for (const item of parsed) {
                    if (item?.name && item?.value) cookies.push(item);
                }
            } else if (parsed && typeof parsed === 'object') {
                for (const [name, value] of Object.entries(parsed)) {
                    if (typeof value === 'string') {
                        cookies.push({ name, value });
                    }
                }
            }
        } catch (err) {
            log.warning(`Failed to parse cookiesJson: ${err.message}`);
        }
    }

    if (!cookies.length) return [];

    const url = new URL(origin);
    return cookies.map((cookie) => ({
        url: cookie.url ?? origin,
        domain: cookie.domain ?? url.hostname,
        path: cookie.path ?? '/',
        httpOnly: cookie.httpOnly ?? false,
        secure: cookie.secure ?? true,
        sameSite: cookie.sameSite ?? 'Lax',
        expires: cookie.expires,
        name: cookie.name,
        value: cookie.value,
    }));
};

await Actor.init();

const input = await Actor.getInput() ?? {};
const {
    startUrls = [],
    url: singleUrl,
    keyword = '',
    category = '',
    location = '',
    language = '',
    collectDetails = true,
    results_wanted: resultsWantedInput,
    max_pages: maxPagesInput = 20,
    proxyConfiguration: proxyInput,
    cookies: rawCookies,
    cookiesJson,
    dedupe = true,
    maxConcurrency: maxConcurrencyInput,
} = input;

const baseOrigin = normaliseOrigin(location);
const resultsWanted =
    resultsWantedInput === undefined || resultsWantedInput === null
        ? 100
        : asPositiveInteger(resultsWantedInput, Number.POSITIVE_INFINITY);
const maxPages = asPositiveInteger(maxPagesInput, 20);
const maxConcurrency = asPositiveInteger(maxConcurrencyInput, 8);

log.info('Using origin:', { baseOrigin });

const proxyConfiguration = await Actor.createProxyConfiguration(proxyInput);
const requestQueue = await Actor.openRequestQueue();

const initialRequests = [];
const productSeenSet = new Set();
let savedCount = 0;

const deriveListingMeta = (url) => {
    try {
        const { origin, pathname, searchParams } = new URL(url);
        const key = `${origin}${pathname}`;
        const page = Number(searchParams.get('page')) || Number(searchParams.get('p')) || 1;
        return { listingKey: key, page };
    } catch {
        return { listingKey: url, page: 1 };
    }
};

const enqueueInitialListing = (url, extraUserData = {}) => {
    if (!url) return;
    const absolute = url.startsWith('http') ? url : `${baseOrigin}${url.startsWith('/') ? '' : '/'}${url}`;
    const meta = deriveListingMeta(absolute);
    const userData = { ...extraUserData };
    if (userData.label === undefined) userData.label = 'LISTING';
    if (userData.page === undefined) userData.page = meta.page;
    if (userData.listingKey === undefined) userData.listingKey = meta.listingKey;

    initialRequests.push({
        url: absolute,
        userData,
    });
};

const enqueueInitialProduct = (url, extraUserData = {}) => {
    if (!url) return;
    const absolute = url.startsWith('http') ? url : `${baseOrigin}${url.startsWith('/') ? '' : '/'}${url}`;
    const userData = { ...extraUserData };
    if (userData.label === undefined) userData.label = 'PRODUCT';

    initialRequests.push({
        url: absolute,
        userData,
    });
};

if (Array.isArray(startUrls) && startUrls.length) {
    const requestList = await Actor.openRequestList('INITIAL_START_URLS', startUrls);
    let sourceRequest;
    // Drain the list so remote request sources are loaded.
    while ((sourceRequest = await requestList.fetchNextRequest())) {
        const targetUrl = sourceRequest.url;
        if (targetUrl?.includes('/pr/') || sourceRequest.userData?.label === 'PRODUCT') {
            enqueueInitialProduct(targetUrl, sourceRequest.userData);
        } else {
            enqueueInitialListing(targetUrl, sourceRequest.userData);
        }
        await requestList.markRequestHandled(sourceRequest);
    }
}

if (!initialRequests.length && singleUrl) {
    if (singleUrl.includes('/pr/')) {
        enqueueInitialProduct(singleUrl);
    } else {
        enqueueInitialListing(singleUrl);
    }
}

if (!initialRequests.length && keyword) {
    const searchUrl = `${baseOrigin}/search?kw=${encodeURIComponent(keyword)}`;
    enqueueInitialListing(searchUrl);
}

if (!initialRequests.length && category) {
    const categorySlug = category.replace(/^\//, '');
    const categoryUrl = `${baseOrigin}/c/${categorySlug}`;
    enqueueInitialListing(categoryUrl);
}

if (!initialRequests.length) {
    throw new Error('No valid start URLs supplied. Provide keyword/category/startUrls/url.');
}

for (const req of initialRequests) {
    await requestQueue.addRequest(req);
}

log.info(`Seeded ${initialRequests.length} initial request${initialRequests.length === 1 ? '' : 's'}.`);

const cookiesForContext = parseCookies(rawCookies, cookiesJson, baseOrigin);

const listingStats = new Map();
let crawler;
let hasAborted = false;

const shouldStop = () => savedCount >= resultsWanted;
const stopIfNeeded = async () => {
    if (shouldStop() && crawler?.autoscaledPool && !hasAborted) {
        hasAborted = true;
        log.info('Result limit reached, stopping crawler...');
        await crawler.autoscaledPool.abort();
    }
};

const extractNextData = async (page) => {
    return page.evaluate(() => {
        const fromWindow = window.__NEXT_DATA__;
        if (fromWindow) return fromWindow;
        const script = document.querySelector('script#__NEXT_DATA__');
        if (!script) return null;
        try {
            return JSON.parse(script.textContent);
        } catch (err) {
            return null;
        }
    });
};

const looksLikeChallengePage = async (page) => {
    const title = (await page.title()).toLowerCase();
    if (title.includes('just a moment') || title.includes('please wait')) return true;

    const bodyText = await page.evaluate(() => document.body?.innerText?.slice(0, 4000)?.toLowerCase() ?? '');
    return bodyText.includes('cf-chl') || bodyText.includes('cloudflare') || bodyText.includes('bot detection');
};

const applyStealthScripts = async (page) => {
    if (page.context().__stealthApplied) return;
    page.context().__stealthApplied = true;
    await page.addInitScript(() => {
        // Hide webdriver flag.
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

        // Provide fake chrome object.
        window.chrome ??= { runtime: {} };

        // Pretend to have plugins and languages.
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4],
        });
        Object.defineProperty(navigator, 'languages', {
            get: () => ['en-US', 'en'],
        });

        // Patch permissions query to avoid notification warnings.
        const originalQuery = navigator.permissions?.query;
        if (originalQuery) {
            navigator.permissions.query = (parameters) =>
                parameters?.name === 'notifications'
                    ? Promise.resolve({ state: Notification.permission })
                    : originalQuery(parameters);
        }

        // Provide stable WebGL vendor.
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function patched(param) {
            if (param === 37445) return 'Intel Inc.';
            if (param === 37446) return 'Intel Iris OpenGL Engine';
            return getParameter.call(this, param);
        };

        // Remove broken iframe contentWindow.
        Object.defineProperty(HTMLIFrameElement.prototype, 'contentWindow', {
            get() {
                return window;
            },
        });
    });
};

const pushListingDatasetItem = async (origin, item) => {
    const productId = item.partNumber || item.id;
    if (!productId) return;
    const productUrl = buildProductUrl(origin, item.slug, productId);
    if (!productUrl) return;
    const priceValue = item.pricing?.price ?? item.price?.value ?? item.price;
    const currencyValue = item.pricing?.currency ?? item.price?.currency ?? item.currency;
    const ratingValue = item.rating ?? item.averageRating ?? item.reviews?.rating;
    const reviewCount = item.numberOfReviews ?? item.reviewsCount ?? item.reviews?.total;
    await Dataset.pushData({
        product_url: productUrl,
        product_id: String(productId),
        product_title: item.displayName,
        brand: item.brand?.name ?? item.brand,
        price: priceValue ? String(priceValue) : undefined,
        currency: currencyValue ?? undefined,
        rating: ratingValue,
        reviews_count: reviewCount,
        image: item.images?.[0]?.url ?? item.imageUrl,
    });
    savedCount++;
};

const pushProductDatasetItem = async (url, product) => {
    const productId = product.partNumber || product.id;
    const priceValue = product.pricing?.price ?? product.price?.value ?? product.price;
    const currencyValue = product.pricing?.currency ?? product.price?.currency ?? product.currency;
    const ratingValue = product.rating ?? product.averageRating ?? product.reviews?.rating;
    const reviewCount = product.numberOfReviews ?? product.reviewsCount ?? product.reviews?.total;
    await Dataset.pushData({
        product_url: url,
        product_id: productId ? String(productId) : undefined,
        product_title: product.displayName,
        brand: product.brand?.name ?? product.brand,
        price: priceValue ? String(priceValue) : undefined,
        currency: currencyValue ?? undefined,
        rating: ratingValue,
        reviews_count: reviewCount,
        availability: product.inventory?.availability,
        description_html: product.overview,
        description_text: product.description,
        images: product.images?.map((img) => img.url).filter(Boolean),
    });
    savedCount++;
};

const enqueueNextListingPage = async ({ request, pageProps, crawler: crawlerInstance }) => {
    const listingKey = request.userData.listingKey ?? request.url;
    const stat = listingStats.get(listingKey) ?? { enqueued: new Set(), maxQueued: 0 };
    const pagination = pageProps.pagination ?? pageProps?.productsMeta?.pagination ?? {};
    const currentPage = Number(request.userData.page ?? pagination.currentPage ?? pagination.page ?? 1);
    const totalPages =
        Number(pagination.totalPages ?? pagination.pageCount ?? pagination.total ?? pagination.lastPage ?? 1);

    if (currentPage >= totalPages) return;

    const nextPage = currentPage + 1;
    if (nextPage > maxPages) return;

    if (shouldStop()) return;

    let nextUrl = pagination?.nextPageUrl ?? pagination?.nextPage ?? null;
    if (nextUrl) {
        if (!nextUrl.startsWith('http')) {
            nextUrl = `${baseOrigin}${nextUrl.startsWith('/') ? '' : '/'}${nextUrl}`;
        }
    } else {
        try {
            const url = new URL(request.url);
            if (url.searchParams.has('page')) {
                url.searchParams.set('page', String(nextPage));
            } else if (url.searchParams.has('p')) {
                url.searchParams.set('p', String(nextPage));
            } else {
                url.searchParams.set('page', String(nextPage));
            }
            nextUrl = url.toString();
        } catch (err) {
            log.warning(`Failed to construct next page URL from ${request.url}: ${err.message}`);
            return;
        }
    }

    if (!nextUrl || stat.enqueued.has(nextUrl)) return;

    stat.enqueued.add(nextUrl);
    stat.maxQueued = Math.max(stat.maxQueued, nextPage);
    listingStats.set(listingKey, stat);

    await crawlerInstance.addRequests([{
        url: nextUrl,
        userData: {
            label: 'LISTING',
            page: nextPage,
            listingKey,
        },
    }]);
};

crawler = new PlaywrightCrawler({
    requestQueue,
    proxyConfiguration,
    maxConcurrency,
    requestHandlerTimeoutSecs: 75,
    navigationTimeoutSecs: 35,
    useSessionPool: true,
    sessionPoolOptions: {
        sessionOptions: {
            maxUsageCount: 15,
            maxAgeSecs: 300,
        },
    },
    browserPoolOptions: {
        useFingerprints: true,
        fingerprintOptions: {
            fingerprintGeneratorOptions: {
                devices: ['desktop'],
                browsers: ['chrome', 'edge'],
                operatingSystems: ['windows'],
                locales: language ? [language] : undefined,
            },
        },
    },
    launchContext: {
        launchOptions: {
            headless: true,
            args: [
                '--disable-dev-shm-usage',
                '--no-sandbox',
            ],
        },
    },
    preNavigationHooks: [
        async (ctx) => {
            const { page, session, browserController } = ctx;

            const originalGoto = ctx.gotoOptions ?? {};
            ctx.gotoOptions = {
                ...originalGoto,
                waitUntil: originalGoto.waitUntil ?? 'domcontentloaded',
                timeout: originalGoto.timeout ?? 60000,
            };

            if (!page.__blockResourcesApplied) {
                await page.route('**/*', (route) => {
                    const type = route.request().resourceType();
                    if (['image', 'media'].includes(type)) {
                        return route.abort();
                    }
                    return route.continue();
                });
                page.__blockResourcesApplied = true;
            }

            if (language) {
                await page.setExtraHTTPHeaders({
                    'Accept-Language': language,
                });
            }

            if (cookiesForContext.length && !page.context().__cookiesApplied) {
                try {
                    await page.context().addCookies(cookiesForContext);
                    page.context().__cookiesApplied = true;
                } catch (err) {
                    log.warning(`Failed to add cookies to context: ${err.message}`);
                }
            }

            await applyStealthScripts(page);

            const fingerprint = browserController?.fingerprint;
            if (fingerprint) {
                const { screen, navigator: nav } = fingerprint;
                if (screen?.width && screen?.height) {
                    await page.setViewportSize({
                        width: screen.width,
                        height: screen.height,
                    });
                }
                const ua = nav?.userAgent;
                if (ua) await page.setUserAgent(ua);
            }

            if (session?.userData) {
                session.userData.lastUrl = page.url();
            }
        },
    ],
    postNavigationHooks: [
        async ({ page, session }) => {
            if (await looksLikeChallengePage(page)) {
                log.warning('Bot challenge detected, retiring session.');
                session?.markBad?.();
                if (session) session.retire();
                throw new Error('Encountered bot challenge / Cloudflare gate');
            }
        },
    ],
    async requestHandler({ page, request, log: crawlerLog, crawler: crawlerInstance }) {
        const { label } = request.userData;
        crawlerLog.info(`Processing ${label ?? 'UNKNOWN'}: ${request.url}`);

        let nextData = null;
        for (let attempt = 0; attempt < 3; attempt++) {
            await page.waitForFunction(
                () => typeof window !== 'undefined' && !!window.__NEXT_DATA__?.props?.pageProps,
                { timeout: 20000 }
            ).catch(() => {});
            nextData = await extractNextData(page);
            if (nextData?.props?.pageProps) break;
            await page.waitForTimeout(1500);
        }

        if (!nextData?.props?.pageProps) {
            crawlerLog.warning('No Next.js payload found, skipping.');
            session?.markBad?.();
            return;
        }

        const pageProps = nextData.props.pageProps;
        session?.markGood?.();

        const collectFromPageProps = (props) => {
            const pools = [];
            if (Array.isArray(props.products)) pools.push(props.products);
            if (Array.isArray(props.productSummaries)) pools.push(props.productSummaries);
            if (props.productGrid) {
                const grid = props.productGrid;
                if (Array.isArray(grid.products)) pools.push(grid.products);
                if (Array.isArray(grid.items)) pools.push(grid.items);
                if (Array.isArray(grid.productSummaries)) pools.push(grid.productSummaries);
                if (Array.isArray(grid.results)) pools.push(grid.results);
            }
            if (props.category?.products) pools.push(props.category.products);
            if (props.category?.productList?.items) pools.push(props.category.productList.items);
            if (props.results?.items) pools.push(props.results.items);
            if (props.searchResults?.products) pools.push(props.searchResults.products);
            if (props.searchResults?.items) pools.push(props.searchResults.items);

            const merged = [];
            for (const arr of pools) {
                if (!Array.isArray(arr)) continue;
                for (const item of arr) {
                    if (!item) continue;
                    merged.push(item);
                }
            }
            return merged;
        };

        const listingProducts = collectFromPageProps(pageProps);

        if (label === 'PRODUCT' || pageProps.product) {
            if (shouldStop()) return;
            const product = pageProps.product ?? pageProps.products?.[0];
            if (!product) {
                crawlerLog.warning('No product data in product pageProps.');
                return;
            }

            const productId = product.partNumber || product.id;
            if (dedupe && productId && productSeenSet.has(productId)) {
                crawlerLog.debug(`Skipping duplicate product ${productId}`);
                return;
            }
            if (dedupe && productId) productSeenSet.add(productId);

            await pushProductDatasetItem(request.url, product);
            crawlerLog.info(`Saved product ${savedCount}${Number.isFinite(resultsWanted) ? `/${resultsWanted}` : ''}`);
            await stopIfNeeded();
            return;
        } else {
            let productsToHandle = listingProducts;

            if (!productsToHandle.length) {
                const domResults = await page.evaluate(() => {
                    const items = [];
                    const seen = new Set();
                    const anchors = Array.from(document.querySelectorAll('a[href*="/pr/"]'));
                    for (const anchor of anchors) {
                        const href = anchor.href;
                        if (!href || seen.has(href)) continue;
                        seen.add(href);
                        const titleNode =
                            anchor.querySelector('h1, h2, h3, [data-element="product-title"], [data-testid="product-card-title"]') ??
                            anchor;
                        const title = titleNode.textContent?.trim();
                        if (!title) continue;
                        const priceNode =
                            anchor.querySelector('[data-element="product-price"], [data-testid="product-card-price"]') ??
                            anchor.closest('[data-element="product-card"]')?.querySelector('[data-element="product-price"]');
                        const priceText = priceNode?.textContent?.trim() ?? null;
                        const priceTextNormalized = priceText?.replace(/\s+/g, ' ') ?? null;
                        const partMatch = href.match(/\/pr\/[^/]+\/([^/?#]+)/);
                        const slugMatch = href.match(/\/pr\/([^/?#]+)/);
                        items.push({
                            href,
                            title,
                            priceText: priceTextNormalized,
                            partNumber: partMatch ? partMatch[1] : undefined,
                            slug: slugMatch ? slugMatch[1] : undefined,
                        });
                    }
                    return items;
                });

                if (domResults.length) {
                    crawlerLog.info(`Falling back to DOM extraction, found ${domResults.length} product anchors.`);
                    productsToHandle = domResults.map((item) => ({
                        partNumber: item.partNumber,
                        slug: item.slug ?? '',
                        displayName: item.title,
                        pricing: (() => {
                            if (!item.priceText) return undefined;
                            const numeric = Number(item.priceText.replace(/[^0-9.,]/g, '').replace(',', '.'));
                            if (Number.isFinite(numeric)) return { price: numeric };
                            return undefined;
                        })(),
                        currency: (() => {
                            if (!item.priceText) return undefined;
                            const match = item.priceText.match(/[A-Z]{3}/);
                            return match ? match[0] : undefined;
                        })(),
                        href: item.href,
                    }));
                }
            }

            if (!productsToHandle.length) {
                crawlerLog.warning(`No products discovered on listing ${request.url}`);
                return;
            }

            crawlerLog.info(`Found ${productsToHandle.length} products on listing page.`);
            const newProductRequests = [];

            for (const item of productsToHandle) {
                if (shouldStop()) break;
                const productId = item.partNumber || item.id;
                const productUrl = buildProductUrl(baseOrigin, item.slug, productId);
                if (!productUrl && item.href) {
                    const normalized = item.href.startsWith('http') ? item.href : `${baseOrigin}${item.href}`;
                    if (normalized.includes('/pr/')) {
                        const dedupeKey = normalized;
                        if (dedupe && productSeenSet.has(dedupeKey)) continue;
                        if (dedupe) productSeenSet.add(dedupeKey);
                        await Dataset.pushData({
                            product_url: normalized,
                            product_title: item.displayName ?? item.title,
                        });
                        savedCount++;
                        continue;
                    }
                }
                if (!productUrl) continue;

                if (dedupe) {
                    const dedupeKey = productId ?? productUrl;
                    if (productSeenSet.has(dedupeKey)) {
                        continue;
                    }
                    productSeenSet.add(dedupeKey);
                }

                if (collectDetails) {
                    newProductRequests.push({
                        url: productUrl,
                        userData: { label: 'PRODUCT' },
                    });
                } else {
                    await pushListingDatasetItem(baseOrigin, item);
                    crawlerLog.info(`Saved product ${savedCount}${Number.isFinite(resultsWanted) ? `/${resultsWanted}` : ''}`);
                }
            }

            if (collectDetails && newProductRequests.length) {
                await crawlerInstance.addRequests(newProductRequests);
            }

            if (!shouldStop()) {
                await enqueueNextListingPage({ request, pageProps, crawler: crawlerInstance });
            } else {
                await stopIfNeeded();
            }
        }
    },
    failedRequestHandler: async ({ request, error, session }) => {
        log.error(`Request failed ${request.url}: ${error?.message}`);
        session?.markBad?.();
        session?.retire?.();
    },
});

await crawler.run();
log.info(`Completed. Saved ${savedCount} products.`);
await Actor.exit();

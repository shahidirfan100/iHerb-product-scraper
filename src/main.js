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
    const scriptLocator = page.locator('script#__NEXT_DATA__');
    if (!(await scriptLocator.count())) return null;
    const payload = await scriptLocator.textContent();
    if (!payload) return null;
    try {
        return JSON.parse(payload);
    } catch {
        return null;
    }
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
    await Dataset.pushData({
        product_url: productUrl,
        product_id: String(productId),
        product_title: item.displayName,
        brand: item.brand?.name ?? item.brand,
        price: item.pricing?.price?.toString(),
        currency: item.pricing?.currency,
        rating: item.rating,
        reviews_count: item.numberOfReviews,
        image: item.images?.[0]?.url ?? item.imageUrl,
    });
    savedCount++;
};

const pushProductDatasetItem = async (url, product) => {
    const productId = product.partNumber || product.id;
    await Dataset.pushData({
        product_url: url,
        product_id: productId ? String(productId) : undefined,
        product_title: product.displayName,
        brand: product.brand?.name ?? product.brand,
        price: product.pricing?.price?.toString(),
        currency: product.pricing?.currency,
        rating: product.rating,
        reviews_count: product.numberOfReviews,
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
                '--disable-blink-features=AutomationControlled',
                '--disable-extensions',
                '--disable-default-apps',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-web-security',
                '--disable-background-timer-throttling',
                '--disable-renderer-backgrounding',
                '--disable-features=IsolateOrigins,site-per-process,TranslateUI',
            ],
        },
    },
    preNavigationHooks: [
        async ({ page, session, browserController, gotoOptions }) => {
            if (gotoOptions) {
                gotoOptions.waitUntil ??= 'networkidle';
                gotoOptions.timeout ??= 45000;
            }

            if (!page.__blockResourcesApplied) {
                await page.route('**/*', (route) => {
                    const type = route.request().resourceType();
                    const url = route.request().url();
                    if (['image', 'media', 'font'].includes(type)) {
                        return route.abort();
                    }
                    if (
                        url.includes('google-analytics') ||
                        url.includes('doubleclick.net') ||
                        url.includes('googletagmanager') ||
                        url.includes('facebook.net') ||
                        url.includes('hotjar.com') ||
                        url.includes('clarity.ms')
                    ) {
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
                if (session) session.retire();
                throw new Error('Encountered bot challenge / Cloudflare gate');
            }
        },
    ],
    async requestHandler({ page, request, log: crawlerLog, crawler: crawlerInstance }) {
        const { label } = request.userData;
        crawlerLog.info(`Processing ${label ?? 'UNKNOWN'}: ${request.url}`);

        try {
            await page.waitForSelector('script#__NEXT_DATA__', { timeout: 20000 });
        } catch (err) {
            if (await looksLikeChallengePage(page)) {
                crawlerLog.warning('Challenge page detected before Next.js payload.');
                throw err;
            }
            crawlerLog.warning(`__NEXT_DATA__ not found within timeout on ${request.url}: ${err.message}`);
        }

        const nextData = await extractNextData(page);
        if (!nextData?.props?.pageProps) {
            crawlerLog.warning('No Next.js payload found, skipping.');
            return;
        }

        const pageProps = nextData.props.pageProps;

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
        }

        if (pageProps.products && Array.isArray(pageProps.products)) {
            crawlerLog.info(`Found ${pageProps.products.length} products on listing page.`);
            const newProductRequests = [];

            for (const item of pageProps.products) {
                if (shouldStop()) break;
                const productId = item.partNumber || item.id;
                const productUrl = buildProductUrl(baseOrigin, item.slug, productId);
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
        } else {
            crawlerLog.warning(`Unhandled label/pageProps combination for ${request.url}`);
        }
    },
});

await crawler.run();
log.info(`Completed. Saved ${savedCount} products.`);
await Actor.exit();

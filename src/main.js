import { Actor, log } from 'apify';
import { Dataset, PlaywrightCrawler } from 'crawlee';
import { Session } from '@crawlee/core';

/**
 * ANTI-CLOUDFLARE CONFIGURATION GUIDE
 * ====================================
 * 
 * iHerb uses Cloudflare protection. If you see "Bot challenge detected":
 * 
 * 1. **ENABLE PROXIES** (MOST IMPORTANT!)
 *    - Set proxyConfiguration.useApifyProxy = true
 *    - Use RESIDENTIAL proxies (best for Cloudflare)
 *    - Datacenter proxies may work but have higher block rate
 * 
 * 2. **Use Low Concurrency**
 *    - maxConcurrency = 1 (safest)
 *    - maxConcurrency = 2-3 (if you have good proxies)
 * 
 * 3. **Increase Delays**
 *    - Default: 2-5 seconds between requests
 *    - If still blocked: increase to 5-10 seconds
 * 
 * 4. **Browser Configuration**
 *    - Uses headless Chromium shipped with the Apify base image
 *    - Enhanced stealth scripts applied automatically
 *    - Realistic fingerprints via Crawlee
 * 
 * 5. **Wait Strategy**
 *    - Using 'load' instead of 'networkidle' for faster response
 *    - 2-second wait after navigation for challenge detection
 * 
 * Expected behavior:
 * - First few requests may be challenged
 * - Session pool will rotate bad sessions
 * - Success rate should be >50% with good proxies
 * - Without proxies: 0-10% success rate (not recommended)
 */

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
    // Handle country codes like 'pk', 'au', 'ru', etc.
    if (cleaned.match(/^[a-z]{2}$/i)) {
        return `https://${cleaned}.iherb.com`;
    }
    return `https://www.iherb.com`;
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
const maxConcurrency = asPositiveInteger(maxConcurrencyInput, 1);

log.info('Using origin:', { baseOrigin });

const proxyConfiguration = await Actor.createProxyConfiguration(proxyInput);

// CRITICAL: Validate proxy configuration
if (!proxyConfiguration || !proxyInput?.useApifyProxy) {
    log.warning('  WARNING: No proxies configured! iHerb WILL block you without proxies.');
    log.warning('  For production use, enable Apify Proxy (RESIDENTIAL recommended)');
    log.warning('  Set proxyConfiguration.useApifyProxy = true in input');
} else {
    log.info(' Proxy configuration enabled:', proxyInput);
}

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
    log.info(`Creating search URL for keyword "${keyword}": ${searchUrl}`);
    enqueueInitialListing(searchUrl);
}

if (!initialRequests.length && category) {
    const categorySlug = category.replace(/^\//, '');
    const categoryUrl = `${baseOrigin}/c/${categorySlug}`;
    log.info(`Creating category URL for "${category}": ${categoryUrl}`);
    enqueueInitialListing(categoryUrl);
}

if (!initialRequests.length) {
    throw new Error('No valid start URLs supplied. Provide keyword/category/startUrls/url.');
}

for (const req of initialRequests) {
    await requestQueue.addRequest(req);
    log.info(`Enqueued ${req.userData.label || 'UNKNOWN'}: ${req.url}`);
}

log.info('========================================');
log.info(`Seeded ${initialRequests.length} initial request${initialRequests.length === 1 ? '' : 's'}`);
log.info(`Configuration: maxConcurrency=${maxConcurrency}, maxRetries=10, browser=Chromium, resultsWanted=${resultsWanted}`);
log.info(`Wait strategy: load (Cloudflare-friendly), delays: 2-5s per request`);
log.info(`Anti-bot measures: Session rotation, enhanced stealth, human-like behavior, random delays`);
log.info(`Proxies: ${proxyConfiguration ? 'ENABLED' : 'DISABLED (will likely be blocked!)'}`);
log.info('========================================');

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
    if (title.includes('just a moment') || title.includes('please wait') || title.includes('access denied') || title.includes('attention required')) return true;

    const bodyText = await page.evaluate(() => document.body?.innerText?.slice(0, 5000)?.toLowerCase() ?? '');
    if (bodyText.includes('cf-chl') || bodyText.includes('cloudflare') || bodyText.includes('bot detection') || 
        bodyText.includes('captcha') || bodyText.includes('rate limit') || bodyText.includes('too many requests')) {
        return true;
    }

    try {
        const response = page.mainFrame().page().context().pages()[0]?.url();
        if (response && response !== page.url() && !response.includes('iherb.com')) {
            return true;
        }
    } catch {
        // Ignore redirect check errors
    }

    return false;
};

const waitForChallengeResolution = async (page, options = {}) => {
    const {
        maxAttempts = 3,
        intervalMs = 5000,
        navigationTimeoutMs = 7000,
    } = options;

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
        // If challenge already gone, succeed
        if (!(await looksLikeChallengePage(page))) {
            return true;
        }

        // Wait a bit to let Cloudflare JS challenge finish
        await page.waitForTimeout(intervalMs);

        // Try to interact with possible challenge elements
        await page.evaluate(() => {
            const selectors = [
                'input[type="button"][value]',
                'button#challenge-verify-button',
                'button[name="verify"]',
                'button[type="submit"]',
                'button:has(span:contains("Verify"))',
                '#challenge-stage button',
                'div[id*="cf-chl"] button',
            ];
            for (const sel of selectors) {
                const btn = document.querySelector(sel);
                if (btn) {
                    btn.click();
                }
            }
        }).catch(() => {});

        // Perform subtle human-like mouse movement
        try {
            const width = await page.evaluate(() => window.innerWidth);
            const height = await page.evaluate(() => window.innerHeight);
            const x = Math.floor(Math.random() * Math.max(width - 20, 20));
            const y = Math.floor(Math.random() * Math.max(height - 20, 20));
            await page.mouse.move(x, y, { steps: 8 });
        } catch {
            // ignore
        }

        // Try to catch automatic redirect / navigation
        try {
            await Promise.race([
                page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: navigationTimeoutMs }),
                page.waitForLoadState('domcontentloaded', { timeout: navigationTimeoutMs }),
            ]);
        } catch {
            // Ignore navigation timeout, we'll re-check the content below
        }

        if (!(await looksLikeChallengePage(page))) {
            return true;
        }
    }

    return !(await looksLikeChallengePage(page));
};

const applyStealthScripts = async (page, fingerprint) => {
    if (page.context().__stealthApplied) return;
    page.context().__stealthApplied = true;

    const stealthPayload = {
        navigatorData: fingerprint?.navigator ?? {},
        screenData: fingerprint?.screen ?? {},
        viewportData: fingerprint?.viewport ?? {},
        timezone: fingerprint?.timezone ?? {},
    };

    await page.addInitScript(({ data }) => {
        const {
            navigatorData = {},
            screenData = {},
            viewportData = {},
            timezone = {},
        } = data ?? {};

        const defineReadonly = (target, key, value) => {
            try {
                Object.defineProperty(target, key, {
                    get: () => value,
                    configurable: true,
                });
            } catch {
                // ignore
            }
        };

        const deletePropertyIfExists = (target, key) => {
            try {
                if (key in target) delete target[key];
            } catch {
                // ignore
            }
        };

        const proto = Object.getPrototypeOf(navigator);
        if (proto && Object.getOwnPropertyDescriptor(proto, 'webdriver')) {
            deletePropertyIfExists(proto, 'webdriver');
        }
        defineReadonly(navigator, 'webdriver', false);

        const buildChromeObject = () => ({
            runtime: {},
            loadTimes: () => {},
            csi: () => {},
            app: {},
        });
        defineReadonly(window, 'chrome', window.chrome ?? buildChromeObject());

        const languages = navigatorData.languages ?? ['en-US', 'en'];
        defineReadonly(navigator, 'languages', languages);
        defineReadonly(navigator, 'language', languages[0]);

        const platform = navigatorData.platform ?? 'Win32';
        defineReadonly(navigator, 'platform', platform);

        if (navigatorData.hardwareConcurrency) {
            defineReadonly(navigator, 'hardwareConcurrency', navigatorData.hardwareConcurrency);
        } else {
            defineReadonly(navigator, 'hardwareConcurrency', 8);
        }
        if (navigatorData.deviceMemory) {
            defineReadonly(navigator, 'deviceMemory', navigatorData.deviceMemory);
        } else {
            defineReadonly(navigator, 'deviceMemory', 8);
        }

        defineReadonly(navigator, 'maxTouchPoints', navigatorData.maxTouchPoints ?? 0);
        defineReadonly(navigator, 'doNotTrack', navigatorData.doNotTrack ?? '1');

        const connection = navigatorData.connection ?? {
            effectiveType: '4g',
            downlink: 10,
            rtt: 50,
            saveData: false,
        };
        defineReadonly(navigator, 'connection', connection);

        const buildPlugins = () => {
            const plugin = { description: '', filename: 'internal', name: 'Internal PDF Viewer' };
            return {
                length: 1,
                0: plugin,
                item: () => plugin,
                namedItem: () => plugin,
                refresh: () => {},
            };
        };
        defineReadonly(navigator, 'plugins', navigatorData.plugins ?? buildPlugins());

        const buildMimeTypes = () => {
            const mime = { type: 'application/pdf', description: 'Portable Document Format', suffixes: 'pdf' };
            return {
                length: 1,
                0: mime,
                item: () => mime,
                namedItem: () => mime,
            };
        };
        defineReadonly(navigator, 'mimeTypes', navigatorData.mimeTypes ?? buildMimeTypes());

        const brands = navigatorData.userAgentData?.brands ?? [
            { brand: 'Chromium', version: '118' },
            { brand: 'Google Chrome', version: '118' },
            { brand: 'Not(A:Brand', version: '24' },
        ];
        const mobile = navigatorData.userAgentData?.mobile ?? false;
        const uaFullVersion = navigatorData.userAgentData?.uaFullVersion ?? navigator.userAgent;

        const userAgentData = {
            brands,
            mobile,
            getHighEntropyValues: async (keys) => {
                const result = {};
                for (const key of keys) {
                    switch (key) {
                        case 'platform':
                            result.platform = platform;
                            break;
                        case 'platformVersion':
                            result.platformVersion = navigatorData.platformVersion ?? '15.0.0';
                            break;
                        case 'architecture':
                            result.architecture = navigatorData.architecture ?? 'x86';
                            break;
                        case 'model':
                            result.model = navigatorData.model ?? '';
                            break;
                        case 'uaFullVersion':
                            result.uaFullVersion = uaFullVersion;
                            break;
                        default:
                            break;
                    }
                }
                return result;
            },
            toJSON() {
                return { brands: this.brands, mobile: this.mobile };
            },
        };
        defineReadonly(navigator, 'userAgentData', userAgentData);

        const {
            width: screenWidth = 1920,
            height: screenHeight = 1080,
            availWidth = screenWidth,
            availHeight = screenHeight - 40,
            colorDepth = 24,
            pixelDepth = 24,
        } = screenData;
        defineReadonly(screen, 'width', screenWidth);
        defineReadonly(screen, 'height', screenHeight);
        defineReadonly(screen, 'availWidth', availWidth);
        defineReadonly(screen, 'availHeight', availHeight);
        defineReadonly(screen, 'colorDepth', colorDepth);
        defineReadonly(screen, 'pixelDepth', pixelDepth);

        const viewportWidth = viewportData.width ?? Math.min(screenWidth - 100, 1366);
        const viewportHeight = viewportData.height ?? Math.min(screenHeight - 120, 768);
        defineReadonly(window, 'innerWidth', viewportWidth);
        defineReadonly(window, 'innerHeight', viewportHeight);
        defineReadonly(window, 'outerWidth', screenWidth);
        defineReadonly(window, 'outerHeight', screenHeight);
        defineReadonly(window, 'devicePixelRatio', navigatorData.devicePixelRatio ?? window.devicePixelRatio ?? 1);

        defineReadonly(window, 'screenX', 0);
        defineReadonly(window, 'screenY', 0);

        if (typeof Notification !== 'undefined') {
            defineReadonly(Notification, 'permission', 'granted');
        }

        if ('permissions' in navigator) {
            const originalQuery = navigator.permissions.query.bind(navigator.permissions);
            navigator.permissions.query = (parameters) => {
                if (parameters?.name === 'notifications') {
                    return Promise.resolve({ state: 'granted' });
                }
                return originalQuery(parameters);
            };
        }

        if (window.WebGLRenderingContext) {
            const getParameter = WebGLRenderingContext.prototype.getParameter;
            WebGLRenderingContext.prototype.getParameter = function patched(param) {
                if (param === 37445) return navigatorData.webglVendor ?? 'Intel Inc.';
                if (param === 37446) return navigatorData.webglRenderer ?? 'Intel Iris OpenGL Engine';
                return getParameter.call(this, param);
            };
        }

        if (typeof WebGL2RenderingContext !== 'undefined') {
            const getParameter2 = WebGL2RenderingContext.prototype.getParameter;
            WebGL2RenderingContext.prototype.getParameter = function patched(param) {
                if (param === 37445) return navigatorData.webglVendor ?? 'Intel Inc.';
                if (param === 37446) return navigatorData.webglRenderer ?? 'Intel Iris OpenGL Engine';
                return getParameter2.call(this, param);
            };
        }

        if (navigator.mediaDevices) {
            navigator.mediaDevices.enumerateDevices = async () => ([
                { kind: 'audioinput', deviceId: 'default', label: 'Default - Microphone', groupId: 'default' },
                { kind: 'audiooutput', deviceId: 'default', label: 'Default - Speakers', groupId: 'default' },
                { kind: 'videoinput', deviceId: 'default', label: 'Integrated Camera', groupId: 'default' },
            ]);
        }

        defineReadonly(navigator, 'getBattery', () => Promise.resolve({
            charging: true,
            chargingTime: 0,
            dischargingTime: Number.MAX_SAFE_INTEGER,
            level: 0.96,
            addEventListener: () => {},
            removeEventListener: () => {},
        }));

        if (window.HTMLCanvasElement) {
            const toDataURL = HTMLCanvasElement.prototype.toDataURL;
            HTMLCanvasElement.prototype.toDataURL = function(...args) {
                return toDataURL.apply(this, args);
            };
        }

        if (window.AudioContext) {
            const originalOscillator = AudioContext.prototype.createOscillator;
            AudioContext.prototype.createOscillator = function() {
                const oscillator = originalOscillator.call(this);
                const originalStart = oscillator.start;
                oscillator.start = function(...args) {
                    try {
                        return originalStart.apply(this, args);
                    } catch {
                        return undefined;
                    }
                };
                return oscillator;
            };
        }

        defineReadonly(HTMLIFrameElement.prototype, 'contentWindow', window);

        const timezoneId = timezone.id ?? 'America/Los_Angeles';
        const originalResolvedOptions = Intl.DateTimeFormat.prototype.resolvedOptions;
        Intl.DateTimeFormat.prototype.resolvedOptions = function(...args) {
            const options = originalResolvedOptions.apply(this, args);
            options.timeZone = timezoneId;
            return options;
        };

        const originalDateToString = Date.prototype.toString;
        Date.prototype.toString = function() {
            const str = originalDateToString.apply(this, []);
            return str.replace(/\(([^)]+)\)/, `(${timezoneId.replace(/_/g, ' ')})`);
        };
    }, { data: stealthPayload });
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

    log.info(`Pagination: Current=${currentPage}, Total=${totalPages}, Max allowed=${maxPages}`);

    if (currentPage >= totalPages) {
        log.info(`Reached last page (${currentPage}/${totalPages})`);
        return;
    }

    const nextPage = currentPage + 1;
    if (nextPage > maxPages) {
        log.info(`Reached max pages limit (${maxPages})`);
        return;
    }

    if (shouldStop()) {
        log.info(`Result limit reached, skipping pagination`);
        return;
    }

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

    // Validate nextUrl is different from current URL
    if (nextUrl === request.url) {
        log.warning(`Next page URL is same as current URL, skipping to prevent loop`);
        return;
    }

    if (!nextUrl || stat.enqueued.has(nextUrl)) {
        log.info(`Pagination URL already enqueued or invalid, skipping`);
        return;
    }

    stat.enqueued.add(nextUrl);
    stat.maxQueued = Math.max(stat.maxQueued, nextPage);
    listingStats.set(listingKey, stat);

    log.info(` Enqueueing page ${nextPage}: ${nextUrl}`);
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
    maxRequestRetries: 10,
    requestHandlerTimeoutSecs: 90,
    navigationTimeoutSecs: 60,
    useSessionPool: true,
    persistCookiesPerSession: true,
    sessionPoolOptions: {
        maxPoolSize: 100,
        sessionOptions: {
            maxUsageCount: 20,
            maxErrorScore: 0.5,
            maxAgeSecs: 900,
        },
        createSessionFunction: (sessionPool, options = {}) => {
            const session = new Session({
                ...(options?.sessionOptions ?? {}),
                sessionPool,
            });
            session.userData = {
                createdAt: Date.now(),
                requestCount: 0,
                challengeCount: 0,
            };
            return session;
        },
    },
    browserPoolOptions: {
        useFingerprints: true,
        fingerprintOptions: {
            fingerprintGeneratorOptions: {
                devices: ['desktop'],
                browsers: ['chrome'],
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
                '--disable-setuid-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-features=IsolateOrigins,site-per-process',
                '--allow-running-insecure-content',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor',
                '--disable-ipc-flooding-protection',
                '--no-first-run',
                '--no-default-browser-check',
                '--disable-backgrounding-occluded-windows',
                '--disable-renderer-backgrounding',
                '--disable-background-timer-throttling',
                '--window-size=1920,1080',
            ],
        },
    },
    preNavigationHooks: [
        async (ctx) => {
            const { page, session, browserController, request } = ctx;

            const originalGoto = ctx.gotoOptions ?? {};
            ctx.gotoOptions = {
                ...originalGoto,
                waitUntil: originalGoto.waitUntil ?? 'load',
                timeout: originalGoto.timeout ?? 60000,
            };

            if (!page.__blockResourcesApplied) {
                await page.route('**/*', (route) => {
                    const requestType = route.request().resourceType();
                    const requestUrl = route.request().url();
                    if (['image', 'media', 'font'].includes(requestType)) {
                        if (/\b__next\/data\b/i.test(requestUrl)
                            || /\/ajax\//i.test(requestUrl)
                            || /cloudflare|cf-chl|captcha|challenge/i.test(requestUrl)) {
                            return route.continue();
                        }
                        return route.abort();
                    }
                    return route.continue();
                });
                page.__blockResourcesApplied = true;
            }

            const fingerprint = browserController?.fingerprint;
            const userAgent = fingerprint?.navigator?.userAgent;

            // Desktop browser headers (keep synced with fingerprint)
            const headerLocale = language || fingerprint?.navigator?.languages?.[0] || 'en-US';
            const secChUa = fingerprint?.navigator?.userAgentData?.brands
                ?.map((brand) => `"${brand.brand}";v="${brand.version}"`)
                .join(', ') ?? '"Chromium";v="118", "Not A(Brand";v="24", "Google Chrome";v="118"';
            const secChUaFullVersion = fingerprint?.navigator?.userAgentData?.uaFullVersion ?? '118.0.5993.118';
            const secChUaPlatform = fingerprint?.navigator?.platform ?? 'Windows';
            const secChUaPlatformVersion = fingerprint?.navigator?.platformVersion ?? '15.0.0';

            const headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'Accept-Language': `${headerLocale},en;q=0.8`,
                'Accept-Encoding': 'gzip, deflate, br, zstd',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'Upgrade-Insecure-Requests': '1',
                'DNT': '1',
                'Sec-CH-UA': secChUa,
                'Sec-CH-UA-Full-Version': `"${secChUaFullVersion}"`,
                'Sec-CH-UA-Mobile': '?0',
                'Sec-CH-UA-Platform': `"${secChUaPlatform}"`,
                'Sec-CH-UA-Platform-Version': `"${secChUaPlatformVersion}"`,
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
            };

            if (session?.userData?.lastUrl) {
                headers['Referer'] = session.userData.lastUrl;
                headers['Sec-Fetch-Site'] = 'same-origin';
            }

            await page.setExtraHTTPHeaders(headers);

            if (cookiesForContext.length && !page.context().__cookiesApplied) {
                try {
                    await page.context().addCookies(cookiesForContext);
                    page.context().__cookiesApplied = true;
                } catch (err) {
                    log.warning(`Failed to add cookies to context: ${err.message}`);
                }
            }

            await applyStealthScripts(page, fingerprint);

            if (fingerprint) {
                const { screen, navigator: nav } = fingerprint;
                if (screen?.width && screen?.height) {
                    const viewportWidth = Math.floor(screen.width + (Math.random() * 100 - 50));
                    const viewportHeight = Math.floor(screen.height + (Math.random() * 100 - 50));
                    await page.setViewportSize({
                        width: Math.max(1024, Math.min(viewportWidth, 1920)),
                        height: Math.max(768, Math.min(viewportHeight, 1080)),
                    });
                }
                const ua = nav?.userAgent || userAgent;
                if (ua) await page.setUserAgent(ua);
            }

            if (session?.userData) {
                session.userData.requestCount = (session.userData.requestCount ?? 0) + 1;
                session.userData.lastUrl = request.url;
            }
        },
    ],
    postNavigationHooks: [
        async ({ page, session, response }) => {
            // First check if we even got a response
            if (!response) {
                log.warning('No response received, possible navigation failure');
                session?.markBad?.();
                if (session) session.retire();
                throw new Error('Navigation failed - no response');
            }

            const status = response.status();
            log.info(`Response status: ${status}`);

            // Check for blocking status codes FIRST
            if (status === 403 || status === 429 || status === 503) {
                log.warning(`Received status ${status}, retiring session.`);
                if (session?.userData) {
                    session.userData.challengeCount = (session.userData.challengeCount ?? 0) + 1;
                }
                session?.markBad?.();
                if (session) session.retire();
                throw new Error(`HTTP ${status} - Request blocked or rate limited`);
            }

            // Only check for challenge page if we got a 200 response
            if (status >= 200 && status < 300) {
                // Wait a bit for page to render
                await page.waitForTimeout(2000);

                if (await looksLikeChallengePage(page)) {
                    log.warning('Bot challenge detected on successful response, attempting bypass.');
                    const challengeCleared = await waitForChallengeResolution(page, {
                        maxAttempts: 3,
                        intervalMs: 4000,
                        navigationTimeoutMs: 6000,
                    });

                    if (!challengeCleared) {
                        log.warning('Bot challenge persists after waiting, retiring session.');
                        if (session?.userData) {
                            session.userData.challengeCount = (session.userData.challengeCount ?? 0) + 1;
                        }
                        session?.markBad?.();
                        if (session && (session.userData?.challengeCount ?? 0) >= 2) {
                            session.retire();
                        }
                        throw new Error('Encountered bot challenge / Cloudflare gate');
                    }

                    log.info('Cloudflare challenge passed automatically.');
                    if (session?.userData) {
                        session.userData.challengeCount = 0;
                    }
                }
            }
        },
    ],
    async requestHandler({ page, request, log: crawlerLog, crawler: crawlerInstance, session }) {
        const { label } = request.userData;
        crawlerLog.info(`Processing ${label ?? 'UNKNOWN'}: ${request.url}`);

        // Random delay to appear more human-like (increased for Cloudflare)
        const delay = 3000 + Math.random() * 4000;
        crawlerLog.info(`Waiting ${Math.round(delay)}ms before processing (anti-bot timing)...`);
        await page.waitForTimeout(delay);

        // Simulate human-like scrolling behavior
        await page.evaluate(async () => {
            const scrollHeight = document.documentElement.scrollHeight;
            const viewportHeight = window.innerHeight;
            const scrollSteps = Math.floor(scrollHeight / viewportHeight);
            
            for (let i = 0; i < Math.min(scrollSteps, 3); i++) {
                window.scrollBy(0, viewportHeight * 0.6);
                await new Promise(resolve => setTimeout(resolve, 300 + Math.random() * 400));
            }
            
            // Scroll back to top
            window.scrollTo(0, 0);
        });

        crawlerLog.info(`Extracting __NEXT_DATA__ from page...`);
        let nextData = null;
        for (let attempt = 0; attempt < 3; attempt++) {
            await page.waitForFunction(
                () => typeof window !== 'undefined' && !!window.__NEXT_DATA__?.props?.pageProps,
                { timeout: 30000 }
            ).catch(() => crawlerLog.warning(`Attempt ${attempt + 1}/3: __NEXT_DATA__ wait timed out`));
            
            nextData = await extractNextData(page);
            if (nextData?.props?.pageProps) {
                crawlerLog.info(` __NEXT_DATA__ extracted successfully on attempt ${attempt + 1}`);
                break;
            }
            crawlerLog.warning(`Attempt ${attempt + 1}/3: No valid __NEXT_DATA__ found, retrying...`);
            await page.waitForTimeout(2000 + Math.random() * 1000);
        }

        if (!nextData?.props?.pageProps) {
            crawlerLog.error('Failed to extract __NEXT_DATA__ after 3 attempts');
            
            // Log page details for debugging
            const pageUrl = page.url();
            const pageTitle = await page.title();
            crawlerLog.info(`Page URL: ${pageUrl}, Title: "${pageTitle}"`);
            
            // Check if __NEXT_DATA__ script tag exists
            const hasNextDataScript = await page.evaluate(() => {
                const script = document.querySelector('script#__NEXT_DATA__');
                if (script) {
                    return { exists: true, length: script.textContent?.length || 0 };
                }
                return { exists: false, length: 0 };
            });
            crawlerLog.info(`__NEXT_DATA__ script tag: ${JSON.stringify(hasNextDataScript)}`);
            
            session?.markBad?.();
            return;
        }

        const pageProps = nextData.props.pageProps;
        session?.markGood?.();

        const collectFromPageProps = (props) => {
            const pools = [];
            const poolNames = [];
            
            if (Array.isArray(props.products)) { 
                pools.push(props.products); 
                poolNames.push(`products[${props.products.length}]`);
            }
            if (Array.isArray(props.productSummaries)) { 
                pools.push(props.productSummaries);
                poolNames.push(`productSummaries[${props.productSummaries.length}]`);
            }
            if (props.productGrid) {
                const grid = props.productGrid;
                if (Array.isArray(grid.products)) {
                    pools.push(grid.products);
                    poolNames.push(`productGrid.products[${grid.products.length}]`);
                }
                if (Array.isArray(grid.items)) {
                    pools.push(grid.items);
                    poolNames.push(`productGrid.items[${grid.items.length}]`);
                }
                if (Array.isArray(grid.productSummaries)) {
                    pools.push(grid.productSummaries);
                    poolNames.push(`productGrid.productSummaries[${grid.productSummaries.length}]`);
                }
                if (Array.isArray(grid.results)) {
                    pools.push(grid.results);
                    poolNames.push(`productGrid.results[${grid.results.length}]`);
                }
            }
            if (props.category?.products) {
                pools.push(props.category.products);
                poolNames.push(`category.products[${props.category.products.length}]`);
            }
            if (props.category?.productList?.items) {
                pools.push(props.category.productList.items);
                poolNames.push(`category.productList.items[${props.category.productList.items.length}]`);
            }
            if (props.results?.items) {
                pools.push(props.results.items);
                poolNames.push(`results.items[${props.results.items.length}]`);
            }
            if (props.searchResults?.products) {
                pools.push(props.searchResults.products);
                poolNames.push(`searchResults.products[${props.searchResults.products.length}]`);
            }
            if (props.searchResults?.items) {
                pools.push(props.searchResults.items);
                poolNames.push(`searchResults.items[${props.searchResults.items.length}]`);
            }

            if (poolNames.length > 0) {
                crawlerLog.info(`Found product data in: ${poolNames.join(', ')}`);
            } else {
                crawlerLog.warning('No product arrays found in pageProps');
                crawlerLog.info(`Available pageProps keys: ${Object.keys(props).join(', ')}`);
            }

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
            crawlerLog.info(` Saved product ${savedCount}${Number.isFinite(resultsWanted) ? `/${resultsWanted}` : ''} - ${product.displayName || 'Unknown'}`);
            await stopIfNeeded();
            return;
        } else {
            // LISTING PAGE
            let productsToHandle = listingProducts;

            if (!productsToHandle.length) {
                crawlerLog.warning('No products found in __NEXT_DATA__, trying DOM fallback...');
                
                const domResults = await page.evaluate(() => {
                    const items = [];
                    const seen = new Set();
                    const anchors = Array.from(document.querySelectorAll('a[href*="/pr/"]'));
                    
                    console.log(`Found ${anchors.length} product links in DOM`);
                    
                    for (const anchor of anchors) {
                        const href = anchor.href;
                        if (!href || seen.has(href)) continue;
                        seen.add(href);
                        const titleNode =
                            anchor.querySelector('h1, h2, h3, [data-element="product-title"], [data-testid="product-card-title"], .product-title') ??
                            anchor;
                        const title = titleNode.textContent?.trim();
                        if (!title || title.length < 3) continue;
                        const priceNode =
                            anchor.querySelector('[data-element="product-price"], [data-testid="product-card-price"], .product-price, .price') ??
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
                    
                    console.log(`Extracted ${items.length} valid products from DOM`);
                    return items;
                });

                if (domResults.length) {
                    crawlerLog.info(` DOM fallback successful: found ${domResults.length} product links`);
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
                // Check limit BEFORE processing to prevent overshooting
                if (shouldStop()) {
                    crawlerLog.info(`Result limit reached (${savedCount}/${resultsWanted}), stopping product collection.`);
                    break;
                }
                
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
                    crawlerLog.info(` Saved product ${savedCount}${Number.isFinite(resultsWanted) ? `/${resultsWanted}` : ''} - ${item.displayName || 'Unknown'}`);
                }
            }

            if (collectDetails && newProductRequests.length) {
                crawlerLog.info(`Enqueueing ${newProductRequests.length} product detail pages`);
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
        const errorMsg = error?.message ?? '';
        const isBlocked = errorMsg.includes('403') || errorMsg.includes('429') || errorMsg.includes('challenge');
        
        log.error(`Request failed ${request.url}: ${errorMsg}`);
        
        if (isBlocked) {
            log.warning(`Detected blocking/challenge for ${request.url}`);
        }
        
        session?.markBad?.();
        session?.retire?.();
    },
});

await crawler.run();
log.info(` Scraping completed successfully!`);
log.info(`Total products saved: ${savedCount}`);
log.info(`Check your dataset for the extracted product data.`);
await Actor.exit();

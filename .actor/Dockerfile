FROM apify/actor-node-playwright:20

ENV PLAYWRIGHT_BROWSERS_PATH=/usr/lib/playwright \
    PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1 \
    NODE_ENV=production \
    APIFY_LOG_LEVEL=INFO

USER root
WORKDIR /usr/src/app

COPY package*.json ./
RUN npm ci --omit=dev --no-audit --no-fund --prefer-offline --unsafe-perm && npm cache clean --force

COPY . .

RUN npm prune --omit=dev && chown -R myuser:myuser /usr/src/app

USER myuser

CMD ["node", "src/main.js"]

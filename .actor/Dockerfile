FROM apify/actor-node-playwright:20

ENV PLAYWRIGHT_BROWSERS_PATH=/usr/lib/playwright \
    PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1 \
    NODE_ENV=production \
    APIFY_LOG_LEVEL=INFO

WORKDIR /usr/src/app

COPY package*.json ./
RUN npm install --omit=dev --no-audit --no-fund --prefer-offline --unsafe-perm && npm cache clean --force

COPY . .

RUN npm prune --omit=dev

CMD ["node", "src/main.js"]

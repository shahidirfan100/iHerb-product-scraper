FROM apify/actor-node-playwright:20

ENV PLAYWRIGHT_BROWSERS_PATH=/usr/lib/playwright \
    NODE_ENV=production \
    APIFY_LOG_LEVEL=INFO

USER root
WORKDIR /usr/src/app

COPY package*.json ./
RUN npm install --omit=dev --no-audit --no-fund --prefer-offline --unsafe-perm && npm cache clean --force

COPY . .

RUN npx playwright install --with-deps chromium && chown -R myuser:myuser /usr/src/app

USER myuser

CMD ["node", "src/main.js"]

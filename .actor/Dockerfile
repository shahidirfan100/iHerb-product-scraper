FROM apify/actor-node-playwright:20

WORKDIR /usr/src/app

COPY package*.json ./
RUN npm ci --omit=dev && rm -rf ~/.npm || true

COPY . ./

ENV APIFY_LOG_LEVEL=INFO \
    PLAYWRIGHT_BROWSERS_PATH=/usr/lib/playwright

CMD ["node", "src/main.js"]

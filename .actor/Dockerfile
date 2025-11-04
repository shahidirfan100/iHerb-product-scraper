# Use Apify's Playwright Chrome base image
FROM apify/actor-node-playwright-chrome:22

# Verify pre-installed packages
RUN npm ls crawlee apify playwright || true

WORKDIR /usr/src/app

# Copy package files
COPY package*.json ./

# Install dependencies (browsers already included in base image)
RUN npm --quiet set progress=false \
    && npm install --omit=dev --omit=optional \
    && echo "Installed NPM packages:" \
    && (npm list --omit=dev --all || true) \
    && echo "Node.js version:" \
    && node --version \
    && echo "NPM version:" \
    && npm --version

# Copy source code
COPY . ./

# Start the actor
CMD npm start --silent

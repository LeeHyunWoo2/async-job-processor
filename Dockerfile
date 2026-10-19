FROM node:24.21.0-alpine

WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY src ./src

USER node
CMD ["node", "src/api/server.ts"]

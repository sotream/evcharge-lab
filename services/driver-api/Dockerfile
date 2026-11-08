# Build context = repo root: docker build -f services/driver-api/Dockerfile .
# syntax=docker/dockerfile:1.7
FROM node:26-alpine AS build
ARG PKG=@evcharge/driver-api
RUN npm install -g pnpm@12.16.1 --no-fund --no-audit
WORKDIR /repo
COPY pnpm-lock.yaml pnpm-workspace.yaml package.json tsconfig.base.json ./
COPY packages ./packages
COPY services ./services
COPY simulator ./simulator
RUN --mount=type=cache,id=pnpm,target=/root/.local/share/pnpm/store pnpm install --frozen-lockfile
RUN pnpm --filter "${PKG}..." build && pnpm --filter "${PKG}" deploy --prod --legacy /out

FROM node:26-alpine AS runtime
ENV NODE_ENV=production
# scrypt (password hashing) runs on the libuv threadpool, which defaults to 4 threads; a login burst would queue
# behind it and starve DNS lookups and file IO that share the pool.
ENV UV_THREADPOOL_SIZE=16
WORKDIR /app
COPY --from=build --chown=node:node /out ./
USER node
EXPOSE 8080
STOPSIGNAL SIGTERM
CMD ["node", "--enable-source-maps", "dist/main.js"]

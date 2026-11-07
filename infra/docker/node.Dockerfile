# Template for Node services (copy to services/<name>/Dockerfile, set PKG). Build context = repo root.
# syntax=docker/dockerfile:1.7
FROM node:26-alpine AS build
ARG PKG=@evcharge/CHANGE-ME
RUN corepack enable
WORKDIR /repo
COPY pnpm-lock.yaml pnpm-workspace.yaml package.json tsconfig.base.json ./
COPY packages ./packages
COPY services ./services
COPY simulator ./simulator
RUN --mount=type=cache,id=pnpm,target=/root/.local/share/pnpm/store pnpm install --frozen-lockfile
RUN pnpm --filter "${PKG}..." build && pnpm --filter "${PKG}" deploy --prod --legacy /out

FROM node:26-alpine AS runtime
ENV NODE_ENV=production
WORKDIR /app
COPY --from=build --chown=node:node /out ./
USER node
EXPOSE 8080
STOPSIGNAL SIGTERM
CMD ["node", "--enable-source-maps", "dist/main.js"]

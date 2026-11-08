# smart-charging (ADR 0006). Build context = repo root.
# syntax=docker/dockerfile:1.7
FROM golang:1.27-alpine AS build
ARG SVC=smart-charging
WORKDIR /src
COPY go.work ./
COPY packages/contracts-go ./packages/contracts-go
COPY packages/platform-go ./packages/platform-go
COPY services ./services
COPY scripts ./scripts
RUN --mount=type=cache,target=/root/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    cd services/${SVC} && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${SVC}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/app"]

# platform-go

Shared production plumbing for the Go services (smart-charging, tariff,
telemetry-ingest). Module `github.com/sotream/evcharge-lab/packages/platform-go`
(Go 1.27, part of the repo `go.work`). It depends on `contracts-go` for the
`Problem` type.

Each package is small and can be used on its own. `server.Run` ties them together.

## Typical `main`

```go
type Config struct {
	config.Base
	DatabaseURL  string   `env:"DATABASE_URL"`
	KafkaBrokers []string `env:"KAFKA_BROKERS" envSeparator:","`
}

func (c Config) Validate() error {
	var p config.Problems
	p.Merge(c.Base.Validate())
	p.Require("DATABASE_URL", c.DatabaseURL)
	return p.Err()
}

func main() {
	var cfg Config
	if err := config.Load(&cfg); err != nil { // lists every problem
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	log := logging.New(os.Stdout, cfg.ServiceName, cfg.LogLevel)
	slog.SetDefault(log)
	ctx := context.Background()

	shutdownTracing, err := telemetry.Setup(ctx, cfg.ServiceName, cfg.OTLPEndpoint)
	must(err)
	pool, err := pg.Connect(ctx, pg.Config{URL: cfg.DatabaseURL, ApplicationName: cfg.ServiceName})
	must(err)
	_, err = pg.Migrate(ctx, pool, migrationsFS, "migrations")
	must(err)

	hc := health.New()
	hc.AddReadinessCheck("postgres", pg.ReadinessCheck(pool))
	reg := metrics.New()

	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: cfg.KafkaBrokers, Group: contracts.GroupTelemetryIngest,
		Topics: []string{contracts.TopicMeterValues}, Registerer: reg, Logger: log,
	}, bulkInsert) // func(ctx, []*kgo.Record) error
	must(err)
	hc.AddReadinessCheck("kafka", consumer.ReadinessCheck)

	mux := http.NewServeMux()
	mux.Handle("POST /v1/quote", httpx.RequireInternalToken(cfg.InternalToken)(quoteHandler))

	err = server.Run(ctx, cfg.Base, mux,
		server.WithLogger(log), server.WithHealth(hc), server.WithMetrics(reg),
		server.WithRunner("consumer", consumer.Run),
		// Closers run in reverse order: consumer, pool, tracer.
		server.WithCloser("tracer", shutdownTracing),
		server.WithCloser("postgres", pg.Closer(pool)),
		server.WithCloser("consumer", consumer.Close),
	)
	if err != nil {
		log.Error("exit", "err", err)
		os.Exit(1)
	}
}
```

## API by package

### `config`
- `Base` holds the shared env fields: `SERVICE_NAME` (required), `LOG_LEVEL`
  (debug|info|warn|error, default info), `HTTP_PORT` (default 8080),
  `INTERNAL_TOKEN` (at least 16 chars), `OTEL_EXPORTER_OTLP_ENDPOINT` (optional
  http(s) URL), `SHUTDOWN_GRACE` (Go duration, default `15s`). `Base.HTTPAddr()`
  returns the listen address.
- `Load(&cfg)` parses the environment with `caarlos0/env` tags and then calls
  `cfg.Validate()`. You get one `*ValidationError` that lists every parse and
  validation problem. `LoadFrom(&cfg, map)` does the same for tests.
- `Problems` (`Add`, `Addf`, `Require`, `Merge`, `Err`) builds service-level
  `Validate` methods.

### `logging`
- `New(w, service, level) *slog.Logger` returns a JSON logger with `service`
  on every record. When the context carries a span, it also adds `trace_id`
  and `span_id`, so use the `*Context` methods: `log.InfoContext(ctx, ...)`.
- `NewTraceHandler(inner)` wraps any `slog.Handler`. `ParseLevel(s)` parses a
  level name.

### `telemetry`
- `Setup(ctx, service, endpoint) (shutdown, error)` sets up an OTLP/HTTP trace
  exporter at `<endpoint>/v1/traces` with a parent-based always-on sampler. An
  empty endpoint gives a no-op provider. It always installs the W3C
  tracecontext and baggage propagator.
- `HTTPHandler(h, op)` and `HTTPTransport(rt)` are the otelhttp server and
  client wrappers. After routing, server span names are updated from
  `Request.Pattern`.
- `InjectKafka(ctx, *kgo.Record)`, `ExtractKafka(ctx, *kgo.Record)` and
  `KafkaHeaderCarrier` carry `traceparent` in Kafka record headers.

### `metrics`
- `New() *Registry` returns a dedicated Prometheus registry with Go and process
  collectors. It embeds `*prometheus.Registry`, so register your own metrics
  on it.
- `Registry.Middleware(next)` records
  `http_server_requests_seconds{method,route,status}`. `route` is the matched
  `ServeMux` pattern path, for example `/v1/tariffs/{id}`, or `unmatched`. Raw
  paths are never used. `Registry.Handler()` serves `/metrics`.

### `health`
- `New(opts...)`, `AddReadinessCheck(name, fn, health.WithTimeout(d))`
  (default timeout 2s per check). Checks run concurrently. A check that hangs
  or panics counts as failed.
- `SetDraining(bool)`: while draining, `/health/ready` returns 503
  `{"status":"draining"}`.
- `Register(mux)` mounts `GET /health/live` (always 200) and
  `GET /health/ready` (200 or 503, with a JSON report per check).

### `server`
- `Run(ctx, cfg config.Base, handler, opts...) error` serves on
  `cfg.HTTPAddr()`. `/health/*` and `/metrics` are served without
  instrumentation. Every other request goes through otelhttp, the metrics
  middleware and panic recovery (500 problem+json) before reaching `handler`.
- Timeouts: ReadHeader 5s, Read 15s, Write 30s, Idle 120s. Change them with
  `WithHTTPServer(func(*http.Server))`.
- Shutdown starts on SIGTERM/SIGINT, when `ctx` is cancelled, or when a runner
  fails:
  1. Set draining, so ready returns 503.
  2. Wait the drain delay (default 3s, at most grace/3).
  3. `Shutdown` the HTTP server within the remaining `SHUTDOWN_GRACE`.
  4. Cancel runners and wait for them.
  5. Run closers in reverse registration order.

  `Run` returns nil after a clean shutdown.
- Options: `WithLogger`, `WithHealth`, `WithMetrics`, `WithCloser(name, fn)`,
  `WithRunner(name, fn)`, `WithDrainDelay`, `WithSignals`, `WithListener`
  (for tests), `WithHTTPServer`. `Handler(...)` exposes the composed root
  handler.

### `httpx`
- Problems (`contracts.Problem`, `application/problem+json`):
  - `Error(w, status, code, detail)`, `WriteProblem(w, p)` and
    `NewProblem(...)` write errors.
  - `WriteError(w, err)` keeps a `*ProblemError`. Any other error becomes a
    500 that does not leak the error text.
  - `ReadProblem(resp) *ProblemError` turns an upstream error response into an
    error. `WriteJSON(w, status, v)` writes a JSON body.
- `DecodeJSON(w, r, dst, limit)` decodes strictly. The body is capped by
  `MaxBytesReader` (default 1 MiB), unknown fields are rejected, and the body
  must hold exactly one value. A non-JSON content type is rejected. It returns
  `*ProblemError` with 400, 413 or 415.
- `RequireInternalToken(token)` is middleware that checks `x-internal-token`
  with `crypto/subtle`. It returns 401 problem+json, and an empty configured
  token rejects every request.
- `NewClient(ClientOptions{AttemptTimeout, MaxAttempts, BaseBackoff, MaxBackoff, InternalToken, Transport})`:
  - `Do(req)` and `DoJSON(ctx, method, url, in, out, header)`. The client
    always uses the otelhttp transport.
  - Each attempt has its own timeout (default 2s). The body stays readable
    until you close it.
  - Retries (default 3 attempts) apply only to GET/HEAD requests, or requests
    carrying `Idempotency-Key`, whose body can be replayed. They happen on
    network errors, attempt timeouts, 5xx (except 501) and 429.
  - Backoff is exponential with full jitter (default 50ms base, 1s cap).
    `Retry-After` is honoured up to the cap.

### `pg`
- `Connect(ctx, pg.Config{URL, ConnectTimeout, StatementTimeout, MaxConns, ApplicationName})`
  returns a `*pgxpool.Pool`.
  - The connect timeout defaults to 5s.
  - `statement_timeout` defaults to 10s and is set as a session runtime
    parameter. A negative value disables it.
  - `Connect` pings the database once before returning.
- `ReadinessCheck(pool)` and `Closer(pool)`.
- `Migrate(ctx, pool, fsys, dir) ([]int, error)` applies `NNNN_name.sql` files
  from an `embed.FS` in version order:
  - It holds a `pg_advisory_lock` (per database) for the whole run.
  - Each file runs in its own transaction. A file can hold several statements.
  - Applied files are recorded in
    `schema_migrations(version, name, checksum, applied_at)`.
  - Changing a file after it was applied is an error.
  - `statement_timeout` is lifted for the migration session.
  - `CREATE INDEX CONCURRENTLY` is not supported.
- `LoadMigrations(fsys, dir)` parses and validates the files without applying
  them.

### `kafka` (franz-go)
- `NewProducer(ProducerConfig{Brokers, ClientID, Linger, MaxRetries, DeliveryTimeout, Logger, Opts})`:
  - Writes are idempotent with acks=all, lz4 compression and a 5ms linger.
  - `ProduceJSON(ctx, topic, key, value, headers)` and `Produce(ctx, rec)` are
    synchronous. Both add `traceparent` and `content-type` headers.
  - `ReadinessCheck`, and `Close(ctx)`, which flushes before closing.
  - **Backpressure on ISR loss:** set `MaxRetries` (for example 3 to 5) so
    that `NOT_ENOUGH_REPLICAS` and `NOT_ENOUGH_REPLICAS_AFTER_APPEND` surface
    as `ErrNotEnoughReplicas`; check with `errors.Is` or
    `IsNotEnoughReplicas(err)`. Use it to shed load, for example with a 503.
    With the franz-go default of unlimited retries, the call blocks until its
    ctx deadline instead.
- `NewConsumer(ConsumerConfig{Brokers, Group, Topics, MaxPollRecords, FetchMaxWait, RetryBackoff, MaxRetryBackoff, CommitTimeout, StartAtLatest, Registerer, Logger, Opts}, handler)`:
  - The handler is a `BatchHandler` (`func(ctx, []*kgo.Record) error`) and
    receives the records of one partition per poll, in order. This suits bulk
    writes in telemetry-ingest. `PerRecord(fn)` adapts a per-record handler
    and starts a consumer span that continues the producer's trace.
  - Delivery is at least once. Auto-commit is off, and rebalances are blocked
    while a poll is processed. Partitions are handled concurrently.
  - A partition whose handler succeeded is committed synchronously. A
    partition that failed is rewound (`SetOffsets`) and redelivered after an
    exponential backoff. Handlers must be idempotent.
  - When a broker or coordinator is lost, `Run` logs, backs off and keeps
    polling while franz-go reconnects and rejoins. Only ctx or `Close` stops
    it.
  - `Run(ctx)` returns nil once the in-flight batch has finished and been
    committed. `Close(ctx)` stops polling, waits for the in-flight batch (and
    cancels the handler context if ctx expires first), then leaves the group.
    `ReadinessCheck` reports the consumer as unhealthy if it was stopped or
    cannot ping the cluster.
  - Metrics: `kafka_consumer_lag{group,topic,partition}` and
    `kafka_consumer_handler_errors_total{group,topic}`.

### `redisx`
- `Connect(ctx, redisx.Config{URL, DialTimeout, ReadTimeout, WriteTimeout, PoolTimeout, PoolSize})`
  returns a `*redis.Client` (go-redis v9).
  - Default timeouts are 2s dial, 1s read, 1s write and 2s pool.
  - `ContextTimeoutEnabled` is on.
  - `Connect` pings once before returning.
- `Options(cfg)`, `ReadinessCheck(c)` and `Closer(c)`.
- The package is named `redisx` so it does not clash with the go-redis import.

## Testing

```sh
cd packages/platform-go
go vet ./... && golangci-lint run ./...        # uses the repo-root .golangci.yml
go test -race -count=1 ./...                   # unit tests only
INTEGRATION=1 go test -race -count=1 ./...     # + integration (needs `make infra-up`)
```

Integration tests run only when `INTEGRATION` is set. They read these
variables and fall back to the compose host ports:

| Variable | Default | Used for |
| --- | --- | --- |
| `TEST_PG_URL` | `postgres://evcharge:evcharge@localhost:25432` | Creates a unique `pgtest_*` database per test and drops it `WITH (FORCE)` |
| `TEST_PG_TELEMETRY_URL` | `postgres://evcharge:evcharge@localhost:25433/telemetry` | TimescaleDB instance |
| `TEST_REDIS_URL` | `redis://localhost:26379` | Redis |
| `TEST_KAFKA_BROKERS` | `localhost:29091,localhost:29092,localhost:29093` | Comma list. Topics are not auto-created, so each test creates a unique RF=3 topic with kadm and deletes it afterwards |

The `NOT_ENOUGH_REPLICAS` path is tested against `kfake`, the in-process fake
cluster from franz-go, which injects the broker error. This keeps the shared
brokers untouched.

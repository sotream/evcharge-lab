// Package contracts mirrors packages/contracts (TypeScript/zod is the source of
// truth; schemas/ holds generated JSON Schema). contract_test.go fails when the
// Go shapes drift from the generated schemas. See ADR 0002.
package contracts

import (
	"encoding/json"
	"time"
)

// Topics (keys documented in packages/contracts/src/topics.ts).
const (
	TopicStationEvents         = "station.events.v1"
	TopicMeterValues           = "telemetry.meter-values.v1"
	TopicStationCommands       = "station.commands.v1"
	TopicStationCommandResults = "station.command-results.v1"
	TopicSessionEvents         = "session.events.v1"
	TopicBillingEvents         = "billing.events.v1"

	GroupTelemetryIngest = "telemetry-ingest.meter-values"
)

// Envelope wraps every Kafka record value.
type Envelope[T any] struct {
	EventID    string    `json:"eventId"`
	Type       string    `json:"type"`
	Version    int       `json:"version"`
	OccurredAt time.Time `json:"occurredAt"`
	Producer   string    `json:"producer"`
	Key        string    `json:"key"`
	Data       T         `json:"data"`
}

// RawEnvelope defers decoding of Data until Type is known.
type RawEnvelope = Envelope[json.RawMessage]

type MeterSample struct {
	Measurand string  `json:"measurand"`
	Value     float64 `json:"value"`
	Unit      string  `json:"unit"`
	Context   string  `json:"context,omitempty"`
	Phase     string  `json:"phase,omitempty"`
}

type MeterReading struct {
	Timestamp time.Time     `json:"timestamp"`
	Samples   []MeterSample `json:"samples"`
}

type MeterValues struct {
	StationID     string         `json:"stationId"`
	ConnectorID   int            `json:"connectorId"`
	TransactionID *int64         `json:"transactionId,omitempty"`
	OcppMessageID string         `json:"ocppMessageId"`
	Replayed      bool           `json:"replayed"`
	Readings      []MeterReading `json:"readings"`
}

const EventTypeMeterValues = "telemetry.meter-values"

// ---- smart-charging POST /v1/allocate

type AllocateSession struct {
	SessionID   string    `json:"sessionId"`
	StationID   string    `json:"stationId"`
	ConnectorID int       `json:"connectorId"`
	MaxW        int64     `json:"maxW"`
	MinW        int64     `json:"minW"`
	MeasuredW   *int64    `json:"measuredW,omitempty"`
	Priority    int       `json:"priority"`
	StartedAt   time.Time `json:"startedAt"`
}

type AllocateReq struct {
	SiteID         string            `json:"siteId"`
	SiteCapW       int64             `json:"siteCapW"`
	IdleConnectors int64             `json:"idleConnectors"`
	StartW         int64             `json:"startW"`
	Sessions       []AllocateSession `json:"sessions"`
}

type Allocation struct {
	SessionID string `json:"sessionId"`
	LimitW    int64  `json:"limitW"`
	Suspended bool   `json:"suspended"`
}

type AllocateRes struct {
	SiteID          string       `json:"siteId"`
	Strategy        string       `json:"strategy"`
	TotalAllocatedW int64        `json:"totalAllocatedW"`
	ReserveW        int64        `json:"reserveW"`
	Allocations     []Allocation `json:"allocations"`
}

// ---- tariff POST /v1/quote

type QuoteReq struct {
	TariffID  string    `json:"tariffId"`
	StartedAt time.Time `json:"startedAt"`
	StoppedAt time.Time `json:"stoppedAt"`
	EnergyWh  int64     `json:"energyWh"`
}

type QuoteLine struct {
	Kind        string  `json:"kind"`
	Description string  `json:"description"`
	Quantity    float64 `json:"quantity"`
	Unit        string  `json:"unit"`
	AmountMinor int64   `json:"amountMinor"`
}

type QuoteRes struct {
	TariffID   string      `json:"tariffId"`
	Currency   string      `json:"currency"`
	Lines      []QuoteLine `json:"lines"`
	TotalMinor int64       `json:"totalMinor"`
}

// Problem is the application/problem+json error body.
type Problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail,omitempty"`
	Code   string `json:"code,omitempty"`
}

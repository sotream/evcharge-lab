package contracts

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

func validate(t *testing.T, schemaName string, v any) {
	t.Helper()
	path := filepath.Join("..", "contracts", "schemas", schemaName+".json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read schema (run `pnpm --filter @evcharge/contracts build`): %v", err)
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	if err := c.AddResource(schemaName, doc); err != nil {
		t.Fatal(err)
	}
	s, err := c.Compile(schemaName)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(v)
	inst, _ := jsonschema.UnmarshalJSON(bytes.NewReader(b))
	if err := s.Validate(inst); err != nil {
		t.Fatalf("%s: Go value does not match schema: %v\n%s", schemaName, err, b)
	}
}

func TestMeterValuesMatchesSchema(t *testing.T) {
	tx := int64(42)
	validate(t, "MeterValuesReceived", Envelope[MeterValues]{
		EventID: "7b0a3c39-3a8e-4c6b-9a43-3c9a3f1c2b11", Type: EventTypeMeterValues, Version: 1,
		OccurredAt: time.Now().UTC(), Producer: "test", Key: "st-1",
		Data: MeterValues{StationID: "st-1", ConnectorID: 1, TransactionID: &tx, OcppMessageID: "m1",
			Readings: []MeterReading{{Timestamp: time.Now().UTC(), Samples: []MeterSample{{Measurand: "Power.Active.Import", Value: 7000, Unit: "W"}}}}},
	})
}

func TestAllocateMatchesSchema(t *testing.T) {
	validate(t, "AllocateReq", AllocateReq{SiteID: "s", SiteCapW: 1000, IdleConnectors: 1, StartW: 4140, Sessions: []AllocateSession{{
		SessionID: "7b0a3c39-3a8e-4c6b-9a43-3c9a3f1c2b11", StationID: "st", ConnectorID: 1, MaxW: 11000, MinW: 4140, StartedAt: time.Now().UTC()}}})
	validate(t, "AllocateRes", AllocateRes{SiteID: "s", Strategy: "optimal", TotalAllocatedW: 1, ReserveW: 4140, Allocations: []Allocation{{SessionID: "7b0a3c39-3a8e-4c6b-9a43-3c9a3f1c2b11", LimitW: 1}}})
}

func TestQuoteMatchesSchema(t *testing.T) {
	validate(t, "QuoteReq", QuoteReq{TariffID: "std", StartedAt: time.Now().UTC(), StoppedAt: time.Now().UTC(), EnergyWh: new(int64)})
	validate(t, "QuoteReq", QuoteReq{TariffID: "tou", StartedAt: time.Now().UTC(), StoppedAt: time.Now().UTC(), EnergyWh: new(int64),
		EnergyProfile: []EnergySample{{At: time.Now().UTC(), EnergyWh: 0}, {At: time.Now().UTC().Add(time.Minute), EnergyWh: 120}}})
	validate(t, "QuoteRes", QuoteRes{TariffID: "std", Currency: "EUR", Lines: []QuoteLine{{Kind: "energy", Description: "d", Quantity: 1, Unit: "kWh", AmountMinor: 35}}, TotalMinor: 35})
}

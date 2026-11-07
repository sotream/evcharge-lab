package pg

import (
	"strings"
	"testing"
	"testing/fstest"
)

func TestLoadMigrationsOrdersAndValidates(t *testing.T) {
	fsys := fstest.MapFS{
		"m/0002_add_index.sql":   {Data: []byte("CREATE INDEX ...;")},
		"m/0001_init.sql":        {Data: []byte("CREATE TABLE ...;")},
		"m/README.md":            {Data: []byte("ignored")},
		"m/0010_more_things.sql": {Data: []byte("SELECT 1;")},
	}
	migs, err := LoadMigrations(fsys, "m")
	if err != nil {
		t.Fatal(err)
	}
	if len(migs) != 3 || migs[0].Version != 1 || migs[0].Name != "init" || migs[1].Version != 2 || migs[2].Version != 10 {
		t.Fatalf("%+v", migs)
	}
	if len(migs[0].Checksum) != 64 {
		t.Fatalf("checksum %q", migs[0].Checksum)
	}
}

func TestLoadMigrationsRejectsBadNames(t *testing.T) {
	for name, fsys := range map[string]fstest.MapFS{
		"bad name":  {"m/1_init.sql": {Data: []byte("x")}},
		"uppercase": {"m/0001_Init.sql": {Data: []byte("x")}},
		"duplicate": {"m/0001_a.sql": {Data: []byte("x")}, "m/0001_b.sql": {Data: []byte("y")}},
	} {
		if _, err := LoadMigrations(fsys, "m"); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	if _, err := LoadMigrations(fstest.MapFS{}, "missing"); err == nil || !strings.Contains(err.Error(), "read migrations") {
		t.Errorf("missing dir: %v", err)
	}
}

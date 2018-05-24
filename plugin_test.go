package qtypespqt_test

import (
	"strings"
	"testing"

	"github.com/piotrkowalczuk/qtypespqt"
)

func TestPlugin_Static(t *testing.T) {
	p := &qtypespqt.Plugin{}
	if c := strings.Count(p.Static(nil), "(MISSING)"); c > 0 {
		t.Fatalf("static output contains `MISSING` %d times", c)
	}
}

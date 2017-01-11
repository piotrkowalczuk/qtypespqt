package qtypespqt_test

import (
	"strings"
	"testing"

	"github.com/piotrkowalczuk/pqt/pqtgo"
	"github.com/piotrkowalczuk/qtypespqt"
)

func TestPlugin_Static(t *testing.T) {
	p := &qtypespqt.Plugin{
		Formatter: &pqtgo.Formatter{
			Visibility: pqtgo.Public,
		},
	}
	if c := strings.Count(p.Static(nil), "(MISSING)"); c > 0 {
		t.Fatal("static output contains `MISSING` %d times", c)
	}
}

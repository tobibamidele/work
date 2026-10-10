package memory_test

import (
	"testing"

	"github.com/tobibamidele/work"
	"github.com/tobibamidele/work/backendtest"
	"github.com/tobibamidele/work/memory"
)

func TestConformance(t *testing.T) {
	backendtest.Run(t, func(t *testing.T) work.Backend { return memory.New() })
}

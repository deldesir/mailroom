package runtime

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestElasticOff(t *testing.T) {
	es, err := newElastic(&Config{ElasticEndpoint: ""})
	require.NoError(t, err)
	assert.False(t, es.Enabled())

	// starting and stopping a disabled instance is a no-op
	assert.NoError(t, es.start())
	es.stop()

	var none *Elastic
	assert.False(t, none.Enabled())
}

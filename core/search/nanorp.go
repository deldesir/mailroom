package search

import (
	"github.com/nyaruka/mailroom/v26/runtime"
)

// isNanorpMode returns true if Elasticsearch is disabled, meaning all search operations
// should fall back to PostgreSQL or return stub responses.
func isNanorpMode(rt *runtime.Runtime) bool {
	return rt.ES.Client == nil
}

package knowledge_test

import (
	"testing"

	"github.com/nyaruka/mailroom/v26/core/knowledge"
	"github.com/nyaruka/mailroom/v26/core/models"
	"github.com/nyaruka/mailroom/v26/runtime"
	"github.com/stretchr/testify/assert"
)

// The embeddings service is optional (see runtime.ServiceOff), and a runtime without one has no knowledge base:
// searching and indexing are refused rather than dereferencing the missing service. Neither needs a database to
// get that far, so the runtime below has none - reaching a query would panic.
func TestWithoutEmbeddings(t *testing.T) {
	rt := &runtime.Runtime{Config: runtime.NewDefaultConfig()}

	assert.NotPanics(t, func() {
		_, err := knowledge.Search(t.Context(), rt, nil, "how do refunds work?", nil, 10)
		assert.ErrorIs(t, err, knowledge.ErrNoEmbeddings)

		err = knowledge.IndexSource(t.Context(), rt, &models.KnowledgeSource{Type: models.KnowledgeSourceTypeShortcuts})
		assert.ErrorIs(t, err, knowledge.ErrNoEmbeddings)
	})
}

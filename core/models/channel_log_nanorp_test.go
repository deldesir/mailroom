package models_test

import (
	"testing"

	"github.com/nyaruka/goflow/core/events"
	"github.com/nyaruka/mailroom/v26/core/models"
	"github.com/nyaruka/mailroom/v26/runtime"
	"github.com/stretchr/testify/assert"
)

// In nanoRP mode DynamoDB is disabled, so its writers are nil and any unguarded Queue/QueueDelete is a nil
// dereference. This needs no database or other fixtures: the runtime below has neither, so reaching either the
// query or the writer would panic.
func TestDeleteChannelLogsForMessagesWithoutDynamo(t *testing.T) {
	rt := &runtime.Runtime{Dynamo: &runtime.Dynamo{}}

	assert.False(t, rt.Dynamo.Enabled())
	assert.False(t, (*runtime.Dynamo)(nil).Enabled())

	assert.NotPanics(t, func() {
		err := models.DeleteChannelLogsForMessages(t.Context(), rt, 1, []events.EventUUID{"0199bad8-f98d-75a3-b641-2718a25ac3f5"})
		assert.NoError(t, err)
	})
}

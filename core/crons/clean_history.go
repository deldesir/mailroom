package crons

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/nyaruka/mailroom/v26/runtime"
)

func init() {
	Register("clean_history", &CleanHistoryCron{})
}

// CleanHistoryCron deletes historic events older than 1 year to replicate DynamoDB's TTL behavior
type CleanHistoryCron struct{}

func (c *CleanHistoryCron) Next(last time.Time) time.Time {
	return Next(last, time.Hour*24)
}

func (c *CleanHistoryCron) AllInstances() bool {
	return false
}

// Run processes the task
func (c *CleanHistoryCron) Run(ctx context.Context, rt *runtime.Runtime) (map[string]any, error) {
	// Only run this if we are acting local-first (Nanorp mode)
	if rt.ES.Client != nil {
		// If they have full dependencies enabled, DynamoDB manages TTL natively
		return nil, nil
	}

	cutoff := time.Now().Add(-365 * 24 * time.Hour)
	slog.Info("running history cleanup", "cutoff", cutoff)

	results := make(map[string]any)

	// Clean older messages
	res, err := rt.DB.ExecContext(ctx, "DELETE FROM msgs_msg WHERE created_on < $1", cutoff)
	if err != nil {
		slog.Error("failed to cleanup historic msgs", "error", err)
	} else if rows, _ := res.RowsAffected(); rows > 0 {
		slog.Info("cleaned historic msgs", "count", rows)
	}

	// Clean older ticket events. tickets_ticketevent was removed in newer
	// RapidPro (migration 0087_delete_ticketevent); skip cleanly when the table
	// is absent so this nanoRP cron does not error on that schema.
	var ticketEventTbl sql.NullString
	if err := rt.DB.GetContext(ctx, &ticketEventTbl, "SELECT to_regclass('public.tickets_ticketevent')::text"); err == nil && ticketEventTbl.Valid {
		res, err = rt.DB.ExecContext(ctx, "DELETE FROM tickets_ticketevent WHERE created_on < $1", cutoff)
		if err != nil {
			slog.Error("failed to cleanup historic ticket events", "error", err)
		} else if rows, _ := res.RowsAffected(); rows > 0 {
			slog.Info("cleaned historic ticket events", "count", rows)
			results["cleaned_tickets"] = rows
		}
	}

	return results, nil
}

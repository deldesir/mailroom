package search

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/goflow/contactql"
	"github.com/nyaruka/goflow/core"
	"github.com/nyaruka/goflow/core/events"
	"github.com/nyaruka/mailroom/v26/core/models"
	"github.com/nyaruka/mailroom/v26/runtime"
)

// The functions in this file are the Postgres equivalents of the Elastic searches in search.go and messages.go, used
// when Elastic is off. Contact queries are converted with SQLConverter and run against contacts_contact directly.

// newContactsQuery builds the WHERE clause shared by the contact searches and returns the converter holding its
// parameters so that callers can bind more.
func newContactsQuery(oa *models.OrgAssets, group *models.Group, status models.ContactStatus, excludeUUIDs []core.ContactUUID, parsed *contactql.ContactQuery) (*SQLConverter, string, error) {
	conv := NewSQLConverter(oa.Env(), assetMapper)

	// deleted contacts are de-indexed from Elastic so they don't match there either
	clauses := []string{"c.org_id = " + conv.param(oa.OrgID()), "c.is_active"}

	if group != nil {
		clauses = append(clauses, fmt.Sprintf("EXISTS (SELECT 1 FROM contacts_contactgroup_contacts gc WHERE gc.contact_id = c.id AND gc.contactgroup_id = %s)", conv.param(group.ID())))
	}
	if status != models.NilContactStatus {
		clauses = append(clauses, "c.status = "+conv.param(status))
	}
	if len(excludeUUIDs) > 0 {
		uuids := make([]string, len(excludeUUIDs))
		for i, u := range excludeUUIDs {
			uuids[i] = string(u)
		}
		clauses = append(clauses, "c.uuid <> ALL("+conv.param(pq.StringArray(uuids))+")")
	}
	if parsed != nil {
		where, err := conv.Query(parsed)
		if err != nil {
			return nil, "", fmt.Errorf("error converting query to SQL: %w", err)
		}
		clauses = append(clauses, where)
	}

	return conv, strings.Join(clauses, " AND "), nil
}

func countContacts(ctx context.Context, rt *runtime.Runtime, where string, args []any) (int64, error) {
	var count int64
	if err := rt.DB.GetContext(ctx, &count, "SELECT COUNT(*) FROM contacts_contact c WHERE "+where, args...); err != nil {
		return 0, fmt.Errorf("error counting contacts: %w", err)
	}
	return count, nil
}

// GetContactTotalPostgres returns the total count of contacts matching the given query
func GetContactTotalPostgres(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, group *models.Group, status models.ContactStatus, parsed *contactql.ContactQuery) (int64, error) {
	conv, where, err := newContactsQuery(oa, group, status, nil, parsed)
	if err != nil {
		return 0, err
	}

	return countContacts(ctx, rt, where, conv.Args())
}

// GetContactUUIDsForQueryPagePostgres returns a page of contact UUIDs for the given query and sort, and the total
// number of matches
func GetContactUUIDsForQueryPagePostgres(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, group *models.Group, status models.ContactStatus, excludeUUIDs []core.ContactUUID, parsed *contactql.ContactQuery, sort string, offset int, pageSize int) ([]core.ContactUUID, int64, error) {
	conv, where, err := newContactsQuery(oa, group, status, excludeUUIDs, parsed)
	if err != nil {
		return nil, 0, err
	}

	// the count query only sees the parameters bound so far
	total, err := countContacts(ctx, rt, where, slices.Clone(conv.Args()))
	if err != nil {
		return nil, 0, err
	}
	if total == 0 {
		return []core.ContactUUID{}, 0, nil
	}

	orderBy, err := conv.Sort(sort, oa.SessionAssets())
	if err != nil {
		return nil, 0, fmt.Errorf("error converting sort to SQL: %w", err)
	}

	query := fmt.Sprintf("SELECT c.uuid FROM contacts_contact c WHERE %s ORDER BY %s LIMIT %s OFFSET %s", where, orderBy, conv.param(pageSize), conv.param(offset))

	uuids := make([]core.ContactUUID, 0, pageSize)
	if err := rt.DB.SelectContext(ctx, &uuids, query, conv.Args()...); err != nil {
		return nil, 0, fmt.Errorf("error querying contacts page: %w", err)
	}

	return uuids, total, nil
}

// GetContactUUIDsForQueryPostgres returns up to limit the contact UUIDs that match the given query, sorted by contact
// ID. Limit of -1 means return all. Results are read in batches by keyset so that no single query is unbounded.
func GetContactUUIDsForQueryPostgres(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, group *models.Group, status models.ContactStatus, parsed *contactql.ContactQuery, limit int) ([]core.ContactUUID, error) {
	const batchSize = 10_000

	uuids := make([]core.ContactUUID, 0, 100)
	if limit == 0 {
		return uuids, nil
	}

	conv, where, err := newContactsQuery(oa, group, status, nil, parsed)
	if err != nil {
		return nil, err
	}
	baseArgs := conv.Args()

	type row struct {
		ID   models.ContactID `db:"id"`
		UUID core.ContactUUID `db:"uuid"`
	}

	afterID := models.ContactID(0)

	for {
		size := batchSize
		if limit != -1 && limit-len(uuids) < size {
			size = limit - len(uuids)
		}

		args := slices.Clone(baseArgs)
		args = append(args, afterID, size)
		query := fmt.Sprintf("SELECT c.id, c.uuid FROM contacts_contact c WHERE %s AND c.id > $%d ORDER BY c.id ASC LIMIT $%d", where, len(args)-1, len(args))

		var batch []row
		if err := rt.DB.SelectContext(ctx, &batch, query, args...); err != nil {
			return nil, fmt.Errorf("error querying contacts: %w", err)
		}

		for _, r := range batch {
			uuids = append(uuids, r.UUID)
			afterID = r.ID
		}

		if len(batch) < size || (limit != -1 && len(uuids) >= limit) {
			break
		}
	}

	return uuids, nil
}

// SearchMessagesPostgres searches messages in the given org for the given text and returns them as msg_received and
// msg_created events, the same shape the Elastic search reads back from DynamoDB. Matching is on whole words, all
// of which must be present, using the text_search tsvector column that the nanoRP database migration adds.
func SearchMessagesPostgres(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID, text string, contactUUID core.ContactUUID, inTicket bool, limit int) ([]MessageResult, error) {
	args := []any{orgID, text}
	clauses := []string{"m.org_id = $1", "m.text_search @@ plainto_tsquery('simple', $2)", "m.visibility = 'V'"}

	// as with Elastic, searching by contact sorts purely by recency, otherwise by relevance then recency, and a search
	// across the org only looks back 180 days
	orderBy := "ts_rank(m.text_search, plainto_tsquery('simple', $2)) DESC, m.created_on DESC, m.id DESC"

	if contactUUID != "" {
		args = append(args, string(contactUUID))
		clauses = append(clauses, fmt.Sprintf("c.uuid = $%d", len(args)))
		orderBy = "m.created_on DESC, m.id DESC"
	} else {
		since := dates.Now().Add(-180 * 24 * time.Hour).UTC()
		args = append(args, time.Date(since.Year(), since.Month(), since.Day(), 0, 0, 0, 0, time.UTC))
		clauses = append(clauses, fmt.Sprintf("m.created_on >= $%d", len(args)))
	}
	if inTicket {
		clauses = append(clauses, "m.ticket_uuid IS NOT NULL")
	}

	args = append(args, limit)
	query := fmt.Sprintf(`SELECT m.uuid, m.text, m.attachments, m.created_on, m.direction, m.ticket_uuid, c.uuid AS contact_uuid
FROM msgs_msg m JOIN contacts_contact c ON c.id = m.contact_id
WHERE %s ORDER BY %s LIMIT $%d`, strings.Join(clauses, " AND "), orderBy, len(args))

	rows, err := rt.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("error searching messages: %w", err)
	}
	defer rows.Close()

	results := make([]MessageResult, 0, limit)

	for rows.Next() {
		var uuid, msgText, direction, contact string
		var attachments pq.StringArray
		var createdOn time.Time
		var ticketUUID *string

		if err := rows.Scan(&uuid, &msgText, &attachments, &createdOn, &direction, &ticketUUID, &contact); err != nil {
			return nil, fmt.Errorf("error scanning message: %w", err)
		}

		eventType := events.TypeMsgReceived
		if models.Direction(direction) == models.DirectionOut {
			eventType = events.TypeMsgCreated
		}
		if attachments == nil {
			attachments = pq.StringArray{}
		}

		event := map[string]any{
			"uuid":       uuid,
			"type":       eventType,
			"created_on": createdOn.UTC().Format(time.RFC3339Nano),
			"msg": map[string]any{
				"uuid":        uuid,
				"text":        msgText,
				"attachments": []string(attachments),
			},
		}
		if ticketUUID != nil {
			event["ticket_uuid"] = *ticketUUID
		}

		results = append(results, MessageResult{ContactUUID: core.ContactUUID(contact), Event: event})
	}

	return results, rows.Err()
}

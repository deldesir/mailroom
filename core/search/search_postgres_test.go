package search_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/goflow/contactql"
	"github.com/nyaruka/goflow/contactql/parse"
	"github.com/nyaruka/goflow/core"
	"github.com/nyaruka/mailroom/v26/core/models"
	"github.com/nyaruka/mailroom/v26/core/search"
	"github.com/nyaruka/mailroom/v26/testsuite"
	"github.com/nyaruka/mailroom/v26/testsuite/testdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The Postgres searches need nothing but the database, so these run whichever way the suite is configured. When it
// runs with Elastic switched off (see testsuite.NanoRP) the tests of the public entry points in search_test.go and
// messages_test.go go through the same code.

func TestContactSearchPostgres(t *testing.T) {
	ctx, rt := testsuite.Runtime(t)

	oa, err := models.GetOrgAssets(ctx, rt, testdb.Org1.ID)
	require.NoError(t, err)

	parseQuery := func(q string) *contactql.ContactQuery {
		if q == "" {
			return nil
		}
		parsed, err := parse.Query(oa.Env(), q, oa.SessionAssets())
		require.NoError(t, err)
		return parsed
	}

	// a deleted contact is invisible to searches, as it is de-indexed from Elastic
	testdb.InsertContact(t, rt, testdb.Org1, "e5b0f1a0-1c3c-4d2b-8f5a-9f6a1a2b3c4d", "Eve", "eng", models.ContactStatusActive)
	rt.DB.MustExec(`UPDATE contacts_contact SET is_active = FALSE WHERE uuid = 'e5b0f1a0-1c3c-4d2b-8f5a-9f6a1a2b3c4d'`)

	totals := []struct {
		group  *testdb.Group
		status models.ContactStatus
		query  string
		total  int64
	}{
		{query: "", total: 124},
		{query: "ann OR bob", total: 2},
		{group: testdb.DoctorsGroup, query: "ann OR bob", total: 1},
		{status: models.ContactStatusActive, query: "cat", total: 1},
		{status: models.ContactStatusBlocked, query: "cat", total: 0},
		{query: "age >= 30", total: 1},
		{query: "joined > 2019-01-01", total: 1},
		{query: "gender = F", total: 1},
		{query: "state = yobe", total: 1},
		{query: `tel = +16055742222`, total: 1},
		{query: `group = doctors`, total: 121},
		{query: `group = testers`, total: 10},
		{query: `name = ""`, total: 120},
		{query: `eve`, total: 0},
	}

	for _, tc := range totals {
		var group *models.Group
		if tc.group != nil {
			group = oa.GroupByID(tc.group.ID)
		}

		total, err := search.GetContactTotalPostgres(ctx, rt, oa, group, tc.status, parseQuery(tc.query))
		assert.NoError(t, err, "error for query %q", tc.query)
		assert.Equal(t, tc.total, total, "total mismatch for query %q", tc.query)
	}

	pages := []struct {
		query    string
		sort     string
		exclude  []core.ContactUUID
		offset   int
		pageSize int
		contacts []core.ContactUUID
		total    int64
	}{
		{query: "cat OR bob", pageSize: 50, contacts: []core.ContactUUID{testdb.Cat.UUID, testdb.Bob.UUID}, total: 2},
		{query: "age >= 30", sort: "-age", pageSize: 50, contacts: []core.ContactUUID{testdb.Cat.UUID}, total: 1},
		{query: "age >= 30", sort: "-age", exclude: []core.ContactUUID{testdb.Cat.UUID}, pageSize: 50, contacts: []core.ContactUUID{}, total: 0},
		{query: "ann OR bob OR cat", sort: "name", pageSize: 50, contacts: []core.ContactUUID{testdb.Ann.UUID, testdb.Bob.UUID, testdb.Cat.UUID}, total: 3},
		{query: "ann OR bob OR cat", sort: "-name", pageSize: 50, contacts: []core.ContactUUID{testdb.Cat.UUID, testdb.Bob.UUID, testdb.Ann.UUID}, total: 3},
		{query: "ann OR bob OR cat", sort: "-name", pageSize: 2, contacts: []core.ContactUUID{testdb.Cat.UUID, testdb.Bob.UUID}, total: 3},
		{query: "ann OR bob OR cat", sort: "-name", offset: 2, pageSize: 2, contacts: []core.ContactUUID{testdb.Ann.UUID}, total: 3},
		{query: "", sort: "id", offset: 1, pageSize: 2, contacts: []core.ContactUUID{testdb.Bob.UUID, testdb.Cat.UUID}, total: 124},
	}

	for i, tc := range pages {
		uuids, total, err := search.GetContactUUIDsForQueryPagePostgres(ctx, rt, oa, nil, models.NilContactStatus, tc.exclude, parseQuery(tc.query), tc.sort, tc.offset, tc.pageSize)
		assert.NoError(t, err, "%d: error for query %q", i, tc.query)
		assert.Equal(t, tc.contacts, uuids, "%d: uuids mismatch for query %q", i, tc.query)
		assert.Equal(t, tc.total, total, "%d: total mismatch for query %q", i, tc.query)
	}

	_, _, err = search.GetContactUUIDsForQueryPagePostgres(ctx, rt, oa, nil, models.NilContactStatus, nil, parseQuery("ann"), "goats", 0, 50)
	assert.EqualError(t, err, "error converting sort to SQL: no such field with key: goats")

	// unpaged results are ordered by id, and a limit of -1 means everything
	all, err := search.GetContactUUIDsForQueryPostgres(ctx, rt, oa, nil, models.NilContactStatus, nil, -1)
	require.NoError(t, err)
	assert.Len(t, all, 124)
	assert.Equal(t, []core.ContactUUID{testdb.Ann.UUID, testdb.Bob.UUID, testdb.Cat.UUID, testdb.Dan.UUID}, all[:4])

	some, err := search.GetContactUUIDsForQueryPostgres(ctx, rt, oa, nil, models.NilContactStatus, nil, 3)
	require.NoError(t, err)
	assert.Equal(t, []core.ContactUUID{testdb.Ann.UUID, testdb.Bob.UUID, testdb.Cat.UUID}, some)

	more, err := search.GetContactUUIDsForQueryPostgres(ctx, rt, oa, nil, models.NilContactStatus, nil, 200)
	require.NoError(t, err)
	assert.Equal(t, all, more)

	none, err := search.GetContactUUIDsForQueryPostgres(ctx, rt, oa, nil, models.NilContactStatus, nil, 0)
	require.NoError(t, err)
	assert.Equal(t, []core.ContactUUID{}, none)

	doctors, err := search.GetContactUUIDsForQueryPostgres(ctx, rt, oa, oa.GroupByID(testdb.DoctorsGroup.ID), models.NilContactStatus, parseQuery("ann OR bob"), -1)
	require.NoError(t, err)
	assert.Equal(t, []core.ContactUUID{testdb.Ann.UUID}, doctors)
}

func TestSearchMessagesPostgres(t *testing.T) {
	ctx, rt := testsuite.Runtime(t)

	// the search needs the column that the nanorp_indexes command adds to a deployment's database - the nanoRP test
	// template has it already and otherwise the test adds it to its own database
	rt.DB.MustExec(`ALTER TABLE msgs_msg ADD COLUMN IF NOT EXISTS text_search tsvector GENERATED ALWAYS AS (to_tsvector('simple', text)) STORED`)

	// pin the clock to just after the message fixtures so they stay within the search's rolling time window
	defer dates.SetNowFunc(time.Now)
	dates.SetNowFunc(dates.NewFixedNow(time.Date(2025, 12, 16, 0, 0, 0, 0, time.UTC)))

	testdb.InsertIncomingMsg(t, rt, testdb.Org1, "019b21e1-ba00-7000-8000-000000000001", testdb.TwilioChannel, testdb.Ann, "hello world", models.MsgStatusHandled, "")
	testdb.InsertIncomingMsg(t, rt, testdb.Org1, "019b2218-a880-7000-8000-000000000002", testdb.TwilioChannel, testdb.Bob, "hello there friend", models.MsgStatusHandled, "019b2218-a880-7000-8000-000000000099")
	testdb.InsertIncomingMsg(t, rt, testdb.Org1, "019b224f-9700-7000-8000-000000000003", testdb.TwilioChannel, testdb.Cat, "goodbye world", models.MsgStatusHandled, "")
	testdb.InsertIncomingMsg(t, rt, testdb.Org2, "019b21e1-ba00-7000-8000-000000000004", testdb.Org2Channel, testdb.Org2Contact, "hello world", models.MsgStatusHandled, "")

	// archived messages are searchable like visible ones, deleted ones aren't, and nor are those of contacts never seen
	testdb.InsertIncomingMsg(t, rt, testdb.Org1, "019b2260-0000-7000-8000-000000000005", testdb.TwilioChannel, testdb.Ann, "hello archived", models.MsgStatusHandled, "")
	testdb.InsertIncomingMsg(t, rt, testdb.Org1, "019b2270-0000-7000-8000-000000000006", testdb.TwilioChannel, testdb.Cat, "hello deleted", models.MsgStatusHandled, "")
	testdb.InsertIncomingMsg(t, rt, testdb.Org1, "019b2280-0000-7000-8000-000000000007", testdb.TwilioChannel, testdb.Dan, "hello dan", models.MsgStatusHandled, "")
	rt.DB.MustExec(`UPDATE msgs_msg SET visibility = 'A' WHERE uuid = '019b2260-0000-7000-8000-000000000005'`)
	rt.DB.MustExec(`UPDATE msgs_msg SET visibility = 'D' WHERE uuid = '019b2270-0000-7000-8000-000000000006'`)

	// outgoing messages are msg_created events, and a search across the org only looks back 180 days
	testdb.InsertOutgoingMsg(t, rt, testdb.Org1, "019b2290-0000-7000-8000-000000000008", testdb.TwilioChannel, testdb.Bob, "hello outgoing", nil, models.MsgStatusSent, false)
	testdb.InsertOutgoingMsgCreatedOn(t, rt, testdb.Org1, "0194a1e0-0000-7000-8000-000000000009", testdb.TwilioChannel, testdb.Ann, "hello old", models.MsgStatusSent, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))

	rt.DB.MustExec(`UPDATE contacts_contact SET last_seen_on = NOW() WHERE id IN ($1, $2, $3, $4)`, testdb.Ann.ID, testdb.Bob.ID, testdb.Cat.ID, testdb.Org2Contact.ID)

	tcs := []struct {
		label       string
		text        string
		contactUUID core.ContactUUID
		inTicket    bool
		limit       int
		expected    []core.ContactUUID
	}{
		{label: "most recent first", text: "hello", limit: 50, expected: []core.ContactUUID{testdb.Bob.UUID, testdb.Ann.UUID, testdb.Bob.UUID, testdb.Ann.UUID}},
		{label: "matching one message", text: "goodbye", limit: 50, expected: []core.ContactUUID{testdb.Cat.UUID}},
		{label: "matching no messages", text: "xyznotfound", limit: 50, expected: []core.ContactUUID{}},
		{label: "filtered by contact has no time window", text: "hello", contactUUID: testdb.Ann.UUID, limit: 50, expected: []core.ContactUUID{testdb.Ann.UUID, testdb.Ann.UUID, testdb.Ann.UUID}},
		{label: "filtered by in_ticket", text: "hello", inTicket: true, limit: 50, expected: []core.ContactUUID{testdb.Bob.UUID}},
		{label: "respects limit", text: "hello", limit: 1, expected: []core.ContactUUID{testdb.Bob.UUID}},
		{label: "multi-word match requires all terms", text: "hello world", limit: 50, expected: []core.ContactUUID{testdb.Ann.UUID}},
		{label: "deleted messages aren't found", text: "deleted", limit: 50, expected: []core.ContactUUID{}},
		{label: "messages of contacts never seen aren't found", text: "dan", limit: 50, expected: []core.ContactUUID{}},
	}

	for _, tc := range tcs {
		t.Run(tc.label, func(t *testing.T) {
			results, err := search.SearchMessagesPostgres(ctx, rt, testdb.Org1.ID, tc.text, tc.contactUUID, tc.inTicket, tc.limit)
			require.NoError(t, err)

			contactUUIDs := make([]core.ContactUUID, len(results))
			for i, r := range results {
				contactUUIDs[i] = r.ContactUUID
			}
			assert.Equal(t, tc.expected, contactUUIDs)
		})
	}

	// results are events of the shape the Elastic search reads back from DynamoDB
	results, err := search.SearchMessagesPostgres(ctx, rt, testdb.Org1.ID, "hello", "", false, 50)
	require.NoError(t, err)
	require.Len(t, results, 4)

	// the outgoing message was created at NOW() so its time is only checked for shape
	_, err = time.Parse(time.RFC3339Nano, results[0].Event["created_on"].(string))
	assert.NoError(t, err)
	delete(results[0].Event, "created_on")

	assert.Equal(t, map[string]any{
		"uuid": "019b2290-0000-7000-8000-000000000008",
		"type": "msg_created",
		"msg":  map[string]any{"uuid": "019b2290-0000-7000-8000-000000000008", "text": "hello outgoing", "attachments": []string{}},
	}, results[0].Event)

	assert.Equal(t, map[string]any{
		"uuid":        "019b2218-a880-7000-8000-000000000002",
		"type":        "msg_received",
		"created_on":  "2025-12-15T13:00:00Z",
		"ticket_uuid": "019b2218-a880-7000-8000-000000000099",
		"msg":         map[string]any{"uuid": "019b2218-a880-7000-8000-000000000002", "text": "hello there friend", "attachments": []string{}},
	}, results[2].Event)
}

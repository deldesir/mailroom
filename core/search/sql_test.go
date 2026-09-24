package search_test

import (
	"testing"
	"time"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/assets/static"
	"github.com/nyaruka/goflow/contactql"
	"github.com/nyaruka/goflow/contactql/parse"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/utils/obfuscate"
	"github.com/nyaruka/mailroom/v26/core/models"
	"github.com/nyaruka/mailroom/v26/core/search"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// maps groups and flows to ids the way the org assets do in production
type sqlTestMapper struct {
	flows  map[assets.FlowUUID]int64
	groups map[assets.GroupUUID]int64
}

func (m *sqlTestMapper) Flow(f assets.Flow) int64   { return m.flows[f.UUID()] }
func (m *sqlTestMapper) Group(g assets.Group) int64 { return m.groups[g.UUID()] }

const (
	ageUUID      = "6b6a43fa-a26d-4017-bede-328bcdd5c93b"
	colorUUID    = "ecc7b13b-c698-4f46-8a90-24a8fab6fe34"
	dobUUID      = "cbd3fc0e-9b74-4207-a8c7-248082bb4572"
	stateUUID    = "67663ad1-3abc-42dd-a162-09df2dea66ec"
	districtUUID = "54c72635-d747-4e45-883c-099d57dd998e"
	wardUUID     = "fde8f740-c337-421b-8abb-83b954897c80"
)

func newSQLTestEnv(t *testing.T) (envs.Environment, contactql.Resolver, *sqlTestMapper) {
	tz, err := time.LoadLocation("Africa/Kigali") // UTC+2 all year, so day boundaries are visible in the args
	require.NoError(t, err)

	env := envs.NewBuilder().WithTimezone(tz).WithObfuscationKey([4]uint32{1, 2, 3, 4}).Build()

	resolver := contactql.NewMockResolver(
		[]assets.Field{
			static.NewField(ageUUID, "age", "Age", assets.FieldTypeNumber),
			static.NewField(colorUUID, "color", "Color", assets.FieldTypeText),
			static.NewField(dobUUID, "dob", "DOB", assets.FieldTypeDatetime),
			static.NewField(stateUUID, "state", "State", assets.FieldTypeState),
			static.NewField(districtUUID, "district", "District", assets.FieldTypeDistrict),
			static.NewField(wardUUID, "ward", "Ward", assets.FieldTypeWard),
		},
		[]assets.Flow{
			static.NewFlow("c261165a-f5b0-40ba-b916-76fb49667a4f", "Registration", []byte(`{}`)),
		},
		[]assets.Group{
			static.NewGroup("8de30b78-d9ef-4db2-b2e8-4f7b6aef64cf", "U-Reporters", ""),
			static.NewGroup("cf51cf8d-94da-447a-b27e-a42a900c37a6", "Testers", ""),
		},
	)

	mapper := &sqlTestMapper{
		flows:  map[assets.FlowUUID]int64{"c261165a-f5b0-40ba-b916-76fb49667a4f": 234},
		groups: map[assets.GroupUUID]int64{"8de30b78-d9ef-4db2-b2e8-4f7b6aef64cf": 345, "cf51cf8d-94da-447a-b27e-a42a900c37a6": 456},
	}

	return env, resolver, mapper
}

// times are compared as strings so that the expected day boundaries can be read in the test cases
func comparableArgs(args []any) []any {
	out := make([]any, len(args))
	for i, a := range args {
		if t, ok := a.(time.Time); ok {
			out[i] = t.Format(time.RFC3339)
		} else {
			out[i] = a
		}
	}
	return out
}

func TestSQLConverterQuery(t *testing.T) {
	env, resolver, mapper := newSQLTestEnv(t)

	ref, err := obfuscate.EncodeID(123, env.ObfuscationKey())
	require.NoError(t, err)

	tcs := []struct {
		query string
		sql   string
		args  []any
		err   string
	}{
		// name
		{query: `name = "Bob"`, sql: `lower(btrim(c.name)) = $1`, args: []any{"bob"}},
		{query: `name != " Bob "`, sql: `(c.name IS NULL OR lower(btrim(c.name)) != $1)`, args: []any{"bob"}},
		{query: `name ~ "Bob"`, sql: `c.name ~* $1`, args: []any{`\m(bob)`}},
		{query: `name ~ "Christopher Robin"`, sql: `c.name ~* $1`, args: []any{`\m(christop|robin)`}},
		{query: `name ~ "O'Neil"`, sql: `c.name ~* $1`, args: []any{`\m(o'neil)`}},
		{query: `Bob`, sql: `c.name ~* $1`, args: []any{`\m(bob)`}},
		{query: `name = ""`, sql: `NOT (c.name IS NOT NULL AND btrim(c.name) != '')`, args: []any{}},
		{query: `name != ""`, sql: `(c.name IS NOT NULL AND btrim(c.name) != '')`, args: []any{}},

		// status, mapped to the codes stored in the database
		{query: `status = active`, sql: `c.status = $1`, args: []any{models.ContactStatusActive}},
		{query: `status = Blocked`, sql: `c.status = $1`, args: []any{models.ContactStatusBlocked}},
		{query: `status != stopped`, sql: `c.status != $1`, args: []any{models.ContactStatusStopped}},
		{query: `status = archived`, sql: `c.status = $1`, args: []any{models.ContactStatusArchived}},

		// language
		{query: `language = ENG`, sql: `c.language = $1`, args: []any{"eng"}},
		{query: `language != eng`, sql: `c.language IS DISTINCT FROM $1`, args: []any{"eng"}},
		{query: `language = ""`, sql: `NOT (c.language IS NOT NULL AND c.language != '')`, args: []any{}},
		{query: `language != ""`, sql: `(c.language IS NOT NULL AND c.language != '')`, args: []any{}},

		// uuid, id and ref
		{query: `uuid = "3ADCDA88-9A9F-4A5D-A8D6-5B92F0E5B8D3"`, sql: `c.uuid = $1`, args: []any{"3adcda88-9a9f-4a5d-a8d6-5b92f0e5b8d3"}},
		{query: `uuid != "3adcda88-9a9f-4a5d-a8d6-5b92f0e5b8d3"`, sql: `c.uuid != $1`, args: []any{"3adcda88-9a9f-4a5d-a8d6-5b92f0e5b8d3"}},
		{query: `id = 123`, sql: `c.id = $1`, args: []any{int64(123)}},
		{query: `id != 123`, sql: `c.id != $1`, args: []any{int64(123)}},
		{query: `id = abc`, err: "'abc' is not a valid contact id"},
		{query: `ref = ` + ref, sql: `c.id = $1`, args: []any{int64(123)}},
		{query: `ref = xyz`, sql: `c.id = $1`, args: []any{int64(0)}},

		// created_on matches whole days in the org timezone
		{query: `created_on = 2024-01-15`, sql: `(c.created_on >= $1 AND c.created_on < $2)`, args: []any{"2024-01-15T00:00:00+02:00", "2024-01-16T00:00:00+02:00"}},
		{query: `created_on != 2024-01-15`, sql: `NOT (c.created_on >= $1 AND c.created_on < $2)`, args: []any{"2024-01-15T00:00:00+02:00", "2024-01-16T00:00:00+02:00"}},
		{query: `created_on > 2024-01-15`, sql: `c.created_on >= $1`, args: []any{"2024-01-16T00:00:00+02:00"}},
		{query: `created_on >= 2024-01-15`, sql: `c.created_on >= $1`, args: []any{"2024-01-15T00:00:00+02:00"}},
		{query: `created_on < 2024-01-15`, sql: `c.created_on < $1`, args: []any{"2024-01-15T00:00:00+02:00"}},
		{query: `created_on <= 2024-01-15`, sql: `c.created_on < $1`, args: []any{"2024-01-16T00:00:00+02:00"}},
		{query: `created_on = "2024-01-15 10:30"`, sql: `(c.created_on >= $1 AND c.created_on < $2)`, args: []any{"2024-01-15T00:00:00+02:00", "2024-01-16T00:00:00+02:00"}},

		// last_seen_on can be unset
		{query: `last_seen_on = ""`, sql: `c.last_seen_on IS NULL`, args: []any{}},
		{query: `last_seen_on != ""`, sql: `c.last_seen_on IS NOT NULL`, args: []any{}},
		{query: `last_seen_on = 2024-01-15`, sql: `(c.last_seen_on >= $1 AND c.last_seen_on < $2)`, args: []any{"2024-01-15T00:00:00+02:00", "2024-01-16T00:00:00+02:00"}},
		{query: `last_seen_on != 2024-01-15`, sql: `(c.last_seen_on IS NULL OR NOT (c.last_seen_on >= $1 AND c.last_seen_on < $2))`, args: []any{"2024-01-15T00:00:00+02:00", "2024-01-16T00:00:00+02:00"}},
		{query: `last_seen_on > 2024-01-15`, sql: `c.last_seen_on >= $1`, args: []any{"2024-01-16T00:00:00+02:00"}},

		// urn of any scheme
		{query: `urn = "+250788123456"`, sql: `EXISTS (SELECT 1 FROM contacts_contacturn u WHERE u.contact_id = c.id AND lower(u.path) = $1)`, args: []any{"+250788123456"}},
		{query: `urn != "Bob@Nyaruka.com"`, sql: `NOT EXISTS (SELECT 1 FROM contacts_contacturn u WHERE u.contact_id = c.id AND lower(u.path) = $1)`, args: []any{"bob@nyaruka.com"}},
		{query: `urn ~ "0788"`, sql: `EXISTS (SELECT 1 FROM contacts_contacturn u WHERE u.contact_id = c.id AND u.path ILIKE $1)`, args: []any{"%0788%"}},
		{query: `urn ~ "50%_x"`, sql: `EXISTS (SELECT 1 FROM contacts_contacturn u WHERE u.contact_id = c.id AND u.path ILIKE $1)`, args: []any{`%50\%\_x%`}},
		{query: `urn = ""`, sql: `NOT EXISTS (SELECT 1 FROM contacts_contacturn u WHERE u.contact_id = c.id)`, args: []any{}},
		{query: `urn != ""`, sql: `EXISTS (SELECT 1 FROM contacts_contacturn u WHERE u.contact_id = c.id)`, args: []any{}},

		// urn of a specific scheme
		{query: `tel = +250788123456`, sql: `EXISTS (SELECT 1 FROM contacts_contacturn u WHERE u.contact_id = c.id AND u.scheme = $1 AND lower(u.path) = $2)`, args: []any{"tel", "+250788123456"}},
		{query: `tel != +250788123456`, sql: `NOT EXISTS (SELECT 1 FROM contacts_contacturn u WHERE u.contact_id = c.id AND u.scheme = $1 AND lower(u.path) = $2)`, args: []any{"tel", "+250788123456"}},
		{query: `tel ~ 0788`, sql: `EXISTS (SELECT 1 FROM contacts_contacturn u WHERE u.contact_id = c.id AND u.scheme = $1 AND u.path ILIKE $2)`, args: []any{"tel", "%0788%"}},
		{query: `tel:+250788123456`, sql: `EXISTS (SELECT 1 FROM contacts_contacturn u WHERE u.contact_id = c.id AND u.scheme = $1 AND lower(u.path) = $2)`, args: []any{"tel", "+250788123456"}},
		{query: `0788-123-456`, sql: `EXISTS (SELECT 1 FROM contacts_contacturn u WHERE u.contact_id = c.id AND u.scheme = $1 AND u.path ILIKE $2)`, args: []any{"tel", "%0788123456%"}},
		{query: `whatsapp = ""`, sql: `NOT EXISTS (SELECT 1 FROM contacts_contacturn u WHERE u.contact_id = c.id AND u.scheme = $1)`, args: []any{"whatsapp"}},
		{query: `whatsapp != ""`, sql: `EXISTS (SELECT 1 FROM contacts_contacturn u WHERE u.contact_id = c.id AND u.scheme = $1)`, args: []any{"whatsapp"}},

		// group
		{query: `group = "u-reporters"`, sql: `EXISTS (SELECT 1 FROM contacts_contactgroup_contacts gc WHERE gc.contact_id = c.id AND gc.contactgroup_id = $1)`, args: []any{int64(345)}},
		{query: `group != Testers`, sql: `NOT EXISTS (SELECT 1 FROM contacts_contactgroup_contacts gc WHERE gc.contact_id = c.id AND gc.contactgroup_id = $1)`, args: []any{int64(456)}},
		{query: `group = ""`, sql: `NOT EXISTS (SELECT 1 FROM contacts_contactgroup_contacts gc JOIN contacts_contactgroup g ON g.id = gc.contactgroup_id WHERE gc.contact_id = c.id AND g.group_type IN ($1, $2))`, args: []any{models.GroupTypeManual, models.GroupTypeSmart}},
		{query: `group != ""`, sql: `EXISTS (SELECT 1 FROM contacts_contactgroup_contacts gc JOIN contacts_contactgroup g ON g.id = gc.contactgroup_id WHERE gc.contact_id = c.id AND g.group_type IN ($1, $2))`, args: []any{models.GroupTypeManual, models.GroupTypeSmart}},

		// flow and history
		{query: `flow = registration`, sql: `c.current_flow_id = $1`, args: []any{int64(234)}},
		{query: `flow != registration`, sql: `c.current_flow_id IS DISTINCT FROM $1`, args: []any{int64(234)}},
		{query: `flow = ""`, sql: `c.current_flow_id IS NULL`, args: []any{}},
		{query: `flow != ""`, sql: `c.current_flow_id IS NOT NULL`, args: []any{}},
		{query: `history = registration`, sql: `EXISTS (SELECT 1 FROM flows_flowrun r WHERE r.contact_id = c.id AND r.flow_id = $1)`, args: []any{int64(234)}},
		{query: `history != registration`, sql: `NOT EXISTS (SELECT 1 FROM flows_flowrun r WHERE r.contact_id = c.id AND r.flow_id = $1)`, args: []any{int64(234)}},
		{query: `history = ""`, sql: `NOT EXISTS (SELECT 1 FROM flows_flowrun r WHERE r.contact_id = c.id)`, args: []any{}},
		{query: `history != ""`, sql: `EXISTS (SELECT 1 FROM flows_flowrun r WHERE r.contact_id = c.id)`, args: []any{}},

		// tickets is the count of open tickets
		{query: `tickets = 2`, sql: `c.ticket_count = $1::numeric`, args: []any{"2"}},
		{query: `tickets != 0`, sql: `c.ticket_count != $1::numeric`, args: []any{"0"}},
		{query: `tickets > 0`, sql: `c.ticket_count > $1::numeric`, args: []any{"0"}},
		{query: `tickets >= 1`, sql: `c.ticket_count >= $1::numeric`, args: []any{"1"}},
		{query: `tickets < 3`, sql: `c.ticket_count < $1::numeric`, args: []any{"3"}},
		{query: `tickets <= 3`, sql: `c.ticket_count <= $1::numeric`, args: []any{"3"}},

		// number fields
		{query: `age = 30`, sql: `(c.fields->$1->>'number')::numeric = $2`, args: []any{ageUUID, "30"}},
		{query: `age != 30`, sql: `(c.fields->$1->>'number')::numeric IS DISTINCT FROM $2`, args: []any{ageUUID, "30"}},
		{query: `age > 30`, sql: `(c.fields->$1->>'number')::numeric > $2`, args: []any{ageUUID, "30"}},
		{query: `age >= 30.5`, sql: `(c.fields->$1->>'number')::numeric >= $2`, args: []any{ageUUID, "30.5"}},
		{query: `age < 30`, sql: `(c.fields->$1->>'number')::numeric < $2`, args: []any{ageUUID, "30"}},
		{query: `age <= 30`, sql: `(c.fields->$1->>'number')::numeric <= $2`, args: []any{ageUUID, "30"}},
		{query: `age = ""`, sql: `NULLIF(c.fields->$1->>'number', '') IS NULL`, args: []any{ageUUID}},
		{query: `age != ""`, sql: `NULLIF(c.fields->$1->>'number', '') IS NOT NULL`, args: []any{ageUUID}},

		// text fields
		{query: `color = "Red"`, sql: `lower(btrim(c.fields->$1->>'text')) = $2`, args: []any{colorUUID, "red"}},
		{query: `color != " red "`, sql: `lower(btrim(c.fields->$1->>'text')) IS DISTINCT FROM $2`, args: []any{colorUUID, "red"}},
		{query: `color = ""`, sql: `NULLIF(c.fields->$1->>'text', '') IS NULL`, args: []any{colorUUID}},
		{query: `color != ""`, sql: `NULLIF(c.fields->$1->>'text', '') IS NOT NULL`, args: []any{colorUUID}},

		// datetime fields
		{query: `dob = 2024-01-15`, sql: `((c.fields->$1->>'datetime')::timestamptz >= $2 AND (c.fields->$1->>'datetime')::timestamptz < $3)`, args: []any{dobUUID, "2024-01-15T00:00:00+02:00", "2024-01-16T00:00:00+02:00"}},
		{query: `dob != 2024-01-15`, sql: `((c.fields->$1->>'datetime')::timestamptz IS NULL OR NOT ((c.fields->$1->>'datetime')::timestamptz >= $2 AND (c.fields->$1->>'datetime')::timestamptz < $3))`, args: []any{dobUUID, "2024-01-15T00:00:00+02:00", "2024-01-16T00:00:00+02:00"}},
		{query: `dob > 2024-01-15`, sql: `(c.fields->$1->>'datetime')::timestamptz >= $2`, args: []any{dobUUID, "2024-01-16T00:00:00+02:00"}},
		{query: `dob >= 2024-01-15`, sql: `(c.fields->$1->>'datetime')::timestamptz >= $2`, args: []any{dobUUID, "2024-01-15T00:00:00+02:00"}},
		{query: `dob < 2024-01-15`, sql: `(c.fields->$1->>'datetime')::timestamptz < $2`, args: []any{dobUUID, "2024-01-15T00:00:00+02:00"}},
		{query: `dob <= 2024-01-15`, sql: `(c.fields->$1->>'datetime')::timestamptz < $2`, args: []any{dobUUID, "2024-01-16T00:00:00+02:00"}},
		{query: `dob = ""`, sql: `NULLIF(c.fields->$1->>'datetime', '') IS NULL`, args: []any{dobUUID}},

		// location fields match on the name of the last level of the stored path
		{query: `state = "Kigali City"`, sql: `lower(btrim(split_part(c.fields->$1->>'state', '>', -1))) = $2`, args: []any{stateUUID, "kigali city"}},
		{query: `state != kigali`, sql: `lower(btrim(split_part(c.fields->$1->>'state', '>', -1))) IS DISTINCT FROM $2`, args: []any{stateUUID, "kigali"}},
		{query: `district = Gasabo`, sql: `lower(btrim(split_part(c.fields->$1->>'district', '>', -1))) = $2`, args: []any{districtUUID, "gasabo"}},
		{query: `ward = Jali`, sql: `lower(btrim(split_part(c.fields->$1->>'ward', '>', -1))) = $2`, args: []any{wardUUID, "jali"}},
		{query: `state != ""`, sql: `NULLIF(c.fields->$1->>'state', '') IS NOT NULL`, args: []any{stateUUID}},

		// combinations
		{query: `name ~ bob AND age > 30`, sql: `(c.name ~* $1 AND (c.fields->$2->>'number')::numeric > $3)`, args: []any{`\m(bob)`, ageUUID, "30"}},
		{query: `age = 30 OR age = 40`, sql: `((c.fields->$1->>'number')::numeric = $2 OR (c.fields->$3->>'number')::numeric = $4)`, args: []any{ageUUID, "30", ageUUID, "40"}},
		{query: `(color = red OR color = blue) AND tel != ""`, sql: `((lower(btrim(c.fields->$1->>'text')) = $2 OR lower(btrim(c.fields->$3->>'text')) = $4) AND EXISTS (SELECT 1 FROM contacts_contacturn u WHERE u.contact_id = c.id AND u.scheme = $5))`, args: []any{colorUUID, "red", colorUUID, "blue", "tel"}},
		{query: `name ~ bob name ~ jim`, sql: `(c.name ~* $1 AND c.name ~* $2)`, args: []any{`\m(bob)`, `\m(jim)`}},
	}

	for _, tc := range tcs {
		parsed, err := parse.Query(env, tc.query, resolver)
		require.NoError(t, err, "unexpected parse error for %s", tc.query)

		conv := search.NewSQLConverter(env, mapper)
		sql, err := conv.Query(parsed)

		if tc.err != "" {
			assert.EqualError(t, err, tc.err, "error mismatch for %s", tc.query)
			continue
		}

		require.NoError(t, err, "unexpected error for %s", tc.query)
		assert.Equal(t, tc.sql, sql, "sql mismatch for %s", tc.query)
		assert.Equal(t, tc.args, comparableArgs(conv.Args()), "args mismatch for %s", tc.query)
	}

	// queries parsed without a resolver can't be converted
	parsed, err := parse.Query(env, `name = "Bob"`, nil)
	require.NoError(t, err)
	_, err = search.NewSQLConverter(env, mapper).Query(parsed)
	assert.EqualError(t, err, "can only convert queries parsed with a resolver")
}

func TestSQLConverterSort(t *testing.T) {
	env, resolver, mapper := newSQLTestEnv(t)

	tcs := []struct {
		sort string
		sql  string
		args []any
		err  string
	}{
		{sort: "", sql: `c.id DESC`, args: []any{}},
		{sort: "id", sql: `c.id ASC`, args: []any{}},
		{sort: "-id", sql: `c.id DESC`, args: []any{}},
		{sort: "name", sql: `lower(c.name) ASC NULLS LAST, c.id ASC`, args: []any{}},
		{sort: "-Name", sql: `lower(c.name) DESC NULLS LAST, c.id DESC`, args: []any{}},
		{sort: "created_on", sql: `c.created_on ASC NULLS LAST, c.id ASC`, args: []any{}},
		{sort: "-created_on", sql: `c.created_on DESC NULLS LAST, c.id DESC`, args: []any{}},
		{sort: "-last_seen_on", sql: `c.last_seen_on DESC NULLS LAST, c.id DESC`, args: []any{}},
		{sort: "language", sql: `c.language ASC NULLS LAST, c.id ASC`, args: []any{}},
		{sort: "age", sql: `(c.fields->$1->>'number')::numeric ASC NULLS LAST, c.id ASC`, args: []any{ageUUID}},
		{sort: "-age", sql: `(c.fields->$1->>'number')::numeric DESC NULLS LAST, c.id DESC`, args: []any{ageUUID}},
		{sort: "color", sql: `lower(c.fields->$1->>'text') ASC NULLS LAST, c.id ASC`, args: []any{colorUUID}},
		{sort: "-dob", sql: `(c.fields->$1->>'datetime')::timestamptz DESC NULLS LAST, c.id DESC`, args: []any{dobUUID}},
		{sort: "state", sql: `lower(btrim(split_part(c.fields->$1->>'state', '>', -1))) ASC NULLS LAST, c.id ASC`, args: []any{stateUUID}},
		{sort: "xyz", err: "no such field with key: xyz"},
		{sort: "-xyz", err: "no such field with key: xyz"},
	}

	for _, tc := range tcs {
		conv := search.NewSQLConverter(env, mapper)
		sql, err := conv.Sort(tc.sort, resolver)

		if tc.err != "" {
			assert.EqualError(t, err, tc.err, "error mismatch for %s", tc.sort)
			continue
		}

		require.NoError(t, err, "unexpected error for %s", tc.sort)
		assert.Equal(t, tc.sql, sql, "sql mismatch for %s", tc.sort)
		assert.Equal(t, tc.args, conv.Args(), "args mismatch for %s", tc.sort)
	}

	// parameters are numbered on from those of the query
	parsed, err := parse.Query(env, `age > 30`, resolver)
	require.NoError(t, err)

	conv := search.NewSQLConverter(env, mapper)
	where, err := conv.Query(parsed)
	require.NoError(t, err)
	orderBy, err := conv.Sort("-dob", resolver)
	require.NoError(t, err)

	assert.Equal(t, `(c.fields->$1->>'number')::numeric > $2`, where)
	assert.Equal(t, `(c.fields->$3->>'datetime')::timestamptz DESC NULLS LAST, c.id DESC`, orderBy)
	assert.Equal(t, []any{ageUUID, "30", dobUUID}, conv.Args())
}

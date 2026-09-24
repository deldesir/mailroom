package search

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/contactql"
	"github.com/nyaruka/goflow/contactql/es"
	"github.com/nyaruka/goflow/core"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/goflow/utils/obfuscate"
	"github.com/nyaruka/mailroom/v26/core/models"
)

// SQLConverter converts contactql queries and sorts to SQL over contacts_contact (aliased as c) with the same
// semantics as goflow's Elastic converter has over the contacts index: text comparisons are case insensitive, dates
// match whole days in the org's timezone, and != conditions match contacts without a value. Values are always bound
// as parameters and never interpolated into the SQL.
type SQLConverter struct {
	env    envs.Environment
	assets es.AssetMapper
	args   []any
}

// NewSQLConverter creates a new converter whose placeholders start at $1
func NewSQLConverter(env envs.Environment, assets es.AssetMapper) *SQLConverter {
	return &SQLConverter{env: env, assets: assets, args: make([]any, 0, 8)}
}

// Args returns the values bound so far, in placeholder order
func (c *SQLConverter) Args() []any {
	return c.args
}

// param binds a value and returns its placeholder
func (c *SQLConverter) param(v any) string {
	c.args = append(c.args, v)
	return fmt.Sprintf("$%d", len(c.args))
}

// Query converts a query to a WHERE clause fragment. The query must have been parsed with a resolver as that's
// what resolves field, group and flow references.
func (c *SQLConverter) Query(query *contactql.ContactQuery) (string, error) {
	if query.Resolver() == nil {
		return "", errors.New("can only convert queries parsed with a resolver")
	}
	if query.Root() == nil {
		return "TRUE", nil
	}

	return c.node(query.Resolver(), query.Root())
}

func (c *SQLConverter) node(resolver contactql.Resolver, node contactql.QueryNode) (string, error) {
	switch n := node.(type) {
	case *contactql.BoolCombination:
		return c.combination(resolver, n)
	case *contactql.Condition:
		return c.condition(resolver, n)
	default:
		return "", fmt.Errorf("unsupported query node type: %T", node)
	}
}

func (c *SQLConverter) combination(resolver contactql.Resolver, comb *contactql.BoolCombination) (string, error) {
	parts := make([]string, len(comb.Children()))
	for i, child := range comb.Children() {
		part, err := c.node(resolver, child)
		if err != nil {
			return "", err
		}
		parts[i] = part
	}

	op := " AND "
	if comb.Operator() == contactql.BoolOperatorOr {
		op = " OR "
	}

	return "(" + strings.Join(parts, op) + ")", nil
}

func (c *SQLConverter) condition(resolver contactql.Resolver, cond *contactql.Condition) (string, error) {
	switch cond.PropertyType() {
	case contactql.PropertyTypeField:
		return c.fieldCondition(resolver, cond)
	case contactql.PropertyTypeAttribute:
		return c.attributeCondition(resolver, cond)
	case contactql.PropertyTypeURN:
		return c.urnCondition(cond, cond.PropertyKey())
	default:
		return "", fmt.Errorf("unsupported property type: %s", cond.PropertyType())
	}
}

func (c *SQLConverter) attributeCondition(resolver contactql.Resolver, cond *contactql.Condition) (string, error) {
	key := cond.PropertyKey()
	value := cond.Value()

	switch key {
	case contactql.AttributeName:
		if isSetCheck(cond) {
			return negateIf(cond.Operator() == contactql.OpEqual, "(c.name IS NOT NULL AND btrim(c.name) != '')"), nil
		}

		switch cond.Operator() {
		case contactql.OpEqual:
			return "lower(btrim(c.name)) = " + c.param(strings.ToLower(strings.TrimSpace(value))), nil
		case contactql.OpNotEqual:
			return "(c.name IS NULL OR lower(btrim(c.name)) != " + c.param(strings.ToLower(strings.TrimSpace(value))) + ")", nil
		case contactql.OpContains:
			pattern, ok := nameContainsPattern(value)
			if !ok {
				return "", fmt.Errorf("contains condition on name needs a token of at least 2 characters")
			}
			return "c.name ~* " + c.param(pattern), nil
		}

	case contactql.AttributeStatus:
		status, ok := models.ContactToModelStatus[core.ContactStatus(strings.ToLower(value))]
		if !ok {
			return "", fmt.Errorf("'%s' is not a valid contact status", value)
		}
		return c.compare(cond, "c.status", c.param(status), false)

	case contactql.AttributeLanguage:
		if isSetCheck(cond) {
			return negateIf(cond.Operator() == contactql.OpEqual, "(c.language IS NOT NULL AND c.language != '')"), nil
		}
		return c.compare(cond, "c.language", c.param(strings.ToLower(value)), true)

	case contactql.AttributeUUID:
		return c.compare(cond, "c.uuid", c.param(strings.ToLower(value)), false)

	case contactql.AttributeID:
		id, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return "", fmt.Errorf("'%s' is not a valid contact id", value)
		}
		return c.compare(cond, "c.id", c.param(id), false)

	case contactql.AttributeRef:
		id, _ := obfuscate.DecodeID(value, c.env.ObfuscationKey()) // an undecodable ref is zero which matches nothing
		return c.compare(cond, "c.id", c.param(id), false)

	case contactql.AttributeCreatedOn:
		return c.dateCondition(cond, "c.created_on", false)

	case contactql.AttributeLastSeenOn:
		if isSetCheck(cond) {
			return isNull(cond, "c.last_seen_on"), nil
		}
		return c.dateCondition(cond, "c.last_seen_on", true)

	case contactql.AttributeURN:
		return c.urnCondition(cond, "")

	case contactql.AttributeGroup:
		if isSetCheck(cond) {
			// only manual and smart groups count, contacts are also members of the status groups
			return negateIf(cond.Operator() == contactql.OpEqual, fmt.Sprintf(
				"EXISTS (SELECT 1 FROM contacts_contactgroup_contacts gc JOIN contacts_contactgroup g ON g.id = gc.contactgroup_id WHERE gc.contact_id = c.id AND g.group_type IN (%s, %s))",
				c.param(models.GroupTypeManual), c.param(models.GroupTypeSmart),
			)), nil
		}

		group := cond.ValueAsGroup(resolver)
		if group == nil {
			return "", fmt.Errorf("'%s' is not a valid group name", value)
		}
		member := fmt.Sprintf("EXISTS (SELECT 1 FROM contacts_contactgroup_contacts gc WHERE gc.contact_id = c.id AND gc.contactgroup_id = %s)", c.param(c.assets.Group(group)))
		return c.exists(cond, member)

	case contactql.AttributeFlow:
		if isSetCheck(cond) {
			return isNull(cond, "c.current_flow_id"), nil
		}

		flow := cond.ValueAsFlow(resolver)
		if flow == nil {
			return "", fmt.Errorf("'%s' is not a valid flow name", value)
		}
		return c.compare(cond, "c.current_flow_id", c.param(c.assets.Flow(flow)), true)

	case contactql.AttributeHistory:
		if isSetCheck(cond) {
			return negateIf(cond.Operator() == contactql.OpEqual, "EXISTS (SELECT 1 FROM flows_flowrun r WHERE r.contact_id = c.id)"), nil
		}

		flow := cond.ValueAsFlow(resolver)
		if flow == nil {
			return "", fmt.Errorf("'%s' is not a valid flow name", value)
		}
		ran := fmt.Sprintf("EXISTS (SELECT 1 FROM flows_flowrun r WHERE r.contact_id = c.id AND r.flow_id = %s)", c.param(c.assets.Flow(flow)))
		return c.exists(cond, ran)

	case contactql.AttributeTickets:
		num, err := cond.ValueAsNumber()
		if err != nil {
			return "", fmt.Errorf("can't convert '%s' to a number", value)
		}
		// ticket_count is an integer column so the parameter is cast rather than left to inference
		return c.compare(cond, "c.ticket_count", c.param(num.Native().String())+"::numeric", false)
	}

	return "", unsupportedOperator(cond)
}

func (c *SQLConverter) fieldCondition(resolver contactql.Resolver, cond *contactql.Condition) (string, error) {
	field := resolver.ResolveField(cond.PropertyKey())
	if field == nil {
		return "", fmt.Errorf("no such field with key: %s", cond.PropertyKey())
	}

	// values are stored keyed by field UUID with a sub-key per type, e.g. {"<uuid>": {"text": "..", "number": ".."}}
	fieldType := field.Type()
	raw := fmt.Sprintf("c.fields->%s->>'%s'", c.param(string(field.UUID())), fieldType)

	if isSetCheck(cond) {
		return isNull(cond, fmt.Sprintf("NULLIF(%s, '')", raw)), nil
	}

	switch fieldType {
	case assets.FieldTypeText:
		return c.compare(cond, fmt.Sprintf("lower(btrim(%s))", raw), c.param(strings.ToLower(strings.TrimSpace(cond.Value()))), true)

	case assets.FieldTypeNumber:
		num, err := cond.ValueAsNumber()
		if err != nil {
			return "", fmt.Errorf("can't convert '%s' to a number", cond.Value())
		}
		return c.compare(cond, fmt.Sprintf("(%s)::numeric", raw), c.param(num.Native().String()), true)

	case assets.FieldTypeDatetime:
		return c.dateCondition(cond, fmt.Sprintf("(%s)::timestamptz", raw), true)

	case assets.FieldTypeState, assets.FieldTypeDistrict, assets.FieldTypeWard:
		// locations are stored as paths like "Rwanda > Kigali" but searched by the name of the last level
		name := fmt.Sprintf("lower(btrim(split_part(%s, '>', -1)))", raw)
		return c.compare(cond, name, c.param(strings.ToLower(strings.TrimSpace(cond.Value()))), true)
	}

	return "", fmt.Errorf("unsupported field type: %s", fieldType)
}

// urnCondition converts a condition on URNs of the given scheme, or on any URN if scheme is empty
func (c *SQLConverter) urnCondition(cond *contactql.Condition, scheme string) (string, error) {
	where := "u.contact_id = c.id"
	if scheme != "" {
		where += " AND u.scheme = " + c.param(scheme)
	}

	if isSetCheck(cond) {
		return negateIf(cond.Operator() == contactql.OpEqual, fmt.Sprintf("EXISTS (SELECT 1 FROM contacts_contacturn u WHERE %s)", where)), nil
	}

	value := strings.ToLower(cond.Value())

	switch cond.Operator() {
	case contactql.OpEqual, contactql.OpNotEqual:
		return c.exists(cond, fmt.Sprintf("EXISTS (SELECT 1 FROM contacts_contacturn u WHERE %s AND lower(u.path) = %s)", where, c.param(value)))
	case contactql.OpContains:
		return fmt.Sprintf("EXISTS (SELECT 1 FROM contacts_contacturn u WHERE %s AND u.path ILIKE %s)", where, c.param("%"+escapeLike(value)+"%")), nil
	}

	return "", unsupportedOperator(cond)
}

// dateCondition converts a condition on a date, matching the whole day of the value in the org's timezone
func (c *SQLConverter) dateCondition(cond *contactql.Condition, expr string, nullable bool) (string, error) {
	value, err := cond.ValueAsDate(c.env)
	if err != nil {
		return "", fmt.Errorf("can't convert '%s' to a date", cond.Value())
	}
	start, end := dates.DayToUTCRange(value, value.Location())

	switch cond.Operator() {
	case contactql.OpEqual:
		return fmt.Sprintf("(%s >= %s AND %s < %s)", expr, c.param(start), expr, c.param(end)), nil
	case contactql.OpNotEqual:
		day := fmt.Sprintf("NOT (%s >= %s AND %s < %s)", expr, c.param(start), expr, c.param(end))
		if nullable {
			return fmt.Sprintf("(%s IS NULL OR %s)", expr, day), nil
		}
		return day, nil
	case contactql.OpGreaterThan:
		return fmt.Sprintf("%s >= %s", expr, c.param(end)), nil
	case contactql.OpGreaterThanOrEqual:
		return fmt.Sprintf("%s >= %s", expr, c.param(start)), nil
	case contactql.OpLessThan:
		return fmt.Sprintf("%s < %s", expr, c.param(start)), nil
	case contactql.OpLessThanOrEqual:
		return fmt.Sprintf("%s < %s", expr, c.param(end)), nil
	}

	return "", unsupportedOperator(cond)
}

// compare converts a comparison of the given expression with the given (already bound) value. If the expression can
// be null, a != condition also matches nulls, as Elastic's NOT term matches documents without the field.
func (c *SQLConverter) compare(cond *contactql.Condition, expr string, value string, nullable bool) (string, error) {
	switch cond.Operator() {
	case contactql.OpEqual:
		return fmt.Sprintf("%s = %s", expr, value), nil
	case contactql.OpNotEqual:
		if nullable {
			return fmt.Sprintf("%s IS DISTINCT FROM %s", expr, value), nil
		}
		return fmt.Sprintf("%s != %s", expr, value), nil
	case contactql.OpGreaterThan:
		return fmt.Sprintf("%s > %s", expr, value), nil
	case contactql.OpGreaterThanOrEqual:
		return fmt.Sprintf("%s >= %s", expr, value), nil
	case contactql.OpLessThan:
		return fmt.Sprintf("%s < %s", expr, value), nil
	case contactql.OpLessThanOrEqual:
		return fmt.Sprintf("%s <= %s", expr, value), nil
	}

	return "", unsupportedOperator(cond)
}

// exists converts an = or != condition expressed as an EXISTS subquery
func (c *SQLConverter) exists(cond *contactql.Condition, subquery string) (string, error) {
	switch cond.Operator() {
	case contactql.OpEqual:
		return subquery, nil
	case contactql.OpNotEqual:
		return "NOT " + subquery, nil
	}

	return "", unsupportedOperator(cond)
}

// Sort converts a sort string to an ORDER BY clause fragment. As with Elastic, the default is most recent first by
// id, a leading - reverses the direction and contacts without a value always sort last.
func (c *SQLConverter) Sort(sortBy string, resolver contactql.Resolver) (string, error) {
	if sortBy == "" {
		return "c.id DESC", nil
	}

	property := sortBy
	direction := "ASC"
	if strings.HasPrefix(sortBy, "-") {
		property = sortBy[1:]
		direction = "DESC"
	}

	property = strings.ToLower(property)

	var expr string

	switch property {
	case contactql.AttributeID:
		return "c.id " + direction, nil
	case contactql.AttributeName:
		expr = "lower(c.name)"
	case contactql.AttributeCreatedOn, contactql.AttributeLastSeenOn, contactql.AttributeLanguage:
		expr = "c." + property
	default:
		field := resolver.ResolveField(property)
		if field == nil {
			return "", fmt.Errorf("no such field with key: %s", property)
		}

		raw := fmt.Sprintf("c.fields->%s->>'%s'", c.param(string(field.UUID())), field.Type())

		switch field.Type() {
		case assets.FieldTypeText:
			expr = fmt.Sprintf("lower(%s)", raw)
		case assets.FieldTypeNumber:
			expr = fmt.Sprintf("(%s)::numeric", raw)
		case assets.FieldTypeDatetime:
			expr = fmt.Sprintf("(%s)::timestamptz", raw)
		case assets.FieldTypeState, assets.FieldTypeDistrict, assets.FieldTypeWard:
			expr = fmt.Sprintf("lower(btrim(split_part(%s, '>', -1)))", raw)
		default:
			return "", fmt.Errorf("unsupported field type: %s", field.Type())
		}
	}

	return fmt.Sprintf("%s %s NULLS LAST, c.id %s", expr, direction, direction), nil
}

// isSetCheck returns whether the condition is a check for the property being set (!= "") or unset (= "")
func isSetCheck(cond *contactql.Condition) bool {
	return (cond.Operator() == contactql.OpEqual || cond.Operator() == contactql.OpNotEqual) && cond.Value() == ""
}

// isNull converts a set/unset check on an expression which is null when unset
func isNull(cond *contactql.Condition, expr string) string {
	if cond.Operator() == contactql.OpEqual {
		return expr + " IS NULL"
	}
	return expr + " IS NOT NULL"
}

func negateIf(negate bool, expr string) string {
	if negate {
		return "NOT " + expr
	}
	return expr
}

func unsupportedOperator(cond *contactql.Condition) error {
	return fmt.Errorf("unsupported operator %s for %s", cond.Operator(), cond.PropertyKey())
}

// nameContainsPattern builds the regex for a contains condition on name. Elastic indexes name with an edge n-gram of
// 2 to 8 characters per token and searches with each query token truncated to 8 characters, so a token matches if it
// is a prefix of any word in the name, and the match succeeds if any token does.
func nameContainsPattern(value string) (string, bool) {
	tokens := make([]string, 0, 2)
	for _, token := range utils.TokenizeStringByUnicodeSeg(strings.ToLower(value)) {
		if len(token) < 2 {
			continue
		}
		if runes := []rune(token); len(runes) > 8 {
			token = string(runes[:8])
		}
		tokens = append(tokens, regexp.QuoteMeta(token))
	}
	if len(tokens) == 0 {
		return "", false
	}

	return `\m(` + strings.Join(tokens, "|") + `)`, true
}

// escapeLike escapes the LIKE wildcards in a value so that it only matches itself
func escapeLike(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

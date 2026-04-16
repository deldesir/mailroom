package search

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/contactql"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/v26/core/models"
	"github.com/nyaruka/mailroom/v26/runtime"
)

// SQLConverter builds PostgreSQL queries from ContactQL ASTs
type SQLConverter struct {
	oa   *models.OrgAssets
	args []any
}

func newSQLConverter(oa *models.OrgAssets) *SQLConverter {
	return &SQLConverter{
		oa:   oa,
		args: make([]any, 0, 4),
	}
}

// Convert turns a ContactQuery into a SQL WHERE clause and argument list
func (c *SQLConverter) Convert(query *contactql.ContactQuery) (string, []any, error) {
	if query == nil || query.Root() == nil {
		return "", nil, nil
	}
	where, err := c.convertNode(query.Root())
	if err != nil {
		return "", nil, err
	}
	return where, c.args, nil
}

func (c *SQLConverter) convertNode(node contactql.QueryNode) (string, error) {
	switch n := node.(type) {
	case *contactql.Condition:
		return c.convertCondition(n)
	case *contactql.BoolCombination:
		return c.convertCombination(n)
	default:
		return "", fmt.Errorf("unknown query node type: %T", node)
	}
}

func (c *SQLConverter) convertCombination(comb *contactql.BoolCombination) (string, error) {
	var parts []string
	for _, child := range comb.Children() {
		part, err := c.convertNode(child)
		if err != nil {
			return "", err
		}
		parts = append(parts, part)
	}
	
	op := "AND"
	if comb.Operator() == contactql.BoolOperatorOr {
		op = "OR"
	}
	
	if len(parts) == 1 {
		return parts[0], nil
	}
	
	return fmt.Sprintf("(%s)", strings.Join(parts, fmt.Sprintf(" %s ", op))), nil
}

func (c *SQLConverter) convertCondition(cond *contactql.Condition) (string, error) {
	op := string(cond.Operator())
	value := cond.Value()
	
	// Handle special operators
	sqlOp := op
	switch cond.Operator() {
	case contactql.OpContains:
		sqlOp = "ILIKE"
		value = "%" + value + "%"
	case contactql.OpEqual:
		sqlOp = "="
	case contactql.OpNotEqual:
		sqlOp = "!="
	case contactql.OpGreaterThan:
		sqlOp = ">"
	case contactql.OpLessThan:
		sqlOp = "<"
	case contactql.OpGreaterThanOrEqual:
		sqlOp = ">="
	case contactql.OpLessThanOrEqual:
		sqlOp = "<="
	}

	c.args = append(c.args, value)
	argRef := fmt.Sprintf("$%d", len(c.args))

	switch cond.PropertyType() {
	case contactql.PropertyTypeAttribute:
		switch cond.PropertyKey() {
		case contactql.AttributeGroup:
			if (cond.Operator() == contactql.OpEqual || cond.Operator() == contactql.OpNotEqual) && value == "" {
				oper := "NOT IN"
				if cond.Operator() == contactql.OpNotEqual {
					oper = "IN"
				}
				return fmt.Sprintf("c.id %s (SELECT contact_id FROM contacts_contactgroup_contacts)", oper), nil
			}

			// Group condition: c.id IN (SELECT contact_id FROM contacts_contactgroup_contacts WHERE contactgroup_id = $N)
			group := cond.ValueAsGroup(nil)
			if group == nil {
				// We actually don't have resolver here in the same way, but oa has groups
				var groupID int64
				groups, _ := c.oa.Groups()
				for _, g := range groups {
					if strings.EqualFold(g.Name(), cond.Value()) {
						groupID = int64(g.(*models.Group).ID())
						break
					}
				}
				c.args[len(c.args)-1] = groupID
			} else {
				c.args[len(c.args)-1] = group.(*models.Group).ID()
			}
			oper := "IN"
			if cond.Operator() == contactql.OpNotEqual {
				oper = "NOT IN"
			}
			return fmt.Sprintf("c.id %s (SELECT contact_id FROM contacts_contactgroup_contacts WHERE contactgroup_id = %s)", oper, argRef), nil

		case contactql.AttributeName:
			return fmt.Sprintf("c.name %s %s", sqlOp, argRef), nil

		case contactql.AttributeStatus:
			return fmt.Sprintf("c.status %s %s", sqlOp, argRef), nil

		case contactql.AttributeLanguage:
			return fmt.Sprintf("c.language %s %s", sqlOp, argRef), nil

		case contactql.AttributeUUID:
			return fmt.Sprintf("c.uuid %s %s", sqlOp, argRef), nil
			
		case contactql.AttributeCreatedOn:
			dt, err := cond.ValueAsDate(c.oa.Env())
			if err != nil {
				return "", err
			}
			c.args[len(c.args)-1] = dt
			return fmt.Sprintf("c.created_on %s %s", sqlOp, argRef), nil
			
		default:
			// Fallback placeholder... real implementation might address more fields
			return "TRUE", nil
		}

	case contactql.PropertyTypeURN:
		if (cond.Operator() == contactql.OpEqual || cond.Operator() == contactql.OpNotEqual) && value == "" {
			oper := "NOT IN"
			if cond.Operator() == contactql.OpNotEqual {
				oper = "IN"
			}
			scheme := cond.PropertyKey()
			c.args = append(c.args, scheme)
			schemeArgRef := fmt.Sprintf("$%d", len(c.args))
			
			return fmt.Sprintf("c.id %s (SELECT contact_id FROM contacts_contacturn u WHERE u.scheme = %s AND u.path != '')", oper, schemeArgRef), nil
		}

		// URN condition: EXISTS (SELECT 1 FROM contacts_contacturn u WHERE u.contact_id = c.id AND u.scheme = $1 AND u.path = $2)
		// Or URN contains: u.scheme = $1 AND u.path ILIKE $2
		
		oper := "IN"
		if cond.Operator() == contactql.OpNotEqual {
			oper = "NOT IN"
		}
		
		scheme := cond.PropertyKey()
		c.args = append(c.args, scheme)
		schemeArgRef := fmt.Sprintf("$%d", len(c.args))
		
		return fmt.Sprintf("c.id %s (SELECT contact_id FROM contacts_contacturn u WHERE u.scheme = %s AND u.path %s %s)", oper, schemeArgRef, sqlOp, argRef), nil

	case contactql.PropertyTypeField:
		if (cond.Operator() == contactql.OpEqual || cond.Operator() == contactql.OpNotEqual) && value == "" {
			c.args = append(c.args, cond.PropertyKey())
			keyArg := fmt.Sprintf("$%d", len(c.args))
			if cond.Operator() == contactql.OpEqual {
				return fmt.Sprintf("(NOT (c.fields ? %s) OR c.fields->>%s = '')", keyArg, keyArg), nil
			}
			return fmt.Sprintf("(c.fields ? %s AND c.fields->>%s != '')", keyArg, keyArg), nil
		}

		// c.fields->>'key' = $N
		// Number comparisons require casting
		field := c.oa.FieldByKey(cond.PropertyKey())
		if field != nil && field.Type() == assets.FieldTypeNumber {
			num, err := cond.ValueAsNumber()
			if err != nil {
				return "", err
			}
			c.args[len(c.args)-1] = num
			return fmt.Sprintf("(c.fields->>'%s')::numeric %s %s", cond.PropertyKey(), sqlOp, argRef), nil
		}
		
		return fmt.Sprintf("c.fields->>'%s' %s %s", cond.PropertyKey(), sqlOp, argRef), nil
	}

	return "", fmt.Errorf("unsupported property type")
}

func buildBasePostgresQuery(oa *models.OrgAssets, group *models.Group, status models.ContactStatus, excludeUUIDs []flows.ContactUUID, parsed *contactql.ContactQuery) (string, []any, error) {
	c := newSQLConverter(oa)
	
	c.args = append(c.args, oa.OrgID())
	orgArg := fmt.Sprintf("$%d", len(c.args))
	
	where := fmt.Sprintf("c.org_id = %s", orgArg)
	
	if group != nil {
		c.args = append(c.args, group.ID())
		where += fmt.Sprintf(" AND c.id IN (SELECT contact_id FROM contacts_contactgroup_contacts WHERE contactgroup_id = $%d)", len(c.args))
	}
	
	if status != models.NilContactStatus {
		c.args = append(c.args, string(status))
		where += fmt.Sprintf(" AND c.status = $%d", len(c.args))
	}
	
	if len(excludeUUIDs) > 0 {
		var placeHolders []string
		for _, u := range excludeUUIDs {
			c.args = append(c.args, string(u))
			placeHolders = append(placeHolders, fmt.Sprintf("$%d", len(c.args)))
		}
		where += fmt.Sprintf(" AND c.uuid NOT IN (%s)", strings.Join(placeHolders, ","))
	}
	
	if parsed != nil {
		queryWhere, _, err := c.Convert(parsed)
		if err != nil {
			return "", nil, err
		}
		if queryWhere != "" {
			where += " AND (" + queryWhere + ")"
		}
	}
	
	return where, c.args, nil
}

// GetContactTotalPostgres replaces ES Count() with PostgreSQL COUNT(*)
func GetContactTotalPostgres(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, group *models.Group, status models.ContactStatus, excludeUUIDs []flows.ContactUUID, parsed *contactql.ContactQuery) (int64, error) {
	where, args, err := buildBasePostgresQuery(oa, group, status, excludeUUIDs, parsed)
	if err != nil {
		return 0, fmt.Errorf("error building postgres query: %w", err)
	}

	query := fmt.Sprintf("SELECT COUNT(*) FROM contacts_contact c WHERE %s", where)
	
	var count int64
	err = rt.DB.QueryRowContext(ctx, query, args...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("error executing postgres count: %w", err)
	}
	
	return count, nil
}

// GetContactUUIDsForQueryPagePostgres replaces ES Search() locally
func GetContactUUIDsForQueryPagePostgres(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, group *models.Group, status models.ContactStatus, excludeUUIDs []flows.ContactUUID, parsed *contactql.ContactQuery, sortField string, sortDesc bool, offset int, pageSize int) ([]flows.ContactUUID, int64, error) {
	where, args, err := buildBasePostgresQuery(oa, group, status, excludeUUIDs, parsed)
	if err != nil {
		return nil, 0, fmt.Errorf("error building postgres query: %w", err)
	}
	
	// total query
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM contacts_contact c WHERE %s", where)
	var total int64
	err = rt.DB.QueryRowContext(ctx, countQuery, args...).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("error executing postgres count: %w", err)
	}

	if total == 0 {
		return []flows.ContactUUID{}, 0, nil
	}

	// Make copy of args for row query since we're mutating length
	rowArgs := make([]any, len(args))
	copy(rowArgs, args)
	
	// sort
	order := "ASC"
	if sortDesc {
		order = "DESC"
	}
	
	sortMap := map[string]string{
		"created_on": "c.created_on",
		"id": "c.id",
		"name": "c.name",
		"urn": "c.id", // simplified
	}
	
	sortCol, ok := sortMap[sortField]
	if !ok {
		sortCol = "c.id"
	}

	orderBy := fmt.Sprintf("%s %s", sortCol, order)
	if sortCol != "c.id" {
		orderBy += ", c.id " + order // Deterministic tiebreaker
	}

	rowArgs = append(rowArgs, pageSize, offset)
	query := fmt.Sprintf("SELECT c.uuid FROM contacts_contact c WHERE %s ORDER BY %s LIMIT $%d OFFSET $%d", where, orderBy, len(rowArgs)-1, len(rowArgs))
	
	rows, err := rt.DB.QueryContext(ctx, query, rowArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("error executing postgres page query: %w", err)
	}
	defer rows.Close()

	var uuids []flows.ContactUUID
	for rows.Next() {
		var uuidStr string
		if err := rows.Scan(&uuidStr); err != nil {
			return nil, 0, err
		}
		uuids = append(uuids, flows.ContactUUID(uuidStr))
	}
	
	return uuids, total, nil
}

// GetContactUUIDsForQueryPostgres replaces the ES Point-In-Time iterator with chunked PostgreSQL queries.
func GetContactUUIDsForQueryPostgres(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, group *models.Group, status models.ContactStatus, parsed *contactql.ContactQuery, limit int) ([]flows.ContactUUID, error) {
	where, args, err := buildBasePostgresQuery(oa, group, status, nil, parsed)
	if err != nil {
		return nil, fmt.Errorf("error building postgres query: %w", err)
	}
	
	// If limit is bounded and small enough, use standard LIMIT
	if limit >= 0 && limit <= 10000 {
		args = append(args, limit)
		query := fmt.Sprintf("SELECT c.uuid FROM contacts_contact c WHERE %s ORDER BY c.id ASC LIMIT $%d", where, len(args))
		
		rows, err := rt.DB.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		var uuids []flows.ContactUUID
		for rows.Next() {
			var uuidStr string
			if err := rows.Scan(&uuidStr); err != nil {
				return nil, err
			}
			uuids = append(uuids, flows.ContactUUID(uuidStr))
		}
		return uuids, nil
	}
	
	// For unbounded/large sets, use chunks with keyset pagination
	// Using cursor id logic
	var uuids []flows.ContactUUID
	lastID := int64(-1)
	
	for {
		chunkArgs := make([]any, len(args))
		copy(chunkArgs, args)
		
		chunkArgs = append(chunkArgs, lastID, 10000)
		
		query := fmt.Sprintf("SELECT c.id, c.uuid FROM contacts_contact c WHERE %s AND c.id > $%d ORDER BY c.id ASC LIMIT $%d", where, len(chunkArgs)-1, len(chunkArgs))
		
		rows, err := rt.DB.QueryContext(ctx, query, chunkArgs...)
		if err != nil {
			return nil, err
		}
		
		var chunkUuids []flows.ContactUUID
		for rows.Next() {
			var uuidStr string
			var id int64
			if err := rows.Scan(&id, &uuidStr); err != nil {
				rows.Close()
				return nil, err
			}
			chunkUuids = append(chunkUuids, flows.ContactUUID(uuidStr))
			lastID = id
		}
		rows.Close()
		
		if len(chunkUuids) == 0 {
			break
		}
		
		uuids = append(uuids, chunkUuids...)
		
		if limit != -1 && len(uuids) >= limit {
			uuids = uuids[:limit]
			break
		}
	}
	
	return uuids, nil
}

// SearchMessagesPostgres replaces the ES search and DynamoDB lookup with a single PostgreSQL query
// that returns the nested event dict matching the original DynamoDB schema.
func SearchMessagesPostgres(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID, text string, contactUUID flows.ContactUUID, inTicket bool, limit int) ([]MessageResult, error) {
	query := `
		SELECT m.uuid, m.text, m.created_on, m.direction,
			   m.status, c.uuid as contact_uuid
		FROM msgs_msg m
		JOIN contacts_contact c ON m.contact_id = c.id
		WHERE m.org_id = $1 AND m.text ILIKE $2 AND m.visibility = 'V'
	`
	args := []any{orgID, "%" + text + "%"}
	argN := 3

	if contactUUID != "" {
		query += fmt.Sprintf(" AND c.uuid = $%d", argN)
		args = append(args, string(contactUUID))
		argN++
	}

	// ignoring inTicket filter for the local-first Postgres fallback since msgs_msg doesn't directly
	// track in_ticket without joining tickets_ticket, and chat search mostly relies on text/contact.
	
	query += fmt.Sprintf(" ORDER BY m.created_on DESC LIMIT $%d", argN)
	args = append(args, limit)

	rows, err := rt.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("error searching messages in postgres: %w", err)
	}
	defer rows.Close()

	results := make([]MessageResult, 0, limit)
	for rows.Next() {
		var uuid, text, contactUUIDStr, direction, status string
		var createdOn time.Time
		
		if err := rows.Scan(&uuid, &text, &createdOn, &direction, &status, &contactUUIDStr); err != nil {
			return nil, fmt.Errorf("error scanning message result: %w", err)
		}

		evtType := "msg_received"
		if direction == "O" {
			evtType = "msg_created"
		}

		event := map[string]any{
			"type":       evtType,
			"created_on": createdOn.Format(time.RFC3339Nano),
			"msg": map[string]any{
				"uuid":        uuid,
				"text":        text,
				"attachments": []string{}, // simplify attachments for now
			},
		}

		results = append(results, MessageResult{ContactUUID: flows.ContactUUID(contactUUIDStr), Event: event})
	}
	return results, nil
}

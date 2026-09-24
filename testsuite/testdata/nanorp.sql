-- What the nanorp_indexes management command adds to a database whose searches run in Postgres rather than
-- Elasticsearch: a trigram index for name matching and a full text column for message search. Applied on top of
-- the upstream dump when the test suite runs with MAILROOM_TEST_NANORP set - see dbtemplate.go.
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS contacts_contact_name_trgm_idx ON contacts_contact USING gin (name gin_trgm_ops);
ALTER TABLE msgs_msg ADD COLUMN IF NOT EXISTS text_search tsvector GENERATED ALWAYS AS (to_tsvector('simple', text)) STORED;
CREATE INDEX IF NOT EXISTS msgs_msg_text_search_idx ON msgs_msg USING gin (text_search);

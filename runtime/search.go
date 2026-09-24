package runtime

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/nyaruka/gocommon/elastic"
)

type Elastic struct {
	Client *elasticsearch.TypedClient
	Writer *elastic.Writer
	Spool  *elastic.Spool
}

// newElastic creates the Elastic client, writer and spool, or a disabled instance if no endpoint is configured
func newElastic(cfg *Config) (*Elastic, error) {
	if cfg.ElasticEndpoint == "" {
		slog.Info("elasticsearch is off, searching in postgres")
		return &Elastic{}, nil
	}

	client, err := elastic.NewClient(cfg.ElasticEndpoint, cfg.ElasticUsername, cfg.ElasticPassword)
	if err != nil {
		return nil, fmt.Errorf("error creating Elasticsearch client: %w", err)
	}

	spool := elastic.NewSpool(client, filepath.Join(cfg.SpoolDir, "elastic"), 30*time.Second)

	return &Elastic{
		Client: client,
		Writer: elastic.NewWriter(client, 500, 250*time.Millisecond, 1000, spool),
		Spool:  spool,
	}, nil
}

// Enabled returns whether Elasticsearch is configured. When it isn't, nothing is indexed and contact and message
// searches run against Postgres instead.
func (s *Elastic) Enabled() bool {
	return s != nil && s.Client != nil
}

func (s *Elastic) start() error {
	if !s.Enabled() {
		return nil
	}

	if err := s.Spool.Start(); err != nil {
		return fmt.Errorf("error starting elastic spool: %w", err)
	}

	s.Writer.Start()
	return nil
}

func (s *Elastic) stop() {
	if !s.Enabled() {
		return
	}

	s.Writer.Stop()
	s.Spool.Stop()
}

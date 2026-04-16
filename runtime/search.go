package runtime

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/nyaruka/gocommon/elastic"
)

type Elastic struct {
	Client *elasticsearch.TypedClient
	Writer *elastic.Writer
	Spool  *elastic.Spool
}

func newElastic(cfg *Config) (*Elastic, error) {
	if cfg.Elastic == "" {
		slog.Info("Elasticsearch disabled (MAILROOM_ELASTIC is empty)")
		return &Elastic{
			Client: nil, // Explicitly nil — read functions check isNanorpMode()
			Writer: nil, // No writer needed without ES
			Spool:  nil, // No spool needed without ES
		}, nil
	}

	client, err := elastic.NewClient(cfg.Elastic, cfg.ElasticUsername, cfg.ElasticPassword)
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

func (s *Elastic) start() error {
	if s.Spool != nil {
		if err := s.Spool.Start(); err != nil {
			return fmt.Errorf("error starting elastic spool: %w", err)
		}
	}

	if s.Writer != nil {
		s.Writer.Start()
	}
	return nil
}

func (s *Elastic) stop() {
	if s.Writer != nil {
		s.Writer.Stop()
	}
	if s.Spool != nil {
		s.Spool.Stop()
	}
}

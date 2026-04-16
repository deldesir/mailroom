package runtime

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/nyaruka/gocommon/aws/dynamo"
)

// DynamoWriter is the interface for DynamoDB writers (real or no-op).
type DynamoWriter interface {
	Queue(i dynamo.ItemMarshaler) (int, error)
	Start()
	Stop()
}

type Dynamo struct {
	Main    DynamoWriter
	History DynamoWriter
	Spool   *dynamo.Spool
	client  *dynamodb.Client
}

func newDynamo(cfg *Config) (*Dynamo, error) {
	if cfg.DynamoTablePrefix == "" {
		slog.Info("DynamoDB disabled (MAILROOM_DYNAMO_TABLE_PREFIX is empty)")
		return &Dynamo{
			Main:    &NopWriter{},
			History: &NopWriter{},
		}, nil
	}

	client, err := dynamo.NewClient(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, cfg.AWSRegion, cfg.DynamoEndpoint)
	if err != nil {
		return nil, fmt.Errorf("error creating DynamoDB client: %w", err)
	}

	spool := dynamo.NewSpool(client, filepath.Join(cfg.SpoolDir, "dynamo"), 30*time.Second)

	return &Dynamo{
		Main:    dynamo.NewWriter(client, cfg.DynamoTablePrefix+"Main", 250*time.Millisecond, 1000, spool),
		History: dynamo.NewWriter(client, cfg.DynamoTablePrefix+"History", 250*time.Millisecond, 1000, spool),
		Spool:   spool,
		client:  client,
	}, nil
}

// Client returns the underlying DynamoDB client, or nil if disabled.
func (d *Dynamo) Client() *dynamodb.Client {
	return d.client
}

func (d *Dynamo) start() error {
	if d.Spool != nil {
		if err := d.Spool.Start(); err != nil {
			return fmt.Errorf("error starting dynamo spool: %w", err)
		}
	}

	d.Main.Start()
	d.History.Start()
	return nil
}

func (d *Dynamo) stop() {
	d.Main.Stop()
	d.History.Stop()
	if d.Spool != nil {
		d.Spool.Stop()
	}
}

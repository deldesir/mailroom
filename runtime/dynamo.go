package runtime

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/nyaruka/gocommon/aws/dynamo"
)

type Dynamo struct {
	Main    *dynamo.Writer
	History *dynamo.Writer
	Spool   *dynamo.Spool
}

func newDynamo(cfg *Config) (*Dynamo, error) {
	// nanoRP hard default: Dynamo — and therefore AWS — is OFF unless a
	// DynamoTablePrefix is explicitly configured. With no prefix we skip client
	// creation entirely, so the AWS SDK never resolves credentials (no IMDS
	// probe, no AWS dependency). The returned struct is "disabled": all callers
	// gate Dynamo work behind Enabled().
	if cfg.DynamoTablePrefix == "" {
		return &Dynamo{}, nil
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
	}, nil
}

// Enabled reports whether Dynamo is configured. nanoRP runs with it off, in
// which case all writes/reads are skipped and no AWS client is ever created.
func (d *Dynamo) Enabled() bool {
	return d != nil && d.Main != nil
}

func (d *Dynamo) start() error {
	if !d.Enabled() {
		return nil
	}
	if err := d.Spool.Start(); err != nil {
		return fmt.Errorf("error starting dynamo spool: %w", err)
	}

	d.Main.Start()
	d.History.Start()
	return nil
}

func (d *Dynamo) stop() {
	if !d.Enabled() {
		return
	}
	d.Main.Stop()
	d.History.Stop()
	d.Spool.Stop()
}

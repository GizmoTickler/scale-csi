package truenas

import (
	"context"
	"fmt"
	"strings"
)

// ZFSPropertyChoices is the backend's own accepted-value list for the tunable
// ZFS properties the driver curates. Validating against these turns a typo into
// an InvalidArgument at CreateVolume instead of an opaque pool.dataset.create
// rejection halfway through provisioning.
type ZFSPropertyChoices struct {
	Recordsize  []string
	Compression []string
	Checksum    []string
}

// Allows reports whether value (case-insensitively) appears in choices. An empty
// choice list means the backend did not answer, and the caller must NOT treat
// that as "unsupported" — see zfsChoiceSetAllows in the driver.
func (c *ZFSPropertyChoices) has(choices []string, value string) bool {
	want := strings.ToUpper(strings.TrimSpace(value))
	for _, choice := range choices {
		if strings.ToUpper(strings.TrimSpace(choice)) == want {
			return true
		}
	}
	return false
}

// AllowsRecordsize / AllowsCompression / AllowsChecksum return (allowed, known).
// known=false means the backend did not report that choice list at all.
func (c *ZFSPropertyChoices) AllowsRecordsize(value string) (allowed, known bool) {
	if c == nil || len(c.Recordsize) == 0 {
		return true, false
	}
	return c.has(c.Recordsize, value), true
}

func (c *ZFSPropertyChoices) AllowsCompression(value string) (allowed, known bool) {
	if c == nil || len(c.Compression) == 0 {
		return true, false
	}
	return c.has(c.Compression, value), true
}

func (c *ZFSPropertyChoices) AllowsChecksum(value string) (allowed, known bool) {
	if c == nil || len(c.Checksum) == 0 {
		return true, false
	}
	return c.has(c.Checksum, value), true
}

// ZFSPropertyChoices reads the live recordsize/compression/checksum choice
// lists. It is called at most once per controller lifetime, and only when a
// StorageClass actually asks for a curated performance class.
func (c *Client) ZFSPropertyChoices(ctx context.Context) (*ZFSPropertyChoices, error) {
	choices := &ZFSPropertyChoices{}
	for _, source := range []struct {
		method string
		target *[]string
	}{
		{"pool.dataset.recordsize_choices", &choices.Recordsize},
		{"pool.dataset.compression_choices", &choices.Compression},
		{"pool.dataset.checksum_choices", &choices.Checksum},
	} {
		result, err := c.Call(ctx, source.method)
		if err != nil {
			return nil, fmt.Errorf("failed to read %s: %w", source.method, err)
		}
		values, parseErr := parseChoiceList(result)
		if parseErr != nil {
			return nil, fmt.Errorf("%s: %w", source.method, parseErr)
		}
		*source.target = values
	}
	return choices, nil
}

// RecommendedZvolBlocksize returns the backend's recommended volblocksize for a
// pool (e.g. "16K" on an all-flash pool).
func (c *Client) RecommendedZvolBlocksize(ctx context.Context, pool string) (string, error) {
	result, err := c.Call(ctx, "pool.dataset.recommended_zvol_blocksize", pool)
	if err != nil {
		return "", fmt.Errorf("failed to read recommended zvol blocksize for %s: %w", pool, err)
	}
	value, ok := result.(string)
	if !ok {
		return "", fmt.Errorf("unexpected recommended_zvol_blocksize response type %T", result)
	}
	return value, nil
}

// PoolHasSpecialVdev reports whether a pool has a `special` allocation-class
// vdev. `special_small_block_size` is meaningless (and potentially harmful to
// set) without one, so the driver validates it before emitting the property.
func (c *Client) PoolHasSpecialVdev(ctx context.Context, pool string) (bool, error) {
	filters := [][]interface{}{{"name", "=", pool}}
	result, err := c.Call(ctx, "pool.query", filters, map[string]interface{}{})
	if err != nil {
		return false, fmt.Errorf("failed to query pool %s: %w", pool, err)
	}
	pools, ok := result.([]interface{})
	if !ok || len(pools) == 0 {
		return false, fmt.Errorf("pool %s not found", pool)
	}
	entry, ok := pools[0].(map[string]interface{})
	if !ok {
		return false, fmt.Errorf("unexpected pool.query entry type %T", pools[0])
	}
	topology, ok := entry["topology"].(map[string]interface{})
	if !ok {
		return false, nil
	}
	special, ok := topology["special"].([]interface{})
	return ok && len(special) > 0, nil
}

// parseChoiceList accepts both response shapes TrueNAS uses for *_choices: a
// plain array of strings, or an object whose KEYS are the accepted values.
func parseChoiceList(result interface{}) ([]string, error) {
	switch value := result.(type) {
	case []interface{}:
		values := make([]string, 0, len(value))
		for _, item := range value {
			if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
				values = append(values, s)
			}
		}
		return values, nil
	case map[string]interface{}:
		values := make([]string, 0, len(value))
		for key := range value {
			if strings.TrimSpace(key) != "" {
				values = append(values, key)
			}
		}
		return values, nil
	default:
		return nil, fmt.Errorf("unexpected choices response type %T", result)
	}
}

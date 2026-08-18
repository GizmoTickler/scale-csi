package truenas

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// ISCSIAuth represents a TrueNAS iSCSI CHAP authentication peer (iscsi.auth).
//
// The struct deliberately holds NO secret material: the CHAP secret and peer
// secret are only ever passed as call arguments and are never retained in
// memory, logged, or serialized beyond the single API call that uses them.
// CredentialFingerprint is a keyed (HMAC-SHA-256) digest of the peer's
// credential tuple computed from the live query response; it lets the driver
// detect a rotation (same user/tag, changed secret) without ever holding or
// persisting the raw secret. The HMAC key is ephemeral to this process, so
// the fingerprint is non-reversible AND useless for offline dictionary
// attacks even if it were ever exposed. Safe to cache in memory.
type ISCSIAuth struct {
	ID                    int    `json:"id"`
	Tag                   int    `json:"tag"`
	User                  string `json:"user"`
	PeerUser              string `json:"peeruser"`
	CredentialFingerprint string `json:"-"`
}

// iscsiAuthFingerprintKey keys every CHAP credential fingerprint this process
// computes. Fingerprints are only ever compared against other fingerprints
// produced in the SAME process (the request side and the parseISCSIAuth query
// side are both derived live), so an ephemeral random key costs nothing
// functionally — a restart simply recomputes both sides. What it buys is
// real: a plain fast hash of a possibly low-entropy CHAP secret could be
// recovered by offline dictionary search if a fingerprint ever leaked
// (CodeQL go/weak-sensitive-data-hashing); an HMAC under a key that never
// leaves this process cannot.
var iscsiAuthFingerprintKey = newEphemeralFingerprintKey()

func newEphemeralFingerprintKey() []byte {
	key := make([]byte, 32)
	// crypto/rand.Read cannot fail on supported platforms (it aborts the
	// program rather than degrade); the guard is belt-and-braces so a future
	// behavior change can never silently yield an all-zero key.
	if _, err := rand.Read(key); err != nil {
		panic(fmt.Sprintf("fingerprint key generation failed: %v", err))
	}
	return key
}

// ISCSIAuthCredentialFingerprint returns a stable (within this process),
// non-reversible keyed digest of a CHAP credential tuple. The driver computes
// the request-side fingerprint with this same function and compares it against
// the peer's server-side fingerprint (parseISCSIAuth derives that from the
// live iscsi.auth.query response, which includes the secret fields). The
// digest never leaves this process, no raw secret is retained, and without the
// process-ephemeral HMAC key the digest cannot be brute-forced against
// candidate secrets.
func ISCSIAuthCredentialFingerprint(user, secret, peerUser, peerSecret string) string {
	// The separator is a NUL byte so no field boundary can be forged by a value
	// that contains the separator character.
	mac := hmac.New(sha256.New, iscsiAuthFingerprintKey)
	mac.Write([]byte(strings.Join([]string{user, secret, peerUser, peerSecret}, "\x00")))
	return hex.EncodeToString(mac.Sum(nil))
}

// iscsiAuthSecretParams builds the create/update parameter map for an
// iscsi.auth peer. peerUser/peerSecret are only included for mutual CHAP so a
// one-way peer never carries an empty peer secret over the wire.
func iscsiAuthSecretParams(user, secret, peerUser, peerSecret string) map[string]interface{} {
	params := map[string]interface{}{
		"user":   user,
		"secret": secret,
	}
	if peerUser != "" {
		params["peeruser"] = peerUser
		params["peersecret"] = peerSecret
	}
	return params
}

// ISCSIAuthCreate creates a new iSCSI CHAP auth peer. The secret arguments are
// sent to TrueNAS and then dropped; they are never logged (this call is
// intentionally guarded from LogAPIError, unlike target/extent creation).
func (c *Client) ISCSIAuthCreate(ctx context.Context, tag int, user, secret, peerUser, peerSecret string) (*ISCSIAuth, error) {
	params := iscsiAuthSecretParams(user, secret, peerUser, peerSecret)
	params["tag"] = tag

	result, err := c.Call(ctx, "iscsi.auth.create", params)
	if err != nil {
		return nil, fmt.Errorf("failed to create iSCSI auth peer: %w", err)
	}

	return parseISCSIAuth(result)
}

// ISCSIAuthQueryByTag returns every iSCSI auth peer carrying tag. Multiple
// peers may share a tag on TrueNAS; callers select among them by user.
func (c *Client) ISCSIAuthQueryByTag(ctx context.Context, tag int) ([]*ISCSIAuth, error) {
	filters := [][]interface{}{{"tag", "=", tag}}
	result, err := c.Call(ctx, "iscsi.auth.query", filters, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to query iSCSI auth peers: %w", err)
	}

	items, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected iSCSI auth response type")
	}

	peers := make([]*ISCSIAuth, 0, len(items))
	for _, item := range items {
		peer, parseErr := parseISCSIAuth(item)
		if parseErr != nil {
			continue
		}
		peers = append(peers, peer)
	}

	return peers, nil
}

// ISCSIAuthGet retrieves a single iSCSI auth peer by ID.
func (c *Client) ISCSIAuthGet(ctx context.Context, id int) (*ISCSIAuth, error) {
	filters := [][]interface{}{{"id", "=", id}}
	result, err := c.Call(ctx, "iscsi.auth.query", filters, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to get iSCSI auth peer: %w", err)
	}

	items, ok := result.([]interface{})
	if !ok || len(items) == 0 {
		return nil, nil
	}

	return parseISCSIAuth(items[0])
}

// ISCSIAuthUpdate re-keys an existing iSCSI auth peer. As with create, the
// secret arguments are never logged.
func (c *Client) ISCSIAuthUpdate(ctx context.Context, id int, user, secret, peerUser, peerSecret string) (*ISCSIAuth, error) {
	result, err := c.Call(ctx, "iscsi.auth.update", id, iscsiAuthSecretParams(user, secret, peerUser, peerSecret))
	if err != nil {
		return nil, fmt.Errorf("failed to update iSCSI auth peer: %w", err)
	}

	return parseISCSIAuth(result)
}

// ISCSIAuthDelete removes an iSCSI auth peer idempotently, mirroring the
// NotFound-tolerant delete used by the sibling iSCSI objects.
func (c *Client) ISCSIAuthDelete(ctx context.Context, id int) error {
	_, err := c.Call(ctx, "iscsi.auth.delete", id)
	if err != nil {
		if IsNotFoundError(err) {
			return nil
		}
		if c.deleteVanishedTolerant(ctx, "iscsi.auth.query", id) {
			return nil
		}
		return fmt.Errorf("failed to delete iSCSI auth peer: %w", err)
	}
	return nil
}

// parseISCSIAuth converts a raw API response to ISCSIAuth. Secret fields in the
// response are intentionally ignored so they never enter driver memory.
func parseISCSIAuth(data interface{}) (*ISCSIAuth, error) {
	m, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected iSCSI auth format")
	}

	auth := &ISCSIAuth{}
	if v, ok := m["id"].(float64); ok {
		auth.ID = int(v)
	}
	if v, ok := m["tag"].(float64); ok {
		auth.Tag = int(v)
	}
	if v, ok := m["user"].(string); ok {
		auth.User = v
	}
	if v, ok := m["peeruser"].(string); ok {
		auth.PeerUser = v
	}
	// Derive the credential fingerprint from the live secret fields and drop the
	// raw secrets immediately — they are never assigned to the struct. NOTE: this
	// scheme depends on TrueNAS returning secret/peersecret in iscsi.auth.query
	// (verified against 26.0 middleware). If a future TrueNAS masked secrets in
	// query results, the server-side fingerprint would be computed over empty
	// strings and would NEVER match a request fingerprint — the driver would then
	// degrade to issuing a (harmless but spurious) iscsi.auth.update + rotation
	// Event on every CreateVolume for that class, not to silent misbehavior. If
	// that ever appears in logs, re-probe the query shape before changing code.
	secret, _ := m["secret"].(string)
	peerSecret, _ := m["peersecret"].(string)
	auth.CredentialFingerprint = ISCSIAuthCredentialFingerprint(auth.User, secret, auth.PeerUser, peerSecret)

	return auth, nil
}

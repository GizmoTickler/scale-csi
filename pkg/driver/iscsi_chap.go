package driver

import (
	"context"
	"hash/fnv"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
	"github.com/GizmoTickler/scale-csi/pkg/util"
)

// CHAP mode strings stamped into target groups and the volume context. They
// match the TrueNAS iscsi.auth authmethod enum and the value the node reads to
// decide which node.session.auth.* parameters to apply.
const (
	iscsiCHAPModeNone   = "NONE"
	iscsiCHAPModeCHAP   = "CHAP"
	iscsiCHAPModeMutual = "CHAP_MUTUAL"
)

// volumeContextCHAPKey is the ONLY CHAP-related volume-context key. It carries
// the mode flag (CHAP or CHAP_MUTUAL) so the node knows to expect a node-stage
// secret and which direction to configure. It holds no credential material;
// volumeContext is persisted in the PV, so secrets must never be written there.
const volumeContextCHAPKey = "chap"

// StorageClass parameter that explicitly opts a class into CHAP. CHAP is also
// implied when a provisioner secret carrying a username is present.
const paramISCSIChAPSecret = "iscsi.chapSecret"

// TrueNAS enforces a 12-16 character inclusive length on CHAP secrets.
const (
	iscsiCHAPSecretMinLen = 12
	iscsiCHAPSecretMaxLen = 16
)

// CHAP secret keys accepted in the per-StorageClass Kubernetes Secret, including
// the legacy open-iscsi-style aliases. The canonical keys are documented in
// docs/reference/storageclass.md.
const (
	chapSecretKeyUsername       = "username"
	chapSecretKeyPassword       = "password"
	chapSecretKeyMutualUsername = "mutualUsername"
	chapSecretKeyMutualPassword = "mutualPassword"
	chapSecretKeyTag            = "tag"

	chapAliasUsername       = "node.session.auth.username"
	chapAliasPassword       = "node.session.auth.password"
	chapAliasMutualUsername = "node.session.auth.username_in"
	chapAliasMutualPassword = "node.session.auth.password_in"
)

// iscsiCHAPSecret is the driver-side parse of a per-StorageClass CHAP Secret.
// It is short-lived: validated, used to ensure the backend peer, and dropped.
type iscsiCHAPSecret struct {
	Username       string
	Password       string
	MutualUsername string
	MutualPassword string
	Tag            int
	HasTag         bool
}

// iscsiCHAPResolution is the result of ensuring a backend CHAP peer: the peer to
// link into target groups and the negotiated mode.
type iscsiCHAPResolution struct {
	Peer   *truenas.ISCSIAuth
	Mutual bool
}

// authMethod returns the TrueNAS authmethod enum for the resolution.
func (r *iscsiCHAPResolution) authMethod() string {
	if r == nil {
		return iscsiCHAPModeNone
	}
	if r.Mutual {
		return iscsiCHAPModeMutual
	}
	return iscsiCHAPModeCHAP
}

// iscsiCHAPContextKey carries the request-scoped CHAP resolution from
// CreateVolume down to the iSCSI share builder without widening the ShareBackend
// interface (which is shared by NFS and NVMe-oF and carries no secrets).
type iscsiCHAPContextKey struct{}

func withISCSIChAPResolution(ctx context.Context, res *iscsiCHAPResolution) context.Context {
	return context.WithValue(ctx, iscsiCHAPContextKey{}, res)
}

func iscsiCHAPResolutionFromContext(ctx context.Context) *iscsiCHAPResolution {
	if res, ok := ctx.Value(iscsiCHAPContextKey{}).(*iscsiCHAPResolution); ok {
		return res
	}
	return nil
}

// firstSecretKey returns the first non-empty value among the given keys.
func firstSecretKey(secrets map[string]string, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(secrets[key]); value != "" {
			return value
		}
	}
	return ""
}

// parseISCSIChAPSecret reads the canonical CHAP keys plus their legacy aliases.
func parseISCSIChAPSecret(secrets map[string]string) iscsiCHAPSecret {
	parsed := iscsiCHAPSecret{
		Username:       firstSecretKey(secrets, chapSecretKeyUsername, chapAliasUsername),
		Password:       firstSecretKey(secrets, chapSecretKeyPassword, chapAliasPassword),
		MutualUsername: firstSecretKey(secrets, chapSecretKeyMutualUsername, chapAliasMutualUsername),
		MutualPassword: firstSecretKey(secrets, chapSecretKeyMutualPassword, chapAliasMutualPassword),
	}
	if rawTag := firstSecretKey(secrets, chapSecretKeyTag); rawTag != "" {
		if tag, err := strconv.Atoi(rawTag); err == nil && tag > 0 {
			parsed.Tag = tag
			parsed.HasTag = true
		}
	}
	return parsed
}

// validateISCSIChAPSecret enforces the TrueNAS CHAP constraints before any API
// call so a bad secret fails fast with InvalidArgument instead of a backend
// rejection (or, worse, a node-side login storm).
func validateISCSIChAPSecret(secret iscsiCHAPSecret) error {
	if secret.Username == "" {
		return status.Error(codes.InvalidArgument, "iSCSI CHAP username is required when CHAP is enabled")
	}
	if err := validateCHAPSecretLength("password", secret.Password); err != nil {
		return err
	}
	if secret.MutualUsername != "" {
		if err := validateCHAPSecretLength("mutualPassword", secret.MutualPassword); err != nil {
			return err
		}
		if secret.MutualPassword == secret.Password {
			return status.Error(codes.InvalidArgument, "iSCSI CHAP mutualPassword must differ from password")
		}
	} else if secret.MutualPassword != "" {
		return status.Error(codes.InvalidArgument, "iSCSI CHAP mutualPassword requires mutualUsername")
	}
	return nil
}

func validateCHAPSecretLength(name, value string) error {
	if value == "" {
		return status.Errorf(codes.InvalidArgument, "iSCSI CHAP %s is required when CHAP is enabled", name)
	}
	if length := len(value); length < iscsiCHAPSecretMinLen || length > iscsiCHAPSecretMaxLen {
		return status.Errorf(codes.InvalidArgument,
			"iSCSI CHAP %s must be %d-%d characters (got %d)", name, iscsiCHAPSecretMinLen, iscsiCHAPSecretMaxLen, length)
	}
	return nil
}

// redactCHAP returns a copy of secrets with every password/secret value masked.
// Use it before logging or embedding any CHAP secret map in an error so
// credentials never reach logs or gRPC status strings.
func redactCHAP(secrets map[string]string) map[string]string {
	if secrets == nil {
		return nil
	}
	redacted := make(map[string]string, len(secrets))
	for key, value := range secrets {
		lower := strings.ToLower(key)
		if strings.Contains(lower, "password") || strings.Contains(lower, "secret") {
			if value != "" {
				redacted[key] = "***"
			}
			continue
		}
		redacted[key] = value
	}
	return redacted
}

// deriveISCSIAuthTag deterministically maps a credential key to an iscsi.auth
// tag in [1000, 61000), avoiding the low operator-reserved range. The key is the
// CHAP username: it is stable per StorageClass credential and unique per
// distinct credential, which is the granularity the tag must be unique at.
func deriveISCSIAuthTag(key string) int {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key))
	return 1000 + int(hasher.Sum32()%60000)
}

// iscsiGroupAuthRef is the single source of truth for the value linked into a
// target group's "auth" field. The TrueNAS SCALE 26.0 schema labels it "ID of
// the authentication credential", so the default is peer.ID. If the live drill
// (design §6.5, gate G1) determines the field is keyed by tag instead, flip the
// returned value to peer.Tag — this is the only site that must change.
func iscsiGroupAuthRef(peer *truenas.ISCSIAuth) int {
	if peer == nil {
		return 0
	}
	return peer.ID
}

// iscsiGroupCHAP resolves the authmethod, auth ref, and auth tag to stamp on a
// freshly built target group. It prefers the request-scoped CreateVolume
// resolution; otherwise it reconstructs the auth ref from the stored dataset
// property so idempotent rebuilds (and fence-adjacent creates) retain CHAP.
// authRef is 0 when CHAP is not active for the dataset; authTag is 0 when only
// the stored ref is known (the tag property is already persisted in that case).
func (d *Driver) iscsiGroupCHAP(ctx context.Context, ds *truenas.Dataset) (authMethod string, authRef, authTag int) {
	if res := iscsiCHAPResolutionFromContext(ctx); res != nil {
		return res.authMethod(), iscsiGroupAuthRef(res.Peer), res.Peer.Tag
	}
	if rawID := datasetUserProperty(ds, PropISCSIAuthID); rawID != "" && rawID != "-" {
		if id, err := strconv.Atoi(rawID); err == nil && id > 0 {
			return d.iscsiCHAPAuthMethod(), id, 0
		}
	}
	return iscsiCHAPModeNone, 0, 0
}

// applyISCSIGroupCHAP stamps authmethod+auth onto a target group when CHAP is
// active (authRef > 0). It is a no-op for non-CHAP datasets so the historical
// authmethod=NONE groups are emitted unchanged.
func applyISCSIGroupCHAP(group *truenas.ISCSITargetGroup, authMethod string, authRef int) {
	if authRef <= 0 || authMethod == iscsiCHAPModeNone {
		return
	}
	ref := authRef
	group.AuthMethod = authMethod
	group.Auth = &ref
}

// iscsiCHAPAuthMethod returns the authmethod enum for the controller's global
// CHAP posture. It is used when rebuilding groups from a stored dataset property
// (fence/idempotent paths) where only the auth ref, not the original mode, is
// persisted.
func (d *Driver) iscsiCHAPAuthMethod() string {
	if d.config.ISCSI.CHAP.Mutual {
		return iscsiCHAPModeMutual
	}
	return iscsiCHAPModeCHAP
}

// chapEnabledForCreate reports whether a CreateVolume request opts into CHAP:
// the controller-wide feature must be on, and either the StorageClass sets
// iscsi.chapSecret=true or the provisioner secret carries a username.
func (d *Driver) chapEnabledForCreate(params, secrets map[string]string) bool {
	if !d.config.ISCSI.CHAP.Enabled {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(params[paramISCSIChAPSecret]), "true") {
		return true
	}
	return firstSecretKey(secrets, chapSecretKeyUsername, chapAliasUsername) != ""
}

// EnsureISCSIAuthPeer validates the CHAP secret and returns the shared backend
// auth peer for the StorageClass credential, creating it on first use and
// adopting it by tag thereafter (idempotent across controller restarts). The
// peer is cached per tag so steady-state provisioning adds zero TrueNAS RTT.
func (d *Driver) EnsureISCSIAuthPeer(ctx context.Context, secrets map[string]string) (*iscsiCHAPResolution, error) {
	secret := parseISCSIChAPSecret(secrets)
	if err := validateISCSIChAPSecret(secret); err != nil {
		return nil, err
	}

	tag := secret.Tag
	if !secret.HasTag {
		if configTag := d.config.ISCSI.CHAP.Tag; configTag > 0 {
			tag = configTag
		} else {
			tag = deriveISCSIAuthTag(secret.Username)
		}
	}

	d.iscsiAuthMu.Lock()
	defer d.iscsiAuthMu.Unlock()

	if d.iscsiResolvedAuth == nil {
		d.iscsiResolvedAuth = make(map[int]*truenas.ISCSIAuth)
	}
	if peer, ok := d.iscsiResolvedAuth[tag]; ok {
		return &iscsiCHAPResolution{Peer: peer, Mutual: secret.MutualUsername != ""}, nil
	}

	peers, err := d.truenasClient.ISCSIAuthQueryByTag(ctx, tag)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to query iSCSI auth peers for tag %d: %v", tag, err)
	}
	for _, peer := range peers {
		if peer.User == secret.Username {
			d.iscsiResolvedAuth[tag] = peer
			return &iscsiCHAPResolution{Peer: peer, Mutual: secret.MutualUsername != ""}, nil
		}
	}
	if len(peers) > 0 {
		// A peer already owns this tag under a different username. Never silently
		// overwrite an operator credential; the collision is a configuration error.
		return nil, status.Errorf(codes.FailedPrecondition,
			"iSCSI auth tag %d is already in use by a different username; set an explicit tag in the CHAP secret", tag)
	}

	peer, err := d.truenasClient.ISCSIAuthCreate(ctx, tag, secret.Username, secret.Password, secret.MutualUsername, secret.MutualPassword)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create iSCSI auth peer for tag %d: %v", tag, err)
	}
	klog.Infof("Ensured shared iSCSI CHAP auth peer: tag=%d id=%d user=%q mutual=%v", tag, peer.ID, secret.Username, secret.MutualUsername != "")
	d.iscsiResolvedAuth[tag] = peer
	return &iscsiCHAPResolution{Peer: peer, Mutual: secret.MutualUsername != ""}, nil
}

// nodeISCSIChAPCredentials builds the node-side CHAP credentials from the volume
// context mode flag and the node-stage secret. It returns nil when the volume is
// not CHAP-enabled (mode absent or NONE), so the connect path applies no auth
// params and behaves exactly as before CHAP existed. When CHAP is expected but
// the secret is missing or invalid it fails fast with InvalidArgument rather
// than letting iscsiadm enter a login retry storm. The returned struct is
// short-lived and never logged.
func nodeISCSIChAPCredentials(volumeContext, secrets map[string]string) (*util.ISCSICHAPCredentials, error) {
	mode := volumeContext[volumeContextCHAPKey]
	if mode == "" || mode == iscsiCHAPModeNone {
		return nil, nil
	}
	secret := parseISCSIChAPSecret(secrets)
	if err := validateISCSIChAPSecret(secret); err != nil {
		return nil, err
	}
	creds := &util.ISCSICHAPCredentials{
		Username: secret.Username,
		Password: secret.Password,
	}
	if mode == iscsiCHAPModeMutual {
		if secret.MutualUsername == "" || secret.MutualPassword == "" {
			return nil, status.Error(codes.InvalidArgument,
				"iSCSI CHAP_MUTUAL volume requires mutualUsername and mutualPassword in the node-stage secret")
		}
		creds.Mutual = true
		creds.MutualUsername = secret.MutualUsername
		creds.MutualPassword = secret.MutualPassword
	}
	return creds, nil
}

package truenas

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testCACertificatePEM(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "scale-csi-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

func TestBuildTLSConfig(t *testing.T) {
	caPEM := testCACertificatePEM(t)

	t.Run("inline CA configures explicit roots", func(t *testing.T) {
		cfg, err := buildTLSConfig(&ClientConfig{CACert: caPEM})
		require.NoError(t, err)
		require.NotNil(t, cfg.RootCAs)
		assert.False(t, cfg.InsecureSkipVerify)
	})

	t.Run("CA file configures explicit roots", func(t *testing.T) {
		caFile := filepath.Join(t.TempDir(), "truenas-ca.pem")
		require.NoError(t, os.WriteFile(caFile, []byte(caPEM), 0o600))
		cfg, err := buildTLSConfig(&ClientConfig{CACertFile: caFile})
		require.NoError(t, err)
		require.NotNil(t, cfg.RootCAs)
		assert.False(t, cfg.InsecureSkipVerify)
	})

	t.Run("insecure overrides CA configuration", func(t *testing.T) {
		cfg, err := buildTLSConfig(&ClientConfig{
			AllowInsecure: true,
			CACert:        "not a certificate",
			CACertFile:    filepath.Join(t.TempDir(), "missing.pem"),
		})
		require.NoError(t, err)
		assert.True(t, cfg.InsecureSkipVerify)
		assert.Nil(t, cfg.RootCAs)
	})

	t.Run("system roots remain implicit without explicit CA", func(t *testing.T) {
		cfg, err := buildTLSConfig(&ClientConfig{})
		require.NoError(t, err)
		assert.Nil(t, cfg.RootCAs)
		assert.False(t, cfg.InsecureSkipVerify)
	})
}

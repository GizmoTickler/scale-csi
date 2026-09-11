package main

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

// TestBlkidFlag is the regression test for a rule that reported clean while
// catching nothing. RG-BLKID-NO-P used to bail out on ANY non-literal argument,
// and every real blkid invocation ends in a device-path variable, so it could
// only ever fire on a fully-literal argv that no CSI driver would write.
//
// The `missingDashP` fixture is deliberately the exact shape that ships. It
// fails against the pre-fix analyzer and passes against the current one, which
// is the only thing that distinguishes a working guard from a decorative one.
//
// This package previously had NO tests at all, which is why the dead rule went
// unnoticed through a full lint-hardening review.
func TestBlkidFlag(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), BlkidFlag, "blkidfixture/pkg/util")
}

// TestHardenedExec covers the other rule that has already been wrong once: it
// matched only the package-local `hardenCmd` and reported four false positives
// the moment the helper was exported as `HardenCmd`. Both spellings, and the
// qualified form, must be accepted; an unhardened exec must still fire.
func TestHardenedExec(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), HardenedExec, "hardenedfixture/pkg/util")
}

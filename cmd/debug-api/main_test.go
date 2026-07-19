package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInsecureFlagDefaultsToFalse(t *testing.T) {
	assert.False(t, *insecure)
}

package driver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// MockClientWithCircuitBreaker is a mock client that returns circuit breaker stats.
type MockClientWithCircuitBreaker struct {
	*truenas.MockClient
	cbStats *truenas.CircuitBreakerStats
}

func (m *MockClientWithCircuitBreaker) CircuitBreakerStats() *truenas.CircuitBreakerStats {
	return m.cbStats
}

func (m *MockClientWithCircuitBreaker) IsConnected() bool {
	return true
}

func (m *MockClientWithCircuitBreaker) ResetCircuitBreaker() {}

// TestHealthStatus_CircuitBreakerField tests that CircuitBreakerHealth is properly
// populated in the HealthStatus struct.
func TestHealthStatus_CircuitBreakerField(t *testing.T) {
	t.Run("CircuitBreakerHealth struct fields", func(t *testing.T) {
		cbHealth := &CircuitBreakerHealth{
			State:             "closed",
			CurrentFailures:   2,
			TotalFailures:     10,
			TotalSuccesses:    100,
			TotalCircuitOpens: 3,
		}

		assert.Equal(t, "closed", cbHealth.State)
		assert.Equal(t, 2, cbHealth.CurrentFailures)
		assert.Equal(t, int64(10), cbHealth.TotalFailures)
		assert.Equal(t, int64(100), cbHealth.TotalSuccesses)
		assert.Equal(t, int64(3), cbHealth.TotalCircuitOpens)
	})

	t.Run("HealthStatus with CircuitBreaker", func(t *testing.T) {
		status := HealthStatus{
			Ready:            true,
			TrueNASConnected: true,
			CircuitBreaker: &CircuitBreakerHealth{
				State:             "open",
				CurrentFailures:   5,
				TotalFailures:     50,
				TotalSuccesses:    200,
				TotalCircuitOpens: 10,
			},
		}

		assert.True(t, status.Ready)
		assert.True(t, status.TrueNASConnected)
		assert.NotNil(t, status.CircuitBreaker)
		assert.Equal(t, "open", status.CircuitBreaker.State)
		assert.Equal(t, 5, status.CircuitBreaker.CurrentFailures)
	})

	t.Run("HealthStatus without CircuitBreaker", func(t *testing.T) {
		status := HealthStatus{
			Ready:            true,
			TrueNASConnected: true,
			CircuitBreaker:   nil,
		}

		assert.True(t, status.Ready)
		assert.True(t, status.TrueNASConnected)
		assert.Nil(t, status.CircuitBreaker)
	})
}

// TestHealthStatus_JSONSerialization tests that HealthStatus serializes correctly to JSON.
func TestHealthStatus_JSONSerialization(t *testing.T) {
	t.Run("With CircuitBreaker", func(t *testing.T) {
		status := HealthStatus{
			Ready:             true,
			TrueNASConnected:  true,
			ControllerRunning: true,
			NodeRunning:       false,
			LastTrueNASCheck:  "2024-01-15T10:30:00Z",
			Error:             "",
			CircuitBreaker: &CircuitBreakerHealth{
				State:             "half-open",
				CurrentFailures:   3,
				TotalFailures:     25,
				TotalSuccesses:    150,
				TotalCircuitOpens: 5,
			},
		}

		data, err := json.Marshal(status)
		require.NoError(t, err)

		var decoded HealthStatus
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)

		assert.Equal(t, true, decoded.Ready)
		assert.Equal(t, true, decoded.TrueNASConnected)
		assert.NotNil(t, decoded.CircuitBreaker)
		assert.Equal(t, "half-open", decoded.CircuitBreaker.State)
		assert.Equal(t, 3, decoded.CircuitBreaker.CurrentFailures)
		assert.Equal(t, int64(25), decoded.CircuitBreaker.TotalFailures)
		assert.Equal(t, int64(150), decoded.CircuitBreaker.TotalSuccesses)
		assert.Equal(t, int64(5), decoded.CircuitBreaker.TotalCircuitOpens)
	})

	t.Run("Without CircuitBreaker (nil)", func(t *testing.T) {
		status := HealthStatus{
			Ready:            true,
			TrueNASConnected: true,
			CircuitBreaker:   nil,
		}

		data, err := json.Marshal(status)
		require.NoError(t, err)

		// Verify circuit_breaker is omitted (not present in JSON)
		var rawMap map[string]interface{}
		err = json.Unmarshal(data, &rawMap)
		require.NoError(t, err)

		// The circuit_breaker key should be present but null, or omitted
		// Based on the struct tag `json:"circuit_breaker,omitempty"`, it should be omitted
		_, hasKey := rawMap["circuit_breaker"]
		assert.False(t, hasKey, "circuit_breaker should be omitted when nil")
	})
}

// TestCheckHealth_WithCircuitBreakerStats tests that checkHealth populates
// CircuitBreaker field when stats are available from the client.
func TestCheckHealth_WithCircuitBreakerStats(t *testing.T) {
	mockClient := &MockClientWithCircuitBreaker{
		MockClient: truenas.NewMockClient(),
		cbStats: &truenas.CircuitBreakerStats{
			State:             truenas.CircuitClosed,
			Failures:          1,
			TotalFailures:     5,
			TotalSuccesses:    50,
			TotalCircuitOpens: 2,
		},
	}

	d := &Driver{
		runController: true,
		runNode:       false,
		truenasClient: mockClient,
	}
	d.ready.Store(true)

	h := &HealthServer{
		driver: d,
	}

	// Call checkHealth
	status := h.checkHealth()

	// Verify circuit breaker info is populated
	assert.NotNil(t, status.CircuitBreaker, "CircuitBreaker should be populated")
	assert.Equal(t, "closed", status.CircuitBreaker.State)
	assert.Equal(t, 1, status.CircuitBreaker.CurrentFailures)
	assert.Equal(t, int64(5), status.CircuitBreaker.TotalFailures)
	assert.Equal(t, int64(50), status.CircuitBreaker.TotalSuccesses)
	assert.Equal(t, int64(2), status.CircuitBreaker.TotalCircuitOpens)
}

// TestCheckHealth_WithoutCircuitBreakerStats tests that checkHealth handles
// nil circuit breaker stats gracefully.
func TestCheckHealth_WithoutCircuitBreakerStats(t *testing.T) {
	mockClient := &MockClientWithCircuitBreaker{
		MockClient: truenas.NewMockClient(),
		cbStats:    nil, // No circuit breaker stats
	}

	d := &Driver{
		runController: true,
		runNode:       true,
		truenasClient: mockClient,
	}
	d.ready.Store(true)

	h := &HealthServer{
		driver: d,
	}

	// Call checkHealth
	status := h.checkHealth()

	// Verify circuit breaker info is nil when stats are unavailable
	assert.Nil(t, status.CircuitBreaker, "CircuitBreaker should be nil when stats unavailable")
	assert.True(t, status.Ready)
	assert.True(t, status.TrueNASConnected)
}

// TestCheckHealth_AllCircuitBreakerStates tests all three circuit breaker states.
func TestCheckHealth_AllCircuitBreakerStates(t *testing.T) {
	testCases := []struct {
		name          string
		state         truenas.CircuitState
		expectedState string
	}{
		{
			name:          "Closed state",
			state:         truenas.CircuitClosed,
			expectedState: "closed",
		},
		{
			name:          "Open state",
			state:         truenas.CircuitOpen,
			expectedState: "open",
		},
		{
			name:          "Half-open state",
			state:         truenas.CircuitHalfOpen,
			expectedState: "half-open",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockClient := &MockClientWithCircuitBreaker{
				MockClient: truenas.NewMockClient(),
				cbStats: &truenas.CircuitBreakerStats{
					State:             tc.state,
					Failures:          0,
					TotalFailures:     0,
					TotalSuccesses:    0,
					TotalCircuitOpens: 0,
				},
			}

			d := &Driver{
				truenasClient: mockClient,
			}
			d.ready.Store(true)

			h := &HealthServer{
				driver: d,
			}

			status := h.checkHealth()

			require.NotNil(t, status.CircuitBreaker)
			assert.Equal(t, tc.expectedState, status.CircuitBreaker.State)
		})
	}
}

// TestHealthEndpoint_WithCircuitBreaker tests the /health HTTP endpoint
// returns circuit breaker information in the JSON response.
func TestHealthEndpoint_WithCircuitBreaker(t *testing.T) {
	mockClient := &MockClientWithCircuitBreaker{
		MockClient: truenas.NewMockClient(),
		cbStats: &truenas.CircuitBreakerStats{
			State:             truenas.CircuitOpen,
			Failures:          5,
			TotalFailures:     100,
			TotalSuccesses:    1000,
			TotalCircuitOpens: 20,
		},
	}

	d := &Driver{
		runController: true,
		runNode:       true,
		truenasClient: mockClient,
	}
	d.ready.Store(true)

	h := &HealthServer{
		driver:    d,
		lastCheck: time.Time{}, // Force cache miss
	}

	// Create test request
	req := httptest.NewRequest(http.MethodGet, "/health", http.NoBody)
	w := httptest.NewRecorder()

	// Call handler
	h.handleHealth(w, req)

	// Check response
	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	var status HealthStatus
	err := json.NewDecoder(resp.Body).Decode(&status)
	require.NoError(t, err)

	// Verify circuit breaker is in the response
	require.NotNil(t, status.CircuitBreaker, "CircuitBreaker should be in response")
	assert.Equal(t, "open", status.CircuitBreaker.State)
	assert.Equal(t, 5, status.CircuitBreaker.CurrentFailures)
	assert.Equal(t, int64(100), status.CircuitBreaker.TotalFailures)
	assert.Equal(t, int64(1000), status.CircuitBreaker.TotalSuccesses)
	assert.Equal(t, int64(20), status.CircuitBreaker.TotalCircuitOpens)
}

// TestHealthEndpoint_ServiceUnavailable tests that the health endpoint
// returns 503 when TrueNAS is not connected, but still includes circuit breaker info.
func TestHealthEndpoint_ServiceUnavailable(t *testing.T) {
	// Create a mock that returns connected=false
	mockClient := &MockClientWithCircuitBreaker{
		MockClient: truenas.NewMockClient(),
		cbStats: &truenas.CircuitBreakerStats{
			State:             truenas.CircuitOpen,
			Failures:          5,
			TotalFailures:     50,
			TotalSuccesses:    100,
			TotalCircuitOpens: 10,
		},
	}

	d := &Driver{
		truenasClient: mockClient,
	}
	d.ready.Store(false) // Not ready

	h := &HealthServer{
		driver:    d,
		lastCheck: time.Time{}, // Force cache miss
	}

	req := httptest.NewRequest(http.MethodGet, "/health", http.NoBody)
	w := httptest.NewRecorder()

	h.handleHealth(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	// Should be 503 because not ready
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	// But should still have circuit breaker info in response
	var status HealthStatus
	err := json.NewDecoder(resp.Body).Decode(&status)
	require.NoError(t, err)

	assert.False(t, status.Ready)
	require.NotNil(t, status.CircuitBreaker)
	assert.Equal(t, "open", status.CircuitBreaker.State)
}

// TestCircuitBreakerHealth_ZeroValues tests that zero values are handled correctly.
func TestCircuitBreakerHealth_ZeroValues(t *testing.T) {
	cbHealth := &CircuitBreakerHealth{
		State:             "closed",
		CurrentFailures:   0,
		TotalFailures:     0,
		TotalSuccesses:    0,
		TotalCircuitOpens: 0,
	}

	// Serialize and deserialize
	data, err := json.Marshal(cbHealth)
	require.NoError(t, err)

	var decoded CircuitBreakerHealth
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, "closed", decoded.State)
	assert.Equal(t, 0, decoded.CurrentFailures)
	assert.Equal(t, int64(0), decoded.TotalFailures)
	assert.Equal(t, int64(0), decoded.TotalSuccesses)
	assert.Equal(t, int64(0), decoded.TotalCircuitOpens)
}

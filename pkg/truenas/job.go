package truenas

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const (
	// ReplicationRunOnetimeMethod is the middleware method used for detached
	// snapshot copies.
	ReplicationRunOnetimeMethod = "replication.run_onetime"
	// UnknownReplicationJobID is returned when a replication launch cannot be
	// proven to have produced a middleware job.
	UnknownReplicationJobID int64 = -1

	ReplicationJobAbortReasonContextEnded = "context_ended"
	ReplicationJobAbortReasonCopyFailed   = "copy_failed"
)

// ReplicationJobAbortRecorder records a successfully aborted replication job.
// The driver supplies its Prometheus recorder when it constructs the client.
type ReplicationJobAbortRecorder func(reason string)

// ReplicationJob is the subset of a core.get_jobs entry needed to identify and
// safely reap detached-copy jobs owned by the driver.
type ReplicationJob struct {
	ID             int64
	Method         string
	State          string
	SourceDatasets []string
	TargetDataset  string

	arguments interface{}
}

// ReplicationJobList returns active one-time replication jobs. Both the method
// and state are filtered server-side, then checked again locally so a malformed
// or unexpectedly broad middleware response can never become an abort license.
func (c *Client) ReplicationJobList(ctx context.Context) ([]*ReplicationJob, error) {
	filters := [][]interface{}{
		{"method", "=", ReplicationRunOnetimeMethod},
		{"state", "in", []string{"RUNNING", "WAITING"}},
	}
	result, err := c.Call(ctx, "core.get_jobs", filters)
	if err != nil {
		return nil, fmt.Errorf("query active one-time replication jobs: %w", err)
	}
	entries, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected core.get_jobs response type %T", result)
	}

	jobs := make([]*ReplicationJob, 0, len(entries))
	for _, entry := range entries {
		job, parseErr := parseReplicationJob(entry)
		if parseErr != nil {
			// A malformed job has no safely provable identity or target and must not
			// prevent well-formed driver jobs from being inspected.
			continue
		}
		if job.Method != ReplicationRunOnetimeMethod || !isActiveReplicationJobState(job.State) {
			continue
		}
		jobs = append(jobs, job)
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].ID < jobs[j].ID })
	return jobs, nil
}

// ReplicationJobAbort invokes core.job_abort and records only confirmed API
// successes. Callers are responsible for using a context that outlives a
// canceled CSI request when this is cleanup work.
func (c *Client) ReplicationJobAbort(ctx context.Context, jobID int64, reason string) error {
	if jobID < 0 {
		return fmt.Errorf("replication job id must be non-negative: %d", jobID)
	}
	if _, err := c.Call(ctx, "core.job_abort", jobID); err != nil {
		return fmt.Errorf("abort replication job %d: %w", jobID, err)
	}
	if c.replicationJobAbortRecorder != nil {
		c.replicationJobAbortRecorder(reason)
	}
	return nil
}

func parseReplicationJob(entry interface{}) (*ReplicationJob, error) {
	raw, ok := entry.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected job response type %T", entry)
	}
	jobID, err := replicationJobID(raw["id"])
	if err != nil {
		return nil, err
	}
	method, _ := raw["method"].(string)
	state, _ := raw["state"].(string)
	arguments := raw["arguments"]
	if arguments == nil {
		arguments = raw["args"]
	}
	params, _ := replicationParamsFromArguments(arguments)

	job := &ReplicationJob{
		ID:        jobID,
		Method:    method,
		State:     strings.ToUpper(strings.TrimSpace(state)),
		arguments: arguments,
	}
	if params == nil {
		return job, nil
	}
	job.TargetDataset, _ = params["target_dataset"].(string)
	job.SourceDatasets = stringSlice(params["source_datasets"])
	return job, nil
}

func replicationParamsFromArguments(arguments interface{}) (map[string]interface{}, bool) {
	if encoded, ok := arguments.(string); ok {
		var decoded interface{}
		if err := json.Unmarshal([]byte(encoded), &decoded); err != nil {
			return nil, false
		}
		arguments = decoded
	}
	switch value := arguments.(type) {
	case map[string]interface{}:
		return value, true
	case []interface{}:
		if len(value) != 1 {
			return nil, false
		}
		params, ok := value[0].(map[string]interface{})
		return params, ok
	default:
		return nil, false
	}
}

func stringSlice(value interface{}) []string {
	switch values := value.(type) {
	case []string:
		return append([]string(nil), values...)
	case []interface{}:
		result := make([]string, 0, len(values))
		for _, item := range values {
			text, ok := item.(string)
			if !ok {
				return nil
			}
			result = append(result, text)
		}
		return result
	default:
		return nil
	}
}

func isActiveReplicationJobState(state string) bool {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case "RUNNING", "WAITING":
		return true
	default:
		return false
	}
}

// recoverReplicationJobID finds the newest active job whose middleware method
// and full argument payload match the ambiguous launch. Full-payload matching is
// required: target-prefix matching alone could select an unrelated operator job.
func (c *Client) recoverReplicationJobID(ctx context.Context, params map[string]interface{}) (int64, error) {
	jobs, err := c.ReplicationJobList(ctx)
	if err != nil {
		return UnknownReplicationJobID, err
	}
	for index := len(jobs) - 1; index >= 0; index-- {
		if replicationArgumentsEqual(jobs[index].arguments, params) {
			return jobs[index].ID, nil
		}
	}
	return UnknownReplicationJobID, fmt.Errorf("no active job matched %s arguments", ReplicationRunOnetimeMethod)
}

func replicationArgumentsEqual(arguments interface{}, expected map[string]interface{}) bool {
	actual, ok := replicationParamsFromArguments(arguments)
	if !ok {
		return false
	}
	actualJSON, actualErr := json.Marshal(actual)
	expectedJSON, expectedErr := json.Marshal(expected)
	return actualErr == nil && expectedErr == nil && bytes.Equal(actualJSON, expectedJSON)
}

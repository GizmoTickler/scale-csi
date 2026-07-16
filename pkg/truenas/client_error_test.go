package truenas

import "testing"

func TestIsNotFoundErrorGenericCodeDoesNotMaskBusyDataset(t *testing.T) {
	err := &APIError{Code: -1, Message: "dataset is busy"}
	if IsNotFoundError(err) {
		t.Fatal("generic TrueNAS code -1 with a busy message must not be classified as not found")
	}
}

func TestIsNotFoundErrorGenericCodeWithNotFoundMessage(t *testing.T) {
	err := &APIError{Code: -1, Message: "dataset not found"}
	if !IsNotFoundError(err) {
		t.Fatal("a realistic not-found message must still be classified as not found")
	}
}

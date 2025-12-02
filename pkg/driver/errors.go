// Package driver provides error handling utilities for the CSI driver.
package driver

import (
	"github.com/GizmoTickler/scale-csi/pkg/truenas"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ToGRPCError converts various error types to appropriate gRPC status errors.
// This centralizes error mapping logic for consistent error handling across the driver.
func ToGRPCError(err error, context string) error {
	if err == nil {
		return nil
	}

	// Check for TrueNAS API errors
	if truenas.IsNotFoundError(err) {
		return status.Errorf(codes.NotFound, "%s: %v", context, err)
	}
	if truenas.IsAlreadyExistsError(err) {
		return status.Errorf(codes.AlreadyExists, "%s: %v", context, err)
	}
	if truenas.IsConnectionError(err) {
		return status.Errorf(codes.Unavailable, "%s: %v", context, err)
	}

	// Default to Internal error
	return status.Errorf(codes.Internal, "%s: %v", context, err)
}

// ToGRPCErrorWithCode converts an error to a gRPC status error with a specific code.
// Use this when you know the appropriate error code.
func ToGRPCErrorWithCode(err error, code codes.Code, context string) error {
	if err == nil {
		return nil
	}
	return status.Errorf(code, "%s: %v", context, err)
}

// WrapNotFound wraps an error as a NotFound gRPC error.
func WrapNotFound(err error, context string) error {
	return ToGRPCErrorWithCode(err, codes.NotFound, context)
}

// WrapInvalidArgument wraps an error as an InvalidArgument gRPC error.
func WrapInvalidArgument(err error, context string) error {
	return ToGRPCErrorWithCode(err, codes.InvalidArgument, context)
}

// WrapInternal wraps an error as an Internal gRPC error.
func WrapInternal(err error, context string) error {
	return ToGRPCErrorWithCode(err, codes.Internal, context)
}

// WrapUnavailable wraps an error as an Unavailable gRPC error.
func WrapUnavailable(err error, context string) error {
	return ToGRPCErrorWithCode(err, codes.Unavailable, context)
}

// WrapAborted wraps an error as an Aborted gRPC error.
func WrapAborted(err error, context string) error {
	return ToGRPCErrorWithCode(err, codes.Aborted, context)
}

// WrapFailedPrecondition wraps an error as a FailedPrecondition gRPC error.
func WrapFailedPrecondition(err error, context string) error {
	return ToGRPCErrorWithCode(err, codes.FailedPrecondition, context)
}

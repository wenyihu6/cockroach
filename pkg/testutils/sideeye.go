// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testutils

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/DataExMachina-dev/side-eye-go/sideeye"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type testingT interface {
	require.TestingT
	TestFatalerLogger
}

type fakeT struct {
	name string
}

// Errorf implements testingT.
func (f *fakeT) Errorf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}

// FailNow implements testingT.
func (f *fakeT) FailNow() {
	panic("unimplemented")
}

// Fatal implements testingT.
func (f *fakeT) Fatal(args ...interface{}) {
	panic("unimplemented")
}

// Fatalf implements testingT.
func (f *fakeT) Fatalf(format string, args ...interface{}) {
	panic("unimplemented")
}

// Helper implements testingT.
func (f *fakeT) Helper() {
}

// Logf implements testingT.
func (f *fakeT) Logf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}

func (f *fakeT) Name() string {
	return f.name
}

var _ testingT = &fakeT{}

func CaptureSideEyeSnapshotNoT(ctx context.Context, name string) {
	CaptureSideEyeSnapshot(ctx, &fakeT{name: name})
}

// CaptureSideEyeSnapshot captures a Side-Eye snapshot if the
// SIDE_EYE_TOKEN env var is set. If the snapshot is captured, the snapshot's
// URL is logged. Snapshots are captured with a 90s timeout.
func CaptureSideEyeSnapshot(ctx context.Context, t testingT) {
	t.Helper()

	if sideEyeToken := os.Getenv("SIDE_EYE_TOKEN"); sideEyeToken == "" {
		t.Logf("not capturing Side-Eye snapshot; SIDE_EYE_TOKEN env var not set. You can find it in slack or confluence " +
			"or on your profile page in the Side-Eye app. If using ./dev, make sure you pass it in the environment: " +
			"`./dev test mytest -- --test_env SIDE_EYE_TOKEN=xxx --strip=never`")
		return
	}

	username := os.Getenv("USER")
	hostname, err := os.Hostname()
	require.NoError(t, err)

	var name string
	if t, ok := t.(TestNamedFatalerLogger); ok {
		name = t.Name()
	} else {
		name = "unknown test"
	}
	name = fmt.Sprintf("%s@%s: %s", username, hostname, name)

	snapshotCtx, cancel := context.WithTimeoutCause(
		ctx, 180*time.Second, errors.New("timed out waiting for Side-Eye snapshot"),
	)
	defer cancel()
	snapshotURL, err := sideeye.CaptureSelfSnapshot(snapshotCtx, name, sideeye.WithEnvironment("unit tests"))
	if err != nil {
		if errors.As(err, &sideeye.BinaryStrippedError{}) {
			t.Logf("failed to capture Side-Eye snapshot because the binary is stripped of debug info; " +
				"if running with `go test` instead of bazel, use `go test -o test.out` " +
				"for creating a non-stripped binary. If running inside bazel, " +
				"add `build --strip=never` to your .bazelrc.user file, or pass `--strip=never` to " +
				"bazel test, or with `dev`: `./dev test mytest -- --strip=never`")
		}
		t.Logf("failed to capture Side-Eye snapshot: %s", err)
		return
	}
	t.Logf("captured Side-Eye snapshot: %s", snapshotURL)

}

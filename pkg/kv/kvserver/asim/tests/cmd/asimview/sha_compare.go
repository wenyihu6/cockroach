package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"log"
)

// ShaComparer handles comparison of test results between two Git SHAs
type ShaComparer struct {
	RepoRoot string
	TempDir  string
}

// NewShaComparer creates a new SHA comparer
func NewShaComparer() (*ShaComparer, error) {
	// Find git repo root
	cmd := exec.Command("git", "rev-parse", "--show-toplevel")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to find git repo root: %w", err)
	}
	repoRoot := strings.TrimSpace(string(output))

	// Create temp directory for SHA comparisons
	tempDir := filepath.Join(repoRoot, "asim_sha_compare_temp")
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create temp directory: %w", err)
	}

	return &ShaComparer{
		RepoRoot: repoRoot,
		TempDir:  tempDir,
	}, nil
}

// GenerateTestDataForSha generates test data for a specific SHA and stores it
func (sc *ShaComparer) GenerateTestDataForSha(sha string) error {
	fmt.Printf("Generating test data for SHA: %s\n", sha)

	// Create directory for this SHA
	shaDir := filepath.Join(sc.TempDir, sha)
	if err := os.MkdirAll(shaDir, 0755); err != nil {
		return fmt.Errorf("failed to create SHA directory: %w", err)
	}

	// Get current branch to restore later
	currentBranch, err := sc.getCurrentBranch()
	if err != nil {
		return fmt.Errorf("failed to get current branch: %w", err)
	}

	// Stash any current changes
	if err := sc.runGitCommand("stash", "push", "-m", "Temporary stash for SHA comparison"); err != nil {
		log.Printf("Warning: Failed to stash changes: %v", err)
	}

	// Checkout the target SHA
	if err := sc.runGitCommand("checkout", sha); err != nil {
		return fmt.Errorf("failed to checkout SHA %s: %w", sha, err)
	}

	// Run the test to generate data
	testCmd := exec.Command("./dev", "test", "pkg/kv/kvserver/asim/tests", "-f", "TestDataDriven", "--ignore-cache", "--rewrite", "--", "--test_env", "COCKROACH_RUN_ASIM_TESTS=true")
	testCmd.Dir = sc.RepoRoot
	testCmd.Stdout = os.Stdout
	testCmd.Stderr = os.Stderr
	
	fmt.Printf("Running test command for SHA %s...\n", sha)
	if err := testCmd.Run(); err != nil {
		// Don't fail immediately, try to restore state first
		log.Printf("Warning: Test command failed for SHA %s: %v", sha, err)
	}

	// Copy generated files to SHA directory
	generatedDir := filepath.Join(sc.RepoRoot, "pkg/kv/kvserver/asim/tests/testdata/generated")
	if err := sc.copyDir(generatedDir, filepath.Join(shaDir, "generated")); err != nil {
		return fmt.Errorf("failed to copy generated files: %w", err)
	}

	// Restore original branch
	if err := sc.runGitCommand("checkout", currentBranch); err != nil {
		return fmt.Errorf("failed to restore branch %s: %w", currentBranch, err)
	}

	// Try to restore stashed changes
	if err := sc.runGitCommand("stash", "pop"); err != nil {
		log.Printf("Warning: Failed to restore stashed changes: %v", err)
	}

	fmt.Printf("Successfully generated test data for SHA %s\n", sha)
	return nil
}

// CompareShAs generates test data for two SHAs and prepares for comparison
func (sc *ShaComparer) CompareShAs(sha1, sha2 string) error {
	fmt.Printf("Starting comparison between SHAs: %s and %s\n", sha1, sha2)

	// Generate data for both SHAs
	if err := sc.GenerateTestDataForSha(sha1); err != nil {
		return fmt.Errorf("failed to generate data for SHA %s: %w", sha1, err)
	}

	if err := sc.GenerateTestDataForSha(sha2); err != nil {
		return fmt.Errorf("failed to generate data for SHA %s: %w", sha2, err)
	}

	fmt.Printf("Successfully prepared comparison data for SHAs %s and %s\n", sha1, sha2)
	fmt.Printf("Data stored in: %s\n", sc.TempDir)
	return nil
}

// GetComparisons returns the list of available SHA comparisons
func (sc *ShaComparer) GetComparisons() ([]string, error) {
	entries, err := os.ReadDir(sc.TempDir)
	if err != nil {
		return nil, err
	}

	var shas []string
	for _, entry := range entries {
		if entry.IsDir() && len(entry.Name()) >= 7 { // SHA should be at least 7 chars
			shas = append(shas, entry.Name())
		}
	}
	return shas, nil
}

// Helper functions

func (sc *ShaComparer) getCurrentBranch() (string, error) {
	cmd := exec.Command("git", "branch", "--show-current")
	cmd.Dir = sc.RepoRoot
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}

func (sc *ShaComparer) runGitCommand(args ...string) error {
	cmd := exec.Command("git", args...)
	cmd.Dir = sc.RepoRoot
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func (sc *ShaComparer) copyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}

		dstPath := filepath.Join(dst, relPath)

		if info.IsDir() {
			return os.MkdirAll(dstPath, info.Mode())
		}

		// Copy file
		srcFile, err := os.Open(path)
		if err != nil {
			return err
		}
		defer srcFile.Close()

		if err := os.MkdirAll(filepath.Dir(dstPath), 0755); err != nil {
			return err
		}

		dstFile, err := os.Create(dstPath)
		if err != nil {
			return err
		}
		defer dstFile.Close()

		_, err = srcFile.WriteTo(dstFile)
		return err
	})
}

// Cleanup removes the temporary directory
func (sc *ShaComparer) Cleanup() error {
	return os.RemoveAll(sc.TempDir)
}
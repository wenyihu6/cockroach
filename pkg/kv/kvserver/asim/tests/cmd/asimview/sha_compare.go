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

	// Create a .gitignore file in the temp directory to prevent tracking
	gitignorePath := filepath.Join(tempDir, ".gitignore")
	if _, err := os.Stat(gitignorePath); os.IsNotExist(err) {
		gitignoreContent := "# Temporary SHA comparison data\n*\n!.gitignore\n"
		if err := os.WriteFile(gitignorePath, []byte(gitignoreContent), 0644); err != nil {
			log.Printf("Warning: Failed to create .gitignore in temp directory: %v", err)
		}
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

	// Check if data already exists for this SHA
	generatedPath := filepath.Join(shaDir, "generated")
	if _, err := os.Stat(generatedPath); err == nil {
		fmt.Printf("Test data already exists for SHA %s, skipping generation\n", sha)
		return nil
	}

	// Get current HEAD to restore later (safer than branch name)
	currentHead, err := sc.getCurrentHead()
	if err != nil {
		return fmt.Errorf("failed to get current HEAD: %w", err)
	}

	// Check if working directory is clean before proceeding
	if !sc.isWorkingDirectoryClean() {
		fmt.Printf("Working directory has changes, stashing them...\n")
		if err := sc.runGitCommand("stash", "push", "-m", "Temporary stash for SHA comparison"); err != nil {
			return fmt.Errorf("failed to stash changes: %w", err)
		}
		defer func() {
			fmt.Printf("Restoring stashed changes...\n")
			if err := sc.runGitCommand("stash", "pop"); err != nil {
				log.Printf("Warning: Failed to restore stashed changes: %v", err)
			}
		}()
	}

	// Checkout the target SHA
	if err := sc.runGitCommand("checkout", sha); err != nil {
		return fmt.Errorf("failed to checkout SHA %s: %w", sha, err)
	}

	// Ensure we restore the original state
	defer func() {
		fmt.Printf("Restoring original HEAD (%s)...\n", currentHead[:12])
		if err := sc.runGitCommand("checkout", currentHead); err != nil {
			log.Printf("Error: Failed to restore HEAD %s: %v", currentHead, err)
		}
	}()

	// Clean any existing generated files to ensure fresh test run
	generatedDir := filepath.Join(sc.RepoRoot, "pkg/kv/kvserver/asim/tests/testdata/generated")
	if err := sc.cleanGeneratedDir(generatedDir); err != nil {
		log.Printf("Warning: Failed to clean generated directory: %v", err)
	}

	// Run the test to generate data
	testCmd := exec.Command("./dev", "test", "pkg/kv/kvserver/asim/tests", "-f", "TestDataDriven", "--ignore-cache", "--rewrite", "--", "--test_env", "COCKROACH_RUN_ASIM_TESTS=true")
	testCmd.Dir = sc.RepoRoot
	testCmd.Stdout = os.Stdout
	testCmd.Stderr = os.Stderr
	
	fmt.Printf("Running test command for SHA %s...\n", sha)
	if err := testCmd.Run(); err != nil {
		return fmt.Errorf("test command failed for SHA %s: %w", sha, err)
	}

	// Copy generated files to SHA directory
	if err := sc.copyDir(generatedDir, generatedPath); err != nil {
		return fmt.Errorf("failed to copy generated files: %w", err)
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

func (sc *ShaComparer) getCurrentHead() (string, error) {
	cmd := exec.Command("git", "rev-parse", "HEAD")
	cmd.Dir = sc.RepoRoot
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}

func (sc *ShaComparer) getCurrentBranch() (string, error) {
	cmd := exec.Command("git", "branch", "--show-current")
	cmd.Dir = sc.RepoRoot
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}

func (sc *ShaComparer) isWorkingDirectoryClean() bool {
	cmd := exec.Command("git", "status", "--porcelain")
	cmd.Dir = sc.RepoRoot
	output, err := cmd.Output()
	if err != nil {
		log.Printf("Warning: Failed to check git status: %v", err)
		return false
	}
	return len(strings.TrimSpace(string(output))) == 0
}

func (sc *ShaComparer) cleanGeneratedDir(dir string) error {
	// Only clean files that are gitignored or untracked to avoid data loss
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	
	for _, entry := range entries {
		if entry.IsDir() {
			subDir := filepath.Join(dir, entry.Name())
			if err := sc.cleanGeneratedDir(subDir); err != nil {
				log.Printf("Warning: Failed to clean subdirectory %s: %v", subDir, err)
			}
		} else if strings.HasSuffix(entry.Name(), ".json") {
			// Only remove JSON files to be safe
			filePath := filepath.Join(dir, entry.Name())
			if err := os.Remove(filePath); err != nil {
				log.Printf("Warning: Failed to remove %s: %v", filePath, err)
			}
		}
	}
	return nil
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
package main

import (
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

//go:embed viewer.html
var viewerHTML string

//go:embed compare.html
var compareHTML string

type FileInfo struct {
	Path     string `json:"path"`
	Name     string `json:"name"`
	TestName string `json:"testName"`
}

type ChangedFile struct {
	Path     string `json:"path"`
	Name     string `json:"name"`
	TestName string `json:"testName"`
	Status   string `json:"status"` // "modified", "added", "deleted"
}

type CommitInfo struct {
	Hash    string `json:"hash"`
	Message string `json:"message"`
}

type CommitsInfo struct {
	Head CommitInfo `json:"head"`
	Prev CommitInfo `json:"prev"`
}

func main() {
	var port int
	flag.IntVar(&port, "port", 8080, "Port to serve on")
	flag.Parse()

	dir := flag.Arg(0)
	if dir == "" {
		// Find git repo root and default to generated testdata
		cmd := exec.Command("git", "rev-parse", "--show-toplevel")
		output, err := cmd.Output()
		if err != nil {
			log.Fatal("Failed to find git repo root:", err)
		}
		repoRoot := strings.TrimSpace(string(output))
		dir = filepath.Join(repoRoot, "pkg/kv/kvserver/asim/tests/testdata/generated")
	}

	absDir, err := filepath.Abs(dir)
	if err != nil {
		log.Fatal("Failed to resolve directory:", err)
	}

	if _, err := os.Stat(absDir); os.IsNotExist(err) {
		log.Fatalf("Directory does not exist: %s", absDir)
	}

	fmt.Printf("Serving files from: %s\n", absDir)
	fmt.Printf("Viewer available at: http://localhost:%d\n", port)
	fmt.Printf("Comparison tool available at: http://localhost:%d/compare\n", port)

	http.HandleFunc("/", serveViewer)
	http.HandleFunc("/compare", serveCompare)
	http.HandleFunc("/api/files", makeFileListHandler(absDir))
	http.HandleFunc("/api/file/", makeFileHandler(absDir))
	http.HandleFunc("/api/commits", makeCommitsHandler())
	http.HandleFunc("/api/changed-files", makeChangedFilesHandler(absDir))
	http.HandleFunc("/api/all-generated-files", makeAllGeneratedFilesHandler(absDir))
	http.HandleFunc("/api/file-at-commit", makeFileAtCommitHandler(absDir))

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
}

func serveViewer(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(viewerHTML))
}

func serveCompare(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(compareHTML))
}

func makeFileListHandler(baseDir string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var files []FileInfo

		err := filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() && strings.HasSuffix(path, ".json") {
				relPath, _ := filepath.Rel(baseDir, path)

				// Extract test name from file name only (ignore directories)
				baseName := filepath.Base(path)
				testName := strings.TrimSuffix(baseName, ".json")

				files = append(files, FileInfo{
					Path:     relPath,
					Name:     baseName,
					TestName: testName,
				})
			}
			return nil
		})

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(files)
	}
}

func makeFileHandler(baseDir string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Extract file path from URL
		filePath := strings.TrimPrefix(r.URL.Path, "/api/file/")

		// Prevent directory traversal
		cleanPath := filepath.Clean(filePath)
		if strings.Contains(cleanPath, "..") {
			http.Error(w, "Invalid path", http.StatusBadRequest)
			return
		}

		fullPath := filepath.Join(baseDir, cleanPath)

		// Check if file exists and is within baseDir
		if !strings.HasPrefix(fullPath, baseDir) {
			http.Error(w, "Invalid path", http.StatusBadRequest)
			return
		}

		file, err := os.Open(fullPath)
		if err != nil {
			http.Error(w, "File not found", http.StatusNotFound)
			return
		}
		defer file.Close()

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		io.Copy(w, file)
	}
}

func makeCommitsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Find git repo root
		repoRoot, err := getGitRepoRoot()
		if err != nil {
			http.Error(w, "Failed to find git repository root", http.StatusInternalServerError)
			return
		}

		// Get HEAD commit info
		headCmd := exec.Command("git", "log", "-1", "--pretty=format:%H|%s", "HEAD")
		headCmd.Dir = repoRoot
		headOutput, err := headCmd.Output()
		if err != nil {
			http.Error(w, "Failed to get HEAD commit info", http.StatusInternalServerError)
			return
		}

		// Get HEAD~1 commit info
		prevCmd := exec.Command("git", "log", "-1", "--pretty=format:%H|%s", "HEAD~1")
		prevCmd.Dir = repoRoot
		prevOutput, err := prevCmd.Output()
		if err != nil {
			http.Error(w, "Failed to get HEAD~1 commit info", http.StatusInternalServerError)
			return
		}

		// Parse commit info
		headParts := strings.SplitN(strings.TrimSpace(string(headOutput)), "|", 2)
		prevParts := strings.SplitN(strings.TrimSpace(string(prevOutput)), "|", 2)

		if len(headParts) != 2 || len(prevParts) != 2 {
			http.Error(w, "Failed to parse commit info", http.StatusInternalServerError)
			return
		}

		commits := CommitsInfo{
			Head: CommitInfo{
				Hash:    headParts[0][:8], // Short hash
				Message: headParts[1],
			},
			Prev: CommitInfo{
				Hash:    prevParts[0][:8], // Short hash
				Message: prevParts[1],
			},
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(commits)
	}
}

func makeChangedFilesHandler(baseDir string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Find git repo root
		repoRoot, err := getGitRepoRoot()
		if err != nil {
			fmt.Printf("Error finding git repo root: %v\n", err)
			http.Error(w, fmt.Sprintf("Failed to find git repository root: %v", err), http.StatusInternalServerError)
			return
		}

		fmt.Printf("Git repo root: %s\n", repoRoot)

		var changedFiles []ChangedFile

		// 1. Get tracked changed files between HEAD and HEAD~1
		cmd := exec.Command("git", "diff", "--name-status", "HEAD~1", "HEAD")
		cmd.Dir = repoRoot
		output, err := cmd.Output()
		if err != nil {
			// Get the actual error message
			if exitError, ok := err.(*exec.ExitError); ok {
				http.Error(w, fmt.Sprintf("Git command failed: %s", string(exitError.Stderr)), http.StatusInternalServerError)
			} else {
				http.Error(w, fmt.Sprintf("Failed to execute git command: %v", err), http.StatusInternalServerError)
			}
			return
		}

		lines := strings.Split(strings.TrimSpace(string(output)), "\n")

		// Pattern to match ASIM test data files (both generated JSON and non_rand txt files)
		testDataPattern := regexp.MustCompile(`pkg/kv/kvserver/asim/tests/testdata/(generated/.*\.json|non_rand/.*\.txt)$`)

		for _, line := range lines {
			if line == "" {
				continue
			}

			parts := strings.Fields(line)
			if len(parts) != 2 {
				continue
			}

			status := parts[0]
			filePath := parts[1]

			// Only include ASIM test data files
			if !testDataPattern.MatchString(filePath) {
				continue
			}

			// Convert git status to our status
			var fileStatus string
			switch status {
			case "M":
				fileStatus = "modified"
			case "A":
				fileStatus = "added"
			case "D":
				fileStatus = "deleted"
			default:
				fileStatus = "modified"
			}

			// Extract test name from file name
			baseName := filepath.Base(filePath)
			testName := strings.TrimSuffix(baseName, filepath.Ext(baseName))

			changedFiles = append(changedFiles, ChangedFile{
				Path:     filePath,
				Name:     baseName,
				TestName: testName,
				Status:   fileStatus,
			})
		}

		// 2. For any changed .txt config files, find corresponding generated JSON files
		correspondingFiles, err := findCorrespondingGeneratedFiles(repoRoot, changedFiles)
		if err != nil {
			fmt.Printf("Warning: failed to find corresponding generated files: %v\n", err)
		} else {
			changedFiles = append(changedFiles, correspondingFiles...)
		}

		// 3. Also check for recently modified files in the generated directory (since they're .gitignored)
		generatedFiles, err := findRecentlyModifiedGeneratedFiles(repoRoot)
		if err != nil {
			// Log error but don't fail the request
			fmt.Printf("Warning: failed to check generated files: %v\n", err)
		} else {
			changedFiles = append(changedFiles, generatedFiles...)
		}

		response := map[string]interface{}{
			"files": changedFiles,
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(response)
	}
}

func makeFileAtCommitHandler(baseDir string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		filePath := r.URL.Query().Get("path")
		commit := r.URL.Query().Get("commit")

		if filePath == "" || commit == "" {
			http.Error(w, "Missing path or commit parameter", http.StatusBadRequest)
			return
		}

		// Prevent directory traversal
		cleanPath := filepath.Clean(filePath)
		if strings.Contains(cleanPath, "..") {
			http.Error(w, "Invalid path", http.StatusBadRequest)
			return
		}

		// Special handling for generated files that are gitignored
		if strings.Contains(filePath, "testdata/generated") && strings.HasSuffix(filePath, ".json") {
			// For generated files, we need to checkout the commit and read from disk
			repoRoot, err := getGitRepoRoot()
			if err != nil {
				http.Error(w, "Failed to find git repository root", http.StatusInternalServerError)
				return
			}

			if commit == "HEAD" {
				// Serve current file from disk (no checkout needed)
				fullPath := filepath.Join(repoRoot, filePath)
				content, err := readGeneratedFile(fullPath)
				if err != nil {
					http.Error(w, fmt.Sprintf("Failed to read current generated file: %v", err), http.StatusNotFound)
					return
				}

				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Access-Control-Allow-Origin", "*")
				w.Write(content)
				return
			} else {
				// For previous commits, gitignored files remain on disk so we can read them directly
				// No checkout needed since these files aren't tracked by git
				fullPath := filepath.Join(repoRoot, filePath)
				content, err := readGeneratedFile(fullPath)
				if err != nil {
					http.Error(w, fmt.Sprintf("Failed to read generated file: %v", err), http.StatusNotFound)
					return
				}

				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Access-Control-Allow-Origin", "*")
				w.Write(content)
				return
			}
		}

		// For non-generated files, use git
		cmd := exec.Command("git", "show", fmt.Sprintf("%s:%s", commit, filePath))
		output, err := cmd.Output()
		if err != nil {
			// Check if it's a "file doesn't exist" error
			if strings.Contains(err.Error(), "does not exist") || strings.Contains(err.Error(), "exists on disk, but not in") {
				http.Error(w, "File not found at commit", http.StatusNotFound)
				return
			}
			http.Error(w, fmt.Sprintf("Failed to get file at commit: %v", err), http.StatusInternalServerError)
			return
		}

		// Set appropriate content type based on file extension
		if strings.HasSuffix(strings.ToLower(filePath), ".json") {
			// Validate that it's valid JSON
			var jsonData interface{}
			if err := json.Unmarshal(output, &jsonData); err != nil {
				http.Error(w, "File content is not valid JSON", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
		} else {
			// For txt files, return as plain text
			w.Header().Set("Content-Type", "text/plain")
		}

		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Write(output)
	}
}

// getGitRepoRoot finds the git repository root directory
func getGitRepoRoot() (string, error) {
	cmd := exec.Command("git", "rev-parse", "--show-toplevel")
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}

// findRecentlyModifiedGeneratedFiles looks for JSON files in the generated directory
// that have been modified more recently than the HEAD~1 commit
func findRecentlyModifiedGeneratedFiles(repoRoot string) ([]ChangedFile, error) {
	var changedFiles []ChangedFile

	// Get the timestamp of HEAD~1 commit
	cmd := exec.Command("git", "log", "-1", "--format=%ct", "HEAD~1")
	cmd.Dir = repoRoot
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to get HEAD~1 timestamp: %v", err)
	}

	commitTimeStr := strings.TrimSpace(string(output))
	commitTime, err := strconv.ParseInt(commitTimeStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse commit timestamp: %v", err)
	}

	// Look for JSON files in the generated directory
	generatedDir := filepath.Join(repoRoot, "pkg/kv/kvserver/asim/tests/testdata/generated")

	err = filepath.Walk(generatedDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories and non-JSON files
		if info.IsDir() || !strings.HasSuffix(strings.ToLower(path), ".json") {
			return nil
		}

		// Check if file was modified after HEAD~1 commit
		if info.ModTime().Unix() > commitTime {
			// Get relative path from repo root
			relPath, err := filepath.Rel(repoRoot, path)
			if err != nil {
				return err
			}

			baseName := filepath.Base(path)
			testName := strings.TrimSuffix(baseName, ".json")

			changedFiles = append(changedFiles, ChangedFile{
				Path:     relPath,
				Name:     baseName,
				TestName: testName,
				Status:   "generated", // Special status for generated files
			})
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to walk generated directory: %v", err)
	}

	return changedFiles, nil
}

// findCorrespondingGeneratedFiles finds JSON files in the generated directory that correspond to changed config files
func findCorrespondingGeneratedFiles(repoRoot string, changedConfigFiles []ChangedFile) ([]ChangedFile, error) {
	var correspondingFiles []ChangedFile
	generatedDir := filepath.Join(repoRoot, "pkg/kv/kvserver/asim/tests/testdata/generated")

	for _, configFile := range changedConfigFiles {
		// Only process .txt files from non_rand directory
		if !strings.HasSuffix(configFile.Path, ".txt") || !strings.Contains(configFile.Path, "non_rand") {
			continue
		}

		// Extract the base name without extension (e.g., "example_skewed_cpu_even_ranges_mma")
		baseName := strings.TrimSuffix(filepath.Base(configFile.Path), ".txt")

		// Look for a directory with this name in the generated folder
		correspondingDir := filepath.Join(generatedDir, baseName)

		if _, err := os.Stat(correspondingDir); os.IsNotExist(err) {
			fmt.Printf("No corresponding generated directory found for %s\n", baseName)
			continue
		}

		// Find all JSON files in the corresponding directory
		err := filepath.Walk(correspondingDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			// Skip directories and non-JSON files
			if info.IsDir() || !strings.HasSuffix(strings.ToLower(path), ".json") {
				return nil
			}

			// Get relative path from repo root
			relPath, err := filepath.Rel(repoRoot, path)
			if err != nil {
				return err
			}

			fileName := filepath.Base(path)
			testName := strings.TrimSuffix(fileName, ".json")

			correspondingFiles = append(correspondingFiles, ChangedFile{
				Path:     relPath,
				Name:     fileName,
				TestName: testName,
				Status:   "corresponding", // Special status for files corresponding to changed configs
			})

			return nil
		})

		if err != nil {
			fmt.Printf("Error walking directory %s: %v\n", correspondingDir, err)
			continue
		}
	}

	return correspondingFiles, nil
}

// readGeneratedFile reads and validates a JSON file from disk
func readGeneratedFile(fullPath string) ([]byte, error) {
	file, err := os.Open(fullPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	// Validate JSON
	var jsonData interface{}
	if err := json.Unmarshal(content, &jsonData); err != nil {
		return nil, fmt.Errorf("invalid JSON: %v", err)
	}

	return content, nil
}

// readGeneratedFileAtCommit temporarily checks out a commit, reads a generated file, then returns to original state
func readGeneratedFileAtCommit(repoRoot, filePath, commit string) ([]byte, error) {
	fmt.Printf("DEBUG: Attempting to read %s at commit %s\n", filePath, commit)

	// Get current HEAD to restore later
	getCurrentCmd := exec.Command("git", "rev-parse", "HEAD")
	getCurrentCmd.Dir = repoRoot
	currentHead, err := getCurrentCmd.Output()
	if err != nil {
		fmt.Printf("DEBUG: Failed to get current HEAD: %v\n", err)
		return nil, fmt.Errorf("failed to get current HEAD: %v", err)
	}
	currentHeadStr := strings.TrimSpace(string(currentHead))
	fmt.Printf("DEBUG: Current HEAD is %s\n", currentHeadStr)

	// Check if working directory is clean
	fmt.Printf("DEBUG: Checking if working directory is clean...\n")
	statusCmd := exec.Command("git", "status", "--porcelain")
	statusCmd.Dir = repoRoot
	statusOutput, err := statusCmd.Output()
	if err != nil {
		fmt.Printf("DEBUG: Failed to check git status: %v\n", err)
		return nil, fmt.Errorf("failed to check git status: %v", err)
	}

	statusStr := strings.TrimSpace(string(statusOutput))
	needsStash := len(statusStr) > 0

	if needsStash {
		fmt.Printf("DEBUG: Working directory is not clean, will stash changes temporarily\n")
		// Stash changes temporarily
		stashCmd := exec.Command("git", "stash", "push", "-m", "temp stash for asimview comparison")
		stashCmd.Dir = repoRoot
		if err := stashCmd.Run(); err != nil {
			fmt.Printf("DEBUG: Failed to stash changes: %v\n", err)
			return nil, fmt.Errorf("failed to stash changes: %v", err)
		}
		fmt.Printf("DEBUG: Successfully stashed changes\n")

		// Add stash pop to defer
		defer func() {
			fmt.Printf("DEBUG: Restoring stashed changes\n")
			popCmd := exec.Command("git", "stash", "pop")
			popCmd.Dir = repoRoot
			if popErr := popCmd.Run(); popErr != nil {
				fmt.Printf("WARNING: Failed to restore stashed changes: %v\n", popErr)
			} else {
				fmt.Printf("DEBUG: Successfully restored stashed changes\n")
			}
		}()
	} else {
		fmt.Printf("DEBUG: Working directory is clean\n")
	}

	// Skip checkout if we're already at the target commit
	if currentHeadStr == commit || strings.HasPrefix(currentHeadStr, commit) {
		fmt.Printf("DEBUG: Already at target commit %s, reading file directly\n", commit)
		fullPath := filepath.Join(repoRoot, filePath)
		content, err := readGeneratedFile(fullPath)
		if err != nil {
			return nil, fmt.Errorf("file not found or invalid at current commit: %v", err)
		}
		return content, nil
	}

	// Checkout the target commit with timeout
	fmt.Printf("DEBUG: Checking out commit %s\n", commit)
	checkoutCmd := exec.Command("git", "checkout", commit)
	checkoutCmd.Dir = repoRoot
	if err := checkoutCmd.Run(); err != nil {
		fmt.Printf("DEBUG: Failed to checkout %s: %v\n", commit, err)
		return nil, fmt.Errorf("failed to checkout %s: %v", commit, err)
	}
	fmt.Printf("DEBUG: Successfully checked out %s\n", commit)

	// Ensure we restore the original state even if reading fails
	defer func() {
		fmt.Printf("DEBUG: Restoring to %s\n", currentHeadStr)
		restoreCmd := exec.Command("git", "checkout", currentHeadStr)
		restoreCmd.Dir = repoRoot
		if restoreErr := restoreCmd.Run(); restoreErr != nil {
			fmt.Printf("WARNING: Failed to restore to %s: %v\n", currentHeadStr, restoreErr)
		} else {
			fmt.Printf("DEBUG: Successfully restored to %s\n", currentHeadStr)
		}
	}()

	// Read the generated file from disk at this commit
	fullPath := filepath.Join(repoRoot, filePath)
	fmt.Printf("DEBUG: Reading file %s\n", fullPath)
	content, err := readGeneratedFile(fullPath)
	if err != nil {
		fmt.Printf("DEBUG: Failed to read file: %v\n", err)
		return nil, fmt.Errorf("file not found or invalid at commit %s: %v", commit, err)
	}

	fmt.Printf("DEBUG: Successfully read %d bytes from file\n", len(content))
	return content, nil
}

// makeAllGeneratedFilesHandler returns all JSON files in the generated directory as a fallback
func makeAllGeneratedFilesHandler(baseDir string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		repoRoot, err := getGitRepoRoot()
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to find git repository root: %v", err), http.StatusInternalServerError)
			return
		}

		var allFiles []ChangedFile
		generatedDir := filepath.Join(repoRoot, "pkg/kv/kvserver/asim/tests/testdata/generated")

		err = filepath.Walk(generatedDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			// Skip directories and non-JSON files
			if info.IsDir() || !strings.HasSuffix(strings.ToLower(path), ".json") {
				return nil
			}

			// Get relative path from repo root
			relPath, err := filepath.Rel(repoRoot, path)
			if err != nil {
				return err
			}

			baseName := filepath.Base(path)
			testName := strings.TrimSuffix(baseName, ".json")

			allFiles = append(allFiles, ChangedFile{
				Path:     relPath,
				Name:     baseName,
				TestName: testName,
				Status:   "available", // Special status for available files
			})

			return nil
		})

		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to scan generated directory: %v", err), http.StatusInternalServerError)
			return
		}

		response := map[string]interface{}{
			"files": allFiles,
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(response)
	}
}

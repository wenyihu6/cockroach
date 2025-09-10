# ASIM Test Results Viewer

Interactive web viewer for ASIM test JSON output files with fuzzy search, multi-file comparison, and git-based change detection.

## Usage

```bash
# Default: serves files from repo's testdata/generated directory
go run .

# Or specify a custom directory
go run . /path/to/json/files

# Custom port
go run . -port 8081
```

Then open:
- **Main Viewer**: http://localhost:8080 
- **Git Comparison Tool**: http://localhost:8080/compare

## Features

### Main Viewer (`/`)
- **Fuzzy Search**: Type any part of test name or file name to filter
- **Multiple Selection**: Select multiple test files to compare side-by-side
- **Synchronized Zoom**: Drag to zoom on any chart, all charts sync automatically
- **Copy Data**: Click the clipboard button on any chart to copy its timeseries data as JSON
- **Auto-discovery**: Recursively finds all JSON files in the specified directory
- **Local Storage**: Remembers your last selection and zoom state

### Git Comparison Tool (`/compare`)
- **Change Detection**: Automatically detects ASIM test files that changed between HEAD and HEAD~1
- **Auto-mapping**: When config files (`.txt`) change, automatically includes corresponding generated files (`.json`)
- **Side-by-Side Comparison**: Compare metrics between current and previous commit
- **Commit Information**: Shows commit hashes and messages for context
- **Selective Loading**: Choose which changed files to compare
- **Synchronized Views**: Zoom and pan work across all comparison charts

## Git Integration

The comparison tool requires the application to be run from within a git repository. It uses:
- `git diff --name-status HEAD~1 HEAD` to detect changed files
- `git show <commit>:<path>` to load file content from specific commits
- `git log` to display commit information

### File Types Supported

1. **Configuration Files** (`.txt` in `non_rand/`): Loaded from both HEAD and HEAD~1
2. **Generated Results** (`.json` in `generated/`): Only current version available (gitignored)

### Workflow for Generated Files

Since generated files are gitignored, they're only available from the current working directory:

1. **Before making changes**: Note current test results
2. **Make configuration changes**: Modify `.txt` files in `testdata/non_rand/`
3. **Regenerate test results**: Run ASIM tests to update `testdata/generated/`
4. **Use comparison tool**: Compare current generated results with current config

**Note**: The tool will show "Generated files are not available in git history" for HEAD~1 comparisons of JSON files, since these files are not tracked in git.

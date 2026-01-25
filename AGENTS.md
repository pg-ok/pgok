# Guidelines for AI Agents Working on PGOK

This document contains specific instructions for AI agents (like Claude, GitHub Copilot, etc.) contributing to the PGOK project.

## Critical Requirements

### 🔴 MANDATORY: Run Linter Before Committing

**ALWAYS run the linter before making any commits or suggesting code changes.**

```bash
make lint
```

**The linter MUST show 0 issues.** If there are any issues, fix them before proceeding.

### Quick Check Commands

Before considering any task complete, run these commands:

```bash
# 1. Run linter (MANDATORY)
make lint

# 2. Run tests
make test

# 3. Check test coverage (optional but recommended)
make test-coverage
```

**All three commands must succeed without errors.**

## Code Quality Standards

### Error Handling in Tests

❌ **NEVER write this:**
```go
defer conn.Close(ctx)
defer testDB.Close(ctx)
defer os.Chdir(origDir)
defer file.Close()
```

✅ **ALWAYS write this:**
```go
// For test assertions
defer func() {
    assert.NoError(t, conn.Close(ctx))
}()

// When error can be safely ignored
defer func() {
    _ = conn.Close(ctx)
}()
```

### Test Structure

All tests MUST follow the **Given-When-Then** pattern:

```go
func TestFeature_Scenario(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping integration test in short mode")
    }

    // Given: Describe the initial state and setup
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
    defer cancel()

    testDB, err := db.SetupTestPostgres(ctx, t)
    require.NoError(t, err)
    defer func() {
        assert.NoError(t, testDB.Close(ctx))
    }()

    // When: Describe the action being tested
    result, err := PerformAction(ctx, params)
    require.NoError(t, err)

    // Then: Describe the expected outcome and verify it
    assert.Equal(t, expected, result)
}
```

### Documentation Comments

Every test function MUST have a doc comment explaining its purpose:

```go
// TestFeature_Scenario verifies that the feature correctly handles
// the specific scenario under test conditions
func TestFeature_Scenario(t *testing.T) {
    // ...
}
```

## Workflow for Making Changes

1. **Read the code** you're about to modify
2. **Make your changes** following the patterns above
3. **Run linter**: `make lint` (must show 0 issues)
4. **Run tests**: `make test` (all must pass)
5. **Check coverage**: `make test-coverage` (aim for >=70%)
6. **Update documentation** if needed (README.md, CONTRIBUTING.md, etc.)
7. **Update todo.md** to reflect progress

## Common Linter Errors and Fixes

### 1. Unchecked errors in defer

**Error:** `Error return value is not checked (errcheck)`

**Fix:**
```go
// Option 1: Assert no error (for tests)
defer func() {
    assert.NoError(t, resource.Close())
}()

// Option 2: Explicitly ignore (when safe)
defer func() {
    _ = resource.Close()
}()
```

### 2. File operations

**Error:** `Error return value of os.Chdir is not checked`

**Fix:**
```go
origDir, err := os.Getwd()
require.NoError(t, err)
defer func() {
    _ = os.Chdir(origDir)
}()
```

### 3. Pipe/Writer close

**Error:** `Error return value of w.Close is not checked`

**Fix:**
```go
_ = w.Close()  // Explicitly ignore if error doesn't matter
```

## Testing Requirements

### Integration Tests

- Use `testcontainers-go` for PostgreSQL
- Always check `testing.Short()` to allow skipping
- Use `t.TempDir()` for temporary files
- Clean up resources properly

### Coverage Goals

- **Database layer:** >= 70%
- **CLI commands:** >= 90% (simple commands should be 100%)
- **Overall project:** >= 70%

## File Naming Conventions

- Tests: `*_test.go` in the same package
- Helper functions: `testing.go` for test utilities
- Documentation: `*.md` in root directory

## When to Update Documentation

Update documentation when you:

- Add new features or commands
- Change existing behavior
- Add new test patterns or utilities
- Fix bugs that users should know about
- Add new dependencies

## Integration with CI/CD

The project uses GitHub Actions for automated testing and linting. On every push and pull request, the following checks are run:

1. **Tests Job** (`.github/workflows/test.yml`)
   - Runs on Ubuntu with Go 1.23
   - Executes `make test` (all tests)
   - Generates coverage report with `make test-coverage`
   - Uploads coverage to Codecov (optional, requires `CODECOV_TOKEN` secret)

2. **Lint Job** (`.github/workflows/test.yml`)
   - Runs golangci-lint with latest version
   - Must show 0 issues for the build to pass

**Your changes will be automatically checked on push.** Save time by running `make lint` and `make test` locally first.

### Setting Up CI/CD

The workflow is already configured in `.github/workflows/test.yml`. To enable coverage reporting:

1. Sign up at [codecov.io](https://codecov.io)
2. Add your repository
3. Add `CODECOV_TOKEN` to GitHub Secrets (Settings > Secrets and variables > Actions)
4. Coverage badge will automatically appear in README.md

### CI/CD Best Practices

- All tests must pass before merging
- Linter must show 0 issues
- Maintain or improve coverage with each PR
- Check the Actions tab on GitHub to see build status

## Useful Commands Reference

```bash
# Development
make build          # Build binary
make test           # Run all tests
make test-short     # Run fast tests (no Docker)
make lint           # Run linter (MANDATORY before commit)
make test-coverage  # Generate coverage report
make clean          # Clean artifacts

# Linter only
golangci-lint run ./...

# Tests only
go test ./...                    # All tests
go test -short ./...             # Skip integration tests
go test -v ./internal/db/...     # Specific package
```

## Remember

🔴 **CRITICAL:** Never commit code that doesn't pass `make lint` with 0 issues.

✅ **BEST PRACTICE:** Run `make lint && make test` before every commit.

📊 **GOAL:** Maintain or improve test coverage with every change.

📝 **DOCUMENTATION:** Update docs when changing behavior.

---

**Last Updated:** 2026-01-24
**Linter:** golangci-lint v2.3
**Go Version:** 1.24+

.PHONY: help check check-with-coverage check-fuzz check-review clean

# Default target - show help
help: ## Show this help message with all available targets
	@echo "Beyond Babel - Makefile targets"
	@echo "================================"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-25s\033[0m %s\n", $$1, $$2}'
	@echo ""

check: ## Run pytest tests
	@echo "========================================"
	@echo "Running Tests with pytest"
	@echo "========================================"
	@echo ""
	@pytest -v tests/

check-with-coverage: ## Run pytest with coverage reporting (generates htmlcov/)
	@echo "========================================"
	@echo "Running Tests with Coverage"
	@echo "========================================"
	@echo ""
	@echo "Installing coverage.py and pytest-cov if needed..."
	@pip3 install coverage pytest-cov --quiet 2>/dev/null || true
	@echo ""
	@echo "Running pytest with coverage..."
	@pytest --cov=bb --cov=aston --cov-report=term --cov-report=html tests/
	@echo ""
	@echo "✓ HTML coverage report generated in htmlcov/index.html"

check-fuzz: ## Run comprehensive fuzz tests (corpus, mutation, generative)
	@echo "========================================"
	@echo "Running Comprehensive Fuzz Tests"
	@echo "========================================"
	@echo ""
	@echo "[1/4] Corpus fuzzing..."
	@python3 tests/aston/fuzz.py --corpus
	@echo ""
	@echo "[2/4] Mutation fuzzing (50 mutations)..."
	@python3 tests/aston/fuzz.py --mutation --mutations 50
	@echo ""
	@echo "[3/4] Generative fuzzing (100 tests)..."
	@SEED=$$(python3 -c "import random; print(random.randint(0, 999999))"); \
	echo "Using random seed: $$SEED"; \
	echo "To reproduce: python3 tests/aston/fuzz.py --generative --tests 100 --seed $$SEED"; \
	python3 tests/aston/fuzz.py --generative --tests 100 --seed $$SEED
	@echo ""
	@echo "[4/4] Full comprehensive fuzzing..."
	@python3 tests/aston/fuzz.py --mutations 20 --tests 50
	@echo ""
	@echo "✓ All fuzz tests passed!"

check-review: ## Check GitHub PR review comments (requires gh CLI and authenticated GitHub)
	@echo "======================================="
	@echo "Checking GitHub PR Review Comments"
	@echo "======================================="
	@echo ""
	@./bin/github-review-threads.py

clean: ## Clean up generated files (htmlcov/, .coverage, __pycache__)
	@echo "Cleaning up generated files..."
	@rm -rf htmlcov/
	@rm -f .coverage
	@rm -f /tmp/aston_fuzz_fail_*.py
	@find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@echo "✓ Cleanup complete"

cosmit: ## Format code with ruff, lint with --fix, and commit if changes
	@echo "======================================="
	@echo "Running cosmit: format, lint, and commit"
	@echo "======================================="
	@echo ""
	@echo "[1/3] Running ruff format..."
	@uv run ruff format --config pyproject.toml
	@echo ""
	@echo "[2/3] Running ruff check with --fix on main source files..."
	@uv run ruff check --fix --config pyproject.toml
	@echo ""
	@echo "[3/3] Checking for changes and committing..."
	@if git diff --quiet; then \
	    echo "No changes detected, skipping commit."; \
	else \
	    echo "Changes detected, creating cosmit commit..."; \
	    git add .; \
	    git commit -m "cosmit"; \
	    echo "✓ cosmit commit created"; \
	fi

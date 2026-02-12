.PHONY: help check check-with-coverage clean miss

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
	@uv run pytest -v tests/

check-with-coverage: ## Run pytest with coverage reporting (generates htmlcov/)
	@echo "========================================"
	@echo "Running Tests with Coverage"
	@echo "========================================"
	@echo ""
	@echo "Running pytest with coverage..."
	@uv run pytest --cov=bb --cov-report=term --cov-report=html tests/
	@echo ""
	@echo "✓ HTML coverage report generated in htmlcov/index.html"

miss: ## List functions below 80% coverage
	@rm -f .coverage coverage.xml
	@uv run pytest --cov=bb --cov-report=xml:coverage.xml tests/ -q 2>/dev/null
	@uv run python scripts/coverage_top.py
	@rm -f coverage.xml

clean: ## Clean up generated files (htmlcov/, .coverage, __pycache__)
	@echo "Cleaning up generated files..."
	@rm -rf htmlcov/
	@rm -f .coverage
	@rm -f /tmp/aston_fuzz_fail_*.py
	@find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@echo "✓ Cleanup complete"

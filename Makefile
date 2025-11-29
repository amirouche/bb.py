.PHONY: help check check-with-coverage check-fuzz clean

# Default target - show help
help:
	@echo "Beyond Babel - Makefile targets"
	@echo "================================"
	@echo ""
	@echo "Available targets:"
	@echo "  make help                 - Show this help message"
	@echo "  make check                - Run basic ASTON round-trip tests"
	@echo "  make check-with-coverage  - Run tests with coverage reporting"
	@echo "  make check-fuzz           - Run comprehensive fuzz tests"
	@echo "  make clean                - Clean up generated files"
	@echo ""
	@echo "Examples:"
	@echo "  make check                     # Quick validation"
	@echo "  make check-fuzz                # Full fuzz suite (~2-3 min)"
	@echo "  make check-with-coverage       # Generate coverage report"

check:
	@echo "========================================"
	@echo "Running ASTON Round-Trip Tests"
	@echo "========================================"
	@echo ""
	@echo "Testing example files..."
	@for file in examples/*.py; do \
		echo "  Testing $$file..."; \
		python3 aston.py --test "$$file" || exit 1; \
	done
	@echo ""
	@echo "✓ All basic tests passed!"

check-with-coverage:
	@echo "========================================"
	@echo "Running Tests with Coverage"
	@echo "========================================"
	@echo ""
	@echo "Installing coverage.py if needed..."
	@pip3 install coverage --quiet 2>/dev/null || true
	@echo ""
	@echo "Running tests with coverage..."
	@coverage run --source=bb,aston tests/aston/fuzz.py --mutations 10 --tests 20
	@echo ""
	@echo "Coverage report:"
	@coverage report
	@echo ""
	@echo "Generating HTML coverage report..."
	@coverage html
	@echo "✓ HTML coverage report generated in htmlcov/index.html"

check-fuzz:
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

clean:
	@echo "Cleaning up generated files..."
	@rm -rf htmlcov/
	@rm -f .coverage
	@rm -f /tmp/aston_fuzz_fail_*.py
	@find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@echo "✓ Cleanup complete"

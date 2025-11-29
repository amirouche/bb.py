.PHONY: help check check-with-coverage check-fuzz clean

# Default target - show help
help:
	@echo "Beyond Babel - Makefile targets"
	@echo "================================"
	@echo ""
	@echo "Available targets:"
	@echo "  make help                 - Show this help message"
	@echo "  make check                - Run pytest tests"
	@echo "  make check-with-coverage  - Run pytest with coverage reporting"
	@echo "  make check-fuzz           - Run comprehensive fuzz tests"
	@echo "  make clean                - Clean up generated files"
	@echo ""
	@echo "Examples:"
	@echo "  make check                     # Quick pytest validation"
	@echo "  make check-fuzz                # Full fuzz suite (~2-3 min)"
	@echo "  make check-with-coverage       # Generate coverage report"

check:
	@echo "========================================"
	@echo "Running Tests with pytest"
	@echo "========================================"
	@echo ""
	@pytest -v tests/

check-with-coverage:
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

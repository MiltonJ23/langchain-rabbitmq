.PHONY: install lint format format-check typecheck security \
        test test-unit test-integration test-e2e \
        clean docs docs-serve

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
install:
	pip install -e ".[dev,docs]"

# ---------------------------------------------------------------------------
# Code quality
# ---------------------------------------------------------------------------
lint:
	ruff check src tests

format:
	ruff format src tests

format-check:
	ruff format --check src tests

typecheck:
	pyright src

security:
	bandit -r src -c pyproject.toml

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
test:
	pytest tests/

test-unit:
	pytest tests/unit/ -m unit

test-integration:
	pytest tests/integration/ -m integration

test-e2e:
	pytest tests/e2e/ -m e2e

# ---------------------------------------------------------------------------
# Documentation
# ---------------------------------------------------------------------------
docs:
	mkdocs build --strict

docs-serve:
	mkdocs serve

# ---------------------------------------------------------------------------
# Housekeeping
# ---------------------------------------------------------------------------
clean:
	rm -rf .pytest_cache htmlcov .coverage coverage.xml dist build site
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete

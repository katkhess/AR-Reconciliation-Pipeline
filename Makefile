.PHONY: install test lint format run-pipeline docker-up docker-down clean help

PYTHON := python3
PIP := pip3
SRC_DIR := src
TEST_DIR := tests

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install all dependencies
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[dev,gx]"

install-airflow: ## Install Airflow dependencies separately (large package)
	$(PIP) install "apache-airflow>=2.8.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.0/constraints-3.11.txt"

test: ## Run all tests with coverage
	pytest $(TEST_DIR) -v --cov=$(SRC_DIR) --cov-report=term-missing --cov-report=html

test-unit: ## Run unit tests only
	pytest $(TEST_DIR)/unit -v

test-integration: ## Run integration tests only
	pytest $(TEST_DIR)/integration -v

lint: ## Run linter (ruff)
	ruff check $(SRC_DIR) $(TEST_DIR)
	mypy $(SRC_DIR) --ignore-missing-imports

format: ## Format code with black and ruff
	black $(SRC_DIR) $(TEST_DIR) --line-length 100
	ruff check $(SRC_DIR) $(TEST_DIR) --fix

run-pipeline: ## Run the AR reconciliation pipeline
	$(PYTHON) -m src.pipeline.orchestrator \
		--invoice-source data/sample/invoices.csv \
		--payment-source data/sample/payments.csv \
		--returns-source data/sample/returns.csv

docker-up: ## Start all Docker services
	docker compose -f docker/docker-compose.yml up -d
	@echo "Waiting for services to be healthy..."
	@sleep 10
	@docker compose -f docker/docker-compose.yml ps

docker-down: ## Stop all Docker services
	docker compose -f docker/docker-compose.yml down -v

docker-build: ## Build the pipeline Docker image
	docker build -f docker/Dockerfile -t ar-reconciliation-pipeline:latest .

docker-logs: ## Tail logs from all Docker services
	docker compose -f docker/docker-compose.yml logs -f

terraform-init: ## Initialize Terraform
	cd infra/terraform && terraform init

terraform-plan: ## Plan Terraform changes
	cd infra/terraform && terraform plan

terraform-apply: ## Apply Terraform changes
	cd infra/terraform && terraform apply

terraform-destroy: ## Destroy Terraform infrastructure
	cd infra/terraform && terraform destroy

clean: ## Remove build artifacts, caches, and coverage reports
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	rm -rf htmlcov/ .coverage coverage.xml dist/ build/
	@echo "Cleaned build artifacts."

validate-schemas: ## Validate JSON schemas
	$(PYTHON) -c "import json, glob; [json.load(open(f)) and print(f'OK: {f}') for f in glob.glob('config/schemas/*.json')]"

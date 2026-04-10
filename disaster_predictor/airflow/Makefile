.PHONY: sync help

help:
	@echo "Available targets:"
	@echo "  sync - Sync parent directory modules to include/ (for Docker builds)"
	@echo "         Local dev can use symlinks in include/, but Docker needs actual files"

sync:
	@echo "Syncing parent modules to include/..."
	@mkdir -p include
	@touch include/__init__.py
	@if [ -f ../.env ]; then \
		rm -f .env && cp ../.env .env && echo "  ✓ .env copied from project root"; \
	fi
	@if [ -d ../utils ]; then \
		rm -rf include/utils && cp -r ../utils include/utils; \
	fi
	@if [ -d ../config ]; then \
		rm -rf include/config && cp -r ../config include/config; \
	fi
	@if [ -d ../risk_assessment ]; then \
		rm -rf include/risk_assessment && cp -r ../risk_assessment include/risk_assessment; \
	fi
	@if [ -d ../llm_interaction ]; then \
		rm -rf include/llm_interaction && cp -r ../llm_interaction include/llm_interaction; \
	fi
	@if [ -d ../ml_training ]; then \
		rm -rf include/ml_training && cp -r ../ml_training include/ml_training; \
	fi
	@if [ -d ../models ]; then \
		mkdir -p include/models && cp -r ../models/*.pkl ../models/*.json include/models/ 2>/dev/null || true; \
	fi
	@echo "Done! include/ now contains actual files (not symlinks) for Docker build"

sync-links:
	@echo "Creating symlinks in include/ (for local dev)..."
	@mkdir -p include
	@touch include/__init__.py
	@rm -rf include/utils include/config include/risk_assessment include/llm_interaction include/ml_training
	@ln -sf ../utils include/utils
	@ln -sf ../config include/config
	@ln -sf ../risk_assessment include/risk_assessment
	@ln -sf ../llm_interaction include/llm_interaction
	@ln -sf ../ml_training include/ml_training
	@ln -sf ../.env .env 2>/dev/null || true
	@echo "Done! include/ now has symlinks for local development"

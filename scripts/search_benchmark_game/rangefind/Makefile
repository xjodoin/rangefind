INDEX_DIR ?= $(CURDIR)/index
RANGEFIND_REPO ?= $(abspath ../../..)
NODE ?= node

.PHONY: clean compile index serve

clean:
	rm -rf "$(INDEX_DIR)"

compile:
	@true

index:
	@if [ "$$RANGEFIND_SBG_FORCE" = "1" ]; then rm -rf "$(INDEX_DIR)"; fi
	mkdir -p "$(INDEX_DIR)"
	@if [ -n "$$CORPUS" ]; then \
		RANGEFIND_SBG_INPUT="$$CORPUS" RANGEFIND_REPO="$(RANGEFIND_REPO)" "$(NODE)" build_index.mjs "$(INDEX_DIR)"; \
	else \
		RANGEFIND_REPO="$(RANGEFIND_REPO)" "$(NODE)" build_index.mjs "$(INDEX_DIR)"; \
	fi

serve:
	RANGEFIND_REPO="$(RANGEFIND_REPO)" "$(NODE)" serve.mjs "$(INDEX_DIR)/public/rangefind"

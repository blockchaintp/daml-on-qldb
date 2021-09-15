MAKEFILE_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
include $(MAKEFILE_DIR)/standard_defs.mk

export AWS_REGION ?= us-east-1
export AWS_ACCESS_KEY_ID ?=
export AWS_SECRET_ACCESS_KEY ?=

CLEAN_DIRS := $(CLEAN_DIRS) test-dars

export LEDGER_NAME = $(ISOLATION_ID)

clean: clean_dirs_daml clean_containers clean_aws

distclean: clean_docker clean_dirs_daml clean_markers

build: $(MARKERS)/build_mvn $(MARKERS)/build_ledgertest

package: $(MARKERS)/package_mvn $(MARKERS)/package_docker

test: $(MARKERS)/test_mvn

test_daml: $(MARKERS)/test_daml ${}

analyze: analyze_fossa analyze_sonar_mvn

publish: $(MARKERS)/publish_mvn

$(MARKERS): test-dars

test-dars:
	mkdir -p test-dars

.PHONY: check_env
check_env:
	@if [ -z "$$AWS_ACCESS_KEY_ID" ] || \
	[ -z "$$AWS_SECRET_ACCESS_KEY" ]; then \
		echo "Env vars AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set"; \
		exit 1; \
	fi

.PHONY: clean_dirs_daml
clean_dirs_daml: fix_permissions
	rm -rf test-dars

$(MARKERS)/build_ledgertest:
	docker build -f docker/ledger-api-testtool.docker -t \
		ledger-api-testtool:$(ISOLATION_ID) . ;
	docker run --rm -v `pwd`/test-dars:/out \
		ledger-api-testtool:$(ISOLATION_ID) bash \
		-c "java -jar ledger-api-test-tool.jar -x && cp *.dar /out"
	touch $@

$(MARKERS)/package_docker: build $(MARKERS)/package_mvn
	docker-compose -f docker-compose.yaml build
	touch $@

$(MARKERS)/test_daml: $(MARKERS)/package_docker
	docker-compose -f docker/daml-test.yaml build
	docker-compose -p $(ISOLATION_ID) -f docker/daml-test.yaml down \
		-v || true
	docker-compose -p $(ISOLATION_ID) -f docker/daml-test.yaml up \
		--exit-code-from ledger-api-testtool || true
	docker logs $(ISOLATION_ID)_ledger-api-testtool_1 | \
		sed -r "s/\x1B\[([0-9]{1,3}(;[0-9]{1,2})?)?[mGK]//g" \
		> build/results.txt 2>&1
	./run_tests ./build/results.txt DAML > build/daml-test.results
	docker-compose -p $(ISOLATION_ID) -f docker/daml-test.yaml down \
		|| true
	touch $@

.PHONY: clean_containers
clean_containers:
	docker-compose -p $(ISOLATION_ID) -f docker/daml-test.yaml \
		rm -f || true
	docker-compose -p $(ISOLATION_ID) -f docker/daml-test.yaml down \
		-v || true

.PHONY: clean_docker
clean_docker:
	docker-compose -p $(ISOLATION_ID) -f docker/daml-test.yaml \
		rm -f || true
	docker-compose -p $(ISOLATION_ID) -f docker/daml-test.yaml down \
		-v --rmi all || true


.PHONY: clean_aws
clean_aws:
	docker run --rm -e AWS_REGION -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
		--entrypoint /bin/bash amazon/aws-cli:latest \
		-c "aws qldb delete-ledger \
			--name ${ISOLATION_ID::-1} --region ${AWS_REGION}" || true

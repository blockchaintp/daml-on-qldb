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

test: $(MARKERS)/test_mvn $(MARKERS)/test_daml

test_daml: $(MARKERS)/test_daml ${}

analyze: analyze_fossa analyze_sonar_mvn

publish: $(MARKERS)/publish_mvn

$(MARKERS): test-dars

test-dars:
	mkdir -p test-dars

$(MARKERS)/binfmt:
	mkdir -p $(MARKERS)
	if [ `uname -m` = "x86_64" ]; then \
		docker run --rm --privileged multiarch/qemu-user-static --reset -p yes; \
	fi
	touch $@

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

$(MARKERS)/package_docker_amd64: build $(MARKERS)/package_mvn $(MARKERS)/binfmt
	docker buildx build --platform linux/amd64 -f docker/daml-on-postgres.docker -t daml-on-postgres-ng:$(ISOLATION_ID) . --load
	docker buildx build --platform linux/amd64 -f docker/daml-on-qldb.docker -t daml-on-qldb:$(ISOLATION_ID) . --load
	touch $@

$(MARKERS)/package_docker_arm64: build $(MARKERS)/package_mvn $(MARKERS)/binfmt
	docker buildx build --platform linux/arm64 -f docker/daml-on-postgres.docker -t daml-on-postgres-ng-arm64:$(ISOLATION_ID) . --load
	docker buildx build --platform linux/arm64 -f docker/daml-on-qldb.docker -t daml-on-qldb-arm64:$(ISOLATION_ID) . --load
	touch $@

$(MARKERS)/package_docker: $(MARKERS)/package_docker_amd64 $(MARKERS)/package_docker_arm64

$(MARKERS)/test_daml: $(MARKERS)/test_daml_postgres $(MARKERS)/test_daml_qldb

$(MARKERS)/test_daml_qldb: $(MARKERS)/package_docker
	docker-compose -f docker/daml-on-qldb-test.yaml build
	docker-compose -p $(ISOLATION_ID) -f docker/daml-on-qldb-test.yaml down \
		-v || true
	docker-compose -p $(ISOLATION_ID) -f docker/daml-on-qldb-test.yaml up \
		--exit-code-from ledger-api-testtool || true
	docker-compose -p $(ISOLATION_ID) -f docker/daml-on-postgres-test.yaml logs \
		ledger-api-testtool | \
		sed -r "s/\x1B\[([0-9]{1,3}(;[0-9]{1,2})?)?[mGK]//g" \
		> build/results_qldb.txt 2>&1
	./run_tests ./build/results_qldb.txt DAML-QLDB > build/daml-on-qldb-test.results
	docker-compose -p $(ISOLATION_ID) -f docker/daml-on-qldb-test.yaml down \
		|| true
	touch $@

$(MARKERS)/test_daml_postgres: $(MARKERS)/package_docker
	docker-compose -f docker/daml-on-postgres-test.yaml build
	docker-compose -p $(ISOLATION_ID) -f docker/daml-on-postgres-test.yaml down \
		-v || true
	docker-compose -p $(ISOLATION_ID) -f docker/daml-on-postgres-test.yaml up \
		--exit-code-from ledger-api-testtool || true
	docker-compose -p $(ISOLATION_ID) -f docker/daml-on-postgres-test.yaml logs \
		ledger-api-testtool | \
		sed -r "s/\x1B\[([0-9]{1,3}(;[0-9]{1,2})?)?[mGK]//g" \
		> build/results_postgres.txt 2>&1
	./run_tests ./build/results_postgres.txt DAML-PG > build/daml-on-postgres-test.results
	docker-compose -p $(ISOLATION_ID) -f docker/daml-on-postgres-test.yaml down \
		|| true
	touch $@


.PHONY: clean_containers
clean_containers:
	docker-compose -p $(ISOLATION_ID) -f docker/daml-on-qldb-test.yaml \
		rm -f || true
	docker-compose -p $(ISOLATION_ID) -f docker/daml-on-qldb-test.yaml down \
		-v || true
	docker-compose -p $(ISOLATION_ID) -f docker/daml-on-postgres-test.yaml \
		rm -f || true
	docker-compose -p $(ISOLATION_ID) -f docker/daml-on-postgres-test.yaml down \
		-v || true

.PHONY: clean_docker
clean_docker:
	docker-compose -p $(ISOLATION_ID) -f docker/daml-on-qldb-test.yaml \
		rm -f || true
	docker-compose -p $(ISOLATION_ID) -f docker/daml-on-qldb-test.yaml down \
		-v --rmi all || true
	docker-compose -p $(ISOLATION_ID) -f docker/daml-on-postgres-test.yaml \
		rm -f || true
	docker-compose -p $(ISOLATION_ID) -f docker/daml-on-postgres-test.yaml down \
		-v --rmi all || true

# Truncate ledger id to 31 chars, a qldb limit
CLIPPED=$(shell echo $(ISOLATION_ID)| sed 's/^\(.\{31\}\).*/\1/g')

.PHONY: clean_aws
clean_aws:
	docker run --rm -e AWS_REGION -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
		--entrypoint /bin/bash amazon/aws-cli:latest \
		-c "aws qldb delete-ledger \
			--name ${CLIPPED} --region ${AWS_REGION}" || true

export ISOLATION_ID ?= local
PWD = $(shell pwd)

ORGANIZATION ?= $(shell git remote show -n origin | grep Fetch | \
												awk '{print $$NF}' | \
												sed -e 's/git@github.com://' | \
												sed -e 's@https://github.com/@@' | \
												awk -F'[/.]' '{print $$1}' )
REPO ?= $(shell git remote show -n origin | grep Fetch | \
												awk '{print $$NF}' | \
												sed -e 's/git@github.com://' | \
												sed -e 's@https://github.com/@@' | \
												awk -F'[/.]' '{print $$2}' )

BRANCH_NAME ?= $(shell git symbolic-ref -q HEAD )
SAFE_BRANCH_NAME ?= $(shell if [ -n "$$BRANCH_NAME" ]; then echo $$BRANCH_NAME; else \
														git symbolic-ref -q HEAD|sed -e \
														's@refs/heads/@@'|sed -e 's@/@_@g'; \
														fi)
VERSION ?= $(shell git describe | cut -c2-  )
LONG_VERSION ?= $(shell git describe --long --dirty |cut -c2- )
UID := $(shell id -u)
GID := $(shell id -g)

MAVEN_SETTINGS ?= $(HOME)/.m2/settings.xml
MAVEN_REVISION != if [ "$(LONG_VERSION)" = "$(VERSION)" ] || \
	(echo "$(LONG_VERSION)" | grep -q dirty); then \
		echo `bin/semver bump patch $(VERSION)`-SNAPSHOT; \
	else \
		echo $(VERSION); \
	fi

TOOLCHAIN := docker run --rm -v $(HOME)/.m2/repository:/root/.m2/repository \
		-v $(MAVEN_SETTINGS):/root/.m2/settings.xml -v `pwd`:/project/$(REPO) \
		toolchain:$(ISOLATION_ID)
DOCKER_MVN := $(TOOLCHAIN) mvn -Drevision=$(MAVEN_REVISION) -B

SONAR_HOST_URL ?= https://sonarqube.dev.catenasys.com
SONAR_AUTH_TOKEN ?=
PMD_IMAGE ?= blockchaintp/pmd:latest

export AWS_REGION ?= us-east-1
export AWS_ACCESS_KEY_ID ?=
export AWS_SECRET_ACCESS_KEY ?=

export LEDGER_NAME = $(ISOLATION_ID)

.PHONY: all
all: clean build test archive

.PHONY: check_env
check_env:
	@if [ -z "$$AWS_ACCESS_KEY_ID" ] || \
	[ -z "$$AWS_SECRET_ACCESS_KEY" ]; then \
		echo "Env vars AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set"; \
		exit 1; \
	fi

.PHONY: dirs
dirs:
	mkdir -p build
	mkdir -p test-dars

.PHONY: clean_dirs
clean_dirs:
	rm -rf build test-dars

.PHONY: build
build: build_toolchain
	$(DOCKER_MVN) compile
	$(TOOLCHAIN) chown -R $(UID):$(GID) /root/.m2/repository
	$(TOOLCHAIN) find /project -type d -name target -exec chown \
		-R $(UID):$(GID) {} \;

.PHONY: fix_permissions
fix_permissions: build_toolchain
	$(TOOLCHAIN) chown -R $(UID):$(GID) /root/.m2/repository
	$(TOOLCHAIN) find /project -type d -name target -exec chown \
		-R $(UID):$(GID) {} \;

.PHONY: build_toolchain
build_toolchain: dirs
	docker-compose -f docker/docker-compose-build.yaml build --parallel
	mkdir -p test-dars && \
		docker run --rm -v `pwd`/test-dars:/out \
			ledger-api-testtool:$(ISOLATION_ID) bash \
			-c "java -jar ledger-api-test-tool.jar -x && cp *.dar /out"

.PHONY: package
package: build
	$(DOCKER_MVN) package verify
	$(TOOLCHAIN) chown -R $(UID):$(GID) /root/.m2/repository
	$(TOOLCHAIN) find /project -type d -name target -exec chown \
		-R $(UID):$(GID) {} \;
	docker-compose -f docker-compose.yaml build

.PHONY: test
test: package test_mvn test_daml

.PHONY: test_mvn
	$(DOCKER_MVN) package verify

.PHONY: test_daml_build
test_daml_build:
	docker-compose -f docker/daml-test.yaml build

.PHONY: test_daml
test_daml: check_env
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

.PHONY: clean_test_daml
clean_test_daml:
	docker-compose -p $(ISOLATION_ID) -f docker/daml-test.yaml \
		rm -f || true
	docker-compose -p $(ISOLATION_ID) -f docker/daml-test.yaml down \
		-v || true

.PHONY: analyze
analyze: analyze_sonar

.PHONY: analyze_sonar
analyze_sonar: package
	[ -z "$(SONAR_AUTH_TOKEN)" ] || \
	$(DOCKER_MVN) sonar:sonar \
			-Dsonar.projectKey=$(ORGANIZATION)_$(REPO):$(SAFE_BRANCH_NAME) \
			-Dsonar.projectName="$(ORGANIZATION)/$(REPO) $(SAFE_BRANCH_NAME)" \
			-Dsonar.projectVersion=$(VERSION) \
			-Dsonar.host.url=$(SONAR_HOST_URL) \
			-Dsonar.login=$(SONAR_AUTH_TOKEN)

.PHONY: clean
clean: clean_dirs clean_test_daml clean_aws
	$(DOCKER_MVN) clean || true
	docker-compose -f docker/docker-compose-build.yaml rm -f || true
	docker-compose -f docker/docker-compose-build.yaml down -v || true

.PHONY: clean_aws
clean_aws:
	docker run -e AWS_REGION -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
		--entrypoint /bin/bash ledger-api-testtool:${ISOLATION_ID} \
		-c "source ./aws-configure.sh && aws qldb delete-ledger \
			--name ${ISOLATION_ID}" || true
	docker run -e AWS_REGION -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
		--entrypoint /bin/bash ledger-api-testtool:${ISOLATION_ID} \
		-c "source ./aws-configure.sh && aws s3 rb s3://valuestore-${ISOLATION_ID} \
			--force" || true


.PHONY: archive
archive: dirs
	git archive HEAD --format=zip -9 --output=build/$(REPO)-$(VERSION).zip
	git archive HEAD --format=tgz -9 --output=build/$(REPO)-$(VERSION).tgz

.PHONY: publish
publish: build_toolchain
	$(DOCKER_MVN) -Drevision=0.0.0 versions:set -DnewVersion=$(MAVEN_REVISION)
	$(DOCKER_MVN) clean deploy

.PHONY: all build test clean build-image push-image
.DEFAULT_GOAL := all

IMAGE_PREFIX ?= grafana
IMAGE_TAG := $(shell ./tools/image-tag)

all: test build-image

build:
	CGO_ENABLED=0 go build -o bigtable-backup -v main.go

test:
	go test -v ./...

clean:
	rm -f ./bigtable-backup
	go clean ./...

build-image: build
	docker build -t $(IMAGE_PREFIX)/bigtable-backup .
	docker tag $(IMAGE_PREFIX)/bigtable-backup $(IMAGE_PREFIX)/bigtable-backup:$(IMAGE_TAG)

push-image:
	docker push $(IMAGE_PREFIX)/bigtable-backup:$(IMAGE_TAG)
	docker push $(IMAGE_PREFIX)/bigtable-backup:latest

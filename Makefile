TARGET = main

.PHONY: proto vet lint build

build: vet test
	go build -o ${TARGET} x.

proto:
	protoc \
		--proto_path=proto \
		--go_out=gen/go \
		--go_opt=paths=source_relative \
		proto/*.proto
vet:
	go vet ./...

test:
	go test ./...

test-cover:
	go test -v -cover -coverprofile=c.out ./...

test-cover-html:
	go tool cover -html=c.out ./...
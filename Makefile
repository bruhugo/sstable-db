.PHONY: proto vet build

build: vet
	go build .

proto:
	protoc \
		--proto_path=proto \
		--go_out=gen/go \
		--go_opt=paths=source_relative \
		proto/*.proto
vet:
	go vet ./...
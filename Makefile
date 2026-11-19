PROTO_MODULE := github.com/Nirmal-22/raft-kv

.PHONY: build test test-short race lint proto cluster stop

build:              ## build bin/kvd
	go build -o bin/kvd ./cmd/kvd

test:               ## all tests, including the fault-injection run
	go test ./... -count=1

test-short:         ## skip the fault-injection run
	go test ./... -count=1 -short

race:               ## all tests under the race detector (what CI runs)
	go test ./... -count=1 -race

lint:               ## formatting and go vet
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed on:"; gofmt -l .; exit 1)
	go vet ./...

proto:              ## regenerate internal/pb from proto/kv.proto (needs protoc and the two Go plugins)
	protoc --proto_path=proto \
	  --go_out=. --go_opt=module=$(PROTO_MODULE) \
	  --go-grpc_out=. --go-grpc_opt=module=$(PROTO_MODULE) \
	  proto/kv.proto

cluster:            ## start a local 3-node cluster
	scripts/cluster.sh start

stop:               ## stop the local cluster (scripts/cluster.sh clean also deletes its data)
	scripts/cluster.sh stop

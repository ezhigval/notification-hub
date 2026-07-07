.PHONY: run test lint docker-up docker-down build migrate-up seed

DATABASE_URL ?= postgres://notify:notify@localhost:5437/notify?sslmode=disable

run:
	DATABASE_URL=$(DATABASE_URL) REDIS_ADDR=localhost:6385 KAFKA_BROKERS=localhost:9093 go run ./cmd/server

test:
	go test ./... -race -count=1

lint:
	golangci-lint run ./...

build:
	CGO_ENABLED=0 go build -o bin/server ./cmd/server

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down

migrate-up:
	goose -dir migrations postgres "$(DATABASE_URL)" up

seed:
	curl -s -X POST localhost:8088/api/v1/templates -H 'Content-Type: application/json' \
	  -d '{"name":"order-shipped","channel":"email","subject_template":"Order {{.OrderID}} shipped","body_template":"Hi {{.Name}}, your order is on the way."}' | jq

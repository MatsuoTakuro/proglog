
run_local:
	go run ./cmd/server/main.go

ch1_test: produce consume

produce:
	http POST localhost:18080 \
		record[value]='TGV0J3MgR28gIzEK'
	http POST localhost:18080 \
		record[value]='TGV0J3MgR28gIzIK'
	http POST localhost:18080 \
		record[value]='TGV0J3MgR28gIzMK'

consume:
	http GET localhost:18080 \
		offset:=0
	http GET localhost:18080 \
		offset:=1
	http GET localhost:18080 \
		offset:=2
	http GET localhost:18080 \
		offset:=3

compile: # first, you need to do 'brew install protoc-gen-go'
	protoc ./api/v1/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.

test:
	go test -race ./...
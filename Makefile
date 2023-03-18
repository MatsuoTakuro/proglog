#	dir path you place generated CAs in
CONFIG_PATH=${HOME}/.proglog/

.PHONY: init
init:
	mkdir -p ${CONFIG_PATH}

.PHONY: gencert
gencert:
#	Initialize a new key and cert for your own CA (certification authority)
#	FYI, csr stands for certificate signing request
	cfssl gencert \
		-initca test/ca-csr.json | cfssljson -bare ca

#	Generate a new key and cert for server
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=server \
		test/server-csr.json | cfssljson -bare server

#	Generate a new key and cert for client
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		test/client-csr.json | cfssljson -bare client

#	Generate a new key and cert for root-client (for auth based on ACL)
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		-cn="root" \
		test/client-csr.json | cfssljson -bare root-client

#	Generate a new key and cert for nobody-client (for auth based on ACL)
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		-cn="nobody" \
		test/client-csr.json | cfssljson -bare nobody-client

#	move all pem and csr files to a config dir path
	mv *pem *.csr ${CONFIG_PATH}

show_configs:
	ls -l ${CONFIG_PATH}

gencert_another_ca_and_client: # for testing
	cfssl gencert \
		-initca test/ca-csr_another.json | cfssljson -bare ca_another

	cfssl gencert \
		-ca=ca_another.pem \
		-ca-key=ca_another-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		test/client-csr.json | cfssljson -bare client_another

	mv *pem *.csr ${CONFIG_PATH}

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

.PHONY: compile
compile: # first, you need to do 'brew install protoc-gen-go'
	protoc ./api/v1/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.


$(CONFIG_PATH)/policy.csv:
	cp test/policy.csv $(CONFIG_PATH)/policy.csv

$(CONFIG_PATH)/model.conf:
	cp test/model.conf $(CONFIG_PATH)/model.conf

.PHONY: test
test: $(CONFIG_PATH)/policy.csv $(CONFIG_PATH)/model.conf
	go test -race ./...
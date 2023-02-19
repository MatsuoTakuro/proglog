
run_local:
	go run ./cmd/server/main.go

test_ch1: produce consume

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
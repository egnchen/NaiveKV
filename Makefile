.PHONY: master primary backup client
.PHONY: zookeeper zk-cli
.PHONY: kill-master

zookeeper:
	docker run --name kv-zookeeper --restart always -d eyek/kv-zookeeper:1.0

master:
	go run cmd/master/main.go

primary:
	go run cmd/primary/main.go -path tmp/data${id} -port $(shell expr ${id} + 7900)

backup:
	go run cmd/backup/main.go -path tmp/data_backup${id} -id ${id}

client:
	go run cmd/client/main.go

zk-cli:
	docker run -it --rm --link kv-zookeeper:zookeeper eyek/kv-zookeeper:1.0 zkCli.sh -server zookeeper

kill-master:
	sudo kill -9 $(shell ps aux | grep cmd/master/main.go | head -1 | awk '{print $$2}')
	sudo kill -9 $(shell lsof -t -i:7899)

PROTO_FILES := $(wildcard proto/*.proto)

# some actual build tarets
proto: $(PROTO_FILES)
	cd proto
	protoc *.proto --go_out=plugins=grpc:.

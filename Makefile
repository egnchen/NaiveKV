# some debug & running shorthands
.PHONY: master primary backup client
.PHONY: zookeeper zookeeper-create-network zk-cli
.PHONY: kill-port

zookeeper-create-network:
	docker network create zk

zookeeper:
	docker run --name zk1 --restart always -d -v $(shell pwd)/deploy/zookeeper/zoo1.cfg:/conf/zoo.cfg -e "ZOO_MY_ID=1" -p 2181:2181 --net zk eyek/kv-zookeeper:1.0
	docker run --name zk2 --restart always -d -v $(shell pwd)/deploy/zookeeper/zoo2.cfg:/conf/zoo.cfg -e "ZOO_MY_ID=2" -p 2182:2181 --net zk eyek/kv-zookeeper:1.0
	docker run --name zk3 --restart always -d -v $(shell pwd)/deploy/zookeeper/zoo3.cfg:/conf/zoo.cfg -e "ZOO_MY_ID=3" -p 2183:2181 --net zk eyek/kv-zookeeper:1.0

zk-cli:
	docker run -it --rm --link zk1:zookeeper --net zk eyek/kv-zookeeper:1.0 zkCli.sh -server zookeeper

master:
	go run cmd/master/main.go

primary:
	go run cmd/primary/main.go -id ${id} -path tmp/data${id} -port $(shell expr ${id} + 7900)

backup:
	go run cmd/backup/main.go -id ${id} -path tmp/data_backup${id}

client:
	go run cmd/client/main.go


kill-port:
	sudo kill -9 $(shell lsof -t -i:${port})

# some build targets
PROTO_FILES := $(wildcard proto/*.proto)
.PHONY: proto
proto: $(PROTO_FILES)
	cd proto; protoc *.proto --go_out=plugins=grpc:.

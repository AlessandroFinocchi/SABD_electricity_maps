.PHONY: gen clean query nifi cp_flow

gen:
	docker compose up -d

clean:
	docker compose down -v
	docker images | grep "sabd*" | awk '{print $3}' | xargs docker rmi

cp_flow:
	docker cp nifi:/opt/nifi/nifi-current/conf/flow.json.gz nifi/flow.json.gz

zip_deps:
	zip -r ./spark/src/deps/deps.zip ./spark/src/deps -x "deps.zip"

query1:
	docker exec -it spark-master /opt/spark/bin/spark-submit \
		--py-files /opt/spark/src/deps/deps.zip /opt/spark/src/query1/query1_rdd.py \


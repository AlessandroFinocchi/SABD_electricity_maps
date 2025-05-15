.PHONY: gen clean query

gen:
	docker compose up -d

clean:
	docker compose down
	docker images | grep "sabd*" | awk '{print $3}' | xargs docker rmi # remove all images with name "hadoop*"

query:
	docker exec -it spark-master /opt/spark/bin/spark-submit /opt/spark/query_test.py

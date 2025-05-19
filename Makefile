.PHONY: gen clean query nifi gzip

gen:
	docker compose up -d

clean:
	docker compose down
	docker volume ls -q | xargs docker volume rm
	docker images | grep "sabd*" | awk '{print $3}' | xargs docker rmi

query:
	docker exec -it spark-master /opt/spark/bin/spark-submit /opt/spark/scripts/query_test.py

nifi:
	docker exec -it spark-master /opt/spark/bin/spark-submit /opt/spark/scripts/nifi_runner.py

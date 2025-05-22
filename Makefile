.PHONY: gen clean scrape query nifi cp_flow

gen:
	docker compose up -d

clean:
	docker compose down -v
	docker images | grep "sabd*" | awk '{print $3}' | xargs docker rmi

scrape:
	docker exec -it spark-master /opt/spark/bin/spark-submit /opt/spark/scripts/scraper.py

query:
	docker exec -it spark-master /opt/spark/bin/spark-submit /opt/spark/scripts/query_test.py

nifi:
	docker exec -it spark-master /opt/spark/bin/spark-submit /opt/spark/scripts/nifi_runner.py

cp_flow:
	docker cp nifi:/opt/nifi/nifi-current/conf/flow.json.gz nifi/flow.json.gz

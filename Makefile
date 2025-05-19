.PHONY: gen clean query nifi gzip

gen:
	docker compose up -d

clean:
	docker compose down
	docker images | grep "sabd*" | awk '{print $3}' | xargs docker rmi
	docker volume ls -q | xargs docker volume rm

scrape:
	docker exec -it spark-master /opt/spark/bin/spark-submit /opt/spark/scripts/scraper.py

query:
	docker exec -it spark-master /opt/spark/bin/spark-submit /opt/spark/scripts/query_test.py

nifi:
	docker exec -it spark-master /opt/spark/bin/spark-submit /opt/spark/scripts/nifi_runner.py

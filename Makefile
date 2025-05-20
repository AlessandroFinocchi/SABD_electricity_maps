.PHONY: gen clean scrape query nifi

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

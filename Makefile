.PHONY: gen clean flow deps query

DEPS = (cd spark/src && rm -f deps.zip && zip -r deps.zip *)

SUBMIT_QUERY = $(DEPS) && docker exec -it spark-master /opt/spark/bin/spark-submit \
		--py-files /opt/spark/code/src/deps.zip /opt/spark/code/src/

gen:
	docker compose -p sabd up -d

gen_s:
	@if [ $(words $(MAKECMDGOALS)) -ne 3 ]; then     \
	  echo "Usage: make query <num_spark_workers> <num_datanodes>"; \
	  exit 1; \
	fi; \
	SW=$(word 2,$(MAKECMDGOALS)); \
	D=$(word 3,$(MAKECMDGOALS)); \
	docker compose -p sabd up -d --scale spark-worker=$$SW --scale datanode=$$D

clean:
	docker compose down -v
	docker images | grep "sabd*" | awk '{print $3}' | xargs docker rmi

flow:
	docker cp nifi:/opt/nifi/nifi-current/conf/flow.json.gz nifi/flow.json.gz

deps:
	$(DEPS)

query:
	@if [ $(words $(MAKECMDGOALS)) -ne 4 ]; then     \
	  echo "Usage: make query <num> <api> <format>"; \
	  exit 1; \
	fi; \
	Q=$(word 2,$(MAKECMDGOALS)); \
	API=$(word 3,$(MAKECMDGOALS)); \
	FMT=$(word 4,$(MAKECMDGOALS)); \
	$(SUBMIT_QUERY)main.py --q $$Q --api $$API --format $$FMT

perf:
	@if [ $(words $(MAKECMDGOALS)) -ne 4 ]; then    \
	  echo "Usage: make perf <num> <api> <format>"; \
	  exit 1; \
	fi; \
	Q=$(word 2,$(MAKECMDGOALS)); \
	API=$(word 3,$(MAKECMDGOALS)); \
	FMT=$(word 4,$(MAKECMDGOALS)); \
	$(SUBMIT_QUERY)benchmark_runner.py --q $$Q --api $$API --format $$FMT
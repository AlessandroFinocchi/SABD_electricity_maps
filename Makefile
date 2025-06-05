.PHONY: gen clean flow deps query

#DEPS = rm -f spark/src/deps.zip && mkdir temp && mkdir temp/env && \
#       cp -r influxdb/env/influxdb2-admin-token temp/env && \
#       cp -r spark/src/deps temp/ && \
#       cp -r spark/src/query1 temp/ && \
#       cp -r spark/src/query2 temp/ && \
#       cd temp && zip -r ../spark/src/deps.zip * && \
#       cd .. && rm -rf temp

DEPS = cd spark/src && \
	   rm -f deps.zip && \
	   zip -r deps.zip * && \
	   mkdir -p env && \
	   cp ../../influxdb/env/influxdb2-admin-token ./env/

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
	docker compose -p sabd down -v
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

all_perf:
	$(SUBMIT_QUERY)benchmark_runner.py --q 1 --api rdd --format csv
	$(SUBMIT_QUERY)benchmark_runner.py --q 1 --api df  --format csv
	$(SUBMIT_QUERY)benchmark_runner.py --q 1 --api sql --format csv
	$(SUBMIT_QUERY)benchmark_runner.py --q 2 --api rdd --format csv
	$(SUBMIT_QUERY)benchmark_runner.py --q 2 --api df --format csv
	$(SUBMIT_QUERY)benchmark_runner.py --q 2 --api sql --format csv
.PHONY: gen clean cp_flow deps

DEPS = (cd spark/src && rm -f deps.zip && zip -r deps.zip *)

SUBMIT_QUERY = $(DEPS) && docker exec -it spark-master /opt/spark/bin/spark-submit \
		--py-files /opt/spark/code/src/deps.zip /opt/spark/code/src/

gen:
	docker compose up -d

clean:
	docker compose down -v
	docker images | grep "sabd*" | awk '{print $3}' | xargs docker rmi

flow:
	docker cp nifi:/opt/nifi/nifi-current/conf/flow.json.gz nifi/flow.json.gz

deps:
	$(DEPS)

query1_rdd_csv:
	$(SUBMIT_QUERY)main.py --q 1 --api rdd --format csv

query1_df_csv:
	$(SUBMIT_QUERY)main.py --q 1 --api df --format csv

query1_sql_csv:
	$(SUBMIT_QUERY)main.py --q 1 --api sql --format csv

query1_rdd_parquet:
	$(SUBMIT_QUERY)main.py --q 1 --api rdd --format parquet

query1_df_parquet:
	$(SUBMIT_QUERY)main.py --q 1 --api df --format parquet

query1_sql_parquet:
	$(SUBMIT_QUERY)main.py --q 1 --api sql --format parquet
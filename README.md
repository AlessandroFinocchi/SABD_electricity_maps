# SABD project: electricity maps analysis

## Usage
1. First, compile spark job dependencies
```sh
    make deps
```

2. Compose docker services with scaling positive integers parameters num_spark_workers 
and num_datanodes 
```sh
    make gen_s <num_spark_workers> <num_datanodes>
```

3. Run queries with parameters where
- num $ \in \{1,2,3\} $
- api $\in \{ \texttt{rdd}, \texttt{df}, \texttt{sql} \}$
- format $\in \{ \texttt{csv}, \texttt{parquet} \}$
```
    make query <num> <api> <format>
```
4. Get performance metrics on InfluxDB
```
    make perf <num> <api> <format>
```

4. Eventually compose down docker services
```sh
    make clean
```
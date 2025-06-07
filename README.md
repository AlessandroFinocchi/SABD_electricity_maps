# SABD project: Electricity Maps analysis

## Usage
1. First, Compose docker services with `make gen`. It's also possible to compose docker 
services with scaling parameter
   - num_spark_workers $\in$ \{ $1, 2, \dots$ \}
   - num_datanodes $\in$ \{ $1, 2, \dots 8$ \}
    ```
    make gen_s <num_spark_workers> <num_datanodes>
    ```
   
2. Run queries with parameters where
   - num $\in$ \{ $1,2$ \}
   - api $\in$ \{ $\texttt{rdd}$, $\texttt{df}$, $\texttt{sql}$ \}
   - format $\in$ \{ $\texttt{csv}$, $\texttt{parquet}$ \}
    ```
    make query <num> <api> <format>
    ```
   
3. Run performance evaluation on all queries with `make all_perf`. It's also possible
to run performance on a single query with a certain configuration using
    ```
    make perf <num> <api> <format>
    ```

4. Eventually compose down docker services `make clean`

Performance experiments have been computed on a personal computer 
(1 processor i7-1260P with 12 cores and 16 GB RAM LPDDR5 (Dual Channel, 5200 MHz).

# SABD project: electricity maps analysis

## Usage
1. First compile spark job dependencies using the command
```sh
    make deps
```

2. Compose docker services
```sh
    make gen
```

3. Run queries with parameters where
- num $\in \{1,2,3\}$
- api $\in \{ \texttt{rdd}, \texttt{df}, \texttt{sql} \}$
- format $\in \{ \texttt{csv}, \texttt{parquet} \}$
```
    make query <num> <api> <format>
```

4. Eventually compose down docker services
```sh
    make clean
```
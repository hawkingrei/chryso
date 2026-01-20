# Example Queries

## Basic Select
```
select id, name from users where id = 42;
```

## Aggregation
```
select region, sum(amount) as total
from sales
where region = 'us'
group by region
order by total desc
limit 10;
```

## Join
```
select t1.id, t2.name
from t1 join t2 on t1.id = t2.id
where t1.id > 10;
```

## Distinct + Offset
```
select distinct id from users order by id offset 5;
```

## DuckDB Demo
```
cargo run --example duckdb_demo --features duckdb
```

create view dim as select * from "dim_customer.parquet";


SELECT f.ds, f.customer_id, d.segment, SUM(f.total) AS gross
    FROM "raw/orders_*.parquet" f
    JOIN dim d USING (customer_id)
    WHERE f.ds BETWEEN DATE '2025-09-01' AND DATE '2025-09-15'
    GROUP BY ALL;


select * from "out/orders_2025-10.parquet";

select * from "dim_customer.parquet";

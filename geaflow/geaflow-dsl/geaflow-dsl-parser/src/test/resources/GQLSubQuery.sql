Match (a where a.id = '1') -[e]->(b)
Where Exists (b) -> (c)
;

Match (a) -[e]->(b)
Where SUM((b) ->(d) => d.amt) > 100
;
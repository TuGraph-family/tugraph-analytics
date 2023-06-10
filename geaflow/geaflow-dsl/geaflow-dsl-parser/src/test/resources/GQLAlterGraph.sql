
Alter Graph g Add Vertex item (
  item_id bigint ID,
  price double
);


Alter Graph g Add Edge collected (
  b_id bigint SOURCE ID,
  t_id bigint DESTINATION ID,
  ts bigint TIMESTAMP,
  weight double
);
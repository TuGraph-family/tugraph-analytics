CREATE TABLE tbl_result (
  _age bigint,
  _lang varchar,
  _wgt1 double
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	_age,
	_lang,
	_wgt1
FROM (
  MATCH (a:person WHERE id = 1)-[e]->(b)
  RETURN SUM(b.age) as _age, MAX(b.lang) as _lang, AVG(e.weight) as _wgt1
  GROUP BY e, b.id
  ORDER BY _age
)

CREATE TABLE tbl_result (
  a_id bigint,
  b_id bigint,
  a_age bigint,
  a_lang varchar,
  b_age bigint,
  b_lang varchar
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a.id,
	b.id,
	a_age,
	a_lang,
	b_age,
  b_lang
FROM (
  MATCH
       (a:person)-(b:software) | (a:software)-(b:person)
       |+| (a:person)-(b:software)
  RETURN distinct a, b, a.age as a_age, a.lang as a_lang, b.age as b_age, b.lang as b_lang
)
;
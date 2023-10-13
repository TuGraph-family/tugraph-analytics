CREATE TABLE IF NOT EXISTS join_to_match_001_result (
	s_id bigint,
	c_id bigint,
  c_name varchar,
  t_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO join_to_match_001_result
SELECT sc.srcId, c.id, c.name, t.id
FROM selectCourse sc, course c, hasTeacher, teacher t
WHERE c.id = sc.targetId AND c.id = hasTeacher.srcId
AND hasTeacher.targetId = t.id AND sc.srcId < 1004
ORDER BY sc.srcId, sc.targetId
;
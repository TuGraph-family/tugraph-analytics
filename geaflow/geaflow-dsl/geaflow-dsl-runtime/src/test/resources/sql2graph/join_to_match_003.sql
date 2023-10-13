CREATE TABLE IF NOT EXISTS join_to_match_003_result (
	s_id bigint,
	c_id bigint,
  c_name varchar,
  t_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO join_to_match_003_result
SELECT s.id, c.id, c.name, hasTeacher.targetId
FROM hasTeacher, course c, selectCourse sc, student s
WHERE c.id = sc.targetId AND c.id = hasTeacher.srcId AND s.id = sc.srcId AND sc.srcId < 1004
ORDER BY sc.srcId, sc.targetId
;
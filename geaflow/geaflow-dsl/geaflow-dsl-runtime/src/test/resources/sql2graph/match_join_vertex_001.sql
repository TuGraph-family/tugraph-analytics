CREATE TABLE IF NOT EXISTS match_join_vertex_001_result (
	s_id bigint,
	c_id bigint,
  c_name varchar
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO match_join_vertex_001_result
SELECT s.id, sc.targetId, c.name
FROM course c JOIN selectCourse sc ON c.id = sc.targetId
JOIN student s ON sc.srcId = s.id
WHERE srcId < 1004
ORDER BY srcId, targetId
;
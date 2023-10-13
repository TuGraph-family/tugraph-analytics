CREATE TABLE IF NOT EXISTS match_join_vertex_003_result (
	s_id bigint,
	c_id bigint,
  c_name varchar
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO match_join_vertex_003_result
SELECT s.id, sc.targetId, c.name
FROM selectCourse sc, course c, student s
WHERE s.id = sc.srcId AND c.id = sc.targetId AND srcId < 1004
ORDER BY srcId, targetId
;
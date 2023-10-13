CREATE TABLE IF NOT EXISTS edge_join_vertex_001_result (
	s_id bigint,
	c_id bigint,
	ts bigint,
  c_id0 bigint,
  c_name varchar
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO edge_join_vertex_001_result
SELECT sc.srcId, sc.targetId, sc.ts, c.id, c.name
FROM course c JOIN selectCourse sc on c.id = sc.targetId
WHERE srcId < 1004
ORDER BY srcId, targetId;
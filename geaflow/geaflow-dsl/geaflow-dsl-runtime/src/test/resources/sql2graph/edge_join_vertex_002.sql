CREATE TABLE IF NOT EXISTS edge_join_vertex_002_result (
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

INSERT INTO edge_join_vertex_002_result
SELECT sc.srcId, sc.targetId, sc.ts, c.id, c.name
FROM course c, selectCourse sc
WHERE c.id = sc.targetId AND srcId < 1004
ORDER BY srcId, targetId;
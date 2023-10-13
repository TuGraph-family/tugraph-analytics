CREATE TABLE IF NOT EXISTS vertex_join_edge_001_result (
	s_id bigint,
	c_id bigint,
	ts bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO vertex_join_edge_001_result
SELECT s.id, sc.targetId, sc.ts FROM student s JOIN selectCourse sc on s.id = sc.srcId
WHERE s.id < 1004
ORDER BY s.id, targetId
;
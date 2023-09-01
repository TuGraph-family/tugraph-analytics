CREATE TABLE IF NOT EXISTS tbl_result_02 (
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

INSERT INTO tbl_result_02
SELECT sc.srcId, sc.targetId, sc.ts, c.id, c.name
FROM course c, selectCourse sc
WHERE c.id = sc.targetId AND srcId < 1004
ORDER BY srcId, targetId;
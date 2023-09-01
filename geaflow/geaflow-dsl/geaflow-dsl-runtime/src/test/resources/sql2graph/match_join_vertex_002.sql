CREATE TABLE IF NOT EXISTS tbl_result_10 (
	s_id bigint,
	c_id bigint,
  c_name varchar
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO tbl_result_10
SELECT s.id, sc.targetId, c.name
FROM student s, selectCourse sc, course c
WHERE s.id = sc.srcId AND c.id = sc.targetId AND srcId < 1004
ORDER BY srcId, targetId
;
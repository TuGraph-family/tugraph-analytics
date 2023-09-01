CREATE TABLE IF NOT EXISTS tbl_result_08 (
	s_id bigint,
	c_id bigint,
  c_name varchar,
  t_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO tbl_result_08
SELECT s.id, sc.targetId, c.name, hasTeacher.targetId
FROM student s, selectCourse sc, course c, hasTeacher
WHERE s.id = sc.srcId AND c.id = sc.targetId AND c.id = hasTeacher.srcId AND sc.srcId < 1004
ORDER BY sc.srcId, sc.targetId
;
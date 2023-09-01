CREATE TABLE IF NOT EXISTS tbl_result_03 (
	s_id bigint,
	c_id bigint,
  c_name varchar,
  t_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO tbl_result_03
SELECT sc.srcId, c.id, c.name, t.id
FROM selectCourse sc, course c, hasTeacher, teacher t
WHERE c.id = sc.targetId AND c.id = hasTeacher.srcId
AND hasTeacher.targetId = t.id AND sc.srcId < 1004
ORDER BY sc.srcId, sc.targetId
;
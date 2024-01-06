CREATE TABLE IF NOT EXISTS match_join_edge_001_result (
	s_id bigint,
	c_id bigint,
  c_name varchar,
  t_name varchar
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO match_join_edge_001_result
SELECT sc.srcId as student, sc.targetId as courseId, c.name as course, t.name as teacherName
FROM teacher t, hasTeacher, course c, selectCourse sc
WHERE c.id = sc.targetId AND c.id = hasTeacher.srcId AND t.id = hasTeacher.targetId
ORDER BY student, courseId
;
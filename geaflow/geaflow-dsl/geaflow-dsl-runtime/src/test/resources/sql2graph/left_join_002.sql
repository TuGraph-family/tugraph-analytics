CREATE TABLE IF NOT EXISTS left_join_002_result (
	f0 bigint,
	f1 bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO left_join_002_result
SELECT student.id as sid, selectCourse.targetId as cid from
student LEFT JOIN selectCourse
on student.id = selectCourse.srcId
order by cid desc, sid
;
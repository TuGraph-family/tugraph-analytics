CREATE TABLE IF NOT EXISTS join_to_match_011_result (
	studentId bigint,
	selectSrc bigint,
	selectTarget bigint,
  courseId bigint,
  cnt bigint,
  teacherId bigint,
  teacherId2 bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO join_to_match_011_result
SELECT studentId, selectSrc, selectTarget, courseId, teacherId, cnt, teacher.id as teacherId2
FROM
(
SELECT s.id as studentId, sc.srcId as selectSrc, sc.targetId as selectTarget, c.id as courseId,
hasTeacher.targetId as teacherId, s.id * 100000000 + c.id * 10000 + hasTeacher.targetId as cnt
FROM student s, selectCourse sc, course c, hasTeacher
WHERE s.id = sc.srcId AND c.id = sc.targetId AND c.id = hasTeacher.srcId
) tableA, teacher
WHERE tableA.teacherId = teacher.id
ORDER BY studentId, selectSrc, selectTarget, courseId, teacherId, cnt
;
CREATE TABLE IF NOT EXISTS join_to_match_010_result (
	studentId bigint,
	selectSrc bigint,
	selectTarget bigint,
  courseId bigint,
  cnt bigint,
  teacherId bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO join_to_match_010_result
SELECT studentId, selectSrc, selectTarget, courseId, cnt, hasTeacher.targetId as teacherId
FROM (
SELECT s.id as studentId, sc.srcId as selectSrc, sc.targetId as selectTarget, c.id as courseId,
s.id * 10000 + c.id as cnt
FROM student s, selectCourse sc, course c
WHERE s.id = sc.srcId AND c.id = sc.targetId
), hasTeacher
WHERE courseId = hasTeacher.srcId
ORDER BY studentId, selectSrc, selectTarget, courseId, cnt, teacherId
;
CREATE TABLE IF NOT EXISTS aggregate_to_match_002_result (
	sId bigint,
	courseNum bigint,
  teacherId bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO aggregate_to_match_002_result
SELECT * FROM (
SELECT sId, teacherId, MAX(student.id) as sId2
FROM (
SELECT DISTINCT student.id as sId, teacher.id as teacherId
FROM student, selectCourse, course, hasTeacher, teacher
WHERE student.id = selectCourse.srcId AND course.id = selectCourse.targetId
  AND course.id = hasTeacher.srcId AND teacher.id = hasTeacher.targetId
), hasMonitor, student
WHERE teacherId = hasMonitor.targetId AND hasMonitor.srcId = student.id
GROUP BY sId, teacherId
)
WHERE sId = sId2
;
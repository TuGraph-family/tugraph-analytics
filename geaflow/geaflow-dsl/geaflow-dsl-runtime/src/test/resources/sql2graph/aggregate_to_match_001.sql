CREATE TABLE IF NOT EXISTS aggregate_to_match_001_result (
	sId bigint,
	sName varchar,
	courseNum bigint,
  teacherId bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO aggregate_to_match_001_result
SELECT sId, sName, courseNum, teacher.id as teacherId
FROM (
SELECT student.id as sId, student.name as sName, student.age, COUNT(DISTINCT course.id) as courseNum
FROM student, selectCourse, course
WHERE student.id = selectCourse.srcId AND course.id = selectCourse.targetId
GROUP BY student.id, student.name, student.age
), hasMonitor, teacher
WHERE sId = hasMonitor.srcId AND teacher.id =  hasMonitor.targetId
ORDER BY courseNum DESC, sId
;
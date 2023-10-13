CREATE TABLE IF NOT EXISTS join_to_match_005_result (
	studentId1 bigint,
	studentName varchar,
  teacherId1 bigint,
  teacherName varchar
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO join_to_match_005_result
SELECT studentId1, studentName, teacherId1, teacherName
FROM
(
    SELECT studentId1, studentName, studentAge, m.srcId as studentId2, m.targetId as teacherId1
    FROM
        (
        SELECT s.id as studentId1, concat('Student-', s.name) as studentName, s.age as studentAge
        FROM student s
        ), hasMonitor m
    WHERE studentId1 = m.srcId AND studentAge > 0
)
, (
    SELECT t.id as teacherId2, concat('Monitor-', t.name) as teacherName
    FROM teacher t
)
WHERE
teacherId1 = teacherId2
ORDER BY studentId1, teacherId1
;
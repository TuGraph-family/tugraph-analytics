CREATE TABLE IF NOT EXISTS match_join_edge_005_result (
	s_id bigint,
	c_id bigint,
	m_id bigint,
  f_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO match_join_edge_005_result
SELECT studentId1, courseId, monitorId, friendId
FROM
(
SELECT studentId1, courseId * 10 as courseId FROM (
SELECT s.id as studentId1, sc.targetId as courseId
FROM student s, selectCourse sc
WHERE s.id = sc.srcId AND sc.targetId > 2004
)), (
SELECT studentId2, monitorId * 100 as monitorId FROM (
SELECT s.id as studentId2, m.targetId as monitorId
FROM student s, hasMonitor m
WHERE s.id = m.srcId AND m.targetId > 9002
)), (
SELECT studentId3, friendId * 1000 as friendId FROM (
SELECT s.id as studentId3, k.targetId as friendId
FROM student s, knows k
WHERE s.id = k.srcId AND k.targetId > 1003
))
WHERE
studentId1 = studentId2 AND studentId2 = studentId3
ORDER BY studentId1, courseId, monitorId, friendId
;
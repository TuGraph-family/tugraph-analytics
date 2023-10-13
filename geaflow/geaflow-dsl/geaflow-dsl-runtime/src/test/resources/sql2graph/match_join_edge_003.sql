CREATE TABLE IF NOT EXISTS match_join_edge_003_result (
	s_id bigint,
	c_id bigint,
  m_id bigint,
  k_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH g_student;

INSERT INTO match_join_edge_003_result
SELECT s.id, sc.targetId as sc_target, m.targetId as m_target, k.targetId as k_target
FROM student s, selectCourse sc, hasMonitor m, knows k
WHERE s.id = sc.srcId AND s.id = m.srcId AND s.id = k.srcId AND sc.srcId < 1004
ORDER BY s.id, sc_target, m_target, k_target
;

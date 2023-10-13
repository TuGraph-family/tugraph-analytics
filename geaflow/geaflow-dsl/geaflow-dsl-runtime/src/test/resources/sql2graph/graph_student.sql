CREATE GRAPH IF NOT EXISTS g_student (
  Vertex student (
    id bigint ID,
    name varchar,
    age int
  ),
  Vertex course (
    id bigint ID,
    name varchar,
    course_hour int
  ),
  Vertex teacher (
    id bigint ID,
    name varchar
  ),
  Vertex gradeClass (
    id bigint ID,
    grade bigint,
    classNumber bigint
  ),
  Edge selectCourse (
    srcId from student SOURCE ID,
    targetId from course DESTINATION ID,
    ts  bigint
  ),
  Edge hasMonitor (
    srcId from student SOURCE ID,
    targetId from teacher DESTINATION ID
  ),
  Edge knows (
    srcId from student SOURCE ID,
    targetId from student DESTINATION ID
  ),
  Edge hasTeacher (
    srcId from course SOURCE ID,
    targetId from teacher DESTINATION ID
  )
) WITH (
	storeType='rocksdb',
	shardCount = 2
);

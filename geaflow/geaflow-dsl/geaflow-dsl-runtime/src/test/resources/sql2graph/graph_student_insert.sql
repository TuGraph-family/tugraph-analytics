CREATE TABLE IF NOT EXISTS v_student (
  id bigint,
  name varchar,
  age int
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/student.txt'
);

CREATE TABLE IF NOT EXISTS v_course (
  id bigint,
  name varchar,
  course_hour int
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/course.txt'
);

CREATE TABLE IF NOT EXISTS v_teacher (
  id bigint,
  name varchar
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/teacher.txt'
);

CREATE TABLE IF NOT EXISTS e_selectCourse (
  srcId bigint,
  targetId bigint,
  ts   bigint
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/selectCourse.txt'
);

CREATE TABLE IF NOT EXISTS e_hasTeacher (
  srcId bigint,
  targetId bigint
) WITH (
	type='file',
	geaflow.dsl.file.path = 'resource:///data/hasTeacher.txt'
);

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
  Edge selectCourse (
    srcId from student SOURCE ID,
    targetId from course DESTINATION ID,
    ts  bigint
  ),
  Edge hasTeacher (
    srcId from course SOURCE ID,
    targetId from teacher DESTINATION ID
  )
) WITH (
	storeType='rocksdb',
	shardCount = 2
);

INSERT INTO g_student.student SELECT * FROM v_student;
INSERT INTO g_student.course SELECT * FROM v_course;
INSERT INTO g_student.teacher SELECT * FROM v_teacher;
INSERT INTO g_student.selectCourse SELECT * FROM e_selectCourse;
INSERT INTO g_student.hasTeacher SELECT * FROM e_hasTeacher;

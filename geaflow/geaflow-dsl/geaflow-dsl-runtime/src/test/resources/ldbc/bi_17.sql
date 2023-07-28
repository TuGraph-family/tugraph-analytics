CREATE GRAPH bi (
  --static
  --Place
  Vertex Country (
    id bigint ID,
    name varchar,
    url varchar
  ),
  Vertex City (
    id bigint ID,
    name varchar,
    url varchar
  ),
  Vertex Continent (
    id bigint ID,
    name varchar,
    url varchar
  ),
  --Organisation
  Vertex Company (
    id bigint ID,
    name varchar,
    url varchar
  ),
  Vertex University (
    id bigint ID,
    name varchar,
    url varchar
  ),
  --Tag
	Vertex TagClass (
	  id bigint ID,
	  name varchar,
	  url varchar
	),
	Vertex Tag (
	  id bigint ID,
	  name varchar,
	  url varchar
	),

  --dynamic
  Vertex Person (
    id bigint ID,
    creationDate bigint,
    firstName varchar,
    lastName varchar,
    gender varchar,
    --birthday Date,
    --email {varchar},
    --speaks {varchar},
    browserUsed varchar,
    locationIP varchar
  ),
  Vertex Forum (
    id bigint ID,
    creationDate bigint,
    title varchar
  ),
  --Message
  Vertex Post (
    id bigint ID,
    creationDate bigint,
    browserUsed varchar,
    locationIP varchar,
    content varchar,
    length bigint,
    lang varchar,
    imageFile varchar
  ),
  Vertex Comment (
    id bigint ID,
    creationDate bigint,
    browserUsed varchar,
    locationIP varchar,
    content varchar,
    length bigint
  ),

  --relations
  --static
	Edge isLocatedIn (
	  srcId bigint SOURCE ID,
	  targetId bigint DESTINATION ID
	),
	Edge isPartOf (
	  srcId bigint SOURCE ID,
	  targetId bigint DESTINATION ID
	),
  Edge isSubclassOf (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID
  ),
  Edge hasType (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID
  ),

  --dynamic
	Edge hasModerator (
	  srcId bigint SOURCE ID,
	  targetId bigint DESTINATION ID
	),
	Edge containerOf (
	  srcId bigint SOURCE ID,
	  targetId bigint DESTINATION ID
	),
	Edge replyOf (
	  srcId bigint SOURCE ID,
	  targetId bigint DESTINATION ID
	),
	Edge hasTag (
	  srcId bigint SOURCE ID,
	  targetId bigint DESTINATION ID
	),
  Edge hasInterest (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID
  ),
  Edge hasCreator (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID
  ),
  Edge workAt (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID,
    workForm bigint
  ),
  Edge studyAt (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID,
    classYear bigint
  ),

  --temporary
  Edge hasMember (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID,
    creationDate bigint
  ),
  Edge likes (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID,
    creationDate bigint
  ),
  Edge knows (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID,
    creationDate bigint
  )
) WITH (
  	storeType='rocksdb'
 );

USE GRAPH bi;

CREATE TABLE tbl_result (
  person1Id bigint,
  messageCount bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

--GQL
INSERT INTO tbl_result
WITH p AS (
    MATCH (tag:Tag where name = 'Huang Bo') RETURN tag.id AS startId
)
Match (:Tag where id = p.startId)<-[:hasTag]-(message2:Post|Comment)
                                         -[:replyOf]->{0,}(post2:Post)
                                         <-[:containerOf]-(forum2:Forum)
    , (:Tag where id = p.startId)<-[:hasTag]-(message1:Post|Comment)
                                         -[:hasCreator]->(person1:Person)
WHERE message2.creationDate > message1.creationDate + 1728000000
  AND COUNT((forum2:Forum)-[:hasMember]->(person1_cyc:Person where id = person1.id)
                                => person1_cyc) = 0
Match (:Tag where id = p.startId)<-[:hasTag]-(comment:Comment)
                                         -[:hasCreator]->(person2:Person)
                                         <-[:hasMember]-(forum1:Forum)
WHERE forum1.id <> forum2.id
Match (:Tag where id = p.startId)<-[:hasTag]-(comment:Comment)
                                         -[:replyOf]->(message2:Post|Comment)
                                         -[:hasCreator]->(person3:Person)
                                         <-[:hasMember]-(forum1:Forum)
WHERE person2.id <> person3.id
Match (:Tag where id = p.startId)<-[:hasTag]-(message1:Post|Comment)
                                         -[:replyOf]->{0,}(post1:Post)
                                         <-[:containerOf]-(forum1:Forum)
RETURN person1.id as person1Id, COUNT(DISTINCT message2.id) as messageCount
GROUP BY person1Id
ORDER BY messageCount DESC, person1Id LIMIT 10
;

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
  person2Id bigint,
  city1Name String,
  score bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO tbl_result
MATCH (:Country where name = 'Belarus')<-[:isPartOf]-(city1:City)
                                       <-[:isLocatedIn]-(person1:Person)
                                       -[:knows]-(person2:Person)
    , (person2:Person)-[:isLocatedIn]->(:City)
                      -[:isPartOf]->(:Country where name = 'China')
    , (person2:Person)<-[:hasCreator]-(:Post|Comment)
                      <-[:replyOf]-(case1:Comment)
                      -[:hasCreator]->(person1)
  |+| (person2:Person)<-[:hasCreator]-(:Comment)
                      -[:replyOf]->(case2:Post|Comment)
                      -[:hasCreator]->(person1)
  |+| (person2:Person)<-[:hasCreator]-(case3:Post|Comment)
                      <-[:likes]-(person1)
  |+| (person2:Person)-[:likes]->(case4:Post|Comment)
                      -[:hasCreator]->(person1)
RETURN person1.id as person1Id, person2.id as person2Id,
       city1.name as cityName,
       SUM(
       case when case1 is not null then 4 else 0 end
     + case when case2 is not null then 1 else 0 end
     + case when case3 is not null then 10 else 0 end
     + case when case4 is not null then 1 else 0 end
       ) as score
GROUP BY person1Id, person2Id, cityName
ORDER BY score DESC, person1Id, person2Id LIMIT 100
;
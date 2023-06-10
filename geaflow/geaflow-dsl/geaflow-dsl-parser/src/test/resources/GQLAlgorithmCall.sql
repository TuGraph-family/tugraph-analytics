CALL SSSP(1, 0) yield (vid, distance)
RETURN vid, distance;

CALL SSSP('test') yield (vid, distance)
RETURN MIN(distance) GROUP BY vid;

CALL PAGERANK( ) yield (vid, distance)
RETURN MIN(distance) GROUP BY vid;

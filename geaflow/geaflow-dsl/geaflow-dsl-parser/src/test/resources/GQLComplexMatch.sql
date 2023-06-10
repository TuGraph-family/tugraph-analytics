Match (a) - (b) | (c) - (d)  RETURN a.name;

Match (a) - (b) |+| (c) - (d)  RETURN a.name;

Match (a) - (b) | (a)  RETURN a.name;

Match (a) | (b) |+| (c), (b) - (c), (a) - (b) |+| (c) - (d) RETURN a.name;

Match(a where id = '1') -[e]-> (b) | (d) - (e)
Let a AS Vertex {
  id = a.id,
  cnt = a.cnt * 2
},
Let e AS Edge {
  srcId = e.srcId,
  targetId = e.targetId,
  weight = e.weight * 10
}
Match(b) -[e2] - (c) |+| (f)
Return a.id, a.cnt, e.weight, b.id
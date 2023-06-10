Match(a where id = '1') -[e]-> (b)
Let a.cnt = a.cnt * 2,
Let e.weight = e.weight * 10
Match(b) -[e2] - (c)
Return a.id, a.cnt, e.weight, b.id
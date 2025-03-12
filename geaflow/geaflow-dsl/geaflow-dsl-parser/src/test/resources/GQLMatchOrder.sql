/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

MATCH (a:person where id = 1)-[e:knows where e.weight > 0.4]->(b:person)
Order by a, e, b RETURN a;

Match (a WHERE name = 'marko')<-[e]-(b) WHERE a.name <> b.name
Order by a.id, a.name, weight, b.id
RETURN e;

Match (a:person|animal|device WHERE name = 'where')-[e]-(b)
Order by b, e, a
RETURN b;

Match (a WHERE name = 'match')-[e]->(b)
Order by a ASC, b desc
RETURN a.name;

Match (a WHERE name = 'knows')<-[e]->(b)
Order by a.id desc, a.name desc, weight desc, b.id desc
RETURN e.test;

MATCH (a)->(b) - (c), (a) -> (d) <- (f)
Order by a, e, b limit 10
RETURN a, b;

MATCH (a)<-(b) <->(c), (c) -> (d) - (f)
limit 10
RETURN b, c, f;


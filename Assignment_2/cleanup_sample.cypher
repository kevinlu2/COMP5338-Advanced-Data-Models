//drop graph
MATCH (n)
DETACH DELETE n;

match ()-[r]->() delete r;
//drop index
DROP INDEX ON :Person(name);

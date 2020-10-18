//drop graph
MATCH (n)
DETACH DELETE n;

match ()-[r]->() delete r;
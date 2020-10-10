//run a query to find a person's grandchildren
MATCH (gp:Person{name:"Michael"})<-[*2]-(gc:Person)
RETURN gp.name as grandparent, collect(gc.name) as grandchildren;

//run another query to find the number of children each person has
MATCH (p:Person)<-[]-(cp:Person)
RETURN p.name as person, count(cp) as children;

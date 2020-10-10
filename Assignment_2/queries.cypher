//run a query to find a person's grandchildren
MATCH (gp:Person{name:"Michael"})<-[*2]-(gc:Person)
RETURN gp.name as grandparent, collect(gc.name) as grandchildren;

//run another query to find the number of children each person has
MATCH (p:Person)<-[]-(cp:Person)
RETURN p.name as person, count(cp) as children;

//Get general tweets
MATCH (n:tweets) WHERE NOT EXISTS(n.retweet_id) and NOT EXISTS(n.replyto_id) RETURN count(n)

MATCH (t: tweets)
WHERE (t)<-[:REPLY]-() AND NOT EXISTS (t.replyto_id) AND  (t)<-[:RETWEET]-() AND NOT EXISTS (t.retweet_id)
RETURN COUNT(*)

//Question 1
MATCH (t: tweets)
WHERE NOT (t)--() AND
NOT EXISTS (t.retweet_id) AND 
NOT EXISTS(t.replyto_id)
RETURN count(t);

//Question 2
MATCH (t:tweets)
WHERE EXISTS (t.hash_tags) AND EXISTS (t.replyto_id) OR (NOT EXISTS (t.retweet_id) AND NOT EXISTS (t.replyto_id))
RETURN coalesce(t.hash_tags) as hash_tag, count(*) AS occurences
ORDER BY occurences DESC
LIMIT 5
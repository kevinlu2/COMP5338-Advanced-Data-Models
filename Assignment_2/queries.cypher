
// Question 1
MATCH (t: tweets)
WHERE NOT (t)--() AND
NOT EXISTS (t.retweet_id) AND 
NOT EXISTS(t.replyto_id)
RETURN count(t);

// Question 2
MATCH (t:tweets)
WHERE NOT (t)<-[:REPLY]-() AND NOT (t)<-[:RETWEET]-()
RETURN t.hash_tags as hash_tag, count(t.hash_tags) AS occurences
ORDER BY occurences DESC
LIMIT 5;

// Question 3
MATCH ()<-[:RETWEET|:REPLY]-(t)
RETURN t.id, COUNT(t) AS OCURANCES
ORDER BY OCURANCES DESC;

// Question 4
MATCH (t:tweets), (c:tweets) 
WHERE (c)<-[:RETWEET|:REPLY]-(t)
RETURN t.id, COUNT(DISTINCT c.user_id) AS OCURANCES
ORDER BY OCURANCES DESC
LIMIT 1;

// Question 5
MATCH path = (:tweets)-[:REPLY|RETWEET*]->(next)
WHERE not((next)-[:REPLY|RETWEET]->())
RETURN length(path), [t in nodes(path) | t.id] AS Tweet_IDs_in_Path
ORDER BY length(path) DESC
LIMIT 1;
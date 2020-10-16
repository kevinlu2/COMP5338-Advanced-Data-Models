
// Question 1
MATCH (t: tweets)
WHERE NOT (t)-[*]->() AND
NOT EXISTS (t.retweet_id) AND 
NOT EXISTS(t.replyto_id)
RETURN count(t);

// Question 2 UP
MATCH (h:tweets)-[:TAGGED]->(t:tweets)
WHERE NOT (t)<-[:RETWEET]-()
RETURN h.text as hash_tag, count(h.text) AS occurences
ORDER BY occurences DESC
LIMIT 5;

// Question 3
MATCH ()<-[:RETWEET|:REPLY]-(t)
RETURN t.id AS Tweet_ID, COUNT(t) AS Decendent_Count
ORDER BY Decendent_Count DESC
LIMIT 1;

// Question 4
MATCH (c:tweets)<-[:RETWEET|:REPLY]-(t:tweets)
RETURN t.id, COUNT(DISTINCT c.user_id) AS OCURANCES
ORDER BY OCURANCES DESC
LIMIT 1;

// Question 5
MATCH path = (:tweets)-[:REPLY|RETWEET*]->(next)
WHERE not((next)-[:REPLY|RETWEET]->())
RETURN length(path) AS path_length, [t in nodes(path) | t.id] AS Tweet_IDs_in_Path
ORDER BY length(path) DESC
LIMIT 1;
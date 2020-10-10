
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
LIMIT 5

// Question 3
MATCH ()<-[:RETWEET|:REPLY]-(t)
RETURN t.id, COUNT(t) AS OCURANCES
ORDER BY OCURANCES DESC
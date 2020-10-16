// Question 1
MATCH (t:Tweets)
WHERE NOT (t)-[*]->() AND
NOT EXISTS (t.retweet_id) AND 
NOT EXISTS(t.replyto_id)
RETURN count(t) AS Q1_results;

// Question 2
MATCH (h:hash_tags)-[:TAGGED]->(t:Tweets)
WHERE NOT (t)<-[:RETWEET]-()
RETURN h.text as Q2_tag, count(h.text) AS tag_count
ORDER BY tag_count DESC
LIMIT 5;

// Question 3
MATCH ()<-[:RETWEET|REPLY*]-(t:Tweets)
RETURN t.id AS Q3_root_id, COUNT(t) AS decendent_count
ORDER BY decendent_count DESC
LIMIT 1;

// Question 4
MATCH (c:Tweets)<-[:RETWEET|REPLY*]-(t:Tweets)
RETURN t.id AS Q4_root_id, COUNT(DISTINCT c.user_id) AS user_count
ORDER BY user_count DESC
LIMIT 1;

// Question 5
MATCH path = (:Tweets)-[:REPLY|RETWEET*]->(next)
WHERE not((next)-[:REPLY|RETWEET]->())
RETURN length(path) AS Q5_length, [t in nodes(path) | t.id] AS tweets
ORDER BY Q5_length DESC
LIMIT 1;

//Question 6
MATCH (t:Tweets)<-[:MENTION]-(m:Mentions), (d:decendents)
WHERE m.id = d.user_id AND NOT (t.id IN d.ids)
RETURN m.id as user_id, count(m.id) AS mention_count
order by mention_count DESC
LIMIT 1;
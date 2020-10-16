
//CREATE INDEXES
CREATE INDEX IF NOT EXISTS FOR (t:Tweets)
ON (t.id);
CREATE INDEX IF NOT EXISTS FOR (m:Mentions)
ON (m.id);
CREATE INDEX IF NOT EXISTS FOR (t:Tweets)
ON (t.user_id);
CREATE INDEX IF NOT EXISTS FOR (t:Tweets)
ON (t.replyto_id);
CREATE INDEX IF NOT EXISTS FOR (t:Tweets)
ON (t.retweet_id);

// Create DB TRUMP CORRECT
CALL apoc.load.json("file:///tweets_cognitive_test.json")
YIELD value
MERGE (t: Tweets {id: value.id})
SET t.user_id = value.user_id,
t.retweet_user_id = value.retweet_user_id,
t.replyto_user_id = value.replyto_user_id,
t.retweet_id = value.retweet_id,
t.replyto_id =value.replyto_id
WITH t, value.hash_tags AS hash_tags
UNWIND hash_tags AS ht
CREATE(h:hash_tags {text:ht.text})
MERGE (h)-[:TAGGED]->(t);

MATCH (a:Tweets),(b:Tweets)
WHERE a.replyto_id = b.id
MERGE (b)-[r:REPLY]->(a);

MATCH (a:Tweets),(b:Tweets)
WHERE a.retweet_id = b.id
MERGE (b)-[r:RETWEET]->(a);

// Create missing parent reply
MATCH (t:Tweets)
WHERE EXISTS (t.replyto_id)
MERGE (s:Tweets {id: t.replyto_id, user_id: t.replyto_user_id})
MERGE (t)<-[r:REPLY]-(s);

//Create missing parent retweets
MATCH (t:Tweets)
WHERE EXISTS (t.retweet_id)
MERGE (s:Tweets {id: t.retweet_id, user_id: t.retweet_user_id})
MERGE (t)<-[r:RETWEET]-(s);

//CREATE RELA MENTION
CALL apoc.load.json("file:///tweets_cognitive_test.json")
YIELD value
MERGE (t:Tweets {id: value.id})
WITH t, value
UNWIND value.user_mentions AS mn
MERGE(m:Mentions{id:mn.id, parent_id:t.id})
MERGE (m)-[r:MENTION]->(t);

//CREATE nodes that hold decendents
MATCH (t:Tweets)<-[*]-(s:Tweets)
WITH COLLECT(t.id) AS decendent, s.user_id AS user
MERGE (a:decendents {user_id: user, ids: decendent});


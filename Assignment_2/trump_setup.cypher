// Create DB TRUMP CORRECT

CALL apoc.load.json("file:///tweets_cognitive_test.json")
YIELD value
MERGE (t:tweets {id: value.id})
SET t.user_id = value.user_id,
t.retweet_user_id = value.retweet_user_id,
t.replyto_user_id = value.replyto_user_id,
t.favorite_count = value.favorite_count,
t.retweet_id = value.retweet_id,
t.replyto_id =value.replyto_id
WITH t, value.hash_tags AS hash_tags
UNWIND hash_tags AS ht
CREATE(h:tweets{text:ht.text})
MERGE (h)-[:TAGGED]->(t)
RETURN t;

//create index
//CREATE INDEX ON :tweets(id);
//CREATE INDEX ON :tweets(retweet_id);
//CREATE INDEX ON :tweets(replyto_id);

MATCH (a:tweets),(b:tweets)
WHERE a.replyto_id = b.id
MERGE (b)-[r:REPLY]->(a)
RETURN type(r);

MATCH (a:tweets),(b:tweets)
WHERE a.retweet_id = b.id
MERGE (b)-[r:RETWEET]->(a)
RETURN type(r);

// Create missing parent reply
MATCH (t: tweets)
WHERE EXISTS (t.replyto_id)
MERGE (s:tweets {id: t.replyto_id, user_id: t.replyto_user_id})
MERGE (t)<-[r:REPLY]-(s);

//Create missing parent retweets
MATCH (t: tweets)
WHERE EXISTS (t.retweet_id)
MERGE (s:tweets {id: t.retweet_id, user_id: t.retweet_user_id})
MERGE (t)<-[r:RETWEET]-(s);


//CREATE RELA MENTION
CALL apoc.load.json("file:///tweets_cognitive_test.json")
YIELD value
MERGE (t:tweets {id: value.id})
WITH t, value AS tweet
UNWIND tweet.user_mentions AS mn
MERGE(m:mentions{mention_id:mn.id, parent_tweet_id:t.id})
MERGE (m)-[r:MENTION]->(t);
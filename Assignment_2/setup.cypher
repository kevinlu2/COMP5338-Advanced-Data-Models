

//load data
//Create DB and Relationships

CALL apoc.load.json("file:///tweets.json")
YIELD value
MERGE (t:tweets {id: value.id})
SET t.user_id = value.user_id,
t.retweet_user_id = value.retweet_user_id,
t.retweet_id = value.retweet_id,
t.replyto_id =value.replyto_id,
t.user_id = value.user_id,
t.created_at = value.created_at
WITH t, value.hash_tags AS hash_tags
UNWIND hash_tags AS ht
SET t.hash_tags = ht.text
with t, value.user_mentions AS mentions
UNWIND user_mentions AS m
SET t.mentions = m.id
RETURN t
LIMIT 20;



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
MERGE (s:tweets {id: t.replyto_id})
MERGE (t)<-[r:REPLY]-(s);

//Create missing parent retweets
MATCH (t: tweets)
WHERE EXISTS (t.retweet_id)
MERGE (s:tweets {id: t.retweet_id})
MERGE (t)<-[r:RETWEET]-(s);


"user_id": 771440940686999552,
  "retweet_user_id": 3246420445,
  "created_at": "2020-09-17 12:47:52",
  "favorite_count": 0,
  "id": 1306575599742394371,
  "text": "RT @eao_jcmt: David Clements, coauthor “as someone who is a science fiction writer as well as a scientist, it almost feels as if I’ve gone…",
  "retweet_id": 1306496876678701058,


CALL apoc.load.json("file:///tweets.json")
YIELD value
MERGE (t:tweets {id: value.id})
SET t.user_id = value.user_id,
t.retweet_user_id = value.retweet_user_id,
t.retweet_id = value.retweet_id,
t.replyto_id =value.replyto_id,
t.user_id = value.user_id,
t.created_at = value.created_at
WITH t, value.hash_tags AS hash_tags
UNWIND hash_tags AS ht
SET t.hash_tags = ht.text
with t, t.user_mentions AS mentions
UNWIND t.user_mentions AS m
SET t.mentions = m.id
RETURN t
LIMIT 20;
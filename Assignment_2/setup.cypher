

//load data
CALL apoc.load.json("file:///tweets.json")
YIELD value
MERGE (t:tweets {id: value.id})
SET t.user_id = value.user_id,
t.retweet_user_id = value.retweet_user_id,
t.created_at = value.created_at,
t.favorite_count = value.favorite_count,
t.retweet_id = value.retweet_id,
t.replyto_id =value.replyto_id,
t.hash_tag = value.hash_tags.text
RETURN value;



MATCH (a:tweets),(b:tweets)
WHERE a.replyto_id = b.id
MERGE (b)-[r:REPLY]->(a)
RETURN type(r);

MATCH (a:tweets),(b:tweets)
WHERE a.retweet_id = b.id
MERGE (b)-[r:RETWEET]->(a)
RETURN type(r);


with t, value.user_mentions AS user_mentions
UNWIND user_mentions
MERGE (um: user_mentions {user_mention_id: user_mentions.id})
MERGE (t) -[:MENTIONS] -> (um)

"user_id": 771440940686999552,
  "retweet_user_id": 3246420445,
  "created_at": "2020-09-17 12:47:52",
  "favorite_count": 0,
  "id": 1306575599742394371,
  "text": "RT @eao_jcmt: David Clements, coauthor “as someone who is a science fiction writer as well as a scientist, it almost feels as if I’ve gone…",
  "retweet_id": 1306496876678701058,
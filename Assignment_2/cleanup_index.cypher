//Kevin Lu
//500403664
//Assignment 2

//drop graph
MATCH (n)
DETACH DELETE n;

//DELETE RELATIONSHIPS
match ()-[r]->() delete r;

//DELETE INDEXES
DROP INDEX ON :Tweets(id);
DROP INDEX ON :Tweets(replyto_id);
DROP INDEX ON :Tweets(retweet_id);
DROP INDEX ON :Tweets(user_id);
DROP INDEX ON :Mentions(id);
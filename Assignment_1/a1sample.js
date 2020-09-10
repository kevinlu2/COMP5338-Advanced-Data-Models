/*
 * This is a sample script for MongoDB Assignment
 * It shows recommended practice and also basic steps
 * to connect to datanbase server, to set default database
 * and to drop a collection's working copy
 * 
 * The sample assumes that you have imported the tweets data
 * to a collection called tweets in a database a1.
*/

// make a connection to the database server
conn = new Mongo();

// set the default database
db = conn.getDB("a1");

// duplicate the tweets collection and update the created_at type
// the new collection name is tweets_v2
// aggregation pipe line is used to avoid transferring the entire
// collection to the client side

db.tweets.aggregate(
[
    {
       $project: {
            _id: 1,
            id: 1,
            retweet_id: 1,
            replyto_id: 1,
            retweet_count: 1,
            hash_tags:1,
            created_at: {
                $dateFromString: {
                dateString: '$created_at',}
           }
       }
    },  
    {
       $out: 'tweets_v2',
    },
 ]);

// you can add index to your new collection

// optionally timing the execution
var start = new Date()

// find out the earliest retweets of every tweet in the data set
// and return 5 of them.

cursor = db.tweets_v2.aggregate(
    [
        {$group: {
            _id: "$retweet_id",
            first_retweet: {$min: "$created_at"}}},
        {$limit: 5}
    ]
)

// display the result
while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}

var end = new Date()
print("\nQuery Execution time: " + (end - start) + "ms")
// drop the newly created collection
db.tweets_v2.drop()
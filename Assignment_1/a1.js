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
db = conn.getDB("assignment1");


cursor = db.tweets.aggregate(
    { $facet: {
        "General Tweet": [{$match: { $and: [ {replyto_id:{$exists:false}}, {retweet_id:{$exists:false}} ]}},
          {$count: "General Tweets"}],
        "Reply":  [{$match: { $and: [ {replyto_id:{$exists:true}}, {retweet_id:{$exists:false}} ]}},
          {$count: "Reply"}],
        "Retweet": [{$match: { $and: [ {replyto_id:{$exists:false}}, {retweet_id:{$exists:true}} ]}},
          {$count: "Retweet"}]
    }}
)

// display the result
while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}



cursor = db.tweets.aggregate
(
    [
        {$match: {$or: [ {$and:[{replyto_id:{$exists:false}}, {retweet_id:{$exists:false}}]}, {$and:[{replyto_id:{$exists:true}}, {retweet_id:{$exists:false}}]}]}},
	    {$unwind: "$hash_tags"},
        {$group:{_id:"$hash_tags.text", numOfHastags: {$sum:1}}},
	    {$sort:{numOfHastags:-1}},
	    {$limit:5}
    ]
)

// display the result
while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}


// cursor = db.tweets.find().forEach(function(doc){
//     doc.created_at = new ISODate(doc.created_at);
//     db.tweets.save(doc)
// });
// cursor.next()



cursor = db.tweets.aggregate
(
    [
        {$match: {replyto_id:{$exists:true}}},
        {$lookup: {
            from: "tweets", 
            localField: "replyto_id", 
            foreignField: "id", 
            as: "parentTweet"}
            },
        {$unwind: "$parentTweet"},
        {$project: { 
            _id: {$toString: "$parentTweet.id"}, 
            "first response in (seconds)": {$divide : [{ $subtract: [ "$created_at", "$parentTweet.created_at" ] }, 1000]}}},
        {$sort:{"first response in (seconds)" : -1}},
        {$limit:1}


    ]
)

// display the result
while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}









/*
// initiate connections to a local MongoDB instance
conn = new Mongo();

// specify the database as "wikipedia"
db = conn.getDB("assignment1");

//run a query and get the returned cursor object
cursor = db.tweets.aggregate(
    { $facet: {
        "General Tweet": [{$match: { $and: [ {replyto_id:{$exists:false}}, {retweet_id:{$exists:false}} ]}},
          {$count: "General Tweets"}],
        "Reply":  [{$match: { $and: [ {replyto_id:{$exists:true}}, {retweet_id:{$exists:false}} ]}},
          {$count: "Reply"}],
        "Retweet": [{$match: { $and: [ {replyto_id:{$exists:false}}, {retweet_id:{$exists:true}} ]}},
          {$count: "Retweet"}]
    }}
) //run a query

//print the results
while (cursor.hasNext()) {
    printjson( cursor.next() );
}
*/
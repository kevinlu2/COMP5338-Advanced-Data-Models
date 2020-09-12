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





cursor = db.tweets.aggregate
(
    [
        // Get general and reply tweets only
        {$match: {$or: [ {$and:[{replyto_id:{$exists:false}}, {retweet_id:{$exists:false}}]}, {replyto_id:{$exists:true}}]}},
        //Join all retweets in the database in a list called retweets.
        {$lookup: { 
            from: "tweets", 
            localField: "id" , 
            foreignField: "retweet_id", 
            as: "retweets"
            } 
        },
        // Count the size of the list and push the id of parent tweet and retweet count.
        { $project:{
            _id: "$id",
            retweet_count: "$retweet_count",
            numOfRetweets:{$size:"$retweets"},
            students:"$retweets"
            }
        },
        // Compare the parent's retweet count and the size of the retweet list. If tweet_count > size of list then returns 1.
        {$project: {missing: {$cmp: ['$retweet_count', '$numOfRetweets']} }},
        // Keep only those that are missing
        {$match: {missing:1}},
        // Count the the number of general and reply tweets that have have missing retweets.
        {$count: "Number of Missing Tweets"}
    ]
)

// display the result
while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}

cursor = db.tweets.aggregate
(
    [
        // Get greply and retweets only
        {$match: {$or: [ {retweet_id:{$exists:true}}, {replyto_id:{$exists:true}}]}},
        // Join parent tweets to reply or retweets if they exist in the database.
        {$lookup: { 
            from: "tweets", 
            localField: "retweet_id" , 
            foreignField: "id", 
            as: "parentRetweet"
            } 
        },
        {$lookup: { 
            from: "tweets", 
            localField: "replyto_id" , 
            foreignField: "id", 
            as: "parentReplytweet"
            } 
        },
        // If parent exists then size of embedded document is greater than 0.
        // If statement used to distignuish between reply and retweets.
        { $project:{
            _id: "$id",
            retweet_id: "$retweet_id",
            replyto_id: "$replyto_id",
            retweet_count: "$retweet_count",
            parentTweetExist: {$cond: {
                if    : {$gt: ['$retweet_id', null]},
                then  : { $size : {$ifNull: [ "$parentRetweet", [] ]} },
                else  : { $size : {$ifNull: [ "$parentReplytweet", [] ]} }
            }},
            parentReplytweet: "$parentReplytweet",
            parentRetweet: "$parentRetweet"
        }},
        // Filter only those with missing parent tweets
        {$match: {parentTweetExist:0}},
        // Count the the number of missing parent tweets.
        {$count: "Number of Missing Parent Tweets"}
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
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

//set up
cursor = db.tweets.aggregate(
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
     ])

while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}

print("\nQuestion 1");
var start = new Date()
// Question 1 
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
cursor = db.tweets_v2.aggregate(
    // facet to output 3 queries
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
var end = new Date()
print("\nQuery Execution time: " + (end - start) + "ms");
print("\nQuestion 2");
var start = new Date()
// Quesiton 2 
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
cursor = db.tweets_v2.aggregate
(
    [
        // Return only general and reply tweets
        {$match: {$or: [ {$and:[{replyto_id:{$exists:false}}, {retweet_id:{$exists:false}}]}, {$and:[{replyto_id:{$exists:true}}, {retweet_id:{$exists:false}}]}]}},
        
        {$unwind: "$hash_tags"},
        // Group hastags by texts to find number of hastags in tweet
        {$group:{_id:"$hash_tags.text", numOfHastags: {$sum:1}}},
        // Sort by decending order so that most common hastag is at the top.
        {$sort:{numOfHastags:-1}},
        // Only show top 5
	    {$limit:5}
    ]
)

// display the result
while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}
var end = new Date()
print("\nQuery Execution time: " + (end - start) + "ms");
print("\nQuestion 3");

var start = new Date()
// Quesiton 3
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// cursor = db.tweets_v2.aggregate
// db.tweets_v2.find().forEach(function(doc){
//     doc.created_at = new ISODate(doc.created_at);
//     db.tweets_v2.save(doc)
// });

cursor = db.tweets_v2.aggregate
(
    [
        {$match: {replyto_id:{$exists:true}}},
        {$lookup: {
            from: "tweets_v2", 
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
var end = new Date()
print("\n Question 4")
print("\nQuery Execution time: " + (end - start) + "ms");
var start = new Date()
// Quesiton 4
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
cursor = db.tweets_v2.aggregate
(
    [
        // Get general and reply tweets only
        {$match: {$or: [ {$and:[{replyto_id:{$exists:false}}, {retweet_id:{$exists:false}}]}, {replyto_id:{$exists:true}}]}},
        //Join all retweets in the database in a list called retweets.
        {$lookup: { 
            from: "tweets_v2", 
            localField: "id" , 
            foreignField: "retweet_id", 
            as: "retweets"
            } 
        },
        // Count the size of the list and push the id of parent tweet and retweet count
        { $project:{
            _id: "$id",
            retweet_count: "$retweet_count",
            numOfRetweets:{$size:"$retweets"},
            }
        },
        // Compare the parent's retweet count and the size of the retweet list. If tweet_count > size of list then returns 1.
        {$project: {missing: {$cmp: ['$retweet_count', '$numOfRetweets']} }},
        // Keep only those that are missing
        {$match: {missing:1}},
        // Count the the number of general and reply tweets that have have missing retweets.
        {$count: "Number of Missing Retweets for General and Reply tweets"}
    ]
)

// display the result
while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}
var end = new Date()
print("\nQuery Execution time: " + (end - start) + "ms");
print("\nQuestion 5");
var start = new Date()
//Question 5
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
cursor = db.tweets_v2.aggregate
(
    [
        // Get greply and retweets only
        {$match: {$or: [ {retweet_id:{$exists:true}}, {replyto_id:{$exists:true}}]}},
        // Join parent tweets to reply or retweets if they exist in the database.
        {$lookup: { 
            from: "tweets_v2", 
            localField: "retweet_id" , 
            foreignField: "id", 
            as: "parentRetweet"
            } 
        },
        {$lookup: { 
            from: "tweets_v2", 
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

var end = new Date()
print("\nQuery Execution time: " + (end - start) + "ms");
print("\nQuestion 6");
var start = new Date()
// Quesiton 6
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
cursor = db.tweets_v2.aggregate
(
    [
        // Get general tweets only.
        {$match: {$and:[{replyto_id:{$exists:false}}, {retweet_id:{$exists:false}}]}},
        //Join all retweets and replies if they exist.
        {$lookup: { 
            from: "tweets_v2", 
            localField: "id" , 
            foreignField: "retweet_id", 
            as: "retweets"
            } 
        },
        {$lookup: { 
            from: "tweets_v2", 
            localField: "id" , 
            foreignField: "replyto_id", 
            as: "replies"
            } 
        },
        // Count the size of the each embedded document.
        { $project:{
            _id: "$id",
            retweet_id: "$retweet_id",
            replyto_id: "$replyto_id",
            retweet_count: "$retweet_count",
            numOfRetweets:{$size:"$retweets"},
            numOfReplies:{$size:"$replies"},
            retweets:"$retweets",
            replies:"$replies"
            }
        },
        // FIlter those that have 0 retweets and replies in the database.
        {$match: {$and: [{numOfRetweets:0}, {numOfReplies:0}]}},
        // Count the the number of general and reply tweets that have have missing retweets.
        {$count: "Number of General Tweets that do not have a reply nor a retweet in the data set"}
    ]
)

// display the result
while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}
var end = new Date()
print("\nQuery Execution time: " + (end - start) + "ms");

db.tweets_v2_v2.drop()
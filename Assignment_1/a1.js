/*
Kevin Lu
500403664
kelu5219
12/09/19
*/

// make a connection to the database server
conn = new Mongo();

// set the default database
db = conn.getDB("a1");
var starting = new Date();
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
};

// Add Indexes
print("\nIndexing");
var start = new Date();
// Indexes 
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
db.tweets_v2.createIndexes
(
    [{id: 1 , replyto_id: 1 },
    { replyto_id: 1 , id: 1 },
    { id: 1 , retweet_id: 1 },
    { retweet_id: 1 , id: 1 },
    { id: 1 , retweet_count: 1 },
    { retweet_count: 1},
    { retweet_id: 1},
    { created_at: 1 }]
)

// // display the result
// while ( cursor.hasNext() ) {
//     printjson( cursor.next() )
// }

var end = new Date();
print("Query Execution time:" + (end - start) + "ms\n");

print("\nQuestion 1");
var start = new Date();
// Question 1 
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
cursor = db.tweets_v2.aggregate(
    { $facet: {
        "General Tweet": [{$match: { $and: [ {replyto_id:{$exists:false}}, {retweet_id:{$exists:false}} ]}},
          {$count: "General Tweets"}],
        "Reply":  [{$match: {replyto_id:{$exists:true}}},
          {$count: "Reply"}],
        "Retweet": [{$match: {retweet_id:{$exists:true}}},
          {$count: "Retweet"}]
    }}
  )

// display the result
while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}
var end = new Date();
print("Query Execution time:" + (end - start) + "ms\n");
print("\nQuestion 2");
var start = new Date();
// Quesiton 2 
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
cursor = db.tweets_v2.aggregate
(
    [
        // Return only general and reply tweets
        {$match: {$or: [ {$and:[{replyto_id:{$exists:false}}, {retweet_id:{$exists:false}}]}, {replyto_id:{$exists:true}}]}},
        
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
var end = new Date();
print("Query Execution time:" + (end - start) + "ms\n");
print("\nQuestion 3");

var start = new Date();
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
        // Get only reply tweets
        {$match: {replyto_id:{$exists:true}}},
        // Embed parent tweet to reply tweet
        {$lookup: {
            from: "tweets_v2", 
            localField: "replyto_id", 
            foreignField: "id", 
            as: "parentTweet"}
            },
        {$unwind: "$parentTweet"},
        // Get the time difference between child and parent tweet.
        // Divide time by 1000 to get seconds
        {$project: { 
            _id: {$toString: "$parentTweet.id"}, 
            "first response in (seconds)": {$divide : [{ $subtract: [ "$created_at", "$parentTweet.created_at" ] }, 1000]}}},
        // Sort by decending so largest time difference is first.
        {$sort:{"first response in (seconds)" : -1}},
        // Only project largest
        {$limit:1}
    ]
)

// display the result
while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}
var end = new Date();
print("Query Execution time:" + (end - start) + "ms\n");
print("\nQuestion 4")
var start = new Date();
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
var end = new Date();
print("Query Execution time:" + (end - start) + "ms\n");
print("\nQuestion 5");
var start = new Date();
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

var end = new Date();
print("Query Execution time:" + (end - start) + "ms\n");
print("\nQuestion 6");
var start = new Date();
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
            numOfRetweets:{$size:"$retweets"},
            numOfReplies:{$size:"$replies"},
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
var end = new Date();
print("Query Execution time:" + (end - start) + "ms\n");
var ending = new Date();
print("Query Execution time:" + (ending - starting) + "ms\n");
// Drop the collection
db.tweets_v2.drop()
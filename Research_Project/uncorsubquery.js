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

//set up
cursor = db.users.explain("executionStats").aggregate([
    {$lookup: {
        from: "tweets",
        let: {
          likes: "$popularity"
        },
        pipeline: [
          { $match: { $expr: { $eq: ["$retweet_count","$$likes"] }}}
        //   {$limit:2}
        ],
        as: "uncorrelatedSubQuery"
      }}
    ]);
printjson(cursor)

// printjson(cursor.explain());



// while ( cursor.hasNext() ) {
//     printjson( cursor.next() );
// };


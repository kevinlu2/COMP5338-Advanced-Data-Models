// initiate connections to a local MongoDB instance
conn = new Mongo();

// specify the database as "wikipedia"
db = conn.getDB("wikipedia");

//run a query and get the returned cursor object
cursor = db.users.find({gender: "female"}) //run a query

//print the results
while (cursor.hasNext()) {
    printjson( cursor.next() );
}
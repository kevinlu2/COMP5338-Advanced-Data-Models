//week 2 query in script format

conn = new Mongo();
db = conn.getDB("wikipedia");

//Find the number of documents in the revisions collection
print("=====================")
print("The row count of revision collection")

row_count = db.revisions.find().count();
print(row_count)

//question 3.1
print("=====================")
print("Find female editors")

cursor = db.users.find({gender:'female'})

while ( cursor.hasNext() ) {
   printjson( cursor.next() );
}
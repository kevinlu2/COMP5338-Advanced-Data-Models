// Duplicate revisions

db.revisions.aggregate(
    [
        {$out: "revisionsWI"}
   ]
)

// Create Index
db.revisionsWI.createIndex({user:1})
db.revisionsWI.createIndex({timestamp:1})
db.revisionsWI.createIndex({title:1})
db.revisionsWI.createIndex({parsedcomment:"text"})

// Check collection statistics
db.revisionsWI.stats({scale:1024})
db.revisions.stats({scale:1024})

//Question 3.a)
db.revisions.find(
    {
        "_id" : ObjectId("5f53204cc89636b3c92e0851")
    }
).explain("executionStats");

db.revisionsWI.find(
    {
        "_id" : ObjectId("5799843ee2cbe65d76ed9133")
    }
).explain("executionStats")

//Question 3.b)
db.revisions.find(
    {user:"Muboshgu"},
    { title: 1}
).explain("executionStats")

db.revisionsWI.find(
    {user:"Muboshgu"},
    { title: 1}
).explain("executionStats");

//Question 3.c)
db.revisions.find(
    {
        "title": "Donald_Trump", 
        "timestamp":{
            $gte: ISODate("2016-07-01"), 
            $lte:ISODate("2016-07-02")
        }
    }
).explain("executionStats")

db.revisionsWI.find(
    {
        "title": "Donald_Trump",  
        "timestamp":{
            $gte: ISODate("2016-07-01"),
            $lte:ISODate("2016-07-02")
        }
    }
).explain("executionStats")

// hint version

db.revisionsWI.find(
    {
        "title": "Donald_Trump", 
        "timestamp":{
            $gte: ISODate("2016-07-01"), 
            $lte:ISODate("2016-07-02")
        }
    }
).hint({title: 1}).explain("executionStats")



//Question 3.d) aggregation

db.revisionsWI.explain("executionStats").aggregate(
    [
        {$match:{title:"Donald_Trump"}},
        {$group:{_id:"$user", numOfEdits: {$sum:1}}},
        {$sort:{numOfEdits:-1}},
        {$limit:5}
    ]
)

db.revisions.explain("executionStats").aggregate(
    [
        {$match:{title:"Donald_Trump"}},
        {$group:{_id:"$user", numOfEdits: {$sum:1}}},
        {$sort:{numOfEdits:-1}},
        {$limit:5}
    ]
)

//Question 4 
//create index on users collection

db.users.createIndex({registration:1,editcount:1})
db.users.createIndex({gender:1})

//Question 5 multi key index
//create index on categores field
db.pagecat.createIndex({categories:1})

//find categories contains 'film'
db.pagecat.find(
    {
        categories: {$regex: "film"}
    }
)

//querying array field
db.pagecat.find(
    {
        categories: 
        {
            $all:
                [
                    "Category:Good articles", 
                    "Category:American films"
                ]
        }
    }
).explain("executionStats")
	
	
db.pagecat.find(
    {
        categories: 
        {
            $in:
	           [
	                "Category:Good articles", 
	                "Category:American films"
	           ]
	    }
	 }
).explain("executionStats")
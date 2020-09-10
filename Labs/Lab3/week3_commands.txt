//Q1 update timestampe field of revisions

db.revisions.find().forEach(function(doc){
    doc.timestamp = new ISODate(doc.timestamp);
    db.revisions.save(doc)
});

//Q2. find out which five editors made 
//the most revisions on Donald Trump page
db.revisions.aggregate
(
    [
        {$match:{title:"Donald_Trump"}},
	    {$group:{_id:"$user", numOfEdits: {$sum:1}}},
	    {$sort:{numOfEdits:-1}},
	    {$limit:5}
    ]
)

//Q2.1 Find the number of editors in each gender: 'female', 
// male' and 'unknown'

db.users.aggregate(
    [
        {$group:{_id: "$gender", numOfEditor: {$sum:1}}}
    ]
) 
 
// Q2.2 Find the number of minor revisions belonging to each page: 

 db.revisions.aggregate
 (
    [
        {$match:{minor: {$exists:true}}},
        {$group:{_id: "$title", numOfEdits: {$sum:1}}}
    ]
)


// 3.a query single array time in an array field

db.pagecat.find(
    {categories: "Category:Good articles"},
    {title:1, _id:0}
)

//Q3.b query combination of items in an array field

db.pagecat.find(
    {
        categories:
        {
            $all:
            [ 
                "Category:Good articles",
                "Category:Football clubs in England"
            ]
        }
    }
)

//Q3.c use $in operator

db.pagecat.find
(
    {
        categories:
        {
            $in:
            [
                "Category:English-language films", 
                "Category:American films"
            ]
        }
    }
)

//Q3.d find all pages belonging  to exactly three different categories.
db.pagecat.find({categories: {$size:3}})

//Q3.e find the top 10 categories with most pages
 db.pagecat.aggregate
 (
    [
        {$match:{categories: {$exists: true}}},
        {$unwind: "$categories" },
        {$group:{_id: '$categories', cat_number: {$sum:1}}},
        {$sort:{cat_number:-1}},
        {$limit: 10}
    ]
)


//users joins revisions

db.users.aggregate(
    [
        {$lookup: {
            from: "revisions",
            localField: "name" ,
            foreignField: "user",
            as: "revisions"
            }
        }
    ]
)

//revisions joins users

db.revisions.aggregate(
    [
        {$lookup: {
            from: "users",
            localField: "user" ,
            foreignField: "name",
            as: "user_detail"
            }
        }
    ]
)


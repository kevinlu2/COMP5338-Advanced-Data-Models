//change the timestamp in the users document to ISODate type.



/**
 * Question 4.1 Find out the female editors who made over 20000 revisions. 
 * Check  if  any  index  used  in  this  query?If yes, which one?  How many query plans are evaluated?

*/
db.users.find(
    {
            gender: "female", 
            editcount:{$gt: 20000}
    }
)


/**
 * Question 4.2 Find out the number of female editors registered before ``2007-01-01'''.
 *
*/
db.users.find(
    {
        gender: "female", 
        registration: {$lt: ISODate("2007-01-01")}
    }
)


/**
 * Question 4.3. Find out the number of gender 'unknown' editors registered before ``2007-01-01''.
*/

db.users.find(
    {
        gender: "unknown", 
        registration: {$lt: ISODate("2007-01-01")}
    }
)




/**
 *  Question 4.5 Find out all users registered before ``2007-01-01'' and has made more than 30000 revisions,
 *  sort the result based on registration time.
 * 
*/ 

db.users.find(
    {
        registration: {$lt: ISODate("2007-01-01")}, 
        editcount: {$gt: 30000}
    }
).sort({registration: 1})
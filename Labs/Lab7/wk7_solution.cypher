//Question 1
//option 1
PROFILE
MATCH (p1:Person{name: "Tom Hanks"})-[:ACTED_IN]->(m:Movie) <-[:ACTED_IN]-(p2:Person) 
WHERE p1 <> p2 
WITH p2, count(distinct m) as csn 
ORDER by csn DESC LIMIT 1
RETURN p2, csn

//Question 2
//This option models  generes as property of Movie node 
//and tags as an extra property of REVIEWED relationship

//add the new reviewer:
CREATE (ms:Person {name: 'Mary Sharp'});

//add the genres property to Sleepless in Seattle and build its relationships 
MATCH (sis:Movie {title: 'Sleepless in Seattle'})
SET sis.genres = ['Comedy', 'Drama', 'Romance']
WITH sis       
MATCH (ms:Person{name: 'Mary Sharp'})      
CREATE (ms)-[:REVIEWED{rating:80, tags:['Seattle', 'Romance']}]->(sis)
WITH sis
MATCH (jt:Person{name: 'Jessica Thompson'})
CREATE (jt)-[:REVIEWED{rating:70, tags:['Tom Hanks', 'secret']}]->(sis)
WITh sis
MATCH (asc:Person{name: 'Angela Scope'})
CREATE (asc)-[:REVIEWED{tags:['Tom Hanks']}]->(sis);

//add the genres property to movie Apollo 13 and buid its relationships

MATCH (a13:Movie {title: 'Apollo 13'})
SET a13.genres = ['Adventure', 'Drama', 'History']
WITH a13
MATCH (ms:Person{name: 'Mary Sharp'})
CREATE (ms)-[:REVIEWED{rating:90, tags:['Space', 'Tom Hanks']}]->(a13)
WITH a13
MATCH (jt:Person{name: 'Jessica Thompson'})
CREATE (jt)-[:REVIEWED{tags:['Tom Hanks']}]->(a13);

//add the genres to movie Da Vinci Code and build/update its relationships

MATCH (dvc:Movie {title: 'The Da Vinci Code'})<-[r:REVIEWED]-(jt:Person {name: 'Jessica Thompson'})
SET dvc.genres = ['Mystery', 'Thriller'],
    r.tags = ["Holy Grail","Priory of Sion"]
WITH dvc
MATCH (asc:Person{name:'Angela Scope' })
CREATE (asc)-[:REVIEWED{rating:75,tag:['Holy Grail', 'opos dei']}]->(dvc)


//Q2.1 Find all movies with tag ``Tom Hanks''
MATCH (n:Movie)<-[r:REVIEWED]-(p:Person)
WHERE 'Tom Hanks' IN r.tags
RETURN DISTINCT(n.title);

//Q2.2 Find the number of movies with tag ``Tom Hanks''
MATCH (n:Movie)<-[r:REVIEWED]-(p:Person)
WHERE 'Tom Hanks' IN r.tags
RETURN COUNT(DISTINCT(n));

//Q2.3 Find all movies in the ``Drama'' genre
MATCH (n:Movie)
WHERE 'Drama' IN n.genres
RETURN n.title;

//Q2.4 Find   all the users who tagged a movie in “Drama” genre
MATCH (n:Movie)<-[r:REVIEWED]-(p:Person)
WHERE size(r.tags) > 0 AND 'Drama' IN n.genres
RETURN DISTINCT(p)

//Q2.5 Find The average rating of all movies in “Drama” genre
MATCH (n:Movie)<-[r:REVIEWED]-(p:Person)
WHERE EXISTS(r.rating) AND 'Drama' IN n.genres
RETURN AVG(r.rating);

//Q2.6 Find The average rating of each movie in “Drama” genre

MATCH (n:Movie)<-[r:REVIEWED]-(p:Person)
WHERE EXISTS(r.rating) AND 'Drama' IN n.genres
RETURN n.title, AVG(r.rating)

//Q2.7 Find the most frequenttag of this movie
MATCH (m:Movie)-[r:REVIEWED]-()
WHERE m.title='Apollo 13'
WITH r.tags as tags
UNWIND tags as tag
WITH tag, count(*) as tag_freq
RETURN tag, tag_freq
ORDER BY tag_freq DESC
LIMIT 1;



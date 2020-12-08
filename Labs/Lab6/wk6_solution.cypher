MATCH (tom:Person {name: "Tom Hanks"})-[:ACTED_IN]->
      (tomHanksMovies) 
RETURN count(*)

MATCH (people:Person)-[:ACTED_IN]->
      (:Movie {title: "Cloud Atlas"}) 
RETURN COUNT(*)

MATCH (people:Person)-[relatedTo:REVIEWED]-
      (movie:Movie {title:"The Replacements"}) 
RETURN avg(relatedTo.rating)

MATCH (people:Person)-[:ACTED_IN]->
      (:Movie {title: "Cloud Atlas"}) 
RETURN avg(2018 - people.born)

MATCH (people:Person)-[relatedTo:REVIEWED]-(movie:Movie) 
RETURN movie.title, 
       avg(relatedTo.rating) as avg_rate 
ORDER BY avg_rate 
DESC LIMIT 1

MATCH (people:Person)-[relatedTo:REVIEWED]-(movie:Movie) 
RETURN movie.title, 
       COUNT(*) as num_rate 
ORDER BY num_rate 
DESC LIMIT 3

MATCH (person1:Person)-[:KNOWS]->(person2:Person), (person1)<-[:C_HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(post:Post)-[:P_HAS_CREATOR]->(person2) RETURN count(*) AS count

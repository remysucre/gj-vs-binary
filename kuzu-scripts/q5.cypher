MATCH (tag1:Tag)<-[:P_HAS_TAG]-(message:Post)<-[:REPLY_OF]-(comment:Comment)-[:C_HAS_TAG]->(tag2:Tag) RETURN count(*) AS count

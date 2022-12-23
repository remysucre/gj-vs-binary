MATCH (:Tag)<-[:P_HAS_TAG]-(message:Post), (message)<-[:LIKES]-(liker:Person), (message)<-[:REPLY_OF]-(comment:Comment) RETURN count(*) AS count

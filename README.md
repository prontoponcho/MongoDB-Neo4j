#MongoDB-Neo4j 
###Database build and query execution code for NoSQL document and graph data models.
####Contents:
<ul>
<li>src/MongoDB/ contains code for building a JSON document store from the large text file crime-data.txt
<li>src/Neo4j/ contains code for building a graph model from the same large text file.
</ul>
This was a group project for a class on modern databases. After building the databases and queries, we compared query execution times and documented
the qualitative experience. MongoDB is easiest to setup because it does not enforce a schema, auto-indexes on keys, and has superb user support. 
Neo4j demands thoughtful schema design choices for comparable perfromance. Neo4j has greater potential for discovering  relationships amongst the data nodes using the Cypher query language.

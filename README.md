#MongoDB-Neo4j 
###Database build and query execution code for NoSQL document and graph data models.
####Contents:
<ul>
<li>src/MongoDB/ contains code for building a JSON document store from the large text file crime-data.txt
<li>src/Neo4j/ contains code for building a graph model from the same large text file.
</ul>
This was a quick-and-dirty group project for a class on NoSQL databases. We used a document store (MongoDB) and a graph (Neo4j) to model ~10^6 crime records from the city of Chicago.

My contribution was designing the schema and building the MongoDB database. I also refactored a group member's Neo4j code by creating indexes and eliminating directed edges to greatly improve query performance (from minutes to seconds).

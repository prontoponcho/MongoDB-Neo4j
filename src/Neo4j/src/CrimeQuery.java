import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Iterator;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

//Written by Richard Sipes
public class CrimeQuery 
{
	private ExecutionEngine QUERY = null;
	private GraphDatabaseService DB = null;
	public CrimeQuery(GraphDatabaseService DB) {
		this.DB = DB;
		QUERY = new ExecutionEngine(DB);
	}
	
	public void findCasesInPM() {
		try ( Transaction tx = DB.beginTx() ) {
			ExecutionResult result = QUERY.execute( "MATCH (m:amPM)-[:TIME_WHEN]-(c:CASE) WHERE m.amPM = \"PM\" RETURN c" );
		    PrintWriter pw = new PrintWriter(new File("query1Neo.txt"));
		    Iterator<Node> itr = result.columnAs("c");
		    
		    int count = 0;
		    while (itr.hasNext()) {
		        Node CASE = itr.next();
		        String number = (String) CASE.getProperty("number");
		        pw.println(number + " " + ++count);
		    }
		    pw.close();
		    tx.success();
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		
	}
}

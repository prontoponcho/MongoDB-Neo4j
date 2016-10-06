import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class Tester 
{

	public static void main(String[] args) {
		 try {
			String current = new java.io.File( "." ).getCanonicalPath();
			System.out.println(current);
		} catch (IOException e) { e.printStackTrace(); }
		
		String path = "C:\\Users\\Richard\\Desktop\\NeoDB";
		GraphDatabaseService DB = new GraphDatabaseFactory().newEmbeddedDatabase(path);
		//System.out.println("Data Base created at: " + path);
		//buildDB(DB);
		CrimeQuery cq = new CrimeQuery(DB);
		cq.findCasesInPM();
		System.out.println("Done");
		DB.shutdown();
	}

	public static void buildDB(GraphDatabaseService DB) {
		String filename;
		Scanner kb = new Scanner(System.in);
		System.out.println("Enter the file name to parse:");
		filename = kb.nextLine();
		NeoParser parser = new NeoParser(filename, DB);
	}
	
}

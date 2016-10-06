import java.io.FileNotFoundException;
import java.io.PrintWriter;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;


public class MongoQueryTester {
	private static PrintWriter wr = null;
	private static DBObject query = null;
	private static DBCursor cursor = null;
	private static Mongo db = null;
	
	private static PrintWriter getWriter(String file) {
		try {
			return new PrintWriter(file);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private static void writeResults(String file, DBCursor cursor) {
		try {
			wr = getWriter(file);
			for (DBObject cur : cursor) {
				wr.println(cur.get("case_number").toString());
				System.out.println(cur.toString());
			}
		} catch (Exception e) {
			e.printStackTrace();;
		} finally {
			wr.close();
			cursor.close();
		}
	}
	
	private static void writeCount(String file, DBCursor cursor) {
		try {
			wr = getWriter(file);
			wr.println(cursor.count());
		} catch (Exception e) {
			e.printStackTrace();;
		} finally {
			wr.close();
			cursor.close();
		}
	}	
	
	public static void main(String[] args) {
		db = new Mongo("localhost", "project", "practice");
		
		/*Query One: select all case_numbers in PM (12PM-11:59PM)*/
		query = new BasicDBObject("day_night", "PM");
		cursor = db.collection.find(query);
		writeResults("query1.txt", cursor);

		
		/*Query Two: select all case_numbers where primary_type = "THEFT" and location_description = "APARTMENT"*/
		query = new BasicDBObject("primary_type", "THEFT").append("location_description", "APARTMENT");
		cursor = db.collection.find(query);
		writeResults("query2.txt", cursor);
		
		/*Query Three: count the number of primary_type = "MURDER"*/
		query = new BasicDBObject("primary_type", "HOMICIDE");
		cursor = db.collection.find(query);
		writeCount("query3.txt", cursor);
		
		/*Query Four: sort on date*/
		/*This query doesn't work without an index; 25,000 records can be sorted in memory w/out an index*/
		db.collection.createIndex(new BasicDBObject("date" , 1));
		DBObject date = new BasicDBObject("date", 1);
		cursor = db.collection.find().sort(date);
		writeResults("query4.txt", cursor);
		
		/*Query Five: find crimes near 41.878152421 latitude, -87.639715217 longitude*/
		/*returns 100 crimes by default*/
		DBObject key = new BasicDBObject("location" , "2d");
		db.collection.createIndex(key);
		BasicDBObject filter = new BasicDBObject("$near", new double[] { -87.639715217, 41.878152421 });
		BasicDBObject query = new BasicDBObject("location", filter);
		cursor = db.collection.find(query);
		writeResults("query5.txt", cursor);

	}

}

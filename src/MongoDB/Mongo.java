import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Scanner;
import java.util.regex.Pattern;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

public class Mongo {
	public DBCollection collection = null;
	BulkWriteOperation builder = null;
	private HashMap<String, ?> map = new HashMap(); //for duplicate case_number matching
	private final static SimpleDateFormat DF = new SimpleDateFormat("MM/dd/yyyy H:m:s a");
	private final static Pattern COMMA_PATTERN = Pattern.compile(",");
	
	public Mongo() {/*empty*/}
	
	public Mongo(String address, String database, String collection) {
		connectMongo(address, database, collection);
	} 
	
	/*builds new mongo collection from specified data file*/
	public void insertAllData(String file) {
		
		/*remove any records in the collection*/
		collection.drop(); 
		
		/*initialize builder for bulk writes*/
		builder = collection.initializeOrderedBulkOperation();
		
		/*read and parse first line for keys*/
		BufferedReader br = getBufferedReader(file);
		String firstLine = readLine(br);
		String[] keys = COMMA_PATTERN.split(firstLine);

		/*read additional lines, parse for values, build document, insert into database*/
		String line;
		for (int i = 0; (line = readLine(br)) != null; i++) {
			String[] vals = COMMA_PATTERN.split(line);
			DBObject doc = buildDoc(keys, vals);
			
			if (doc != null) 			//add documents to the batch
				builder.insert(doc);
	
			if (i == 999) { 			//bulk insert every 1000 documents
				builder.execute();
				builder = collection.initializeOrderedBulkOperation();
				i = 0;
			} 
		}
		
		/*bulk insert remaining documents*/
		builder.execute();
		closeBufferedReader(br);
	}
	
	/*build JSON document*/
	private BasicDBObject buildDoc(String[] keys, String[] vals) {
		if (isDuplicate(vals[0])) return null; //no duplicate case_numbers allowed
		
		/*mongo-generated _id*/
		BasicDBObject doc = new BasicDBObject();
		
		/*case_number*/
		doc.put(keys[0], vals[0]);
		
		/*make Date object from date, time, day_night*/
		Date date = buildDate(vals[1], vals[2], vals[3]);
		doc.put(keys[1], date);
		
		/*amPM*/
		doc.put(keys[3], vals[3]);
		
		/*primary_type*/
		doc.put(keys[4], vals[4]);
		
		/*description*/
		doc.put(keys[5], vals[5]);
		
		/*location_description*/
		doc.put(keys[6], vals[6]);
		
		/*arrest*/
		doc.put(keys[7], Boolean.parseBoolean(vals[7]));
		
		/*domestic*/
		doc.put(keys[8], Boolean.parseBoolean(vals[8]));
		
		/*beat*/
		doc.put(keys[9], Integer.parseInt(vals[9]));
		
		/*district*/
		doc.put(keys[10], Integer.parseInt(vals[10]));
		
		/*ward*/
		doc.put(keys[11], Integer.parseInt(vals[11]));
		
		/*community_area*/
		doc.put(keys[12], Integer.parseInt(vals[12]));
		
		/*location : [long, lat]*/
		BasicDBList list = new BasicDBList();
		list.add(Double.parseDouble(vals[14]));
		list.add(Double.parseDouble(vals[13]));
		doc.put("location", list);

		return doc;	
	}
	
	public void connectMongo(String address, String database, String collection) {
		try {
			MongoClient client = new MongoClient(address);
			DB db = client.getDB(database);
			this.collection =  db.getCollection(collection);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}	
	}
	
	private static BufferedReader getBufferedReader(String file) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return br;
	}
	
	private static String readLine(BufferedReader br) {
		try {
			return br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private static void closeBufferedReader(BufferedReader br) {
		try {
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	private static Date buildDate(String date, String time, String amPM) {
		StringBuilder sb = new StringBuilder();
		sb.append(date); sb.append(" "); 
		sb.append(time); sb.append(" ");
		sb.append(amPM); sb.append(" ");
		try {
			return DF.parse(sb.toString());
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return new Date();
	}
	
	private boolean isDuplicate(String case_number) {
		if (map.containsKey(case_number)) return true;
		map.put(case_number, null);       return false;
	}
	
	public void printCollection() {
		System.out.println("\nCollection Contents:"); 
		DBCursor cursor = collection.find();
		System.out.println(cursor.count() + " docs in collection");
		try {
			for (DBObject cur : cursor)
				System.out.println(cur.toString());
		} finally { //always close cursor b/c it's "in the server"
			cursor.close();
		}
	}
	
	/*creates the full database*/
	public static void main(String[] args) {
		Mongo db = new Mongo("localhost", "project", "practice");
		db.insertAllData("thesisFinal.txt");
	}
}
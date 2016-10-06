import java.io.File;
import java.io.FileNotFoundException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Pattern;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.UniqueFactory;
import org.neo4j.graphdb.schema.Schema;

/*Modified Matt's NeoParser to create indexes and single relationships between nodes*/
/*Some naming conventions were changed for my own understanding*/

public class NeoParser  {
	private final static Pattern COMMA_PATTERN = Pattern.compile(",");
	private HashMap<String, ?> map = new HashMap(); //for duplicate case_number matching
	
    private enum RelTypes implements RelationshipType {
        ARRESTED, COMMITTED, YEAR_WHEN, MONTH_WHEN, 
        LOCATION_WHERE, COMMUNITY_WHERE, BEAT_WHERE, 
        DISTRICT_WHERE, HOME_WHERE, TIME_WHEN
    }
    
	public NeoParser(String fileName, GraphDatabaseService DB) {
		buildDB(fileName, DB);
	}
	

	private boolean isDuplicate(String case_number) {
		if (map.containsKey(case_number)) return true;
		map.put(case_number, null);       return false;
	}
	
	/*ADDED: Creates indexes on CASE.number and amPM.amPM*/
	private static void createIndexes(GraphDatabaseService DB) {
		try ( Transaction tx = DB.beginTx() ) {
		    Schema schema = DB.schema();
		    schema.indexFor( DynamicLabel.label( "CASE" ) ).on( "number" ).create();
		    tx.success();
		}
		
		try ( Transaction tx = DB.beginTx() ) {
		    Schema schema = DB.schema();
		    schema.indexFor( DynamicLabel.label( "amPM" ) ).on( "amPM" ).create();
		    tx.success();
		}		
	}
	
	/* Parses the rows of the crimes file
	 * Each row has 13 columns after the parsing:
	 * id(0), date(1), time(2), AM/PM(3), primary_type(4), 
	 * description(5), location_description(6), arrest(7), 
	 * domestic(8), beat(9), district(10), ward(11), 
	 * community(12), latitude(13), longitude(14)
	 */
	private void buildDB(String fileName, GraphDatabaseService DB) {
		final int batchSize = 30000;
		int heapCount =0;

		/*ADDED*/
		createIndexes(DB);
		
		try{
			Scanner sc = new Scanner(new File(fileName));
		
			//Node Factories
			UniqueFactory.UniqueNodeFactory ampm            = getFactory(DB, "amPM");
			UniqueFactory.UniqueNodeFactory month           = getFactory(DB, "month");
			UniqueFactory.UniqueNodeFactory year            = getFactory(DB, "year");
			UniqueFactory.UniqueNodeFactory beat            = getFactory(DB, "beat");
			UniqueFactory.UniqueNodeFactory community       = getFactory(DB, "community");
			UniqueFactory.UniqueNodeFactory loc_description = getFactory(DB, "loc_description");
			UniqueFactory.UniqueNodeFactory primary_type    = getFactory(DB, "primary_type");
			UniqueFactory.UniqueNodeFactory district        = getFactory(DB, "district");
			UniqueFactory.UniqueNodeFactory domestic        = getFactory(DB, "domestic");
			UniqueFactory.UniqueNodeFactory arrest          = getFactory(DB, "arrest");
			
			//get keys from first line of data file
			sc.nextLine();
		
			//initial transaction
			Transaction tx = DB.beginTx();
		
			/*read and parse data*/
			while (sc.hasNextLine()) {
					String line = sc.nextLine();
					String[] vals = COMMA_PATTERN.split(line);
					
	    			if (!isDuplicate(vals[0])) {
	    				
	    				//Create case node and set case number 
		    			Node caseNode = DB.createNode(DynamicLabel.label("CASE"));
		    			caseNode.setProperty("number", vals[0]);
	    				
		    			//Connect the caseNode to other nodes
		    			cnxDomestic(DB, domestic, caseNode, vals[8]);
		    			cnxArrest(DB, arrest, caseNode, vals[7]);
		    			cnxAmPm(DB, ampm, caseNode, vals);
		    			cnxMonth(DB, month, caseNode, vals);
		    			cnxYear(DB, year, caseNode, vals);
						cnxPrimaryType(DB, primary_type, caseNode, vals);	
						cnxDistrict(DB,district, caseNode, vals);
						cnxBeat(DB, beat, caseNode, vals);
						cnxCommunity(DB, community, caseNode, vals);
						cnxLocDescription(DB, loc_description, caseNode, vals[6]);
						
						//Flush the heap of the transaction and start a new transaction
						if(++heapCount % batchSize == 0)
						{
							tx.success();
							tx.close();
							tx = DB.beginTx();
							System.out.println(heapCount + " flushed");
						}
	    			}
			}//end while
			
			tx.success();
			tx.close();
			
			/*ADDED*/
			sc.close();
			
		} catch (Exception e) { e.printStackTrace(); }
		
	}//end buildDB
	
	//Create an index for a group of nodes, with the same name for label and internal property
	private UniqueFactory.UniqueNodeFactory getFactory(GraphDatabaseService DB, final String index) {
		try (Transaction tx = DB.beginTx()) {
			UniqueFactory.UniqueNodeFactory factory = new UniqueFactory.UniqueNodeFactory(DB, index) {
				@Override
				protected void initialize(Node created, Map<String, Object> properties)  {
					created.addLabel(DynamicLabel.label(index));
					created.setProperty(index, properties.get(index));
				}
			};
			tx.success();
			return factory;
	    }
	}
	
	//All methods below either create or get an indexed node and connect it to the CASE node for a crime
	//There are differences in properties on the edges and internally on some indexes
	private void cnxArrest(GraphDatabaseService DB, UniqueFactory.UniqueNodeFactory arrestFactory, Node caseNode, String val) {
		Node arrested = arrestFactory.getOrCreate("arrest", Boolean.parseBoolean(val));
		caseNode.createRelationshipTo(arrested, RelTypes.ARRESTED);
	}
	
	
	private void cnxDomestic(GraphDatabaseService DB, UniqueFactory.UniqueNodeFactory domesticFactory, Node caseNode, String val) {
		Node domesticNode = domesticFactory.getOrCreate("domestic", Boolean.parseBoolean(val));
		caseNode.createRelationshipTo(domesticNode, RelTypes.HOME_WHERE);
	}
	
	//Two nodes (AM or PM) in entire graph are indexed as AM/PM, their relationships store the HOUR and MINUTE of incident
	private void cnxAmPm(GraphDatabaseService DB, UniqueFactory.UniqueNodeFactory amPMFactory, Node caseNode, String [] vals) {
		Relationship rel1;
		int hour = Integer.parseInt(vals[2].split(":")[0]);
		int minute = Integer.parseInt(vals[2].split(":")[1]);
		Node time = amPMFactory.getOrCreate("amPM", vals[3]);
		rel1 = caseNode.createRelationshipTo(time, RelTypes.TIME_WHEN);
		rel1.setProperty("hour", hour);
		rel1.setProperty("minute", minute);
	}
	
	private void cnxMonth(GraphDatabaseService DB, UniqueFactory.UniqueNodeFactory monthFactory, Node caseNode, String [] vals)
	{
		Relationship rel1;
		//Create or Grab Month Node of current case
		Node month = monthFactory.getOrCreate("month", Integer.parseInt(vals[1].split("/")[0]));
		rel1 = caseNode.createRelationshipTo(month, RelTypes.MONTH_WHEN);
		rel1.setProperty("day", Integer.parseInt(vals[1].split("/")[1]));
	}
	
	private void cnxYear(GraphDatabaseService DB, UniqueFactory.UniqueNodeFactory yearFactory, Node caseNode, String [] vals) {	
		Relationship rel1;
		Node year = yearFactory.getOrCreate("year", Integer.parseInt(vals[1].split("/")[2]));
		rel1 = caseNode.createRelationshipTo(year, RelTypes.YEAR_WHEN);
		rel1.setProperty("month", Integer.parseInt(vals[1].split("/")[0]));
	}
	
	private void cnxPrimaryType(GraphDatabaseService DB, UniqueFactory.UniqueNodeFactory primaryType, Node caseNode, String[] vals) {
		Relationship rel1;
		Node crime = primaryType.getOrCreate("primary_type", vals[4]);
		rel1 = caseNode.createRelationshipTo(crime, RelTypes.COMMITTED);
		rel1.setProperty("arrested", Boolean.parseBoolean(vals[7]));

	}
	
	private void cnxDistrict(GraphDatabaseService DB, UniqueFactory.UniqueNodeFactory districtFactory, Node caseNode, String [] vals) {
		Relationship rel1;
		Node district = districtFactory.getOrCreate("district", vals[10]);
		rel1= caseNode.createRelationshipTo(district, RelTypes.DISTRICT_WHERE);
		rel1.setProperty("latitude", vals[13]);
		rel1.setProperty("longitude", vals[14]);
	}
	
	private void cnxBeat(GraphDatabaseService DB, UniqueFactory.UniqueNodeFactory beatFactory, Node caseNode, String [] vals) {
		Node beat = beatFactory.getOrCreate("beat", vals[9]);
		caseNode.createRelationshipTo(beat, RelTypes.BEAT_WHERE);
	}
	
	private void cnxCommunity(GraphDatabaseService DB, UniqueFactory.UniqueNodeFactory communityFactory, Node caseNode, String [] vals) {
		Relationship rel1;
		Node community = communityFactory.getOrCreate("community", vals[12]);
		rel1 = caseNode.createRelationshipTo(community, RelTypes.COMMUNITY_WHERE);
		rel1.setProperty("latitude", vals[13]);
		rel1.setProperty("longitude", vals[14]);

	}
	
	private void cnxLocDescription(GraphDatabaseService DB, UniqueFactory.UniqueNodeFactory loc_descriptions, Node caseNumber, String val) {
		Node ld = loc_descriptions.getOrCreate("loc_description", val);
		caseNumber.createRelationshipTo(ld, RelTypes.LOCATION_WHERE);
	}
	
	private static void printValues(String[] vals) {
		String line = "";
		for (String s : vals)
			line += s + ", ";
		System.out.println(line);
	}
	
}


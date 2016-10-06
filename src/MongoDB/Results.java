import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Scanner;

/*Takes two text file arguments and compares their contents*/

public class Results {
	private static HashMap<String, Boolean> map;
	
	private Results() {} //public xtor unnecessary
	
	private static Scanner getScanner(String fileName) {
		try {
			return new Scanner(new File(fileName));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private static void buildMap(String file) {
		map = new HashMap<String, Boolean>();
		Scanner sc = getScanner(file);
		while (sc.hasNext()) {
			String line = sc.nextLine();
			if (map.containsKey(line)) {
				System.out.println(file + " contains duplicates!");
				System.exit(0);
			}
			map.put(line, true);
		}
		sc.close();		
	}
	
	private static void reduceMap(String file) {
		Scanner sc = getScanner(file);
		while (sc.hasNext()) {
			String line = sc.nextLine();
			if (map.remove(line) == null) { //found extra record
				map.put(line, true); 
				break;
			}
		}
		sc.close();
	}
	
	public static boolean match(String file1, String file2) {
		if (file1.equals(file2)) return true;
		buildMap(file1);
		reduceMap(file2);
		return map.size() == 0;
	}
	
	public static boolean inOrder(String file1, String file2) {
		if (file1.equals(file2)) return true;
		
		Scanner sc1 = getScanner(file1);
		Scanner sc2 = getScanner(file2);
		
		boolean inOrder = true; 
		while (sc1.hasNext() && inOrder) {
			if (sc2.hasNext()) {
				String line1 = sc1.nextLine();
				String line2 = sc2.nextLine();
				if (!line1.equals(line2)) 
					inOrder = false;    //records out of order
			} else  inOrder = false; 	//file1 has more lines than file2
		}
		if (sc2.hasNext() && inOrder)     
			inOrder = false;            //file2 has more lines than file1	
		sc1.close(); sc2.close();
		return inOrder;							            
	}
	
	public static boolean hasDuplicates(String file) {
		map = new HashMap<String, Boolean>();
		Scanner sc = getScanner(file);
		while (sc.hasNext()) {
			String line = sc.nextLine();
			if (map.containsKey(line)) {
				return true;
			}
			map.put(line, true);
		}
		sc.close();
		return false;
	}
	
	/*testing*/
	public static void main(String[] args) {
		System.out.println("query 1 match: " + match("query1Parsed.txt", "query1NeoParsed.txt"));
		System.out.println("query 2 match : " + match("query2Parsed.txt", "query2NeoParsed.txt"));
		System.out.println("query1NeoParsed.txt duplicate free? " + !hasDuplicates("query1NeoParsed.txt"));
		System.out.println("query2NeoParsed.txt duplicate free? " + !hasDuplicates("query2NeoParsed.txt"));
		System.out.println("query1Parsed.txt duplicate free? " + !hasDuplicates("query1Parsed.txt"));
		System.out.println("query2Parsed.txt duplicate free? " + !hasDuplicates("query2Parsed.txt"));
			
	}	
}

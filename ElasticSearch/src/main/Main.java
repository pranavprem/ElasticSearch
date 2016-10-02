package main;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import beans.JustABean;
import dao.ElasticDAO;

public class Main {
	
	static String pathToElasticHome = "C:\\ElasticSearch\\elasticsearch-2.4.1\\elasticsearch-2.4.1";
	
	private static void makeTui(){
		Map<String, String> queries = new HashMap<String, String>();
		String more="";
		ElasticDAO dao = new ElasticDAO(pathToElasticHome);
		String indexName = "documentrepository";
		String typeName = "document";
		boolean flag = true;
		Scanner sc = new Scanner(System.in);
		int choice = 0;
		int id=1;
		int idChoice = 0;
		String key, value;
		
		
		
		while(flag){
			System.out.println("what do you want to do?");
			System.out.println("1. Create Doc");
			System.out.println("2. Get Doc");
			System.out.println("3. Search Doc");
			System.out.println("4. Remove Doc");
			System.out.println("5. Exit");
			System.out.println("Choice: ");
			choice = Integer.parseInt(sc.next());
			
			switch(choice){
			case 1:
				System.out.println("id  is: "+id);
				System.out.println("Enter the specs of the new doc: ");
				boolean flag1 = true;
				JustABean bean = new JustABean();
				while(flag1){
					key = "";
					value="";
					System.out.println("Enter key of data to store for doc (enter 'EXIT' if completed defining doc): ");
					key = sc.next();
					if(key.equals("EXIT")){
						flag1=false;
					}
					else{
						System.out.println("Enter the value for the key - "+key+": ");
						value=sc.next();		
					}
					bean.addToMap(key, value);
				}
				
				dao.createDocument(bean, indexName, typeName, ""+id++);
				break;
				
			case 2:
				idChoice = 0;
				System.out.println("Enter the Id of the doc you need: ");
				idChoice = Integer.parseInt(sc.next());
				System.out.println(dao.getDocument(indexName, typeName, ""+id));
				break;
				
			case 3:
				more = "y";
				queries = new HashMap<String, String>();
				while(!more.toLowerCase().equals("n")){
					System.out.println("Enter the field you wish to search for");
					key = "";
					key = sc.next();
					System.out.println("Enter the value in the field you wish to search for");
					value="";
					value = sc.next();
					
					queries.put(key, value);
					System.out.println("more?");
					more = sc.next();
					
				}
				System.out.println("And(1)/Or(2)?");
				more=sc.next();
				if(more.equals("1")){
					dao.searchDocument(queries, indexName, typeName, Boolean.TRUE);
				}else{
					dao.searchDocument(queries, indexName, typeName, Boolean.FALSE);
				}
				break;
			case 4:
				System.out.println("Enter the id of the doc you wish to delete: ");
				idChoice = Integer.parseInt(sc.next());
				dao.deleteDocument(indexName, typeName, ""+idChoice);
				break;
			case 6:
				id = Integer.parseInt(sc.next());
				break;
			default:
				flag=false;
				break;
			}	
		}
		sc.close();
	}

	public static void main(String[] args) {
		makeTui();

	}

}

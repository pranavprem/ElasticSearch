package main;

import beans.JustABean;
import dao.ElasticDAO;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/** Text menu over {@link ElasticDAO}. Field values are a single token, as they were in 2016. */
public class Main {

  static final String DEFAULT_URL = "http://localhost:9200";

  public static void main(String[] args) throws IOException {
    String url = args.length > 0 ? args[0] : getenvOrDefault("ELASTICSEARCH_URL", DEFAULT_URL);
    String apiKey = blankToNull(System.getenv("ELASTICSEARCH_API_KEY"));
    String username = blankToNull(System.getenv("ELASTICSEARCH_USERNAME"));
    String password = System.getenv("ELASTICSEARCH_PASSWORD");
    try (ElasticDAO dao = new ElasticDAO(url, apiKey, username, password);
        Scanner scanner = new Scanner(System.in)) {
      System.out.println("Using Elasticsearch at " + url);
      makeTui(dao, scanner);
    }
  }

  static void makeTui(ElasticDAO dao, Scanner scanner) {
    String indexName = "documentrepository";
    String typeName = "document";
    boolean running = true;
    int id = 1;

    while (running) {
      System.out.println("what do you want to do?");
      System.out.println("1. Create Doc");
      System.out.println("2. Get Doc");
      System.out.println("3. Search Doc");
      System.out.println("4. Remove Doc");
      System.out.println("5. Exit");
      System.out.println("6. Set next id");
      System.out.println("Choice: ");
      String rawChoice = scanner.next();
      int choice;
      try {
        choice = Integer.parseInt(rawChoice);
      } catch (NumberFormatException e) {
        System.out.println("Please enter a number.");
        continue;
      }

      try {
        switch (choice) {
          case 1:
            System.out.println("id  is: " + id);
            System.out.println("Enter the specs of the new doc: ");
            JustABean bean = readBean(scanner);
            dao.createDocument(bean, indexName, typeName, "" + id++);
            break;
          case 2:
            System.out.println("Enter the Id of the doc you need: ");
            int idChoice = Integer.parseInt(scanner.next());
            System.out.println(dao.getDocument(indexName, typeName, "" + idChoice));
            break;
          case 3:
            Map<String, String> queries = new HashMap<>();
            String more = "y";
            while (!more.toLowerCase().equals("n")) {
              System.out.println("Enter the field you wish to search for");
              String key = scanner.next();
              System.out.println("Enter the value in the field you wish to search for");
              String value = scanner.next();
              queries.put(key, value);
              System.out.println("more?");
              more = scanner.next();
            }
            System.out.println("And(1)/Or(2)?");
            more = scanner.next();
            if (more.equals("1")) {
              dao.searchDocument(queries, indexName, typeName, Boolean.TRUE);
            } else {
              dao.searchDocument(queries, indexName, typeName, Boolean.FALSE);
            }
            break;
          case 4:
            System.out.println("Enter the id of the doc you wish to delete: ");
            int deleteId = Integer.parseInt(scanner.next());
            dao.deleteDocument(indexName, typeName, "" + deleteId);
            break;
          case 6:
            System.out.println("Enter the next id: ");
            id = Integer.parseInt(scanner.next());
            break;
          default:
            running = false;
            break;
        }
      } catch (NumberFormatException e) {
        System.out.println("Please enter a number.");
      } catch (RuntimeException e) {
        System.out.println("Request failed: " + e.getMessage());
      }
    }
  }

  static JustABean readBean(Scanner scanner) {
    JustABean bean = new JustABean();
    boolean defining = true;
    while (defining) {
      System.out.println(
          "Enter key of data to store for doc (enter 'EXIT' if completed defining doc): ");
      String key = scanner.next();
      if (key.equals("EXIT")) {
        defining = false;
      } else {
        System.out.println("Enter the value for the key - " + key + ": ");
        String value = scanner.next();
        bean.addToMap(key, value);
      }
    }
    return bean;
  }

  private static String getenvOrDefault(String name, String fallback) {
    String value = System.getenv(name);
    if (value == null || value.isBlank()) {
      return fallback;
    }
    return value;
  }

  private static String blankToNull(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return value;
  }
}

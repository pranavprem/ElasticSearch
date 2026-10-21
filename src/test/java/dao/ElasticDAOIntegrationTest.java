package dao;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import beans.JustABean;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Runs against a live cluster. Skips when nothing is listening at {@code ELASTICSEARCH_URL}. */
class ElasticDAOIntegrationTest {

  private static String serverUrl;
  private static String index;
  private static ElasticDAO dao;

  @BeforeAll
  static void connect() throws Exception {
    serverUrl = System.getenv().getOrDefault("ELASTICSEARCH_URL", "http://localhost:9200");
    Assumptions.assumeTrue(reachable(serverUrl), "Elasticsearch is not reachable at " + serverUrl);
    index = "it-" + UUID.randomUUID();
    dao = new ElasticDAO(serverUrl);
  }

  @AfterAll
  static void cleanup() throws Exception {
    if (dao != null) {
      dao.close();
    }
    if (index != null && serverUrl != null && reachable(serverUrl)) {
      HttpRequest request =
          HttpRequest.newBuilder(URI.create(serverUrl + "/" + index)).DELETE().build();
      HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.discarding());
    }
  }

  @Test
  void createGetSearchAndDelete() {
    String created =
        capture(
            () ->
                dao.createDocument(bean("color", "red", "animal", "fox"), index, "document", "1"));
    assertTrue(created.contains("Inserted? true"), created);
    capture(
        () -> dao.createDocument(bean("color", "blue", "animal", "fox"), index, "document", "2"));

    JustABean loaded = dao.getDocument(index, "document", "1");
    assertEquals("red", loaded.getMetaData().get("color"));
    assertEquals("fox", loaded.getMetaData().get("animal"));

    String andHits =
        capture(
            () ->
                dao.searchDocument(
                    Map.of("color", "red", "animal", "fox"), index, "document", Boolean.TRUE));
    assertTrue(andHits.contains("color=red"), andHits);
    assertFalse(andHits.contains("color=blue"), andHits);

    String orHits =
        capture(
            () ->
                dao.searchDocument(
                    Map.of("color", "red", "animal", "fox"), index, "document", Boolean.FALSE));
    assertTrue(orHits.contains("color=red"), orHits);
    assertTrue(orHits.contains("color=blue"), orHits);

    String deleted = capture(() -> dao.deleteDocument(index, "document", "1"));
    assertTrue(deleted.contains("Index: " + index), deleted);
    assertTrue(deleted.contains("Type: document"), deleted);
    assertTrue(deleted.contains("Id: 1"), deleted);
    assertEquals("", dao.getDocument(index, "document", "1").toString());

    String missing = capture(() -> dao.deleteDocument(index, "document", "missing"));
    assertTrue(missing.contains("Id: missing"), missing);
    assertTrue(missing.contains("Version:"), missing);
  }

  private static JustABean bean(
      String firstKey, String firstValue, String secondKey, String secondValue) {
    JustABean bean = new JustABean();
    bean.addToMap(firstKey, firstValue);
    bean.addToMap(secondKey, secondValue);
    return bean;
  }

  private static String capture(Runnable action) {
    PrintStream original = System.out;
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    System.setOut(new PrintStream(buffer));
    try {
      action.run();
    } finally {
      System.setOut(original);
    }
    return buffer.toString();
  }

  private static boolean reachable(String url) {
    try {
      HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
      HttpRequest request =
          HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(2)).GET().build();
      HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
      return response.statusCode() == 200;
    } catch (Exception e) {
      return false;
    }
  }
}

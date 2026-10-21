package dao;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import beans.JustABean;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ElasticDAOTest {

  @Test
  void andQueryRequiresEveryField() {
    String json = ElasticDAO.matchQuery(Map.of("title", "hello", "color", "red"), true).toString();
    assertTrue(json.contains("must"), json);
    assertFalse(json.contains("should"), json);
    assertTrue(json.contains("hello"), json);
    assertTrue(json.contains("red"), json);
  }

  @Test
  void orQueryMatchesAnyField() {
    String json = ElasticDAO.matchQuery(Map.of("title", "hello"), false).toString();
    assertTrue(json.contains("should"), json);
    assertFalse(json.contains("must"), json);
    assertTrue(json.contains("hello"), json);
  }

  @Test
  void blankServerUrlIsRejected() {
    assertThrows(IllegalArgumentException.class, () -> new ElasticDAO("  "));
  }

  @Test
  void passwordWithoutUsernameIsRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new ElasticDAO("http://localhost:9200", null, null, "secret"));
  }

  @Test
  void blankTypeIsRejectedBeforeARequest() throws Exception {
    try (ElasticDAO dao = new ElasticDAO("http://127.0.0.1:9")) {
      assertThrows(
          IllegalArgumentException.class,
          () -> dao.createDocument(new JustABean(), "index", " ", "1"));
    }
  }
}

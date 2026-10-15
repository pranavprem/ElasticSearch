package main;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import beans.JustABean;
import dao.ElasticDAO;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import org.junit.jupiter.api.Test;

class MainTest {

  @Test
  void exitKeyIsNotStoredAsAField() {
    JustABean bean = Main.readBean(new Scanner("title\nhello\nEXIT\n"));
    assertEquals("hello", bean.getMetaData().get("title"));
    assertFalse(bean.getMetaData().containsKey("EXIT"));
  }

  @Test
  void getUsesTheIdThatWasTyped() {
    RecordingDao dao = new RecordingDao();
    runMenu(dao, "2\n7\n5\n");
    assertEquals("7", dao.lastGetId);
  }

  @Test
  void createStoresOnlyTheFieldsThatWereEntered() {
    RecordingDao dao = new RecordingDao();
    runMenu(dao, "1\ncity\nParis\nEXIT\n5\n");
    assertEquals(List.of("1"), dao.createdIds);
    assertEquals("Paris", dao.created.get(0).getMetaData().get("city"));
    assertFalse(dao.created.get(0).getMetaData().containsKey("EXIT"));
  }

  @Test
  void setNextIdAppliesToTheFollowingCreate() {
    RecordingDao dao = new RecordingDao();
    runMenu(dao, "6\n9\n1\nname\nada\nEXIT\n5\n");
    assertEquals(List.of("9"), dao.createdIds);
  }

  @Test
  void nonNumericChoiceStaysInTheMenu() {
    RecordingDao dao = new RecordingDao();
    runMenu(dao, "nope\n5\n");
    assertTrue(dao.createdIds.isEmpty());
    assertNull(dao.lastGetId);
  }

  private static void runMenu(ElasticDAO dao, String input) {
    PrintStream original = System.out;
    System.setOut(new PrintStream(new ByteArrayOutputStream()));
    try {
      Main.makeTui(dao, new Scanner(input));
    } finally {
      System.setOut(original);
    }
  }

  static final class RecordingDao extends ElasticDAO {
    final List<String> createdIds = new ArrayList<>();
    final List<JustABean> created = new ArrayList<>();
    String lastGetId;

    @Override
    public void createDocument(JustABean bean, String indexName, String typeName, String id) {
      createdIds.add(id);
      created.add(bean);
    }

    @Override
    public JustABean getDocument(String indexName, String typeName, String id) {
      lastGetId = id;
      return new JustABean();
    }

    @Override
    public void searchDocument(
        Map<String, String> searches, String indexName, String typeName, Boolean andOr) {}

    @Override
    public void deleteDocument(String indexName, String typeName, String id) {}
  }
}

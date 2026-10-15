package beans;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class JustABeanTest {

  @Test
  void newBeanIsEmpty() {
    JustABean bean = new JustABean();
    assertTrue(bean.getMetaData().isEmpty());
    assertEquals("", bean.toString());
  }

  @Test
  void addAndRemoveFields() {
    JustABean bean = new JustABean();
    bean.addToMap("title", "hello");
    bean.addToMap("title", "again");
    bean.addToMap("color", "red");
    bean.removeFromMap("color");
    assertEquals("again", bean.getMetaData().get("title"));
    assertFalse(bean.getMetaData().containsKey("color"));
    assertTrue(bean.toString().contains("title : again\n"));
  }

  @Test
  void nullMetadataDoesNotCrashPrintingOrLaterEdits() {
    JustABean bean = new JustABean();
    bean.setMetaData(null);
    assertEquals("", bean.toString());
    bean.removeFromMap("missing");
    bean.addToMap("title", "hello");
    assertEquals("hello", bean.getMetaData().get("title"));
  }

  @Test
  void missingKeyIsAbsent() {
    assertNull(new JustABean().getMetaData().get("nope"));
  }
}

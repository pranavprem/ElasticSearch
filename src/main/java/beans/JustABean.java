package beans;

import java.util.HashMap;
import java.util.Map;

/** A document is just a map of fields. Elasticsearch stores {@link #metaData} as the source. */
public class JustABean {

  private Map<String, Object> metaData;

  public JustABean() {
    this.metaData = new HashMap<>();
  }

  public void addToMap(String key, Object value) {
    if (this.metaData == null) {
      this.metaData = new HashMap<>();
    }
    this.metaData.put(key, value);
  }

  public void removeFromMap(String key) {
    if (this.metaData != null) {
      this.metaData.remove(key);
    }
  }

  public Map<String, Object> getMetaData() {
    return metaData;
  }

  public void setMetaData(Map<String, Object> metaData) {
    this.metaData = metaData;
  }

  @Override
  public String toString() {
    String returnString = "";
    if (this.metaData != null) {
      for (Map.Entry<String, Object> entry : this.metaData.entrySet()) {
        returnString += entry.getKey() + " : " + entry.getValue() + "\n";
      }
    }
    return returnString;
  }
}

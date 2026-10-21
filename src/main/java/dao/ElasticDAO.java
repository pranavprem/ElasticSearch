package dao;

import beans.JustABean;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import java.io.IOException;
import java.util.Map;

/**
 * Create, get, search, and delete for one bean.
 *
 * <p>The 2016 version started an embedded Elasticsearch 2.4 node from a filesystem path and stored
 * each document under an index plus a mapping type. Mapping types and the embedded node are gone.
 * {@code typeName} is still required so the original calls stay the same, and the document source
 * is still the bean's map. Requests go to {@code indexName} over HTTP. Index and delete refresh
 * immediately so the menu can read its own writes.
 */
public class ElasticDAO implements AutoCloseable {

  private final ElasticsearchClient client;

  public ElasticDAO(String serverUrl) {
    this(serverUrl, null, null, null);
  }

  /**
   * @param serverUrl cluster URL, for example {@code http://localhost:9200}
   * @param apiKey API key, or null when the cluster has no security or uses a username
   * @param username basic-auth user, ignored when {@code apiKey} is set
   * @param password basic-auth password; required when {@code username} is set
   */
  public ElasticDAO(String serverUrl, String apiKey, String username, String password) {
    requireText(serverUrl, "serverUrl");
    if (password != null
        && !password.isBlank()
        && (username == null || username.isBlank())
        && (apiKey == null || apiKey.isBlank())) {
      throw new IllegalArgumentException("password requires username");
    }
    this.client =
        ElasticsearchClient.of(
            builder -> {
              builder.host(serverUrl);
              if (apiKey != null && !apiKey.isBlank()) {
                builder.apiKey(apiKey);
              } else if (username != null && !username.isBlank()) {
                builder.usernameAndPassword(username, password == null ? "" : password);
              }
              return builder;
            });
  }

  /** Does not open a connection. Tests subclass this and override the operations. */
  protected ElasticDAO() {
    this.client = null;
  }

  public void createDocument(JustABean bean, String indexName, String typeName, String id) {
    requireBean(bean);
    requireText(indexName, "indexName");
    requireText(typeName, "typeName");
    requireText(id, "id");
    try {
      IndexResponse indexResponse =
          client.index(
              request ->
                  request
                      .index(indexName)
                      .id(id)
                      .document(bean.getMetaData())
                      .refresh(Refresh.True));
      System.out.println("Inserted? " + (indexResponse.result() == Result.Created));
    } catch (IOException e) {
      throw requestFailed(e);
    }
  }

  public JustABean getDocument(String indexName, String typeName, String id) {
    requireText(indexName, "indexName");
    requireText(typeName, "typeName");
    requireText(id, "id");
    JustABean bean = new JustABean();
    try {
      GetResponse<Map<String, Object>> response =
          client.get(request -> request.index(indexName).id(id), documentClass());
      if (response.found() && response.source() != null) {
        bean.setMetaData(response.source());
      }
    } catch (ElasticsearchException e) {
      if (e.status() != 404) {
        throw e;
      }
    } catch (IOException e) {
      throw requestFailed(e);
    }
    return bean;
  }

  /**
   * Prints each clause, then every hit, up to 10,000 documents. {@code andOr} true means every
   * clause must match; anything else means any clause may match.
   */
  public void searchDocument(
      Map<String, String> searches, String indexName, String typeName, Boolean andOr) {
    requireText(indexName, "indexName");
    requireText(typeName, "typeName");
    if (searches == null) {
      throw new IllegalArgumentException("searches is required");
    }
    for (Map.Entry<String, String> search : searches.entrySet()) {
      System.out.println("Searching for " + search.getValue() + " in " + search.getKey());
    }
    try {
      SearchResponse<Map<String, Object>> response =
          client.search(
              request ->
                  request
                      .index(indexName)
                      .size(10000)
                      .query(matchQuery(searches, Boolean.TRUE.equals(andOr))),
              documentClass());
      for (Hit<Map<String, Object>> hit : response.hits().hits()) {
        System.out.println("------------------------------");
        System.out.println(hit.source());
      }
    } catch (IOException e) {
      throw requestFailed(e);
    }
  }

  public void deleteDocument(String indexName, String typeName, String id) {
    requireText(indexName, "indexName");
    requireText(typeName, "typeName");
    requireText(id, "id");
    try {
      DeleteResponse response =
          client.delete(request -> request.index(indexName).id(id).refresh(Refresh.True));
      printDeleted(response.index(), typeName, response.id(), Long.toString(response.version()));
    } catch (ElasticsearchException e) {
      if (e.status() == 404) {
        printDeleted(indexName, typeName, id, "(not found)");
        return;
      }
      throw e;
    } catch (IOException e) {
      throw requestFailed(e);
    }
  }

  @Override
  public void close() throws IOException {
    if (client != null) {
      client.close();
    }
  }

  static Query matchQuery(Map<String, String> searches, boolean andOr) {
    BoolQuery.Builder bool = new BoolQuery.Builder();
    for (Map.Entry<String, String> search : searches.entrySet()) {
      String field = search.getKey();
      String text = search.getValue();
      if (andOr) {
        bool.must(clause -> clause.match(match -> match.field(field).query(text)));
      } else {
        bool.should(clause -> clause.match(match -> match.field(field).query(text)));
      }
    }
    BoolQuery built = bool.build();
    return Query.of(query -> query.bool(built));
  }

  private static void printDeleted(String index, String type, String id, String version) {
    System.out.println("Information on deleted Document");
    System.out.println("Index: " + index);
    System.out.println("Type: " + type);
    System.out.println("Id: " + id);
    System.out.println("Version: " + version);
  }

  private static IllegalStateException requestFailed(IOException e) {
    return new IllegalStateException("Elasticsearch request failed", e);
  }

  private static void requireBean(JustABean bean) {
    if (bean == null) {
      throw new IllegalArgumentException("bean is required");
    }
  }

  private static void requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " is required");
    }
  }

  @SuppressWarnings("unchecked")
  private static Class<Map<String, Object>> documentClass() {
    return (Class<Map<String, Object>>) (Class<?>) Map.class;
  }
}

package dao;

import java.util.ArrayList;
import java.util.Map;

import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;

import beans.JustABean;

public class ElasticDAO {

	/**
	 * Objects needed by Elastic Client is the elastic client object that
	 * creates all the builders. Each of the other builders is used in its own
	 * specific usecase to generate requests to elastic
	 */
	Client client;
	IndexRequestBuilder indexRequestBuilder;
	SearchRequestBuilder searchRequestBuilder;
	DeleteRequestBuilder deleteRequestBuilder;
	GetRequestBuilder getRequestBuilder;

	/**
	 * Constructor Initializes the client
	 * 
	 * @param pathToElasticHome
	 */

	public ElasticDAO(String pathToElasticHome) {
		Node node = new NodeBuilder().settings(Settings.builder().put("path.home", pathToElasticHome)).node();
		this.client = node.client();
	}

	/**
	 * "Document" in this case is how elastic will store things. "Index" is the
	 * index of the database in question. Not to be confused with 'indexing'.
	 * "Type" is the type within the index. To create a 'document', pass it the
	 * bean to store (which contains the map) "id" is the id of the document
	 * within the index and type The function displays if the insert was
	 * successful
	 * 
	 * @param bean
	 * @param indexName
	 * @param typeName
	 * @param id
	 */
	public void createDocument(JustABean bean, String indexName, String typeName, String id) {
		this.indexRequestBuilder = this.client.prepareIndex(indexName, typeName, id);
		IndexResponse indexResponse = indexRequestBuilder.setSource(bean.getMetaData()).execute().actionGet();
		System.out.println("Inserted? " + indexResponse.isCreated());
	}

	/**
	 * 
	 * "Document" in this case is how elastic will store things. "Index" is the
	 * index of the database in question. Not to be confused with 'indexing'.
	 * "Type" is the type within the index. "id" is the id of the document
	 * within the index and type
	 * 
	 * Function returns the requested bean from elasticSearch
	 * 
	 * @param indexName
	 * @param typeName
	 * @param id
	 * @return
	 */
	public JustABean getDocument(String indexName, String typeName, String id) {
		JustABean bean = new JustABean();
		this.getRequestBuilder = this.client.prepareGet(indexName, typeName, id);
		GetResponse getResponse = this.client.prepareGet(indexName, typeName, id).execute().actionGet();
		bean.setMetaData(getResponse.getSource());
		return bean;

	}

	/**
	 * 
	 * 
	 * "Document" in this case is how elastic will store things. "Index" is the
	 * index of the database in question. Not to be confused with 'indexing'.
	 * "Type" is the type within the index. "id" is the id of the document
	 * 
	 * Function takes multiple search parameters the runs a search for AND of
	 * all search parameters / OR of all search parameters
	 * 
	 * @param searches
	 * @param indexName
	 * @param typeName
	 * @param andOr
	 */
	@SuppressWarnings("deprecation")
	public void searchDocument(Map<String, String> searches, String indexName, String typeName, Boolean andOr) {
		SearchResponse response = null;
		SearchHit[] results = new SearchHit[0];
		ArrayList<MatchQueryBuilder> queryBuilders = new ArrayList<MatchQueryBuilder>();
		this.searchRequestBuilder = this.client.prepareSearch(indexName).setTypes(typeName);
		this.searchRequestBuilder.setSize(10000);
		for (Map.Entry<String, String> search : searches.entrySet()) {
			System.out.println("Searching for " + search.getValue() + " in " + search.getKey());
			queryBuilders.add(QueryBuilders.matchQuery(search.getKey(), search.getValue()));
		}
		if (andOr) {
			this.searchRequestBuilder.setQuery(
					QueryBuilders.andQuery(queryBuilders.toArray(new MatchQueryBuilder[queryBuilders.size()])));
		} else {
			this.searchRequestBuilder.setQuery(
					QueryBuilders.orQuery(queryBuilders.toArray(new MatchQueryBuilder[queryBuilders.size()])));
		}
		response = this.searchRequestBuilder.execute().actionGet();
		results = response.getHits().getHits();

		for (SearchHit hit : results) {
			System.out.println("------------------------------");
			Map<String, Object> result = hit.getSource();
			System.out.println(result);
		}
	}

	/**
	 * 
	 * "Document" in this case is how elastic will store things. "Index" is the
	 * index of the database in question. Not to be confused with 'indexing'.
	 * "Type" is the type within the index. "id" is the id of the document
	 * 
	 * Function deletes document with the provided id.
	 * 
	 * @param indexName
	 * @param typeName
	 * @param id
	 */
	public void deleteDocument(String indexName, String typeName, String id) {
		this.deleteRequestBuilder = this.client.prepareDelete(indexName, typeName, id);
		DeleteResponse response = this.deleteRequestBuilder.execute().actionGet();
		System.out.println("Information on deleted Document");
		System.out.println("Index: " + response.getIndex());
		System.out.println("Type: " + response.getType());
		System.out.println("Id: " + response.getId());
		System.out.println("Version: " + response.getVersion());
	}

}

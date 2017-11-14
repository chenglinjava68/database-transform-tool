//package com.share.common.elasticsearch.rest;
//
//import java.util.List;
//import java.util.Map;
//
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//import org.elasticsearch.action.bulk.BulkRequest;
//import org.elasticsearch.action.bulk.BulkResponse;
//import org.elasticsearch.action.delete.DeleteRequest;
//import org.elasticsearch.action.delete.DeleteResponse;
//import org.elasticsearch.action.get.GetRequest;
//import org.elasticsearch.action.get.GetResponse;
//import org.elasticsearch.action.index.IndexRequest;
//import org.elasticsearch.action.index.IndexResponse;
//import org.elasticsearch.action.search.SearchRequest;
//import org.elasticsearch.action.search.SearchResponse;
//import org.elasticsearch.action.search.SearchType;
//import org.elasticsearch.action.update.UpdateRequest;
//import org.elasticsearch.action.update.UpdateResponse;
//import org.elasticsearch.client.RestHighLevelClient;
//import org.elasticsearch.common.UUIDs;
//import org.elasticsearch.common.xcontent.XContentType;
//import org.elasticsearch.index.query.BoolQueryBuilder;
//import org.elasticsearch.index.query.QueryBuilders;
//import org.elasticsearch.search.aggregations.AggregationBuilders;
//import org.elasticsearch.search.builder.SearchSourceBuilder;
//import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONObject;
//import com.share.common.util.StringUtil;
//
//public class ElasticsearchHighRestFactory extends ElasticsearchRestFactory{
//	private static Logger logger = LogManager.getLogger();
//	private RestHighLevelClient xclient=null;
//	
//	public ElasticsearchHighRestFactory() {
//		super();
//	}
//	public ElasticsearchHighRestFactory(String servers) {
//		super(servers);
//	}
//	public ElasticsearchHighRestFactory(String servers,int port) {
//		super(servers, port);
//	}
//	public ElasticsearchHighRestFactory(String clusterName, String servers,int port) {
//		super(clusterName, servers, port);
//	}
//	public ElasticsearchHighRestFactory(String clusterName, String servers, String username, String password) {
//		super(clusterName, servers, username, password);
//	}
//	public ElasticsearchHighRestFactory(String clusterName, String servers, String username, String password,int port) {
//		super(clusterName, servers, username, password, port);
//	}
//
//	/**
//	 * @description Elasticsearch服务配置
//	 * @author yi.zhang
//	 * @time 2017年4月19日 上午10:38:42
//	 * @throws Exception
//	 */
//	public void init(){
//		try {
//			super.init();
//			xclient = new RestHighLevelClient(super.getClient());
//		} catch (Exception e) {
//			logger.error("-----Elasticsearch Config init Error-----", e);
//		}
//	}
//	
//	public RestHighLevelClient getXClient(){
//		return xclient;
//	}
//	
//	public String insert(String index,String type,String json){
//		try {
//			if(xclient==null){
//				init();
//			}
////			XContentBuilder builder = XContentFactory.jsonBuilder();
////			builder.startObject();
////			{
////			    builder.field("user", "kimchy");
////			    builder.field("postDate", new Date());
////			    builder.field("message", "trying out Elasticsearch");
////			}
////			builder.endObject();
//			IndexRequest request = new IndexRequest(index, type);
//			request.source(json,XContentType.JSON);
//			IndexResponse response = xclient.index(request);
////			String _index = response.getIndex();
////			String _type = response.getType();
////			String id = response.getId();
////			long version = response.getVersion();
////			if (response.getResult() == DocWriteResponse.Result.CREATED) {
////			    
////			} else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
////			    
////			}
////			ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
////			if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
////			    
////			}
////			if (shardInfo.getFailed() > 0) {
////			    for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
////			        String reason = failure.reason(); 
////			    }
////			}
//			return response.toString();
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		return null;
//	}
//	public String update(String index,String type,String id,String json){
//		try {
//			if(xclient==null){
//				init();
//			}
//			UpdateRequest request = new UpdateRequest(index, type, id);
//			request.doc(json,XContentType.JSON);
//			UpdateResponse response = xclient.update(request);
//			return response.toString();
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		return null;
//	}
//	public String upsert(String index,String type,String id,String json){
//		try {
//			if(xclient==null){
//				init();
//			}
////			IndexRequest indexRequest = new IndexRequest(index, type, id).source(json,XContentType.JSON);
////			UpdateRequest updateRequest = new UpdateRequest(index, type, id).doc(json,XContentType.JSON).upsert(indexRequest);
//			UpdateRequest request = new UpdateRequest(index, type, id);
//			request.upsert(json,XContentType.JSON);
//			UpdateResponse response = xclient.update(request);
//			return response.toString();
//		}catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		return null;
//	}
//	public String delete(String index,String type,String id){
//		try {
//			if(xclient==null){
//				init();
//			}
//			DeleteRequest request = new DeleteRequest(index, type, id);
//			DeleteResponse result = xclient.delete(request);
//			return result.toString();
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		return null;
//	}
//	public String bulkUpsert(String index,String type,List<String> jsons){
//		try {
//			if(xclient==null){
//				init();
//			}
//			BulkRequest request = new BulkRequest();
//			for (String json : jsons) {
//				JSONObject obj = JSON.parseObject(json);
//				String id = UUIDs.base64UUID();
//				if(obj.containsKey("id")){
//					id = obj.getString("id");
//					obj.remove("id");
//				}
////				if(obj.containsKey("id")){
////					request.add(new UpdateRequest(index, type, id).doc(obj.toJSONString(),XContentType.JSON));
////				}else{
////					request.add(new IndexRequest(index, type).source(obj.toJSONString(),XContentType.JSON));
////				}
//				request.add(new UpdateRequest(index, type, id).upsert(obj.toJSONString(),XContentType.JSON));
//			}
//			BulkResponse result = xclient.bulk(request);
//			return result.toString();
//		}catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		return null;
//	}
//	public String bulkDelete(String index,String type,String... ids){
//		try {
//			if(xclient==null){
//				init();
//			}
//			BulkRequest request = new BulkRequest();
//			for (String id : ids) {
//				request.add(new DeleteRequest(index, type, id));
//			}
//			BulkResponse result = xclient.bulk(request);
//			return result.toString();
//		}catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		return null;
//	}
//	
//	public String select(String index,String type,String id){
//		try {
//			if(xclient==null){
//				init();
//			}
//			GetRequest request = new GetRequest(index, type, id);
//			GetResponse result = xclient.get(request);
//			return result.getSourceAsString();
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		return null;
//	}
//	public String selectAll(String indexs,String types,String condition){
//		try {
//			if(StringUtil.isEmpty(indexs))indexs="_all";
//			if(xclient==null){
//				init();
//			}
//			SearchSourceBuilder search = new SearchSourceBuilder();
//			search.query(QueryBuilders.queryStringQuery(condition)); 
//			search.from(0);
//			search.size(50);
//			search.explain(true);
//			SearchRequest request = new SearchRequest();
//			request.searchType(SearchType.DFS_QUERY_THEN_FETCH);
//			request.source(search);
//			request.indices(indexs.split(","));
//			request.types(types.split(","));
//			SearchResponse response = xclient.search(request);
//			return response.toString();
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		return null;
//	}
//
//	public String selectMatchAll(String indexs,String types,String field,String value){
//		try {
//			if(StringUtil.isEmpty(indexs))indexs="_all";
//			if(xclient==null){
//				init();
//			}
//			SearchSourceBuilder search = new SearchSourceBuilder();
//			if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)&&!(field.matches(regex)||field.matches(value))){
//				search.query(QueryBuilders.matchQuery(field, value));
//			}
//			search.aggregation(AggregationBuilders.terms("data").field(field+".keyword"));
//			search.from(0);
//			search.size(50);
//			search.explain(true);
//			SearchRequest request = new SearchRequest();
//			request.searchType(SearchType.DFS_QUERY_THEN_FETCH);
//			request.source(search);
//			request.indices(indexs.split(","));
//			request.types(types.split(","));
//			SearchResponse response = xclient.search(request);
//			return response.toString();
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		return null;
//	}
//	public String selectMatchAll(String indexs,String types,Map<String,String> must,Map<String,String> should,Map<String,String> must_not){
//		try {
//			if(StringUtil.isEmpty(indexs))indexs="_all";
//			if(xclient==null){
//				init();
//			}
//			BoolQueryBuilder boolquery = QueryBuilders.boolQuery();
//			HighlightBuilder highlight = new HighlightBuilder();
//			if(must!=null&&must.size()>0){
//				for (String field : must.keySet()) {
//					if(field.matches(regex)){
//						continue;
//					}
//					String value = must.get(field);
//					if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
//						if(value.startsWith("[")&&value.endsWith("]")){
//							List<String> values = JSON.parseArray(value, String.class);
//							for (String _value : values) {
//								if(!_value.matches(regex)){
//									boolquery.should(QueryBuilders.matchQuery(field, value));
//								}
//							}
//						}else{
//							if(!value.matches(regex)){
//								boolquery.must(QueryBuilders.matchQuery(field, value));
//							}
//						}
//					}
//					highlight.field(field);
//				}
//			}
//			if(should!=null&&should.size()>0){
//				for (String field : should.keySet()) {
//					if(field.matches(regex)){
//						continue;
//					}
//					String value = should.get(field);
//					if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
//						if(value.startsWith("[")&&value.endsWith("]")){
//							List<String> values = JSON.parseArray(value, String.class);
//							for (String _value : values) {
//								if(!_value.matches(regex)){
//									boolquery.should(QueryBuilders.matchQuery(field, value));
//								}
//							}
//						}else{
//							if(!value.matches(regex)){
//								boolquery.should(QueryBuilders.matchQuery(field, value));
//							}
//						}
//					}
//					highlight.field(field);
//				}
//			}
//			if(must_not!=null&&must_not.size()>0){
//				for (String field : must_not.keySet()) {
//					if(field.matches(regex)){
//						continue;
//					}
//					String value = must_not.get(field);
//					if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
//						if(value.startsWith("[")&&value.endsWith("]")){
//							List<String> values = JSON.parseArray(value, String.class);
//							for (String _value : values) {
//								if(!_value.matches(regex)){
//									boolquery.mustNot(QueryBuilders.matchQuery(field, value));
//								}
//							}
//						}else{
//							if(!value.matches(regex)){
//								boolquery.mustNot(QueryBuilders.matchQuery(field, value));
//							}
//						}
//					}
//					highlight.field(field);
//				}
//			}
//			SearchSourceBuilder search = new SearchSourceBuilder();
//			search.query(boolquery);
//			search.highlighter(highlight);
//			search.from(0);
//			search.size(50);
//			search.explain(true);
//			SearchRequest request = new SearchRequest();
//			request.searchType(SearchType.DFS_QUERY_THEN_FETCH);
//			request.source(search);
//			request.indices(indexs.split(","));
//			request.types(types.split(","));
//			SearchResponse response = xclient.search(request);
//			return response.toString();
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		return null;
//	}
//}
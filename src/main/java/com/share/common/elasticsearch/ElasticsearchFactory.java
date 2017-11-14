package com.share.common.elasticsearch;

import java.util.List;
import java.util.Map;

public interface ElasticsearchFactory {
	public String insert(String index,String type,String json);
	public String update(String index,String type,String id,String json);
	public String upsert(String index,String type,String id,String json);
	public String delete(String index,String type,String id);
	public String bulkUpsert(String index,String type,List<String> jsons);
	public String bulkDelete(String index,String type,String... ids);
	public String drop(String indexs);
	public String select(String index,String type,String id);
	public String selectAll(String indexs,String types,String condition);
	public String selectMatchAll(String indexs,String types,String field,String value);
	public String selectMatchAll(String indexs,String types,Map<String,String> must,Map<String,String> should,Map<String,String> must_not);
}

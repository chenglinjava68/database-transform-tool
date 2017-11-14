package com.share.common.elasticsearch.http;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.UUIDs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.share.common.elasticsearch.AbstractElasticsearchFactory;
import com.share.util.HttpUtil;
import com.share.util.StringUtil;

public class ElasticsearchHttpFactory extends AbstractElasticsearchFactory{
	private static Logger logger = LogManager.getLogger();
	
	/**
	 * 认证信息(username:password)
	 */
	private static String auth=null;
	/**
	 * http访问API
	 */
	private static String api="http://localhost:9200";
	
	private static int DEFAULT_PORT = 9200;
	
	public int defaultPort() {
		return DEFAULT_PORT;
	}
	public ElasticsearchHttpFactory() {
		super();
	}
	public ElasticsearchHttpFactory(String servers) {
		super(servers);
	}
	public ElasticsearchHttpFactory(String servers,int port) {
		super(servers, port);
	}
	public ElasticsearchHttpFactory(String clusterName, String servers,int port) {
		super(clusterName, servers, port);
	}
	public ElasticsearchHttpFactory(String clusterName, String servers, String username, String password) {
		super(clusterName, servers, username, password);
	}
	public ElasticsearchHttpFactory(String clusterName, String servers, String username, String password,int port) {
		super(clusterName, servers, username, password, port);
	}

//	/**
//	 * @description Elasticsearch服务配置
//	 * @author yi.zhang
//	 * @time 2017年4月19日 上午10:38:42
//	 * @throws Exception
//	 */
	public void init(){
		try {
			if(!StringUtil.isEmpty(username)&&!StringUtil.isEmpty(password)){
				auth = username+":"+password;
			}
			for(String server : servers.split(",")){
				String[] address = server.split(":");
				String ip = address[0];
				int _port=port;
				if(address.length>1){
					_port = Integer.valueOf(address[1]);
				}
				api = "http://"+ip+":"+_port;
				boolean flag = HttpUtil.checkConnection(api,auth);
				if(flag){
					logger.info("--------------Elasticsearch["+api+"] connect success-------------");
					break;
				}else{
					logger.warn("--------------Elasticsearch["+api+"] connect error-------------");
				}
			}
		} catch (Exception e) {
			logger.error("-----Elasticsearch Config init Error-----", e);
		}
	}
	
	public String base(String uri,String method,String body){
		try {
			String url = uri;
			if(uri!=null&&!(uri.startsWith("http://")||uri.startsWith("https://"))){
				url= api+(uri.startsWith("/")?"":"/")+uri;
			}
			if(body!=null){
				body = url.contains("_bulk")?body:JSON.parseObject(body).toJSONString();
			}
			String result  = HttpUtil.urlRequest(url, method, body, auth);
			if(result!=null&&result.contains("Connection refused")){
				init();
			}
			return result;
		} catch (Exception e) {
			logger.error("----Elasticsearch RESTFull[http api] 访问失败!------------",e);
		}
		return null;
	}
	public String indices() {
		String uri = "/_cat/indices?v";
		String result = base(uri, HttpUtil.METHOD_GET,  null);
		if(!StringUtil.isEmpty(result)){
			String[] data = result.split("\n");
			if(data!=null&&data.length>1){
				List<JSONObject> list = new ArrayList<JSONObject>();
				String[] header = data[0].split("(\\s+)");
				for(int i=1;i<data.length;i++){
					String[] values = data[i].split("(\\s+)");
					JSONObject json = new JSONObject();
					for(int j=0;j<values.length;j++){
						json.put(header[j], values[j]);
					}
					list.add(json);
				}
				if(list!=null&&list.size()==1){
					return list.get(0).toJSONString();
				}else{
					return JSON.toJSONString(list);
				}
			}
		}
		return result;
	}
	public String nodes() {
		String uri = "/_cat/nodes?v";
		String result = base(uri, HttpUtil.METHOD_GET,  null);
		if(!StringUtil.isEmpty(result)){
			String[] data = result.split("\n");
			if(data!=null&&data.length>1){
				List<JSONObject> list = new ArrayList<JSONObject>();
				String[] header = data[0].split("(\\s+)");
				for(int i=1;i<data.length;i++){
					String[] values = data[i].split("(\\s+)");
					JSONObject json = new JSONObject();
					for(int j=0;j<values.length;j++){
						json.put(header[j], values[j]);
					}
					list.add(json);
				}
				if(list!=null&&list.size()==1){
					return list.get(0).toJSONString();
				}else{
					return JSON.toJSONString(list);
				}
			}
		}
		return result;
	}
	
	public String mapping(String index, String type, @SuppressWarnings("rawtypes") Class clazz) {
		boolean isCustom = true;
		String uri = "/"+index;
		String result = base(uri, HttpUtil.METHOD_GET,  null);
		JSONObject target = JSON.parseObject(result);
		if(target!=null&&target.getIntValue("status")==404){
			String body="{mappings:{"+type+":{properties:"+JSON.toJSONString(reflect(clazz, isCustom))+"}},settings:"+analyzer(index).toJSONString()+"}";
			result = base(uri, HttpUtil.METHOD_PUT, JSON.parseObject(body).toJSONString());
		}else{
			if(!(target!=null&&target.getJSONObject(index).getJSONObject("settings").getJSONObject("index").containsKey("analysis")&&target.getJSONObject(index).getJSONObject("settings").getJSONObject("index").getJSONObject("analysis").getJSONObject("analyzer").containsKey("es_analyzer"))){
				isCustom = false;
			}
			if(!(target!=null&&target.getJSONObject(index).getJSONObject("mappings").containsKey(type))){
				String body = "{properties:"+JSON.toJSONString(reflect(clazz, isCustom))+"}";
				result = base(uri+"/_mapping/"+type, HttpUtil.METHOD_PUT, JSON.parseObject(body).toJSONString());
			}
		}
		return result;
	}
	
	@Override
	public String insert(String index, String type, String json) {
		String uri = "/"+index+"/"+type;
		JSONObject body = JSON.parseObject(json);
		String result = base(uri, HttpUtil.METHOD_POST,  body.toJSONString());
		return result;
	}

	@Override
	public String update(String index, String type, String id, String json) {
		String uri = "/"+index+"/"+type+"/"+id;
		String result = base(uri, HttpUtil.METHOD_GET,null);
		JSONObject target = JSON.parseObject(result);
		if(target.getBooleanValue("found")){
			JSONObject body = JSON.parseObject(json);
			result = base(uri, HttpUtil.METHOD_PUT, body.toJSONString());
		}
		return result;
	}

	@Override
	public String upsert(String index, String type, String id, String json) {
		String uri = "/"+index+"/"+type+"/"+id;
		JSONObject body = JSON.parseObject(json);
		String result = base(uri, HttpUtil.METHOD_PUT, body.toJSONString());
		return result;
	}
	@Override
	public String delete(String index, String type, String id) {
		String uri = "/"+index+"/"+type+"/"+id;
		String result = base(uri, HttpUtil.METHOD_DELETE, null);
		return result;
	}
	@Override
	public String bulkUpsert(String index, String type, List<String> jsons) {
		String uri = "/_bulk";
		String body = "";
		for (String json : jsons) {
			JSONObject obj = JSON.parseObject(json);
			String id = null;
			if(obj.containsKey("id")||obj.containsKey("_id")){
				if(obj.containsKey("_id")){
					id = obj.getString("_id");
					obj.remove("_id");
				}else{
					id = obj.getString("id");
					obj.remove("id");
				}
			}
			if(!StringUtil.isEmpty(id)){
				String action = "{update:{_index:'"+index+"',_type:'"+type+"',_id:'"+id+"'}}";
				String _body ="{doc:"+obj.toJSONString()+"}";
				body += JSON.parseObject(action).toJSONString()+"\n"+JSON.parseObject(_body).toJSONString()+"\n";
			}else{
				id = UUIDs.base64UUID();
				String action = "{create:{_index:'"+index+"',_type:'"+type+"',_id:'"+id+"'}}";
				body += JSON.parseObject(action).toJSONString()+"\n"+obj.toJSONString()+"\n";
			}
		}
		String result = base(uri, HttpUtil.METHOD_POST, body);
		return result;
	}

	@Override
	public String bulkDelete(String index, String type, String... ids) {
		String uri = "/_bulk";
		String body = "";
		for (String id : ids) {
			String action = "{delete:{_index:'"+index+"',_type:'"+type+"',_id:'"+id+"'}}";
			body += JSON.parseObject(action).toJSONString()+"\n";
		}
		String result = base(uri, HttpUtil.METHOD_POST, body);
		return result;
	}
	public String drop(String indexs) {
		String uri = "/"+indexs;
		String result = base(uri, HttpUtil.METHOD_DELETE,null);
		return result;
	}
	@Override
	public String select(String index, String type, String id) {
		String uri = "/"+index+"/"+type+"/"+id;
		String result = base(uri, HttpUtil.METHOD_GET,null);
		return result;
	}

	@Override
	public String selectAll(String indexs, String types, String condition) {
		if(StringUtil.isEmpty(indexs))indexs="_all";
		String uri = "/"+indexs+(StringUtil.isEmpty(types)?"":"/"+types)+"/_search?pretty";
		if(!StringUtil.isEmpty(condition))uri+="&q="+HttpUtil.encode(condition);
		String result = base(uri, HttpUtil.METHOD_GET,null);
		return result;
	}

	@Override
	public String selectMatchAll(String indexs, String types, String field, String value) {
		if(StringUtil.isEmpty(indexs))indexs="_all";
		String uri = "/"+indexs+(StringUtil.isEmpty(types)?"":"/"+types)+"/_search?pretty";
		String body = "";
		if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)&&!(field.matches(regex)||field.matches(value))){
			String query = "{query:{match:{"+field+":'"+value+"'}}}";
			body = JSON.parseObject(query).toJSONString();
		}
		String result = base(uri, HttpUtil.METHOD_POST, body);
		return result;
	}

	@Override
	public String selectMatchAll(String indexs, String types,Map<String,String> must,Map<String,String> should,Map<String,String> must_not) {
		if(StringUtil.isEmpty(indexs))indexs="_all";
		String uri = "/"+indexs+(StringUtil.isEmpty(types)?"":"/"+types)+"/_search?pretty";
		List<JSONObject> must_matchs = new ArrayList<JSONObject>();
		List<JSONObject> should_matchs = new ArrayList<JSONObject>();
		List<JSONObject> must_not_matchs = new ArrayList<JSONObject>();
		if(must!=null&&must.size()>0){
			for (String field : must.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				String value = must.get(field);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					if(value.startsWith("[")&&value.endsWith("]")){
						List<String> values = JSON.parseArray(value, String.class);
						for (String _value : values) {
							if(!_value.matches(regex)){
								String match = "{match:{"+field+":'"+_value+"'}}";
								should_matchs.add(JSON.parseObject(match));
							}
						}
					}else{
						if(!value.matches(regex)){
							String match = "{match:{"+field+":'"+value+"'}}";
							must_matchs.add(JSON.parseObject(match));
						}
					}
				}
			}
		}
		if(should!=null&&should.size()>0){
			for (String field : should.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				String value = should.get(field);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					if(value.startsWith("[")&&value.endsWith("]")){
						List<String> values = JSON.parseArray(value, String.class);
						for (String _value : values) {
							if(!_value.matches(regex)){
								String match = "{match:{"+field+":'"+_value+"'}}";
								should_matchs.add(JSON.parseObject(match));
							}
						}
					}else{
						if(!value.matches(regex)){
							String match = "{match:{"+field+":'"+value+"'}}";
							should_matchs.add(JSON.parseObject(match));
						}
					}
				}
			}
		}
		if(must_not!=null&&must_not.size()>0){
			for (String field : must_not.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				String value = must_not.get(field);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					if(value.startsWith("[")&&value.endsWith("]")){
						List<String> values = JSON.parseArray(value, String.class);
						for (String _value : values) {
							if(!_value.matches(regex)){
								String match = "{match:{"+field+":'"+_value+"'}}";
								must_not_matchs.add(JSON.parseObject(match));
							}
						}
					}else{
						if(!value.matches(regex)){
							String match = "{match:{"+field+":'"+value+"'}}";
							must_not_matchs.add(JSON.parseObject(match));
						}
					}
				}
			}
		}
		String query = "{query:{bool:{must:"+JSON.toJSONString(must_matchs)+",must_not:"+JSON.toJSONString(must_not_matchs)+",should:"+JSON.toJSONString(should_matchs)+"}}}";
		String body = JSON.parseObject(query).toJSONString();
		String result = base(uri, HttpUtil.METHOD_POST, body);
		return result;
	}
}
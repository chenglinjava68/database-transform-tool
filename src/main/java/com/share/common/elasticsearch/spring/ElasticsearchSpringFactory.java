//package com.share.common.elasticsearch.spring;
//
//import java.util.Properties;
//
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//import org.springframework.data.elasticsearch.client.TransportClientFactoryBean;
//import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
//
//import com.share.common.util.StringUtil;
//
//public class ElasticsearchSpringFactory {
//	private static Logger logger = LogManager.getLogger();
//	protected ElasticsearchTemplate template;
//	private String clusterName;
//	private String servers;
//	private String username;
//	private String password;
//	
//	public String getClusterName() {
//		return clusterName;
//	}
//
//	public void setClusterName(String clusterName) {
//		this.clusterName = clusterName;
//	}
//
//	public String getServers() {
//		return servers;
//	}
//
//	public void setServers(String servers) {
//		this.servers = servers;
//	}
//
//	public String getUsername() {
//		return username;
//	}
//
//	public void setUsername(String username) {
//		this.username = username;
//	}
//
//	public String getPassword() {
//		return password;
//	}
//
//	public void setPassword(String password) {
//		this.password = password;
//	}
//
////	/**
////	 * @description Elasticsearch服务配置
////	 * @author yi.zhang
////	 * @time 2017年4月19日 上午10:38:42
////	 * @throws Exception
////	 */
//	public void init(String clusterName,String servers,String username,String password){
//		try {
//			TransportClientFactoryBean client = new TransportClientFactoryBean();
//			client.setClusterName(clusterName);
//			String clusterNodes = "";
//			for(String server : servers.split(",")){
//				String[] address = server.split(":");
//				String ip = address[0];
//				int port=9300;
//				if(address.length>1){
//					port = Integer.valueOf(address[1]);
//				}
//				if(StringUtil.isEmpty(clusterNodes)){
//					clusterNodes = ip+":"+port;
//				}else{
//					clusterNodes +=","+ ip+":"+port;
//				}
//			}
//			client.setClusterNodes(clusterNodes);
//			if(!StringUtil.isEmpty(username)&&!StringUtil.isEmpty(password)){
//				Properties properties = new Properties();
//				properties.put("xpack.security.user",username+":"+password);
//				client.setProperties(properties);
//			}
//			client.afterPropertiesSet();
//			template = new ElasticsearchTemplate(client.getObject());
//		} catch (Exception e) {
//			logger.error("-----Elasticsearch Config init Error-----", e);
//		}
//	}
//}

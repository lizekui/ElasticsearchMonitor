package com.xinhua.esmonitor;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.SQLFeatureNotSupportedException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.nlpcn.es4sql.SearchDao;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.SqlElasticSearchRequestBuilder;

import com.xinhua.mysqlmonitor.DBHelper;
import com.xinhua.mysqlmonitor.HelloMysql;
import com.xinhua.util.TimeUtil;

import jxl.Workbook;
import jxl.write.Label;
import jxl.write.WritableSheet;
import jxl.write.WritableWorkbook;

/**
 * hello elasticsearch！
 * 
 * @author: zkli  
 * @date: 2017年5月8日
 */
public class HelloElasticsearch 
{
	public static final String CLUSTER_NAME = "xinhuashe"; //实例名称  
    private static final String IP = "172.20.80.130";  
    private static final int PORT = 8301;  //端口  
    
    private static final String INDEX = "major_info";  
    private static final String TYPE = "major_info"; 
    
	public static final String sql_query = "SELECT * FROM "+INDEX+"/"+TYPE+" WHERE update_time > '2017-04-28 00:00:00' LIMIT 100";
	public static final String datestr = "2017-05-03";
	public static final String output_excel = "output-"+datestr+".csv";
   
    private static Settings settings;
    private static TransportClient client;  
    private static List<String> list_param_S = new ArrayList<String>();
    
    public HelloElasticsearch() throws UnknownHostException {
    	// Elasticsearch
    	settings = Settings  
                .settingsBuilder()  
                .put("cluster.name", CLUSTER_NAME)  
                .put("client.transport.sniff", true)  
                .build(); 
    	client = TransportClient.builder().settings(settings).build()  
                .addTransportAddress(new InetSocketTransportAddress(
                		InetAddress.getByName(IP), PORT));
    	
    	// Param_S
    	list_param_S.add("isi3");
    	list_param_S.add("isi1");
    	list_param_S.add("isi0");
	}
    
    // Run
    public static void main(String args[]) throws Exception {
    	HelloElasticsearch es_Test = new HelloElasticsearch();
    	es_Test.getDataList_mysql2es();
	}
    
    /**
     * 先在mysql中查到uuid list 再在es中查各种time
     * @throws Exception
     */
    private void getDataList_mysql2es() throws Exception {
    	OutputStream os = new FileOutputStream(output_excel);
    	Connection connection = DBHelper.getConn();
		WritableWorkbook wwb = Workbook.createWorkbook(os);
		
		// Iter Param S
		for (String param_S : list_param_S) {
			WritableSheet ws = wwb.createSheet(param_S, 0);
			
			Label label = new Label(0, 0, "flag_S");
			ws.addCell(label);
			label = new Label(1, 0, "source_url");
			ws.addCell(label);
			label = new Label(2, 0, "title");
			ws.addCell(label);
			label = new Label(3, 0, "UUID");
			ws.addCell(label);
			label = new Label(4, 0, "网站发布时间");
			ws.addCell(label);
			label = new Label(5, 0, "采集入库时间");
			ws.addCell(label);
			label = new Label(6, 0, "入ES时间");
			ws.addCell(label);
			label = new Label(7, 0, "计算为采用的时间");
			ws.addCell(label);
			label = new Label(8, 0, "网站发布 -> 采集入库时间");
			ws.addCell(label);
			label = new Label(9, 0, "网站发布 -> 入es时间");
			ws.addCell(label);
			label = new Label(10, 0, "入es时间 -> 计算为采用时间");
			ws.addCell(label);
			label = new Label(11, 0, "网站发布 -> 计算为采用总时间");
			ws.addCell(label);
			
			DecimalFormat df=new DecimalFormat("00");
			int lcnt = 1;
			int num_adopt_docs = 0;
			// Iter 24 hours per Param S
			for (int i = 0; i < 24; i++) {
	    		String time_end = (i+1==24)?":00:00":":59:59";
	    		int j = (i+1==24)?i:i+1;
	    		String time_period_begin = datestr+" "+df.format(i)+":00:00";
	        	String time_period_end = datestr+" "+df.format(j)+time_end;
	        	Map<String, String> hm_uuid_updatetime = HelloMysql.get_updatetimelist_from_mysql(connection, time_period_begin, time_period_end, param_S);
	        	num_adopt_docs+=hm_uuid_updatetime.size();
	        	// Iter uuid list per hours in every Param S
	        	for(Entry<String, String> uuid_updatetime:hm_uuid_updatetime.entrySet())
	    		{
	    			String uuid = uuid_updatetime.getKey();
	    			String update_time = uuid_updatetime.getValue();
	    			String sql = "SELECT * FROM "+INDEX+"/"+TYPE+" "
		    				+ "WHERE uuid='"+uuid+"'";
		    		SearchHits response = query(sql);
		    		System.out.println(sql+"\t"+response.getTotalHits()+"\t"+lcnt);
		        	for (SearchHit hit : response.getHits()){
		        		Map<String, Object> hm = hit.getSource();
		        		// source S
		        		String flag_S = hm.containsKey("S")?hm.get("S").toString():"NULL";
		        		// source_url
		        		String source_url = hm.containsKey("source_url")?hm.get("source_url").toString():"NULL";
		        		// title
		        		String title = hm.containsKey("title")?hm.get("title").toString():"NULL";
		        		// 网站转载时间
		        		String pubtime = hm.containsKey("pubtime")?hm.get("pubtime").toString():"NULL";
		        		// 采集入库时间
		        		String insert_time = hm.containsKey("insert_time")?hm.get("insert_time").toString():"NULL";
		        		// 入ES时间
		        		String es_updatetime = hm.containsKey("es_updatetime")?hm.get("es_updatetime").toString():"NULL";
		        		// 计算为采用的时间
		        		System.out.println("UUID：\t"+uuid);
		    			System.out.println("落地链接：\t"+source_url);
		        		System.out.println("落地时间：\t"+pubtime + "\t" + flag_S);
		        		System.out.println("采集：\t"+insert_time+"\t入ES：\t"+es_updatetime+"\t被计算为采用：\t"+update_time);
		        		// 发布时间 -> 采集入库时间
		        		String diff1 = TimeUtil.getDistanceTime(pubtime, insert_time);
		        		// 采集入库时间 -> 入es时间
		        		String diff2 = TimeUtil.getDistanceTime(insert_time, es_updatetime);
		        		// 入es时间 -> 计算为采用时间
		        		String diff3 = TimeUtil.getDistanceTime(es_updatetime, update_time);
		        		// 发布时间 -> 计算为采用总时间
		        		String diff4 = TimeUtil.getDistanceTime(pubtime, update_time);
		        		System.out.println(diff1+"\t"+diff2+"\t"+diff3+"\t"+diff4+"\n");
		        		
		        		label = new Label(0, lcnt, flag_S);
		        		ws.addCell(label);
		        		label = new Label(1, lcnt, source_url);
		        		ws.addCell(label);
		        		label = new Label(2, lcnt, title);
		        		ws.addCell(label);
		        		label = new Label(3, lcnt, uuid);
		        		ws.addCell(label);
		        		label = new Label(4, lcnt, pubtime);
		        		ws.addCell(label);
		        		label = new Label(5, lcnt, insert_time);
		        		ws.addCell(label);
		        		label = new Label(6, lcnt, es_updatetime);
		        		ws.addCell(label);
		        		label = new Label(7, lcnt, update_time);
		        		ws.addCell(label);
		        		label = new Label(8, lcnt, diff1);
		        		ws.addCell(label);
		        		label = new Label(9, lcnt, diff2);
		        		ws.addCell(label);
		        		label = new Label(10, lcnt, diff3);
		        		ws.addCell(label);
		        		label = new Label(11, lcnt, diff4);
		        		ws.addCell(label);
		        		lcnt ++;
		        	}
	    		}// End iter uuid list per hours in every Param S
			}// End iter 24 hours per Param S
			System.out.println("Param_S：\t"+param_S+"\tAdopt doc number：\t"+num_adopt_docs);
		}// End iter Param S
		
    	wwb.write();
		wwb.close();
		DBHelper.close(connection, null, null, null);
	}
    
    private SearchHits query(String query) throws SqlParseException, SQLFeatureNotSupportedException, SQLFeatureNotSupportedException {
    	SearchDao searchDao = new SearchDao(client);
        SqlElasticSearchRequestBuilder select = (SqlElasticSearchRequestBuilder) searchDao.explain(query).explain();
        return ((SearchResponse)select.get()).getHits();
    }
    
    private static TransportClient getTransportClient() {  
        return client;  
    }  
}

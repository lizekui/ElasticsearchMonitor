package com.xinhua.mysqlmonitor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * hello mysql！  
 * 
 * @author: zkli  
 * @date: 2017年5月8日
 */
public class HelloMysql {  
  
    static String sql = null;  
    static DBHelper db1 = null;  
    static ResultSet ret = null;  
  
    public static void main(String[] args) {  
    	Connection connection = DBHelper.getConn();
    	String uuid = "b154d1dc4c34e3023af64b0c2d8768c9z1";
    	String update_time = get_updatetime_from_mysql(connection, uuid);
    	DBHelper.close(connection, null, null, null);
    	
    	System.out.println(update_time);
    }

    /**
     * 获取单个uuid的update_time
     * @param connection
     * @param uuid
     * @return
     */
	public static String get_updatetime_from_mysql(Connection connection, String uuid) {
		String sql = "select update_time from xhs_news_adopt WHERE caiji_article_mongoid=?";
		System.out.println(sql);
    	PreparedStatement preparedStatement = null;
        try {
        	preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, uuid);
            ResultSet rs = preparedStatement.executeQuery();
            
            while (rs.next()) {
                String update_time = rs.getString("update_time");
                return update_time;
            }
 
        } catch (SQLException e) {
            e.printStackTrace();
        }
        DBHelper.close(null, null, preparedStatement, null);
        return "NULL";
	}  
	

	/**
	 * 对应时间区间的uuid List
	 * @param connection
	 * @param time_period_begin
	 * @param time_period_end
	 * @param param_S 
	 * @return
	 */
	public static Map<String, String> get_updatetimelist_from_mysql(Connection connection, String time_period_begin,
			String time_period_end, String param_S) {
		Map<String, String> hm_uuid_updatetime = new LinkedHashMap<String, String>();
//		String sql = "select * from xhs_media_adopt"
//				+ " WHERE update_time>? and update_time<? and caiji_searchengine=? LIMIT 20";
		String sql = "select * from xhs_media_adopt"
				+ " WHERE update_time>? and update_time<? and caiji_searchengine=?";
    	PreparedStatement preparedStatement = null;
        try {
        	preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, time_period_begin);
            preparedStatement.setString(2, time_period_end);
            preparedStatement.setString(3, param_S);
            ResultSet rs = preparedStatement.executeQuery();
            
            while (rs.next()) {
                String uuid = rs.getString("caiji_article_mongoid");
                String update_time = rs.getString("update_time");
                hm_uuid_updatetime.put(uuid, update_time);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        DBHelper.close(null, null, preparedStatement, null);
		return hm_uuid_updatetime;
	}
  
}  
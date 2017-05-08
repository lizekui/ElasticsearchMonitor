package com.xinhua.mysqlmonitor;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * hello database helper！
 * 
 * @author: zkli  
 * @date: 2017年5月8日
 */
public class DBHelper {
	public static final String url = "jdbc:mysql://172.20.67.92:3308/xhs_show";
	public static final String name = "com.mysql.jdbc.Driver";
	public static final String user = "$USER$";
	public static final String password = "$PASSWORD$";

	public static Connection getConn() {
		Connection conn = null;
		try {
			Class.forName(name);// 指定连接类型
			conn = DriverManager.getConnection(url, user, password);// 获取连接
		} catch (Exception e) {
			e.printStackTrace();
		}
		return conn;
	}

	public static PreparedStatement prepareStmt(Connection conn, String sql) {
		PreparedStatement pst = null;
		try {
			pst = conn.prepareStatement(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return pst;
	}

	public static ResultSet executeQuery(Statement stmt, String sql) {
		ResultSet rs = null;
		try {
			rs = stmt.executeQuery(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return rs;
	}

	public static void close(Connection conn, Statement pst, PreparedStatement preStatement, ResultSet rs) {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			conn = null;
		}
		if (pst != null) {
			try {
				pst.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			pst = null;
		}
		if (preStatement != null) {
			try {
				preStatement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			preStatement = null;
		}

		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			rs = null;
		}
	}
}
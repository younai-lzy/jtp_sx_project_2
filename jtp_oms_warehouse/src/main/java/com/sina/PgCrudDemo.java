package com.sina;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 使用JDBC操作PG数据，实现数据库表数据查询和插入操作。
 * @author xuanyu
 * @date 2025/7/25
 */
public class PgCrudDemo {

	/**
	 * 获取数据库连接
	 */
	public static Connection getPgConn() throws Exception{
		return DriverManager.getConnection(
			"jdbc:postgresql://node101:5432/db_test",
			"postgres",
			"123456"
		);
	}


	/**
	 * 从PG数据库中查询表数据
	 */
	public static void queryData() throws Exception{
		// 1. 连接
		Connection conn = getPgConn();
		// 2. Statement对象
		PreparedStatement ptst = conn.prepareStatement(
			"SELECT id, username, email, created_date FROM users"
		);
		// 3. 执行
		ResultSet resultSet = ptst.executeQuery();
		// 4. 遍历结果
		while (resultSet.next()){
			Object id = resultSet.getObject(1);
			String username = resultSet.getString("username");
			String email = resultSet.getString("email");
			Object createdDate = resultSet.getObject("created_date");
			System.out.println(id + "," + username + "," + email + "," + createdDate);
		}
		// 5. 关闭
		resultSet.close();
		ptst.close();
	}


	/**
	 * 向PG数据库中插入数据
	 */
	public static void insertData() throws Exception{
		// 1. 连接
		Connection conn = getPgConn();
		// 2. Statement对象
		PreparedStatement ptst = conn.prepareStatement(
			"INSERT INTO users (username, email) VALUES (?, ?)"
		);
		// 3. 设置值
		ptst.setObject(1, "qianba");
		ptst.setObject(2, "qb@126.com");
		// 4. 执行
		ptst.execute();
		// 5. 关闭
		ptst.close();
	}


	public static void main(String[] args) throws Exception{
		// 插入数据
		insertData() ;

		// 查询数据
		 queryData() ;
	}

}

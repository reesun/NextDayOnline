package utils;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class DBHandler {
	public static Connection getConnection () throws Exception {
		return getConnection("db.properties");
	}
	
	public static Connection getConnection (String path)throws Exception {
		Properties conf = new Properties();
		FileInputStream fis = new FileInputStream(path);
		conf.load(fis);
		return  DriverManager.getConnection(conf.getProperty("db_url"),conf.getProperty("db_user"), conf.getProperty("db_passwd"));
	}
}

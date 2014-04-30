package utils;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

import utils.handler.GetOneItemHandler;

public class DataWarehouseUtils {
	public static Integer getNewuserNumber(Connection conn, QueryRunner run, Integer datesk, Integer productid) throws SQLException {
		ResultSetHandler<Integer> newUserHandler = new GetOneItemHandler<Integer>();
		String sql = "select newusers from razor_sum_basic_product " 
			+ " where product_id ="+productid+" and date_sk = " + datesk;
		return run.query(conn, sql, newUserHandler);
	}
}

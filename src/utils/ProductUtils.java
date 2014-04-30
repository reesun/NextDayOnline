package utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;

import beans.ProductID;

public class ProductUtils {
	public static List<ProductID> getProductIDList(Connection conn, QueryRunner run) throws SQLException{
		ResultSetHandler<List<ProductID>> productIdHandler = new BeanListHandler<ProductID>(ProductID.class);
		String sql = "SELECT id FROM razor_product where active = 1";
		return run.query(conn, sql, productIdHandler);
	}
}

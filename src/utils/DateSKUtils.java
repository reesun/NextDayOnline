package utils;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

import utils.handler.DateSKHandler;

public class DateSKUtils {

	public static Integer getDateSK(Connection conn, QueryRunner run, String dateString) throws SQLException {
		ResultSetHandler<Integer> dateHandler = new DateSKHandler();
		String sql = "SELECT date_sk FROM  razor_dim_date WHERE datevalue=?";
		return run.query(conn, sql, dateHandler, dateString);
	}
}

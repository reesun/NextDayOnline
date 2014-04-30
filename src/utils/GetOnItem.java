package utils;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

import utils.handler.GetOneItemHandler;

public class GetOnItem {
	public static Long getCount(Connection conn, QueryRunner run, String sql) throws SQLException {
		ResultSetHandler<Long> resultSetHandler = new GetOneItemHandler<Long>();
		GetOneItemHandler<Long> resultHandler = new GetOneItemHandler<Long>();
		return run.query(conn, sql, resultHandler);
	}
}

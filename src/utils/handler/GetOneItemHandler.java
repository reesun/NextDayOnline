package utils.handler;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.dbutils.ResultSetHandler;

public class GetOneItemHandler<T> implements ResultSetHandler<T> {

	@SuppressWarnings("unchecked")
	@Override
	public T handle(ResultSet rs) throws SQLException {
		if (!rs.next())
			return null;
		
		return (T) rs.getObject(1);
	}

}

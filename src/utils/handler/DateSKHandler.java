package utils.handler;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DateSKHandler extends GetOneItemHandler<Integer> {
	@Override
	public Integer handle(ResultSet rs) throws SQLException {
		Integer ret = super.handle(rs);
		if(null == ret)
			return -1;
		else 
			return ret;				
	}
}

import java.sql.Connection;

import org.apache.commons.dbutils.QueryRunner;

import utils.DBHandler;
import utils.DateUtils;
import utils.handler.GetOneItemHandler;

public class MonthlyUserOnline {

	public static void main(String[] args) throws Exception {
		String appID = "";
		
		if(args.length != 1)
			return;
		else 
			appID = args[0];
		
		// connections
		Connection dashreportConn = DBHandler.getConnection("conf/dashreports.properties");
		Connection snsConn = DBHandler.getConnection("conf/snswebbus.properties");
		if (dashreportConn == null || snsConn == null) {
			System.out.println("get connection error!!");
			return;
		}
		QueryRunner run = new QueryRunner();
		
		String onlineDateString  = DateUtils.getBeforeDays(-1);
		String monthlyOnlineDate = DateUtils.getBeforeDays(-30);
		
		
		GetOneItemHandler<Object> getOneItemHandler = new GetOneItemHandler<Object>();
		String sql = "SELECT COUNT( 1 ) AS num FROM pre_phone_user_device_info_new " +
				" WHERE app_id =? AND mysql_timestamp >=  ?";
		
		Object dailyNum = run.query(snsConn, sql, getOneItemHandler, appID, onlineDateString);
		System.out.println(dailyNum.toString());
		Object monthlyNum = run.query(snsConn, sql, getOneItemHandler, appID, monthlyOnlineDate);
		System.out.println(monthlyNum.toString());
		
		sql = "INSERT INTO online_user_num(date_time, day_online, month_online) VALUES (?,?,?)";
		run.update(dashreportConn, sql, onlineDateString, dailyNum, monthlyNum);
	}

}

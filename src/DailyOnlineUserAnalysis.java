import java.sql.Connection;
import java.util.List;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;

import beans.OnlineUserDayliy;
import beans.Region;
import utils.DBHandler;
import utils.DateUtils;
import utils.IP2Address;

public class DailyOnlineUserAnalysis {

	public static void main(String[] args) throws Exception {
		String yesterday = DateUtils.getYesterday();
		int app_id = 19;
		
		Connection snswebbusConn = DBHandler.getConnection("conf/snswebbus.properties");
		Connection dashreportConn = DBHandler.getConnection("conf/dashreports.properties");
		if (snswebbusConn == null || dashreportConn == null)
			return;
		
		QueryRunner run = new QueryRunner();
				
		// get daily user
		ResultSetHandler<List<OnlineUserDayliy>> onlineUserHandler = new BeanListHandler<OnlineUserDayliy>(OnlineUserDayliy.class);
		String sql = "SELECT uid, ip, '"+yesterday+"' as date_time, app_version FROM pre_phone_user_device_info_new "
				+ " WHERE app_id = "+ app_id + " AND mysql_timestamp >= '" +yesterday+"'";
		
		List<OnlineUserDayliy> onlineUsers = run.query(snswebbusConn, sql, onlineUserHandler);
		
		// clean 
		sql = "TRUNCATE TABLE online_user_classify_daily";
		run.update(dashreportConn, sql);
		
		sql = "INSERT INTO online_user_classify_daily (date_time, area, city, app_version, ip, uid) " +
				" VALUES (?,?,?,?,?,?)";
		
		// get region and city
		for(OnlineUserDayliy user:onlineUsers){
			String ip = user.getIp();
			String[] ips = ip.split(",");
			if(ips.length > 1)
				ip = ips[0];
			try{
				Region region = IP2Address.getAdressByBaidu(ip);
				run.update(dashreportConn, sql, 
					user.getDate_time(), region.getRegion(), 
					region.getCity(), user.getApp_version(),
					ip,user.getUid());
				//Thread.sleep(100);
			}catch (Exception e){
				e.printStackTrace();
				continue;
			}
		}
				
		// group
		sql = "SELECT '"+yesterday+"' as date_time,app_version, area,city,count(1) as num FROM "
				+ " online_user_classify_daily"
				+ " group by app_version, area,city";
		
		onlineUsers = run.query(dashreportConn, sql, onlineUserHandler);
		sql = "insert into online_user_classify(date_time, app_version, area, city, num)" +
				" values (?,?,?,?,?)";
		
		for(OnlineUserDayliy user:onlineUsers){
			run.update(dashreportConn, sql, user.getDate_time(), user.getApp_version(),
					user.getArea(), user.getCity(), user.getNum());
		}
	}

}

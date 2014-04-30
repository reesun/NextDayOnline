
import java.sql.Connection;
import java.util.List;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;

import beans.DeviceInfo;
import utils.DBHandler;
import utils.DateUtils;
import utils.handler.DateSKHandler;
import utils.handler.GetOneItemHandler;

public class NextDayOnlineUser {

	public static void main(String[] args) throws Exception {
		Connection snswebbusConn = DBHandler.getConnection("conf/snswebbus.properties");
		Connection dashreportConn = DBHandler.getConnection("conf/dashreports.properties");
		Connection dwConn = DBHandler.getConnection("conf/db.properties");
		if (snswebbusConn == null || dashreportConn == null || dwConn == null)
			return;
		
		QueryRunner run = new QueryRunner();
		
		String newuserDay = DateUtils.getBeforeDays(-2);
		String threeDay = DateUtils.getBeforeDays(-3);
		String fourDay = DateUtils.getBeforeDays(-4);
		String deviceDay = DateUtils.getYesterday();
		String today = DateUtils.getCurr();
		
		ResultSetHandler<Integer> dateHandler = new DateSKHandler();
		String sql = "SELECT date_sk FROM  razor_dim_date WHERE datevalue=?";
		Integer datesk = run.query(dwConn, sql, dateHandler, newuserDay);
		
		// 将pre_phone_user_device_info 前一天的数据刷到dashreport中的pre_phone_user_device_info
		sql = "TRUNCATE TABLE uniq_device_info";
		run.update(dashreportConn, sql);
		
		sql = "select uid from snswebbus.pre_phone_user_device_info_new "
				+ " where mysql_timestamp >= '" + deviceDay +"'"
				+ " And app_id = 13";
		ResultSetHandler<List<DeviceInfo>> deviceHandler = new BeanListHandler<DeviceInfo>(DeviceInfo.class);
		List<DeviceInfo> deviceInfos = run.query(snswebbusConn, sql, deviceHandler);
		sql = "insert into uniq_device_info (uid) values (?)";
		for(DeviceInfo deviceInfo : deviceInfos){
			run.update(dashreportConn,sql,deviceInfo.getUid());
		}
		
		// 将再提前一天的newuser刷到dashreport中的newusers
		sql = "TRUNCATE TABLE newusers";
		run.update(dashreportConn, sql);
		
		sql = "select deviceidentifier as uid from razor_fact_clientdata "
				+ " where date_sk = "+ datesk +" and `isnew` = 1";
		
		List<DeviceInfo> newusers = run.query(dwConn, sql, deviceHandler);
		sql = "insert into newusers (uid) values (?)";
		for(DeviceInfo newuser:newusers){
			run.update(dashreportConn,sql,newuser.getUid());
		}
		
		// 计算次日用户存留
		sql = "select count(1)  from newusers "
				+ " where uid in (select uid from uniq_device_info)";
		ResultSetHandler<Long> nextDayUserHandler = new GetOneItemHandler<Long>();
		Long nextDayUser = run.query(dashreportConn, sql, nextDayUserHandler);
		
		// 获得每天新增用户
		Integer newuserNumber = 0;
		ResultSetHandler<Integer> newUserHandler = new GetOneItemHandler<Integer>();
		sql = "select newusers from razor_sum_basic_product " 
				+ " where product_id =1 and date_sk = " + datesk;
		newuserNumber = run.query(dwConn, sql, newUserHandler);
//		System.out.println(newuserDay + " " + datesk + " "+ newusers);
		
		// insert
		sql = "insert into online_nexday_user (datetime,newusers,onlineusers) values (?,?,?)";
		run.update(dashreportConn, sql, newuserDay, newuserNumber,nextDayUser);
		
		// 3days
		sql = "TRUNCATE TABLE newusers";
		run.update(dashreportConn, sql);
		
		datesk -= 1;
		sql = "select deviceidentifier as uid from razor_fact_clientdata "
				+ " where date_sk = "+ datesk +" and `isnew` = 1";
		
		List<DeviceInfo> thressDayNewusers = run.query(dwConn, sql, deviceHandler);
		sql = "insert into newusers (uid) values (?)";
		for(DeviceInfo newuser:thressDayNewusers){
			run.update(dashreportConn,sql,newuser.getUid());
		}
		
		sql = "select count(1)  from newusers "
				+ " where uid in (select uid from uniq_device_info)";
//		ResultSetHandler<Long> nextDayUserHandler = new GetOneItemHandler<Long>();
		Long threeDayUser = run.query(dashreportConn, sql, nextDayUserHandler);
		
		sql = "select count(1) from online_nexday_user where datetime = '" + threeDay + "'";
		if(run.query(dashreportConn, sql, nextDayUserHandler) > 0){
			sql = "update online_nexday_user set 3days="+threeDayUser+" where datetime='" + threeDay + "'";
			run.update(dashreportConn, sql);
		}else {
			sql = "insert into online_nexday_user (datetime,3days) values (?,?)";
			run.update(dashreportConn, sql, threeDay, threeDayUser);
		}
		
		// 4days
		sql = "TRUNCATE TABLE newusers";
		run.update(dashreportConn, sql);
		
		datesk -= 1;
		sql = "select deviceidentifier as uid from razor_fact_clientdata "
				+ " where date_sk = "+ datesk +" and `isnew` = 1";
		
		List<DeviceInfo> fourDayNewusers = run.query(dwConn, sql, deviceHandler);
		sql = "insert into newusers (uid) values (?)";
		for(DeviceInfo newuser:fourDayNewusers){
			run.update(dashreportConn,sql,newuser.getUid());
		}
		
		sql = "select count(1)  from newusers "
				+ " where uid in (select uid from uniq_device_info)";
//		ResultSetHandler<Long> nextDayUserHandler = new GetOneItemHandler<Long>();
		Long fourDayUser = run.query(dashreportConn, sql, nextDayUserHandler);
		
		sql = "select count(1) from online_nexday_user where datetime = '" + fourDay + "'";
		if(run.query(dashreportConn, sql, nextDayUserHandler) > 0){
			sql = "update online_nexday_user set 4days="+fourDayUser+" where datetime='" + fourDay + "'";
			run.update(dashreportConn, sql);
		}else {
			sql = "insert into online_nexday_user (datetime,4days) values (?,?)";
			run.update(dashreportConn, sql, fourDay, fourDayUser);
		}
		
	}

}

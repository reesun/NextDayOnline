import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;

import beans.DeviceInfo;
import utils.DBHandler;
import utils.DataWarehouseUtils;
import utils.DateSKUtils;
import utils.DateUtils;
import utils.handler.GetOneItemHandler;

public class NextDayOnlineUser {

	public static void main(String[] args) throws Exception {
		Connection snswebbusConn = DBHandler.getConnection("conf/snswebbus.properties");
		Connection dashreportConn = DBHandler.getConnection("conf/dashreports.properties");
		Connection dwConn = DBHandler.getConnection("conf/db.properties");
		if (snswebbusConn == null || dashreportConn == null || dwConn == null)
			return;

		QueryRunner run = new QueryRunner();

		int appID = 13;
		int productID=1;

		List<DateObject> dateLists = generateDateList(dwConn, run);
		
		// 将pre_phone_user_device_info
		// 前一天的数据刷到dashreport中的pre_phone_user_device_info
		restoreOnlieUserUID(dashreportConn, snswebbusConn, run, DateUtils.getYesterday(), appID);

		for (DateObject dateObject : dateLists) {
			int datesk = dateObject.dateSK;
			String dateString = dateObject.dateTimeString;
			String columnNmeString = dateObject.columnString;
			
			//根据datesk获得当日的新增用户数
			Long nextDayUser = getOnlineUsersByDateSK(dashreportConn, dwConn,run, datesk, dateString, appID);

			// 获得每天新增用户
			int newuserNumber = DataWarehouseUtils.getNewuserNumber(dwConn, run, datesk, productID);

			// 更新online_nexday_user
			updateOnlineNextDayUser(dashreportConn, run, dateString, columnNmeString, newuserNumber, nextDayUser);
		}
	}

	/**
	 * 根据datesk获得当日新增用户数，并将其转存到dashreports中的newusers表中
	 * 
	 * @param dashreportConn
	 * @param dwConn
	 * @param run
	 * @param deviceHandler
	 * @param datesk
	 * @throws SQLException
	 */
	public static Long getOnlineUsersByDateSK(Connection dashreportConn,
			Connection dwConn, QueryRunner run, int datesk, String newuserDay,
			Integer appID) throws SQLException {

		ResultSetHandler<List<DeviceInfo>> deviceHandler = new BeanListHandler<DeviceInfo>(
				DeviceInfo.class);

		String sql = "TRUNCATE TABLE newusers";
		run.update(dashreportConn, sql);

		sql = "select deviceidentifier as uid from razor_fact_clientdata "
				+ " where date_sk = " + datesk + " and `isnew` = 1";

		List<DeviceInfo> newusers = run.query(dwConn, sql, deviceHandler);
		sql = "insert into newusers (uid) values (?)";
		for (DeviceInfo newuser : newusers) {
			run.update(dashreportConn, sql, newuser.getUid());
		}

		// 计算次日用户存留
		sql = "select count(1)  from newusers "
				+ " where uid in (select uid from uniq_device_info)";
		ResultSetHandler<Long> nextDayUserHandler = new GetOneItemHandler<Long>();
		return run.query(dashreportConn, sql, nextDayUserHandler);
	}

	public static void restoreOnlieUserUID(Connection dashreportConn,
			Connection snswebbusConn, QueryRunner run, String deviceDay,
			int appID) throws Exception {
		String sql = "TRUNCATE TABLE uniq_device_info";
		run.update(dashreportConn, sql);

		sql = "select uid from snswebbus.pre_phone_user_device_info_new "
				+ " where mysql_timestamp >= '" + deviceDay + "'"
				+ " And app_id = " + appID;
		ResultSetHandler<List<DeviceInfo>> deviceHandler = new BeanListHandler<DeviceInfo>(
				DeviceInfo.class);
		List<DeviceInfo> deviceInfos = run.query(snswebbusConn, sql,
				deviceHandler);
		sql = "insert into uniq_device_info (uid) values (?)";
		for (DeviceInfo deviceInfo : deviceInfos) {
			run.update(dashreportConn, sql, deviceInfo.getUid());
		}
	}

	public static void updateOnlineNextDayUser(Connection dashreportConn,
			QueryRunner run, String dateTime, String colume, int newusers, Long dayUsers)
			throws Exception {

		ResultSetHandler<Long> nextDayUserHandler = new GetOneItemHandler<Long>();
		String sql = "select count(1) from online_nexday_user where datetime = '"
				+ dateTime + "'";
		if (run.query(dashreportConn, sql, nextDayUserHandler) > 0) {
			sql = "update online_nexday_user set `" + colume + "`=" + dayUsers
					+ " where datetime='" + dateTime + "'";
			run.update(dashreportConn, sql);
		} else {
			sql = "insert into online_nexday_user (datetime,newusers,onlineusers) values (?,?,?)";
			run.update(dashreportConn, sql, dateTime, newusers, dayUsers);
		}

	}

	public static List<DateObject> generateDateList(Connection dwConn,
			QueryRunner run) throws SQLException {
		ArrayList<DateObject> dateList = new ArrayList<DateObject>();

		int numbers[] = { -2, -3, -4, -5, -6, -7, -14, -30 };
		String columnNames[] = { "onlineusers", "3days", "4days", "5days",
				"6days", "7days", "14days", "30days" };

		for (int i = 0; i < 8; i++) {
			int a= numbers[i];
			System.out.println(a);
			String dateTimeString = DateUtils.getBeforeDays(numbers[i]);
			int datesk = DateSKUtils.getDateSK(dwConn, run, dateTimeString);
			DateObject dateObject = new DateObject(datesk, dateTimeString,
					columnNames[i]);
			dateList.add(dateObject);
		}
		return dateList;
	}

}

class DateObject {
	public String dateTimeString;
	public int dateSK;
	public String columnString;

	public DateObject(int datesk, String dateTime, String column) {
		this.dateTimeString = dateTime;
		this.dateSK = datesk;
		this.columnString = column;
	}
}
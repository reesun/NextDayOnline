import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.dbutils.QueryRunner;

import utils.DBHandler;
import utils.DataWarehouseUtils;
import utils.DateSKUtils;
import utils.DateUtils;
import utils.GetOnItem;
import utils.ProductUtils;
import utils.handler.GetOneItemHandler;
import beans.ProductID;

public class NextDayStartupUser {
	public static void main(String[] args) throws Exception {
		System.out.println("in NextDayStartupUser");
		// Connection snswebbusConn =
		// DBHandler.getConnection("conf/snswebbus.properties");
		Connection baobiaoConn = DBHandler
				.getConnection("conf/baobiao.properties");
		Connection dwConn = DBHandler.getConnection("conf/db.properties");
		if (baobiaoConn == null || dwConn == null)
			return;

		QueryRunner run = new QueryRunner();

		String newuserDay = DateUtils.getBeforeDays(-2);
		String threeDay = DateUtils.getBeforeDays(-3);
		String fourDay = DateUtils.getBeforeDays(-4);
		String fiveDay = DateUtils.getBeforeDays(-5);
		String sixDay = DateUtils.getBeforeDays(-6);
		String sevenDay = DateUtils.getBeforeDays(-7);
		String fourteenDay = DateUtils.getBeforeDays(-14);
		String monthDay = DateUtils.getBeforeDays(-30);

		// String deviceDay = DateUtils.getYesterday();
		// String today = DateUtils.getCurr();

		Integer newUserDatesk = 0, startupDateSK = 0, threeDdateSK = 0, fourDdateSK = 0, fiveDateDK = 0;
		Integer sixDateSK = 0, sevenDateSK = 0, fourteenDateSK = 0, monthDateSK = 0;
		try {
			newUserDatesk = DateSKUtils.getDateSK(dwConn, run, newuserDay);
			startupDateSK = newUserDatesk + 1;
			threeDdateSK = newUserDatesk - 1;
			fourDdateSK = newUserDatesk - 2;
			fiveDateDK = newUserDatesk - 3;
			sixDateSK = newUserDatesk - 4;
			sevenDateSK = newUserDatesk - 5;
			fourteenDateSK = newUserDatesk - 12;
			monthDateSK = newUserDatesk - 28;

		} catch (SQLException e) {
			System.out.println("query datesk error");
			return;
		}

		// 获得所有productid
		List<ProductID> productids = ProductUtils.getProductIDList(baobiaoConn,
				run);
		if (productids.size() <= 0)
			return;

		for (ProductID productid : productids) {
			Integer pid = productid.getId();

			// 次日活跃
			Integer newuserNumber = DataWarehouseUtils.getNewuserNumber(dwConn,
					run, newUserDatesk, pid);
			Long starupNum = getStartUpUser(dwConn, run, newUserDatesk,
					startupDateSK, pid);

			String sql = "INSERT INTO razor_fact_nexday_startup (datetime, newusers, nextdaystartup, productid) VALUES (?,?,?,?)";
			run.update(dwConn, sql, newuserDay, newuserNumber, starupNum, pid);

			// 第三日活跃
			newuserNumber = DataWarehouseUtils.getNewuserNumber(dwConn, run,
					threeDdateSK, pid);
			long threeDaysStartup = getStartUpUser(dwConn, run, threeDdateSK, startupDateSK, pid);

			sql = "select count(1) from razor_fact_nexday_startup where datetime = '"
					+ threeDay + "' and productid = " + pid;
			if (GetOnItem.getCount(dwConn, run, sql) > 0) {
				sql = "update razor_fact_nexday_startup set newusers = "
						+ newuserNumber + ", 3days = " + threeDaysStartup
						+ " where datetime = '" + threeDay
						+ "' and productid = " + pid;
				run.update(dwConn, sql);
			} else {
				// 更新次日活跃
				sql = "insert into razor_fact_nexday_startup (datetime,newusers, 3days, productid) values (?,?,?,?)";
				run.update(dwConn, sql, threeDay, newuserNumber, threeDaysStartup, pid);
			}

			// 第四日活跃
			newuserNumber = DataWarehouseUtils.getNewuserNumber(dwConn, run, fourDdateSK, pid);
			long fourDaysStartup = getStartUpUser(dwConn, run, fourDdateSK,
					startupDateSK, pid);

			sql = "select count(1) from razor_fact_nexday_startup where datetime = '"
					+ fourDay + "' and productid = " + pid;
			if (GetOnItem.getCount(dwConn, run, sql) > 0) {
				sql = "update razor_fact_nexday_startup set newusers = "
						+ newuserNumber + ", 4days = " + fourDaysStartup
						+ " where datetime = '" + fourDay
						+ "' and productid = " + pid;
				run.update(dwConn, sql);
			} else {
				// 次日活跃
				sql = "insert into razor_fact_nexday_startup (datetime,newusers, 4days, productid) values (?,?,?,?)";
				run.update(dwConn, sql, fourDay, newuserNumber, fourDaysStartup, pid);
			}

			// 第五日
			newuserNumber = DataWarehouseUtils.getNewuserNumber(dwConn, run,fiveDateDK, pid);
			long fiveDaysStartup = getStartUpUser(dwConn, run, fiveDateDK,
					startupDateSK, pid);

			sql = "select count(1) from razor_fact_nexday_startup where datetime = '"
					+ fiveDay + "' and productid = " + pid;
			if (GetOnItem.getCount(dwConn, run, sql) > 0) {
				sql = "update razor_fact_nexday_startup set newusers = "
						+ newuserNumber + ", 5days = " + fiveDaysStartup
						+ " where datetime = '" + fiveDay
						+ "' and productid = " + pid;
				run.update(dwConn, sql);
			} else {
				// 次日活跃
				sql = "insert into razor_fact_nexday_startup (datetime,newusers, nextdaystartup, 5days, productid) values (?,?,?,?)";
				run.update(dwConn, sql, fiveDay, newuserNumber, starupNum, fiveDaysStartup, pid);
			}

			// 第六日
			newuserNumber = DataWarehouseUtils.getNewuserNumber(dwConn, run, sixDateSK, pid);
			long sixDaysStartup = getStartUpUser(dwConn, run, sixDateSK, startupDateSK, pid);

			sql = "select count(1) from razor_fact_nexday_startup where datetime = '"
					+ sixDay + "' and productid = " + pid;
			if (GetOnItem.getCount(dwConn, run, sql) > 0) {
				sql = "update razor_fact_nexday_startup set newusers = "
						+ newuserNumber + ", 6days = " + sixDaysStartup
						+ " where datetime = '" + sixDay
						+ "' and productid = " + pid;
				run.update(dwConn, sql);
			} else {
				// 次日活跃
				sql = "insert into razor_fact_nexday_startup (datetime,newusers, 6days, productid) values (?,?,?,?)";
				run.update(dwConn, sql, sixDay, newuserNumber, sixDaysStartup, pid);
			}
			
			// 第七日
			newuserNumber = DataWarehouseUtils.getNewuserNumber(dwConn, run,
					sevenDateSK, pid);
			long sevenDaysStartup = getStartUpUser(dwConn, run, sevenDateSK,
					startupDateSK, pid);

			sql = "select count(1) from razor_fact_nexday_startup where datetime = '"
					+ sevenDay + "' and productid = " + pid;
			if (GetOnItem.getCount(dwConn, run, sql) > 0) {
				sql = "update razor_fact_nexday_startup set newusers = "
						+ newuserNumber + ", 7days = " + sevenDaysStartup
						+ " where datetime = '" + sevenDay + "' and productid = "
						+ pid;
				run.update(dwConn, sql);
			} else {
				// 次日活跃
				sql = "insert into razor_fact_nexday_startup (datetime,newusers, 7days, productid) values (?,?,?,?)";
				run.update(dwConn, sql, sevenDay, newuserNumber, sevenDaysStartup,pid);
			}
			
			// 第14日
			newuserNumber = DataWarehouseUtils.getNewuserNumber(dwConn, run,
					fourteenDateSK, pid);
			long fourteenDaysStartup = getStartUpUser(dwConn, run, fourteenDateSK,
					startupDateSK, pid);

			sql = "select count(1) from razor_fact_nexday_startup where datetime = '"
					+ fourteenDay + "' and productid = " + pid;
			if (GetOnItem.getCount(dwConn, run, sql) > 0) {
				sql = "update razor_fact_nexday_startup set newusers = "
						+ newuserNumber + ", 14days = " + fourteenDaysStartup
						+ " where datetime = '" + fourteenDay
						+ "' and productid = " + pid;
				run.update(dwConn, sql);
			} else {
				// 次日活跃
				sql = "insert into razor_fact_nexday_startup (datetime,newusers, 14days, productid) values (?,?,?,?)";
				run.update(dwConn, sql, fourteenDay, newuserNumber,
						fourteenDaysStartup, pid);
			}
			
			// 第30日
			newuserNumber = DataWarehouseUtils.getNewuserNumber(dwConn, run,
					monthDateSK, pid);
			long monthDaysStartup = getStartUpUser(dwConn, run,
					monthDateSK, startupDateSK, pid);

			sql = "select count(1) from razor_fact_nexday_startup where datetime = '"
					+ monthDay + "' and productid = " + pid;
			if (GetOnItem.getCount(dwConn, run, sql) > 0) {
				sql = "update razor_fact_nexday_startup set newusers = "
						+ newuserNumber + ", 30days = " + monthDaysStartup
						+ " where datetime = '" + monthDay
						+ "' and productid = " + pid;
				run.update(dwConn, sql);
			} else {
				// 次日活跃
				sql = "insert into razor_fact_nexday_startup (datetime,newusers, 30days, productid) values (?,?,?,?)";
				run.update(dwConn, sql, monthDay, newuserNumber,
						monthDaysStartup, pid);
			}
		}

		dwConn.close();
		baobiaoConn.close();
	}

	public static Long getStartUpUser(Connection conn, QueryRunner run,
			Integer datesk, Integer startupDateSK, Integer productid)
			throws SQLException {
		GetOneItemHandler<Long> resultHandler = new GetOneItemHandler<Long>();

		String sql = "SELECT COUNT(DISTINCT `deviceidentifier`) FROM `razor_fact_clientdata` "
				+ " WHERE `product_sk` IN ( SELECT `product_sk` FROM `razor_dim_product` WHERE `product_id` = "
				+ productid
				+ " )"
				+ " AND date_sk = "
				+ datesk
				+ " AND `isnew` =1"
				+ " AND deviceidentifier IN ( SELECT deviceidentifier FROM razor_fact_clientdata WHERE date_sk ="
				+ startupDateSK + " AND isnew  = 0)";

		return run.query(conn, sql, resultHandler);
	}
}

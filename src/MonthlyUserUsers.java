import java.sql.Connection;
import java.util.List;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;

import beans.MonthlyUser;
import utils.DBHandler;
import utils.DateUtils;
import utils.handler.DateSKHandler;

public class MonthlyUserUsers {

	public static void main(String[] args) throws Exception {
		// connections
		Connection dashreportConn = DBHandler.getConnection("conf/dashreports.properties");
		Connection dwConn = DBHandler.getConnection("conf/db.properties");	
		if(dashreportConn == null || dwConn == null){
			System.out.println("get connection error!!");
			return;
		}
		QueryRunner run = new QueryRunner();
		
		// get date 
		String lastMonth = DateUtils.getLastMonth();
		String currMonth = DateUtils.getCurrMonth();
		
		ResultSetHandler<Integer> dateHandler = new DateSKHandler();
		String sql = "SELECT date_sk FROM  razor_dim_date WHERE datevalue=?";
		Integer fromDatesk = run.query(dwConn, sql, dateHandler, lastMonth+"-01");
		Integer toDatesk = run.query(dwConn, sql, dateHandler, currMonth+"-01");
		
		sql = "TRUNCATE TABLE `tmp_monthly_user`";
		run.update(dashreportConn, sql);
		
		// 从razor_fact_clientdata中得到自然月的用户id以及日期
		sql = "SELECT  `deviceidentifier` as uid , date_sk FROM  `razor_fact_clientdata` "
				+ " WHERE date_sk >="+fromDatesk+" AND date_sk <"+toDatesk;
		ResultSetHandler<List<MonthlyUser>> monthlyUserHandler = new BeanListHandler<MonthlyUser>(MonthlyUser.class);
		
		List<MonthlyUser> monthlyUsers = run.query(dwConn, sql, monthlyUserHandler);
		sql = "INSERT INTO tmp_monthly_user(uid, date_sk) VALUES (?,?)";
		for (MonthlyUser user:monthlyUsers) {
			run.update(dashreportConn, sql, user.getUid(), user.getDate_sk());
		}
		
		// 计算每个uid的启动天数
		sql = "SELECT days, COUNT( 1 ) nums FROM "
				+ "  (SELECT uid, COUNT( DISTINCT date_sk ) AS days "
				+ "   FROM  `tmp_monthly_user` GROUP BY uid)tmp"
				+ " GROUP BY days";
		
		monthlyUsers = run.query(dashreportConn, sql,monthlyUserHandler);
		
		int oneDay = 0, twoDays = 0, g3 = 0;
		int g6 =0, g11 = 0, g19 = 0;
//		List<Map<String, Integer>> monthlyUserList = new ArrayList<Map<String,Integer>>();
//		Map<String, Integer> map = new HashMap<String, Integer>();
		for(MonthlyUser user: monthlyUsers){
			int days = user.getDays();
			if(days == 1)
				oneDay += user.getNums();
			else if(days == 2)
				twoDays+= user.getNums();
			else if(days >=3 && days <=5)
				g3+= user.getNums();
			else if(days >=6 && days <= 10)
				g6+= user.getNums();
			else if(days >= 11 && days <= 18)
				g11+= user.getNums();
			else 
				g19+= user.getNums();
		}
		
		sql = "INSERT INTO user_startup_monthly_sum(date_time, start_num, value)"
				+ " VALUES (?,?,?)";
		run.update(dashreportConn, sql, lastMonth, "1天", oneDay);
		run.update(dashreportConn, sql, lastMonth, "2天", twoDays);
		run.update(dashreportConn, sql, lastMonth, "3-5天", g3);
		run.update(dashreportConn, sql, lastMonth, "6-10天", g6);
		run.update(dashreportConn, sql, lastMonth, "11-18天", g11);
		run.update(dashreportConn, sql, lastMonth, "大于19天", g19);
		
	}

}

import java.nio.channels.SelectableChannel;
import java.sql.Connection;
import java.util.List;

import org.apache.commons.dbutils.QueryRunner;

import beans.ProductID;

import utils.DBHandler;
import utils.DateSKUtils;
import utils.DateUtils;
import utils.ETL;
import utils.ProductUtils;
import utils.handler.GetOneItemHandler;

public class StartupGap {

	public static void main(String[] args) throws Exception {
		Connection baobiaoConn = DBHandler.getConnection("conf/baobiao.properties");
		Connection dwConn = DBHandler.getConnection("conf/db.properties");
		QueryRunner run = new QueryRunner();
		if(null == dwConn || null == baobiaoConn || null == run)
			return;
		
		String dateString = DateUtils.getCurr();
		int endDateSK = 0, fromDateSK = 0;
		endDateSK= DateSKUtils.getDateSK(dwConn, run, dateString);
		if(0 == endDateSK)
			return;
		else 
			fromDateSK = endDateSK - 30;
		
		List<ProductID> productIDs = ProductUtils.getProductIDList(baobiaoConn, run);
		for(ProductID productID : productIDs){
			int prodcutid = productID.getId();
			// 取前30天的启动记录
			long s = System.currentTimeMillis();
//			ETL.restoreStartupClientdata(dwConn, run, fromDateSK, endDateSK-1, prodcutid);
			long e = System.currentTimeMillis();
			System.out.println("Time: " + (e - s) / 1000);
			
			GetOneItemHandler<Object> onHandler = new GetOneItemHandler<Object>();
			
			// 计算 0-24小时启动数
			String sql = "SELECT sum(num-1) FROM `razor_tmp_startupgap_1` ";
			Object hour = run.query(dwConn, sql, onHandler);
			System.out.println(hour.toString());
			
			// 30天内，只有一次启动
			sql = "select count(1) from (SELECT `deviceidentifier`,count(1) num FROM `razor_tmp_startupgap_1` group by `deviceidentifier`having num = 1) tt";
			Object oneStart = run.query(dwConn, sql, onHandler);
			System.out.println(oneStart.toString());
			
			Object day1, day2, day3, day4, day5, day6, day7, day14, day30;
			sql = "SELECT DISTINCT  `deviceidentifier`  FROM  `razor_tmp_startupgap_1` ";
			
			
			// +1天
			
			// +2
			
			// +3
			
			// +4
			
			// +5
			
			// +6
			
			// +7
			
			// +14 
			
			// +30 
			
			
			sql = "insert into  razor_sum_startup_gap(endTime, onstart, hour, product_id) values(?,?,?,?)";
//			run.update(dwConn, sql, dateString, oneStart, hour, prodcutid);
			
			e = System.currentTimeMillis();
			System.out.println("aa time:"+ (e - s) / 1000);
		}
		
	}
	
	

}

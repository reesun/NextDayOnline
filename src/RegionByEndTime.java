

import java.sql.Connection;
import java.sql.SQLClientInfoException;
import java.util.List;

import org.apache.commons.dbutils.QueryRunner;

import utils.DBHandler;
import utils.DateSKUtils;
import utils.ProductUtils;

import beans.ProductID;

public class RegionByEndTime {

	public static void main(String[] args) throws Exception {
		Long s = System.currentTimeMillis();
		Connection dwConn = DBHandler.getConnection("conf/db.properties");
		Connection baobiaoConn = DBHandler
				.getConnection("conf/baobiao.properties");
		if (null == dwConn || null == baobiaoConn){
			throw new SQLClientInfoException();
		}

		QueryRunner run = new QueryRunner();

		String currDate = utils.DateUtils.getCurr();
		
		int curDateSK = DateSKUtils.getDateSK(dwConn, run, currDate);
		int fromWeek = 0, fromMonth = 0, endDate = 0;
		if (curDateSK >= 0) {
			fromWeek = curDateSK - 7;
			fromMonth = curDateSK - 30;
			endDate = curDateSK - 1;
		}
		System.out.println(fromWeek + " " + fromMonth + " " + endDate);
		
		// ETL
		String sql = "TRUNCATE TABLE razor_tmp_regionbasic";
		run.update(dwConn, sql);
		
		sql = "insert into razor_tmp_regionbasic" +
				" SELECT `product_sk`, `location_sk`, `deviceidentifier`, `date_sk`, `isnew` " +
				" FROM `razor_fact_clientdata` " +
				" WHERE date_sk between ? and ?";
				
		run.update(dwConn, sql, fromMonth, endDate);
		
		// 获得所有productid
		List<ProductID> productids = ProductUtils.getProductIDList(baobiaoConn, run);
		if (productids.size() <= 0){
			System.err.println("product ids null");
			return;
		}
		
		for(ProductID productID : productids){
			int pid = productID.getId();
			
			String insertCountryWeekQuery = "insert into razor_sum_basic_region_country(enddate, country, week_active, week_new, productid)" +
					" select  '"
					+ currDate
					+ "' as enddate, if(l.country<>'', l.country, '其他') country,"
					+ "		count(f.deviceidentifier) as week_active, sum(isnew) week_new, "+pid+" productid"
					+ "	from    razor_tmp_regionbasic     f,"
					+ "			razor_dim_location     l" + "	where   f.location_sk = l.location_sk"
					+ "		and f.product_sk in (SELECT product_sk FROM `razor_dim_product` " +
					"								where product_id = ? and `product_active` = 1 " +
					" 								and `channel_active` = 1 and `version_active` = 1 )"
					+ "		and f.date_sk between ? and ?	group by l.country" +
					" ON DUPLICATE KEY UPDATE week_active=values(week_active), week_new = values(week_new)";
			
			run.update(dwConn, insertCountryWeekQuery, pid, fromWeek, endDate);
			System.out.println(insertCountryWeekQuery);
			
			String insertCountryMonthQuery = "insert into razor_sum_basic_region_country(enddate, country, month_active, month_new, productid)" +
					" select  '"
					+ currDate
					+ "' as enddate, if(l.country<>'', l.country, '其他') country,"
					+ "		count(f.deviceidentifier) as month_active, sum(isnew) month_new, "+pid+" productid"
					+ "	from    razor_tmp_regionbasic     f,"
					+ "			razor_dim_location     l" + "	where   f.location_sk = l.location_sk"
					+ "		and f.product_sk in (SELECT product_sk FROM `razor_dim_product` " +
					"								where product_id = ? and `product_active` = 1 " +
					" 								and `channel_active` = 1 and `version_active` = 1 )"
					+ "		and f.date_sk between ? and ?	group by l.country" +
					" ON DUPLICATE KEY UPDATE month_active=values(month_active), month_new = values(month_new)";
			
			run.update(dwConn, insertCountryMonthQuery, pid, fromMonth, endDate);
			
			String insertRegionWeekQuery = "insert into razor_sum_basic_region(enddate, region, week_active, week_new, productid)" +
					" select  '"
					+ currDate
					+ "' as enddate, if(l.region<>'', l.region, '其他') country,"
					+ "		count(f.deviceidentifier) as week_active, sum(isnew) week_new, "+pid+" productid"
					+ "	from    razor_tmp_regionbasic     f,"
					+ "			razor_dim_location     l" + "	where   f.location_sk = l.location_sk"
					+ "		and f.product_sk in (SELECT product_sk FROM `razor_dim_product` " +
					"								where product_id = ? and `product_active` = 1 " +
					" 								and `channel_active` = 1 and `version_active` = 1 )"
					+ "		and f.date_sk between ? and ?" +
					" and l.country = '中国'	group by l.region" +
					" ON DUPLICATE KEY UPDATE week_active=values(week_active), week_new = values(week_new)";
			
			run.update(dwConn, insertRegionWeekQuery, pid, fromWeek, endDate);
			
			String insertRegionMonthQuery = "insert into razor_sum_basic_region(enddate, region, month_active, month_new, productid)" +
					" select  '"
					+ currDate
					+ "' as enddate, if(l.region<>'', l.region, '其他') country,"
					+ "		count(f.deviceidentifier) as month_active, sum(isnew) month_new, "+pid+" productid"
					+ "	from    razor_tmp_regionbasic     f,"
					+ "			razor_dim_location     l" + "	where   f.location_sk = l.location_sk"
					+ "		and f.product_sk in (SELECT product_sk FROM `razor_dim_product` " +
					"								where product_id = ? and `product_active` = 1 " +
					" 								and `channel_active` = 1 and `version_active` = 1 )"
					+ "		and f.date_sk between ? and ?	" +
					" and l.country = '中国' group by l.region" +
					" ON DUPLICATE KEY UPDATE month_active=values(month_active), month_new = values(month_new)";
			
			run.update(dwConn, insertRegionMonthQuery, pid, fromMonth, endDate);
			
		}
		
		dwConn.close();
		baobiaoConn.close();
		
		Long e = System.currentTimeMillis();
		
		System.out.println(endDate + " down!\t"+ (e-s)/1000);
	}

}

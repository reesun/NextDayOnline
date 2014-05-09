import java.sql.Connection;
import java.util.List;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;

import beans.DateSK;
import beans.DeviceInfo;
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
			ETL.restoreStartupClientdata(dwConn, run, fromDateSK, endDateSK-1, prodcutid);
			long e = System.currentTimeMillis();
			System.out.println("Time: " + (e - s) / 1000);
			
			GetOneItemHandler<Object> onHandler = new GetOneItemHandler<Object>();
			
			// 计算 0-24小时启动数
			String sql = "SELECT sum(num-1) FROM `razor_tmp_startupgap_1` ";
			Object hour = run.query(dwConn, sql, onHandler);
			System.out.println(hour.toString());
			
			// 30天内，只有一次启动
			sql = "select count(1) from " +
				" (SELECT `deviceidentifier`,count(1) num " +
				"	FROM `razor_tmp_startupgap_1` " +
				"	group by `deviceidentifier`having num = 1) tt";
			
			Object oneStart = run.query(dwConn, sql, onHandler);
			System.out.println(oneStart.toString());
			
			int day1 = 0, day2 =0, day3=0, day4=0, day5=0, day6=0, day7=0, day14=0, day30=0;
			sql = "SELECT `deviceidentifier` uid FROM `razor_tmp_startupgap_1` " +
				" group by `deviceidentifier` having count(1) > 1 ";
			
			ResultSetHandler<List<DeviceInfo>> deviceInfoHandler = 
						new BeanListHandler<DeviceInfo>(DeviceInfo.class);
			
			ResultSetHandler<List<DateSK>> dataskHandler = 
					new BeanListHandler<DateSK>(DateSK.class);
			
			String deviceQuery = "SELECT date_sk FROM  `razor_tmp_startupgap_1` " +
					" WHERE  `deviceidentifier` =  ?";
			
			List<DeviceInfo> devices = run.query(dwConn, sql, deviceInfoHandler);
			System.out.println(devices.size());
			for(DeviceInfo device: devices){
				List<DateSK> datesks = run.query(dwConn, deviceQuery, dataskHandler, device.getUid());
				
				for(int index = 0; index < datesks.size()-1; index++){
					int from = datesks.get(index).getDate_sk();
					int next = datesks.get(index+1).getDate_sk();
					int startgap = next - from;
					switch (startgap) {
					case 1:
						day1 += 1;
						break;
					case 2:
						day2 += 1;
						break;
					case 3:
						day3 += 1;
						break;
					case 4:
						day4 += 1;
						break;
					case 5:
						day5 += 1;
						break;
					case 6:
						day6 += 1;
						break;
					case 7:
						day7 += 1;
						break;
					case 14:
						day14 += 1;
						break;
					case 30:
						day30 += 1;
						break;

					}
				}
			}
			
			sql = "insert into  razor_sum_startup_gap(endTime, onestart, hour, 1day, 2day, 3day, 4day, 5day, 6day, 7day, 14day, 30day, product_id)" +
					" values(?,?,?,?,?,?,?,?,?,?,?,?,?)";
			run.update(dwConn, sql, dateString, oneStart.toString(), hour.toString(), 
					day1, day2, day3, day4, day5, day6, day7, day14, day30, prodcutid);
			
			e = System.currentTimeMillis();
			System.out.println("aa time:"+ (e - s) / 1000);
		}
		
	}
	
	

}


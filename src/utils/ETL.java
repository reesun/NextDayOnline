package utils;

import java.sql.Connection;

import org.apache.commons.dbutils.QueryRunner;

public class ETL {
	public static void restoreStartupClientdata(Connection conn, QueryRunner run, 
			int fromdatesk, int todatesk, int productid) throws Exception{
		String sql = "TRUNCATE TABLE  `razor_tmp_startupgap`";
		run.update(conn, sql);
		
		sql = "INSERT INTO  `razor_tmp_startupgap` (`deviceidentifier` ,`date_sk` ,`product_id`)" +
				"SELECT deviceidentifier, date_sk, " + productid +
				" FROM razor_fact_clientdata " +
				" WHERE date_sk between " +fromdatesk + " AND "+ todatesk +
				" AND `product_sk` IN " +
				" ( SELECT `product_sk` FROM `razor_dim_product` WHERE `product_id` = " + productid+ " )";
		
		run.update(conn, sql);
		
		sql = "TRUNCATE TABLE  `razor_tmp_startupgap_1`";
		run.update(conn, sql);
		
		sql = "insert into `razor_tmp_startupgap_1`(`deviceidentifier`, `date_sk`, `num`)" +
				" SELECT `deviceidentifier`, `date_sk`, count(1)  FROM `razor_tmp_startupgap`" +
				" group by `deviceidentifier`, `date_sk`" +
				" order by `deviceidentifier`, `date_sk`";
		run.update(conn, sql);
		
	}
}

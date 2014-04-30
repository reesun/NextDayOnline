package utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {
	public static String getYesterday() {
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
		Calendar rightNow = Calendar.getInstance();
		rightNow.add(Calendar.DAY_OF_YEAR,-1); // 前一天
		Date dt1=(Date) rightNow.getTime();
		
		return sdf.format(dt1);
	
	}
	
	public static String getLastWeek() {
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
		Calendar rightNow = Calendar.getInstance();
		rightNow.add(Calendar.WEEK_OF_YEAR,-1); // 前一天
		Date dt1=(Date) rightNow.getTime();
		
		return sdf.format(dt1);
	}
	
	public static String getCurr() {
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
		Calendar rightNow = Calendar.getInstance();
		Date dt1=(Date) rightNow.getTime();
		
		return sdf.format(dt1);
	
	}
	
	public static String getCurrMonth() {
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM");
		Calendar rightNow = Calendar.getInstance();
		Date dt1=(Date) rightNow.getTime();
		
		return sdf.format(dt1);
	
	}
	
	public static String getBeforeDays(int days) {
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
		Calendar rightNow = Calendar.getInstance();
		rightNow.add(Calendar.DAY_OF_YEAR,days); // 前一天
		Date dt1=(Date) rightNow.getTime();
		
		return sdf.format(dt1);
	}
	public static String getLastMonth() {
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM");
		Calendar rightNow = Calendar.getInstance();
		rightNow.add(Calendar.MONTH,-1);
		Date dt1=(Date) rightNow.getTime();
		
		return sdf.format(dt1);
	
	}
	
}

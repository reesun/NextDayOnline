package beans;

public class MonthlyUser {
	private String uid;
	private int date_sk;
	private int days;
	private long nums;
	
	public String getUid() {
		return uid;
	}
	public void setUid(String uid){
		this.uid = uid;
	}
	
	public int getDate_sk(){
		return date_sk;
	}
	public void setDate_sk(int date_sk){
		this.date_sk = date_sk;
	}
	
	public int getDays(){
		return days;
	}
	public void setDays(int days){
		this.days = days;
	}

	public long getNums(){
		return nums;
	}
	public void setNums(long nums){
		this.nums = nums;
	}
}

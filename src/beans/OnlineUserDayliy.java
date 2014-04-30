package beans;

public class OnlineUserDayliy {
	private String date_time;
	private String uid;
	private String ip;
	private String area;
	private String city;
	private String app_version;
	private int num;
	
	public String getDate_time() {
		return date_time;
	}
	public void setDate_time(String date_time){
		this.date_time = date_time;
	}
	
	public String getUid() {
		return uid;
	}
	public void setUid(String uid){
		this.uid = uid;
	}
	
	public String getIp() {
		return ip;
	}
	public void setIp(String ip){
		this.ip = ip;
	}
	
	public String getArea() {
		return area;
	}
	public void setArea(String area){
		this.area = area;
	}
	
	public String getCity(){
		return city;
	}
	public void setCity(String city){
		this.city = city;
	}
	
	public String getApp_version(){
		return app_version;
	}
	public void setApp_version(String app_version){
		this.app_version = app_version;
	}
	
	public int getNum(){
		return num;
	}
	public void setNum(int num){
		this.num = num;
	}
}

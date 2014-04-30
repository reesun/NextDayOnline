package beans;

public class Region {
	private String country;
	private String region;
	private String city;
	
	public Region(){
		this.country = "其他";
		this.region = "其他";
		this.city = "其他";
	}
	
	public String getCountry(){
		return country;
	}
	public void setCountry(String country){
		this.country = country;
	}
	
	public String getRegion(){
		return region;
	}
	public void setRegion(String region){
		this.region = region;
	}
	
	public String getCity(){
		return city;
	}
	public void setCity(String city){
		this.city = city;
	}
}

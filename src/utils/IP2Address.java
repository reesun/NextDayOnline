package utils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;

import beans.Region;

import net.sf.json.JSONObject;

public class IP2Address {
	public static Region getAdressByTaobao(String ip) throws Exception {
		// 远程获取当前ip所处的城市
		Region region = new Region();
		String areaStr = "";
		URL url = new URL("http://ip.taobao.com/service/getIpInfo.php?ip=" + ip);
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		connection.connect();
		InputStream inputStream = connection.getInputStream();
		// 对应的字符编码转换
		Reader reader = new InputStreamReader(inputStream, "UTF-8");
		BufferedReader bufferedReader = new BufferedReader(reader);
		String str = null;
		StringBuffer sb = new StringBuffer();
		while ((str = bufferedReader.readLine()) != null) {
			sb.append(str);
		}
		reader.close();
		connection.disconnect();
		areaStr = sb.toString();
		JSONObject object = JSONObject.fromObject(areaStr);
		if (object != null) {
			JSONObject data = object.getJSONObject("data");
			try {
				region.setCountry(data.getString("country"));

			} catch (Exception e) {
				region.setCountry("其他");
			}
			try {
				region.setRegion(data.getString("region"));
			} catch (Exception e) {
				region.setRegion("其他");
			}

			try {
				region.setCity(data.getString("city"));
			} catch (Exception e) {
				region.setCity("其他");
			}
		}
		return region;
	}

	public static Region getAdress(String ip) throws Exception {
		// 远程获取当前ip所处的城市
		Region region = new Region();
		String areaStr = "";
		URL url = new URL(
				"http://int.dpool.sina.com.cn/iplookup/iplookup.php?ip=" + ip
						+ "&format=json");
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		connection.connect();
		InputStream inputStream = connection.getInputStream();
		// 对应的字符编码转换
		Reader reader = new InputStreamReader(inputStream, "UTF-8");
		BufferedReader bufferedReader = new BufferedReader(reader);
		String str = null;
		StringBuffer sb = new StringBuffer();
		while ((str = bufferedReader.readLine()) != null) {
			sb.append(str);
		}
		reader.close();
		connection.disconnect();
		areaStr = sb.toString();
		JSONObject object = JSONObject.fromObject(areaStr);
		if (object != null) {
			try {
				region.setCountry(object.getString("country"));

			} catch (Exception e) {
				region.setCountry("其他");
			}
			try {
				region.setRegion(object.getString("province"));
			} catch (Exception e) {
				region.setRegion("其他");
			}

			try {
				region.setCity(object.getString("city"));
			} catch (Exception e) {
				region.setCity("其他");
			}
		}
		return region;
	}

	public static Region getAdressByBaidu(String ip) throws Exception {
		// 远程获取当前ip所处的城市
		Region region = new Region();
		String areaStr = "";
		URL url = new URL(
				"http://api.map.baidu.com/location/ip?ak=bd31IK1AuRPNwG26ORw6aZOs&coor=bd09ll&ip="+ ip);
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		connection.connect();
		InputStream inputStream = connection.getInputStream();
		// 对应的字符编码转换
		Reader reader = new InputStreamReader(inputStream, "UTF-8");
		BufferedReader bufferedReader = new BufferedReader(reader);
		String str = null;
		StringBuffer sb = new StringBuffer();
		while ((str = bufferedReader.readLine()) != null) {
			sb.append(str);
		}
		reader.close();
		connection.disconnect();
		areaStr = sb.toString();
		JSONObject object = JSONObject.fromObject(areaStr);
		if (object != null) {
			if (0 == object.getInt("status")) {
				object = object.getJSONObject("content");
				JSONObject address = object.getJSONObject("address_detail");
				region.setCountry("中国");
				try {
					region.setRegion(address.getString("province"));
				} catch (Exception e) {
					region.setRegion("其他");
				}

				try {
					region.setCity(address.getString("city"));
				} catch (Exception e) {
					region.setCity("其他");
				}
			}
		}
		return region;
	}

	public static void main(String[] args) throws Exception {
		Region region = getAdressByBaidu("211.103.188.89");
		System.out.println(region.getCountry() + " " + region.getRegion() + " "
				+ region.getCity());
	}
}

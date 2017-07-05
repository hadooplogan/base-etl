package com.chinadaas.newEnt.etl.gaoDe;


import com.chinadaas.newEnt.etl.general.HttpUtils;
import com.chinadaas.newEnt.etl.table.Geo;
import com.chinadaas.newEnt.etl.table.Location;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URLEncoder;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * @author ShiLin
 * @since 2017-02-21
 */
public class GaoDeUtils {

	public static List<Geo> getLocation(List<Map<String, String>> list, List<Object[]> params) throws IOException, SQLException {
		StringBuffer sb = new StringBuffer();
		for (Map<String, String> map : list) {
			sb.append(map.get("DOM").trim().replace(" ", ""));
			sb.append("|");
		}
		String strUrl = sb.toString();
		strUrl = URLEncoder.encode(strUrl.substring(0, strUrl.length() - 1), "UTF-8");
		String result = HttpUtils.get("https://restapi.amap.com/v3/geocode/geo?key=cf7e93a51829b3b0b3cbdad3a7123b34&batch=true&address=" + strUrl);
		//{"status":"1","info":"OK","infocode":"10000","count":"1","geocodes":[{"formatted_address":"广西壮族自治区桂林市七星区六合路|98号","province":"广西壮族自治区","citycode":"0773","city":"桂林市","district":"七星区","township":[],"neighborhood":{"name":[],"type":[]},"building":{"name":[],"type":[]},"adcode":"450305","street":"六合路","number":"98号","location":"110.333780,25.279878","level":"门牌号"}]}
		ObjectMapper objectMapper = new ObjectMapper();
		Location location = objectMapper.readValue(result.replace("[]", "\"\""), Location.class);

		List<Geo> geoList = location.getGeocodes();
		if(geoList!=null && geoList.size()>0){
			for (int i = 0; i < location.getGeocodes().size(); i++) {
				Map<String, String> map = list.get(i);
				Geo geo = geoList.get(i);
				geo.setPripid(map.get("PRIPID"));
				geo.setDom(map.get("DOM"));

				params.add(new Object[]{
						geo.getPripid(),geo.getDom(),geo.getFormatted_address(),geo.getProvince(),geo.getCity(),
						geo.getCitycode(),geo.getDistrict(),geo.getAdcode(),geo.getLocation(),geo.getLevel()
				});
			}
		}

		return geoList;
	}

}

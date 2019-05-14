package com.qs.screen.sm_server.rpc.user.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.log4j.Logger;

import com.qs.screen.SMCommon.RPCMessage;
import com.qs.screen.SMCommon.bean.AddressList;
import com.qs.screen.SMCommon.bean.City;
import com.qs.screen.SMCommon.bean.Community;
import com.qs.screen.SMCommon.bean.County;
import com.qs.screen.SMCommon.bean.Livingarea;
import com.qs.screen.SMCommon.bean.Province;
import com.qs.screen.SMCommon.bean.SMUser;
import com.qs.screen.SMCommon.bean.SMUserConfig;
import com.qs.screen.SMCommon.bean.Town;
import com.qs.screen.sm_server.rpc.user.ISMUser;
import com.yeild.common.Utils.CommonUtils;
import com.yeild.common.dbtools.database.DbConnectionManager;

public class SMUserImpl implements ISMUser {
	private Logger logger = Logger.getLogger(getClass());

	@Override
	public RPCMessage<?> getUserConfig(String from) {
		RPCMessage<?> result = new RPCMessage<>();
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			con = DbConnectionManager.getConnection();
			result = getUserConfig(con, pstmt, rs, from);
			if(result == null) {
				result = new RPCMessage<>();
				result.setRpccode(-1);
				result.setMessage("获取用户数据失败");
				return result;
			}
		} catch (Exception e) {
			logger.error("getUserConfig error:"+"\ndata->"+from+"\nerror info->"+CommonUtils.getExceptionInfo(e));
			result.setRpccode(-999);
			result.setMessage("数据库操作异常");
		} finally {
			DbConnectionManager.closeConnection(rs, pstmt, con);
		}
		return result;
	}

	@Override
	public RPCMessage<AddressList> getProvinces(String from) {
		RPCMessage<AddressList> result = new RPCMessage<>();
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			con = DbConnectionManager.getConnection();
			AddressList data = getProvinces(con, pstmt, rs, from);
			if(data == null) {
				result.setRpccode(-1);
				result.setMessage("获取区划数据失败");
				return result;
			}
			result.setRpccode(1);
			result.setMessage("获取区划数据成功");
			result.setDataContent(data);
		} catch (Exception e) {
			logger.error("getProvinces error:"+"\ndata->"+from+"\nerror info->"+CommonUtils.getExceptionInfo(e));
			result.setRpccode(-999);
			result.setMessage("数据库操作异常");
		} finally {
			DbConnectionManager.closeConnection(rs, pstmt, con);
		}
		return result;
	}

	@Override
	public RPCMessage<Province> getCities(String from, int province) {
		RPCMessage<Province> result = new RPCMessage<>();
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			con = DbConnectionManager.getConnection();
			Province data = getCities(con, pstmt, rs, from, province);
			if(data == null) {
				result.setRpccode(-1);
				result.setMessage("获取区划数据失败");
				return result;
			}
			result.setRpccode(1);
			result.setMessage("获取区划数据成功");
			result.setDataContent(data);
		} catch (Exception e) {
			logger.error("getCities error:"+"\ndata->"+from+" code->"+province+"\nerror info->"+CommonUtils.getExceptionInfo(e));
			result.setRpccode(-999);
			result.setMessage("数据库操作异常");
		} finally {
			DbConnectionManager.closeConnection(rs, pstmt, con);
		}
		return result;
	}

	@Override
	public RPCMessage<City> getCounties(String from, int city) {
		RPCMessage<City> result = new RPCMessage<>();
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			con = DbConnectionManager.getConnection();
			City data = getCounties(con, pstmt, rs, from, city);
			if(data == null) {
				result.setRpccode(-1);
				result.setMessage("获取区划数据失败");
				return result;
			}
			result.setRpccode(1);
			result.setMessage("获取区划数据成功");
			result.setDataContent(data);
		} catch (Exception e) {
			logger.error("getCounties error:"+"\ndata->"+from+" code->"+city+"\nerror info->"+CommonUtils.getExceptionInfo(e));
			result.setRpccode(-999);
			result.setMessage("数据库操作异常");
		} finally {
			DbConnectionManager.closeConnection(rs, pstmt, con);
		}
		return result;
	}

	@Override
	public RPCMessage<County> getTowns(String from, int county) {
		RPCMessage<County> result = new RPCMessage<>();
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			con = DbConnectionManager.getConnection();
			County data = getTowns(con, pstmt, rs, from, county);
			if(data == null) {
				result.setRpccode(-1);
				result.setMessage("获取区划数据失败");
				return result;
			}
			result.setRpccode(1);
			result.setMessage("获取区划数据成功");
			result.setDataContent(data);
		} catch (Exception e) {
			logger.error("getTowns error:"+"\ndata->"+from+" code->"+county+"\nerror info->"+CommonUtils.getExceptionInfo(e));
			result.setRpccode(-999);
			result.setMessage("数据库操作异常");
		} finally {
			DbConnectionManager.closeConnection(rs, pstmt, con);
		}
		return result;
	}

	@Override
	public RPCMessage<Town> getCommunities(String from, int town) {
		RPCMessage<Town> result = new RPCMessage<>();
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			con = DbConnectionManager.getConnection();
			Town data = getCommunities(con, pstmt, rs, from, town);
			if(data == null) {
				result.setRpccode(-1);
				result.setMessage("获取区划数据失败");
				return result;
			}
			result.setRpccode(1);
			result.setMessage("获取区划数据成功");
			result.setDataContent(data);
		} catch (Exception e) {
			logger.error("getCommunities error:"+"\ndata->"+from+" code->"+town+"\nerror info->"+CommonUtils.getExceptionInfo(e));
			result.setRpccode(-999);
			result.setMessage("数据库操作异常");
		} finally {
			DbConnectionManager.closeConnection(rs, pstmt, con);
		}
		return result;
	}

	@Override
	public RPCMessage<Community> getLivingareas(String from, int community) {
		RPCMessage<Community> result = new RPCMessage<>();
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			con = DbConnectionManager.getConnection();
			Community data = getLivingareas(con, pstmt, rs, from, community);
			if(data == null) {
				result.setRpccode(-1);
				result.setMessage("获取区划数据失败");
				return result;
			}
			result.setRpccode(1);
			result.setMessage("获取区划数据成功");
			result.setDataContent(data);
		} catch (Exception e) {
			logger.error("getLivingareas error:"+"\ndata->"+from+" code->"+community+"\nerror info->"+CommonUtils.getExceptionInfo(e));
			result.setRpccode(-999);
			result.setMessage("数据库操作异常");
		} finally {
			DbConnectionManager.closeConnection(rs, pstmt, con);
		}
		return result;
	}

	public RPCMessage<SMUserConfig> getUserConfig(Connection con, PreparedStatement pstmt, ResultSet rs, String from) throws Exception {
		RPCMessage<SMUserConfig> result = new RPCMessage<>();
		String sql = "select mu.id userid,mu.username,us.surname,us.type,us.phone,us.sex"
				+ ",us.areacode,us.town_id,us.com_id,us.livingarea_ids"
				+ " from users.users us"
				+ " left join mqtt_user mu on us.user_id=mu.id"
				+ " where us.username=?";
		pstmt = con.prepareStatement(sql);
		pstmt.setString(1, from);
		rs = pstmt.executeQuery();
		if(!rs.next()) {
			result.setRpccode(-10);
			result.setMessage("用户不存在");
			return result;
		}
		SMUserConfig config = new SMUserConfig();
		config.surname = rs.getString("surname");
		config.type = rs.getInt("type");
		config.phone = rs.getString("phone");
        config.user = new SMUser();
        config.user.id = rs.getInt("userid");
        config.user.username = rs.getString("username");
//        String areacode = rs.getString("areacode");
//        int town_id = rs.getInt("town_id");
//        int com_id = rs.getInt("com_id");
//        Array livingarea_ids = rs.getArray("livingarea_ids");
        
        sql = "with areas as (select"
        		+ " substr(areacode::varchar, 1, 6)::int county"
        		+ ", substr(areacode::varchar, 1, 4)::int*100 city"
        		+ ", substr(areacode::varchar, 1, 2)::int*10000 province"
        		+ ",town_id,com_id,livingarea_ids"
        		+ " from users.users where user_id=?"
        		+ ")"
        		+ "select substr(pro.areacode::varchar, 1, 2)::int pro,pro.areaname pro_name"
        		+ ",substr(city.areacode::varchar, 3, 2)::int city,city.areaname city_name"
        		+ ",substr(coun.areacode::varchar, 5, 2)::int coun,coun.areaname coun_name"
        		+ ",tow.town_id, tow.name tow_name,comm.com_id, comm.name com_name"
        		+ ",liv.liv_id, liv.name liv_name"
        		+ " from areas ar"
        		+ " left join livingarea.livingarea liv on liv.liv_id = any (ar.livingarea_ids)"
        		+ " left join community.community comm on comm.com_id=liv.com_id or (liv.com_id is null and comm.com_id=ar.com_id)"
        		+ " left join town.town tow on tow.town_id=comm.town_id or (comm.com_id is null and tow.town_id=ar.town_id)"
        		+ " left join dictionary.area coun on coun.areacode=substr(tow.areacode::varchar, 1, 6)::int or (tow.areacode is null and coun.areacode=ar.county)"
        		+ " left join dictionary.area city on city.areacode=substr(coun.areacode::varchar, 1, 4)::int*100 or (coun.areacode is null and city.areacode=ar.city)"
        		+ " left join dictionary.area pro on pro.areacode=substr(city.areacode::varchar, 1, 2)::int*10000 or (city.areacode is null and pro.areacode=ar.province)";
        pstmt = con.prepareStatement(sql);
        pstmt.setInt(1, config.user.id);
//        pstmt.setString(1, areacode);
//        pstmt.setString(2, areacode);
//        pstmt.setString(3, areacode);
//        pstmt.setInt(4, town_id);
//        pstmt.setInt(5, com_id);
//        pstmt.setArray(6, livingarea_ids);
        rs = pstmt.executeQuery();
        config.address = new AddressList();
		while(rs.next()) {
			int pro = rs.getInt("pro");
			if(pro < 1) {
				continue;
			}
			Province province = config.address.getSubByCode(pro);
			if(province == null) {
				province = new Province();
				province.areacode = pro;
				province.areaname = rs.getString("pro_name");
				config.address.addProvince(province);
			}
			int cit = rs.getInt("city");
			if(cit < 1) {
				continue;
			}
			City city = province.getSubByCode(cit);
			if(city == null) {
				city = new City();
				city.areacode = cit;
				city.areaname = rs.getString("city_name");
				province.addCity(city);
			}
			int coun = rs.getInt("coun");
			if(coun < 1) {
				continue;
			}
			County county = city.getSubByCode(coun);
			if(county == null) {
				county = new County();
				county.areacode = coun;
				county.areaname = rs.getString("coun_name");
				city.addCounty(county);
			}
			int townid = rs.getInt("town_id");
			if(townid < 1) {
				continue;
			}
			Town town = county.getSubByCode(townid);
			if(town == null) {
				town = new Town();
				town.town_id = townid;
				town.name = rs.getString("tow_name");
				county.addTown(town);
			}
			int comid = rs.getInt("com_id");
			if(comid < 1) {
				continue;
			}
			Community community = town.getSubByCode(comid);
			if(community == null) {
				community = new Community();
				community.com_id = comid;
				community.name = rs.getString("com_name");
				town.addCommunity(community);
			}
			int livid = rs.getInt("liv_id");
			if(livid < 1) {
				continue;
			}
			Livingarea livingarea = community.getSubByCode(livid);
			if(livingarea == null) {
				livingarea = new Livingarea();
				livingarea.liv_id = livid;
				livingarea.name = rs.getString("liv_name");
				community.addLivingarea(livingarea);
			}
		}
		result.setRpccode(1);
		result.setMessage("获取数据成功");
        result.setDataContent(config);
        return result;
	}

	public AddressList getProvinces(Connection con, PreparedStatement pstmt, ResultSet rs, String from) {
        AddressList result = new AddressList();
        Province province = new Province();
        province.areacode = 51;
        province.areaname = "四川省";
        result.addProvince(province);
        province = new Province();
        province.areacode = 42;
        province.areaname = "云南省";
        result.addProvince(province);
        province = new Province();
        province.areacode = 52;
        province.areaname = "贵州省";
        result.addProvince(province);
        return result;
	}

	public Province getCities(Connection con, PreparedStatement pstmt, ResultSet rs, String from, int province) {
		Province result = new Province();
		result.areacode = 51;
		result.areaname = "四川省";
        City city = new City();
        city.areacode = 5101;
        city.areaname = "成都市";
        result.addCity(city);
        city = new City();
        city.areacode = 5102;
        city.areaname = "凉山州";
        result.addCity(city);
        return result;
	}

	public City getCounties(Connection con, PreparedStatement pstmt, ResultSet rs, String from, int city) {
        City result = new City();
        result.areacode = 5101;
        result.areaname = "成都市";
        County county = new County();
        county.areacode = 510101;
        county.areaname = "锦江区";
        result.addCounty(county);
        county = new County();
        county.areacode = 510102;
        county.areaname = "成华区";
        result.addCounty(county);
        county = new County();
        county.areacode = 510103;
        county.areaname = "新都区";
        result.addCounty(county);
        return result;
	}

	public County getTowns(Connection con, PreparedStatement pstmt, ResultSet rs, String from, int county) {
		County result = new County();
        result.areacode = 5101;
        result.areaname = "锦江区";
        Town town = new Town();
        town.town_id = 1;
        town.name = "锦江街道办1";
        result.addTown(town);
        town = new Town();
        town.town_id = 2;
        town.name = "锦江街道办2";
        result.addTown(town);
        town = new Town();
        town.town_id = 3;
        town.name = "锦江街道办3";
        result.addTown(town);
        return result;
	}

	public Town getCommunities(Connection con, PreparedStatement pstmt, ResultSet rs, String from, int town) {
        Town result = new Town();
        result.town_id = 1;
        result.name = "锦江街道办1";
        Community community = new Community();
        community.com_id = 1;
        community.name = "锦江社区1";
        result.addCommunity(community);
        community = new Community();
        community.com_id = 2;
        community.name = "锦江社区2";
        result.addCommunity(community);
        return result;
	}

	public Community getLivingareas(Connection con, PreparedStatement pstmt, ResultSet rs, String from, int community) {
		Community result = new Community();
        result.com_id = 1;
        result.name = "锦江社区1";
        Livingarea livingarea = new Livingarea();
        livingarea.liv_id = 1;
        livingarea.name = "华润小区";
        result.addLivingarea(livingarea);
        livingarea = new Livingarea();
        livingarea.liv_id = 2;
        livingarea.name = "东湖公园2";
        result.addLivingarea(livingarea);
        return result;
	}
}

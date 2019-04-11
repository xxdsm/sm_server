package com.qs.screen.sm_server.rpc.user.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

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
import com.qs.screen.SMCommon.constant.SMUserType;
import com.qs.screen.sm_server.rpc.user.ISMUser;
import com.yeild.common.JsonUtils.JsonUtils;
import com.yeild.common.Utils.CommonUtils;
import com.yeild.common.dbtools.database.DbConnectionManager;

public class SMUserImpl implements ISMUser {
	private Logger logger = Logger.getLogger(getClass());

	@Override
	public RPCMessage getUserConfig(String from) {
		RPCMessage result = new RPCMessage();
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			con = DbConnectionManager.getConnection();
			SMUserConfig userConfig = getUserConfig(con, pstmt, rs, from);
			if(userConfig == null) {
				result.setRpccode(-1);
				result.setMessage("获取用户数据失败");
				return result;
			}
			result.setRpccode(1);
			result.setMessage("获取用户数据成功");
			result.setDataContent(JsonUtils.objToJson(userConfig));
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
	public RPCMessage getProvinces(String from) {
		RPCMessage result = new RPCMessage();
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
			result.setDataContent(JsonUtils.objToJson(data));
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
	public RPCMessage getCities(String from, int province) {
		RPCMessage result = new RPCMessage();
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
			result.setDataContent(JsonUtils.objToJson(data));
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
	public RPCMessage getCounties(String from, int city) {
		RPCMessage result = new RPCMessage();
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
			result.setDataContent(JsonUtils.objToJson(data));
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
	public RPCMessage getTowns(String from, int county) {
		RPCMessage result = new RPCMessage();
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
			result.setDataContent(JsonUtils.objToJson(data));
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
	public RPCMessage getCommunities(String from, int town) {
		RPCMessage result = new RPCMessage();
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
			result.setDataContent(JsonUtils.objToJson(data));
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
	public RPCMessage getLivingareas(String from, int community) {
		RPCMessage result = new RPCMessage();
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
			result.setDataContent(JsonUtils.objToJson(data));
		} catch (Exception e) {
			logger.error("getLivingareas error:"+"\ndata->"+from+" code->"+community+"\nerror info->"+CommonUtils.getExceptionInfo(e));
			result.setRpccode(-999);
			result.setMessage("数据库操作异常");
		} finally {
			DbConnectionManager.closeConnection(rs, pstmt, con);
		}
		return result;
	}

	public SMUserConfig getUserConfig(Connection con, PreparedStatement pstmt, ResultSet rs, String from) throws SQLException {
		SMUserConfig config = new SMUserConfig();
		config.surname = "测试用户";
		config.type = SMUserType.Type_Manager;
		config.phone = "13125737883";
        config.user = new SMUser();
        config.user.id = 20;
        config.user.username = "yp142";
        config.address = new AddressList();
        Province province = new Province();
        province.areacode = 51;
        province.areaname = "四川省";
        City city = new City();
        city.areacode = 5101;
        city.areaname = "成都市";
        province.addCity(city);
        city = new City();
        city.areacode = 5134;
        city.areaname = "凉山州";
        province.addCity(city);
        config.address.addProvince(province);
        return config;
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

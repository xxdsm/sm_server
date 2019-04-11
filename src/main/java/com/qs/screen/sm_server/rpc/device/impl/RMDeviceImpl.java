package com.qs.screen.sm_server.rpc.device.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.log4j.Logger;

import com.qs.screen.SMCommon.RPCMessage;
import com.qs.screen.SMCommon.bean.RMDeviceListFilter;
import com.qs.screen.SMCommon.bean.RMDeviceReg;
import com.qs.screen.SMCommon.bean.RMDeviceRegList;
import com.qs.screen.sm_server.rpc.device.IRMDevice;
import com.yeild.common.JsonUtils.JsonUtils;
import com.yeild.common.dbtools.database.DbConnectionManager;

public class RMDeviceImpl implements IRMDevice {
	private Logger logger = Logger.getLogger(getClass());
	
	@Override
	public RPCMessage getDeviceList(String from, RMDeviceListFilter filter) {
		RPCMessage result = new RPCMessage();
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			con = DbConnectionManager.getConnection();
			result = getDeviceList(con, pstmt, rs, from, filter);
		} catch (Exception e) {
			logger.error("getDeviceList:"+from+" filter->"+JsonUtils.objToJson(filter), e);
			result.setRpccode(-99);
			result.setMessage("获取数据异常");
		} finally {
			DbConnectionManager.closeConnection(rs, pstmt, con);
		}
		return result;
	}
	
	@Override
	public RPCMessage getDeviceRegList(String from, String identify, int operator) {
		RPCMessage result = new RPCMessage();
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			con = DbConnectionManager.getConnection();
			result = getDeviceRegList(con, pstmt, rs, from, identify, operator);
		} catch (Exception e) {
			logger.error("getDeviceRegList:"+from+" identify->"+identify+" operator->"+operator, e);
			result.setRpccode(-99);
			result.setMessage("获取数据异常");
		} finally {
			DbConnectionManager.closeConnection(rs, pstmt, con);
		}
		return result;
	}
	
	@Override
	public RPCMessage getDeviceDetail(String from, int rmd_id) {
		RPCMessage result = new RPCMessage();
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			con = DbConnectionManager.getConnection();
			result = getDeviceDetail(con, pstmt, rs, from, rmd_id);
		} catch (Exception e) {
			logger.error("getDeviceDetail:"+from+" rmd_id->"+rmd_id, e);
			result.setRpccode(-99);
			result.setMessage("获取数据异常");
		} finally {
			DbConnectionManager.closeConnection(rs, pstmt, con);
		}
		return result;
	}
	
	@Override
	public RPCMessage regDevice(String from, RMDeviceReg device) {
		RPCMessage result = new RPCMessage();
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			con = DbConnectionManager.getConnection();
			con.setAutoCommit(false);
			result = regDevice(con, pstmt, rs, from, device);
			if(result.getRpccode() > 0) con.commit();
			else con.rollback();
		} catch (Exception e) {
			logger.error("regDevice:"+from+" device->"+JsonUtils.objToJson(device), e);
			try{ if(con != null) con.rollback(); } catch (Exception e1) { }
			result.setRpccode(-99);
			result.setMessage("获取数据异常");
		} finally {
			try{ if(con != null) con.setAutoCommit(true); } catch (Exception e1) { }
			DbConnectionManager.closeConnection(rs, pstmt, con);
		}
		return result;
	}
	
	public RPCMessage getDeviceList(Connection con, PreparedStatement pstmt, ResultSet rs, String from, RMDeviceListFilter filter) throws Exception {
		RPCMessage result = new RPCMessage();
		String sql = "select  rmd.rmd_id, rmd.create_date"
				+ ",rmdi.gpsx,rmdi.gpsy,rmdi.com_id,rmdi.liv_id,rmdi.name rmd_name,rmds.status,coun.areacode"
				+ ",liv.name liv_name,comm.name comm_name,tow.name town_name,coun.areaname county,city.areaname city,prov.areaname prov_name"
				+ " from rmdevice.rmdevice_info rmdi"
				+ " left join rmdevice.rmdevice rmd on rmd.rmd_id = rmdi.rmd_id"
				+ " left join rmdevice.rmdevice_state rmds on rmds.rmd_id=rmdi.rmd_id"
				+ " inner join livingarea.livingarea liv on liv.liv_id=rmdi.liv_id"
				+ " inner join community.community comm on comm.com_id=liv.com_id"
				+ " inner join town.town tow on tow.town_id=comm.town_id"
				+ " inner join dictionary.area coun on coun.areacode=substr(tow.areacode::varchar, 1, 6)::int"
				+ " inner join dictionary.area city on city.areacode=substr(coun.areacode::varchar, 1, 4)::int*100"
				+ " inner join dictionary.area prov on prov.areacode=substr(city.areacode::varchar, 1, 2)::int*10000"
				+ " order by rmd.rmd_id desc";
		return result;
	}
	
	public RPCMessage getDeviceRegList(Connection con, PreparedStatement pstmt, ResultSet rs, String from, String identify, int operator) throws Exception {
		RPCMessage result = new RPCMessage();
		StringBuilder condition = new StringBuilder(" where");
		if(operator > 0) {
			// 已填写注册信息
			condition.append(" rmdr.operator_reg=").append(operator);
		} else {
			// 未填写注册信息
			condition.append(" rmdr.operator_reg < 1 or rmdr.operator_reg is null ");
		}
		if(identify != null && identify.length() > 0) {
			condition.append(" and rmd.identify like '%").append(identify.replaceAll(" ", "%")).append("%'");
		}
		String sql = "select rmd.rmd_id, rmd.create_date,rmdr.gpsx,rmdr.gpsy,operator_reg,operator_cfg,rmd.identify"
				+ " from rmdevice.rmdevice_reg rmdr"
				+ " left join rmdevice.rmdevice rmd on rmd.rmd_id = rmdr.rmd_id"+condition.toString()
				+ " order by rmdr.rmd_id desc";
		pstmt = con.prepareStatement(sql);
		rs = pstmt.executeQuery();
		RMDeviceRegList deviceList = new RMDeviceRegList();
		while(rs.next()) {
			RMDeviceReg device = new RMDeviceReg();
			device.rmd_id = rs.getInt("rmd_id");
			device.gpsx = rs.getFloat("gpsx");
			device.gpsy = rs.getFloat("gpsy");
			device.operator_reg = rs.getInt("operator_reg");
			device.operator_cfg = rs.getInt("operator_cfg");
			device.identify = rs.getString("identify");
			device.create_date = rs.getString("create_date");
			deviceList.addDevice(device);
		}
		result.setRpccode(1);
		result.setMessage("获取数据成功");
		result.setDataContent(JsonUtils.objToJson(deviceList));
		return result;
	}

	public RPCMessage getDeviceDetail(Connection con, PreparedStatement pstmt, ResultSet rs, String from, int rmd_id) throws Exception {
		RPCMessage result = new RPCMessage();
		String sql = "select  rmd.rmd_id, rmd.create_date"
				+ ",rmdi.gpsx,rmdi.gpsy,rmdi.com_id,rmdi.liv_id,rmdi.name rmd_name,rmds.status,coun.areacode"
				+ ",liv.name liv_name,comm.name comm_name,tow.name town_name,coun.areaname county,city.areaname city,prov.areaname prov_name"
				+ " from rmdevice.rmdevice_info rmdi"
				+ " left join rmdevice.rmdevice rmd on rmd.rmd_id = rmdi.rmd_id"
				+ " left join rmdevice.rmdevice_state rmds on rmds.rmd_id=rmdi.rmd_id"
				+ " inner join livingarea.livingarea liv on liv.liv_id=rmdi.liv_id"
				+ " inner join community.community comm on comm.com_id=liv.com_id"
				+ " inner join town.town tow on tow.town_id=comm.town_id"
				+ " inner join dictionary.area coun on coun.areacode=substr(tow.areacode::varchar, 1, 6)::int"
				+ " inner join dictionary.area city on city.areacode=substr(coun.areacode::varchar, 1, 4)::int*100"
				+ " inner join dictionary.area prov on prov.areacode=substr(city.areacode::varchar, 1, 2)::int*10000"
				+ " where rmdi.rmd_id=?";
		return result;
	}

	public RPCMessage regDevice(Connection con, PreparedStatement pstmt, ResultSet rs, String from, RMDeviceReg device) throws Exception {
		RPCMessage result = new RPCMessage();
		String sql = "update rmdevice.rmdevice_reg set gpxs=?,gpsy=?,operator_reg=?,update_date=now()"
				+ " where rmd_id=? returning operator_reg";
		pstmt = con.prepareStatement(sql);
		pstmt.setFloat(1, device.gpsx);
		pstmt.setFloat(2, device.gpsy);
		pstmt.setInt(3, device.operator_reg);
		pstmt.setInt(4, device.rmd_id);
		rs = pstmt.executeQuery();
		if(!rs.next()) {
			result.setRpccode(-1);
			result.setMessage("设备注册失败");
			return result;
		}
		result.setRpccode(1);
		result.setMessage("设备注册成功");
		return result;
	}
}

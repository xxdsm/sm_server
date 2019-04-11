package com.qs.screen.sm_server.rpc.ld;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.qs.screen.SMCommon.RPCMessage;
import com.qs.screen.SMCommon.bean.SMUser;
import com.qs.screen.SMCommon.constant.LDStateName;
import com.qs.screen.SMCommon.constant.LDWarnLevel;
import com.qs.screen.SMCommon.constant.LDWarnType;
import com.qs.screen.SMCommon.ld.LDInitInfo;
import com.qs.screen.SMCommon.ld.LDPlcData;
import com.qs.screen.SMCommon.ld.LDRegInfo;
import com.qs.screen.SMCommon.ld.LDStateSwitch;
import com.qs.screen.SMCommon.ld.LDWarnInfo;
import com.qs.screen.sm_server.Application;
import com.yeild.common.JsonUtils.JsonUtils;
import com.yeild.common.Utils.CommonUtils;
import com.yeild.common.Utils.ConvertUtils;
import com.yeild.common.dbtools.database.DbConnectionManager;

public class LDManager {
	private Logger logger = Logger.getLogger(LDManager.class);
	private static final LDManager manager = new LDManager();
	
	public static String handle(String topic, String data) {
		try {
			String []topicParts = topic.split("/");
			Method method;
			Object []args;
			if(topic.startsWith(Application.Topic_LD_init)) {
				method = manager.getClass().getDeclaredMethod(topicParts[2], String.class);
				args = new Object[]{data};
			} else {
				String from = null;
				String func = topicParts[topicParts.length - 1];
				String part2 = topicParts[topicParts.length - 2];
				if(!func.equals("status")) func = part2;
				else from = part2;
				
				String part3 = topicParts[topicParts.length - 3];
				if(Application.Topic_LD_ctl.equals("/"+part3)) {
					func = topicParts[2]+"_"+part3+"_"+func;
					from = topicParts[topicParts.length - 4];
				} else {
					func = topicParts[2]+"_"+func;
					if(from == null) from = part3;
				}
				method = manager.getClass().getDeclaredMethod(func, String.class, String.class, String.class);
				
				args = new Object[]{from, topicParts[topicParts.length - 1], data};
			}
			method.setAccessible(true);
			try {
				Object result = method.invoke(manager, args);
				return result == null?null:(String)result;
			} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
				manager.logger.error("invoke error for "+topic+"  with data:"+data+"\nerror:"+CommonUtils.getExceptionInfo(e));
			}
		} catch (NoSuchMethodException | SecurityException e) {
			manager.logger.error("unsurported call for "+topic+"  with data:"+data);
		}
		return null;
	}
	
	public String init(String req) {
		RPCMessage result = new RPCMessage();
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			LDRegInfo regInfo = JsonUtils.jsonToObj(req, LDRegInfo.class);
			con = DbConnectionManager.getConnection();
			result = deviceReg(con, pstmt, rs, regInfo);
		} catch (Exception e) {
			logger.error("deviceReg error:"+"\ndata->"+req+"\nerror info->"+CommonUtils.getExceptionInfo(e));
			result.setRpccode(-999);
			result.setMessage("数据库操作异常");
		} finally {
			DbConnectionManager.closeConnection(rs, pstmt, con);
		}
		return JsonUtils.objToJson(result);
	}
	
	public void rep_status(String from, String reqid, String data) {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			int rmd_id = ConvertUtils.parseInt(from);
			if(rmd_id < 1) throw new Exception("parse rmd_id with from failed.");
			int status = (int) ConvertUtils.parseInt(data);
			if(status == 0) throw new Exception("parse status with data failed.");
			con = DbConnectionManager.getConnection();
			repStatus(con, pstmt, rs, rmd_id, status);
		} catch (Exception e) {
			logger.error("rep_status error:"+"\nfrom:"+from+" reqid:"+reqid+" data->"+data+"\nerror info->"+CommonUtils.getExceptionInfo(e));
		} finally {
			DbConnectionManager.closeConnection(rs, pstmt, con);
		}
	}
	
	public void rep_dev_state(String from, String reqid, String data) {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			int rmd_id = ConvertUtils.parseInt(from);
			if(rmd_id < 1) throw new Exception("parse rmd_id with from failed.");
			LDStateSwitch stateSwitch = JsonUtils.jsonToObj(data, LDStateSwitch.class);
			if(stateSwitch == null) throw new Exception("parse LDStateSwitch with data failed.");
			con = DbConnectionManager.getConnection();
			repDev_state(con, pstmt, rs, rmd_id, stateSwitch);
		} catch (Exception e) {
			logger.error("rep_dev_state error:"+"\nfrom:"+from+" reqid:"+reqid+" data->"+data+"\nerror info->"+CommonUtils.getExceptionInfo(e));
		} finally {
			DbConnectionManager.closeConnection(rs, pstmt, con);
		}
	}
	
	public void rep_plc(String from, String reqid, String data) {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			int rmd_id = ConvertUtils.parseInt(from);
			if(rmd_id < 1) throw new Exception("parse rmd_id with from failed.");
			LDPlcData plcData = JsonUtils.jsonToObj(data, LDPlcData.class);
			if(plcData == null) throw new Exception("parse to LDPlcData failed.");
			con = DbConnectionManager.getConnection();
			repPLC(con, pstmt, rs, rmd_id, plcData);
		} catch (Exception e) {
			logger.error("rep_plc error:"+"\nfrom:"+from+" reqid:"+reqid+" data->"+data+"\nerror info->"+CommonUtils.getExceptionInfo(e));
		} finally {
			DbConnectionManager.closeConnection(rs, pstmt, con);
		}
	}
	
	public void rep_warn(String from, String reqid, String data) {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			int rmd_id = ConvertUtils.parseInt(from);
			if(rmd_id < 1) throw new Exception("parse rmd_id with from failed.");
			LDWarnInfo warnInfo = JsonUtils.jsonToObj(data, LDWarnInfo.class);
			if(warnInfo == null) throw new Exception("parse to LDWarnInfo failed.");
			con = DbConnectionManager.getConnection();
			repWarn(con, pstmt, rs, rmd_id, warnInfo);
		} catch (Exception e) {
			logger.error("rep_plc error:"+"\nfrom:"+from+" reqid:"+reqid+" data->"+data+"\nerror info->"+CommonUtils.getExceptionInfo(e));
		} finally {
			DbConnectionManager.closeConnection(rs, pstmt, con);
		}
	}
	
//  TODO 分隔线
	
	public RPCMessage deviceReg(Connection con, PreparedStatement pstmt, ResultSet rs, LDRegInfo regInfo) throws SQLException {
		String sql = "select rmd_id from rmdevice.rmdevice where identify=?";
		pstmt = con.prepareStatement(sql);
		pstmt.setString(1, regInfo.identify);
		rs = pstmt.executeQuery();
		if(rs.next()) {
			RPCMessage result = getDeviceInfo(con, pstmt, rs, rs.getInt("rmd_id"));
			if(result.getRpccode() > 0) {
				result.setRpccode(2);
			}
			return result;
		}
		RPCMessage result = new RPCMessage();
		sql = "insert into rmdevice.rmdevice(serial_no,identify"
				+ ") values(?,?) returning rmd_id";
		pstmt = con.prepareStatement(sql);
		pstmt.setString(1, regInfo.serial_no);
		pstmt.setString(2, regInfo.identify);
		rs = pstmt.executeQuery();
		int rmd_id = 0;
		if(!rs.next()) {
			result.setRpccode(-1);
			result.setMessage("设备添加失败");
			return result;
		}
		rmd_id = rs.getInt("rmd_id");
		sql = "insert into rmdevice.rmdevice_reg(rmd_id) values(?) returning rmd_id";
		pstmt = con.prepareStatement(sql);
		pstmt.setInt(1, rmd_id);
		rs = pstmt.executeQuery();
		if(!rs.next()) {
			result.setRpccode(-1);
			result.setMessage("设备注册失败");
			return result;
		}
		result.setDataContent(String.valueOf(rmd_id));
		result.setRpccode(1);
		result.setMessage("设备注册成功");
		return result;
	}

	public RPCMessage getDeviceInfo(Connection con, PreparedStatement pstmt, ResultSet rs, int deviceid) throws SQLException {
		RPCMessage result = new RPCMessage();
		String sql = "select rmd.rmd_id,mu.id user_id,mu.username,mu.password,rinfo.com_id,rinfo.liv_id,town.town_id"
				+ ",town.areacode,rinfo.rmd_id infoid"
				+ " from rmdevice.rmdevice rmd"
				+ " left join rmdevice.rmdevice_cfg rcfg on rcfg.rmd_id=rmd.rmd_id"
//				+ " left join users.users us on us.user_id=rcfg.user_id"
				+ " left join public.mqtt_user mu on mu.id=rcfg.user_id"
				+ " left join rmdevice.rmdevice_info rinfo on rinfo.rmd_id=rmd.rmd_id"
				+ " left join livingarea.livingarea la on la.liv_id=rinfo.liv_id"
				+ " left join community.community comm on comm.com_id=(case when la.com_id>0 then la.com_id else rinfo.com_id end)"
				+ " left join town.town town on town.town_id=comm.town_id"
				+ " where rmd.rmd_id=?";
		pstmt = con.prepareStatement(sql);
		pstmt.setInt(1, deviceid);
		rs = pstmt.executeQuery();
		if(!rs.next()) {
			result.setRpccode(-1);
			result.setMessage("未查询到该设备："+deviceid);
			return result;
		}
		LDInitInfo initInfo = new LDInitInfo();
		initInfo.user = new SMUser();
		initInfo.user.id = rs.getInt("user_id");
		initInfo.user.username = rs.getString("username");
		initInfo.user.password = rs.getString("password");
		initInfo.rmd_id = rs.getInt("rmd_id");
		initInfo.areacode = rs.getString("areacode");
		initInfo.town_id = rs.getInt("town_id");
		initInfo.com_id = rs.getInt("com_id");
		initInfo.liv_id = rs.getInt("liv_id");
		if(initInfo.user.id < 1) {
			result.setRpccode(-10);
			result.setMessage("未配置设备账号");
			return result;
		}
		if(rs.getInt("infoid") < 1) {
			result.setRpccode(-11);
			result.setMessage("未配置设备信息");
			return result;
		}
		if(initInfo.areacode == null || initInfo.areacode.length()<6) {
			result.setRpccode(-12);
			result.setMessage("未配置设备所属区域");
			return result;
		}
		result.setRpccode(1);
		result.setMessage("获取设备数据成功");
		result.setDataContent(JsonUtils.objToJson(initInfo));
		return result;
	}

	public void repStatus(Connection con, PreparedStatement pstmt, ResultSet rs, int from, int status) throws Exception {
		String sql = "select 1 from rmd_event.rmd_event where rmd_id=? and flag_code=?";
		pstmt = con.prepareStatement(sql);
		pstmt.setInt(1, from);
		pstmt.setInt(2, status);
		rs = pstmt.executeQuery();
		if(rs.next()) {
			return; // 已处理事件
		}
		sql = "insert into rmd_event.rmd_event(rmd_id,event,level,flag_code"
				+ ") values(?,?,?,?) returning event_id";
		pstmt = con.prepareStatement(sql);
		pstmt.setInt(1, from);
		pstmt.setString(2, status>0?"设备上线":"设备离线");
		pstmt.setInt(3, status>0?0:1);
		pstmt.setInt(4, status);
		rs = pstmt.executeQuery();
		if(!rs.next()) {
			throw new Exception("insert rmd_event failed");
		}
		LDStateSwitch stateSwitch = new LDStateSwitch();
		stateSwitch.name = LDStateName.STATE_DEV_ONLINE;
		stateSwitch.state = status<0 ? 0:1;
		repDev_state(con, pstmt, rs, from, stateSwitch);
	}

	public void repPLC(Connection con, PreparedStatement pstmt, ResultSet rs, int from, LDPlcData plcData) throws Exception {
		int setIndex = 1;
		String sql = "insert into rmdevice_plc.rmdevice_plc(rmd_id,temp,elec,volt"
				+ ") values(?,?,?,?) returning plc_id";
		pstmt = con.prepareStatement(sql);
		pstmt.setInt(setIndex++, from);
		pstmt.setFloat(setIndex++, plcData.temp);
		pstmt.setFloat(setIndex++, plcData.elec);
		pstmt.setFloat(setIndex++, plcData.volt);
		rs = pstmt.executeQuery();
		if(!rs.next()) {
			throw new Exception("insert rmdevice_plc failed");
		}
		sql = "update rmdevice.rmdevice_state set temp=?,elec=?,volt=?"
				+ ",lcd_on=?,light_on=?,fan_on=?,lock=?"
				+ ",update_date=now(),last_rep_time=now() where rmd_id=? returning rmd_id";
		pstmt = con.prepareStatement(sql);
		setIndex = 1;
		pstmt.setFloat(setIndex++, plcData.temp);
		pstmt.setFloat(setIndex++, plcData.elec);
		pstmt.setFloat(setIndex++, plcData.volt);
		pstmt.setInt(setIndex++, plcData.lcd_on);
		pstmt.setInt(setIndex++, plcData.light_on);
		pstmt.setInt(setIndex++, plcData.fan_on);
		pstmt.setInt(setIndex++, plcData.lock);
		pstmt.setInt(setIndex++, from);
		rs = pstmt.executeQuery();
	}

	public void repWarn(Connection con, PreparedStatement pstmt, ResultSet rs, int from, LDWarnInfo warnInfo) throws Exception {
		int setIndex = 1;
		String sql = "insert into rmdevice_warning.rmdevice_warning(rmd_id,warn_type,level,msg"
				+ ") values(?,?,?,?) returning warn_id";
		pstmt = con.prepareStatement(sql);
		pstmt.setInt(setIndex++, warnInfo.rmd_id);
		pstmt.setInt(setIndex++, warnInfo.warn_type);
		pstmt.setInt(setIndex++, warnInfo.level);
		pstmt.setString(setIndex++, warnInfo.msg);
		rs = pstmt.executeQuery();
		if(!rs.next()) {
			throw new Exception("insert rmdevice_warning failed");
		}
	}

	public void repDev_state(Connection con, PreparedStatement pstmt, ResultSet rs, int from, LDStateSwitch stateSwitch) throws Exception {
		int setIndex = 1;
        LDWarnInfo warnInfo = new LDWarnInfo();
        warnInfo.rmd_id = from;
		switch(stateSwitch.name) {
		case LDStateName.STATE_DEV_ONLINE:
	        warnInfo.warn_type = LDWarnType.WARN_TYPE_SYS_STATE;
	        warnInfo.level = stateSwitch.state > 0 ? LDWarnLevel.WARN_LEVEL_NORMAL:LDWarnLevel.WARN_LEVEL_SERIOUS;
	        warnInfo.msg = stateSwitch.state > 0 ? null:"设备离线";
			break;
		case LDStateName.STATE_HDMI:
			warnInfo.warn_type = LDWarnType.WARN_TYPE_SCREEN;
			warnInfo.level = stateSwitch.state > 0 ? LDWarnLevel.WARN_LEVEL_NORMAL:LDWarnLevel.WARN_LEVEL_WARN;
			warnInfo.msg = stateSwitch.state > 0 ? "显示器已连接":"显示器连接断开";
			break;
		case LDStateName.STATE_EST:
	        warnInfo.warn_type = LDWarnType.WARN_TYPE_CARD_READER;
	        warnInfo.level = stateSwitch.state > 0 ? LDWarnLevel.WARN_LEVEL_NORMAL:LDWarnLevel.WARN_LEVEL_WARN;
	        warnInfo.msg = stateSwitch.state > 0 ? "读卡器已连接":"读卡器连接断开";
			break;
		case LDStateName.STATE_CTL:
	        warnInfo.warn_type = LDWarnType.WARN_TYPE_SP_CTL;
	        warnInfo.level = stateSwitch.state > 0 ? LDWarnLevel.WARN_LEVEL_NORMAL:LDWarnLevel.WARN_LEVEL_SERIOUS;
	        warnInfo.msg = stateSwitch.state > 0 ? "控制器已连接":"控制器连接断开";
			break;
		case LDStateName.STATE_LCD_ON:
			warnInfo.warn_type = LDWarnType.WARN_TYPE_LCD;
			warnInfo.level = stateSwitch.state > 0 ? LDWarnLevel.WARN_LEVEL_NORMAL:LDWarnLevel.WARN_LEVEL_WARN;
			warnInfo.msg = stateSwitch.state > 0 ? "屏电源开启":"屏电源关闭";
			break;
		case LDStateName.STATE_LIGHT_ON:
			warnInfo.warn_type = LDWarnType.WARN_TYPE_LIGHT;
			warnInfo.level = stateSwitch.state > 0 ? LDWarnLevel.WARN_LEVEL_NORMAL:LDWarnLevel.WARN_LEVEL_WARN;
			warnInfo.msg = stateSwitch.state > 0 ? "照明开启":"照明关闭";
			break;
		case LDStateName.STATE_FAN_ON:
			warnInfo.warn_type = LDWarnType.WARN_TYPE_FAN;
			warnInfo.level = LDWarnLevel.WARN_LEVEL_NORMAL;
			warnInfo.msg = stateSwitch.state > 0 ? "散热风扇开启":"散热风扇关闭";
			break;
		case LDStateName.STATE_LOCK:
			warnInfo.warn_type = LDWarnType.WARN_TYPE_LOCK;
	        warnInfo.level = stateSwitch.state > 0 ? LDWarnLevel.WARN_LEVEL_WARN:LDWarnLevel.WARN_LEVEL_NORMAL;
			warnInfo.msg = stateSwitch.state > 0 ? "门锁被打开":"门锁已关闭";
			break;
		}
        if(warnInfo.msg != null) repWarn(con, pstmt, rs, from, warnInfo);
		String sql = "update rmdevice.rmdevice_state set "+stateSwitch.name+"=?"
				+ ",update_date=now() where rmd_id=? returning rmd_id";
		pstmt = con.prepareStatement(sql);
		pstmt.setFloat(setIndex++, stateSwitch.state);
		pstmt.setInt(setIndex++, from);
		rs = pstmt.executeQuery();
		if(!rs.next()) {
			throw new Exception("insert rmdevice_plc failed");
		}
	}
}

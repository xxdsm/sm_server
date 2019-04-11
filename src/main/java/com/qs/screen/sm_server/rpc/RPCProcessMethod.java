package com.qs.screen.sm_server.rpc;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.qs.screen.SMCommon.RPCMessage;
import com.qs.screen.SMCommon.bean.RMDeviceReg;
import com.qs.screen.sm_server.rpc.device.impl.RMDeviceImpl;
import com.qs.screen.sm_server.rpc.user.impl.SMUserImpl;
import com.yeild.common.JsonUtils.JsonUtils;
import com.yeild.common.Utils.ConvertUtils;

public class RPCProcessMethod extends AbstractDataProcessMethod {
	private Logger logger = Logger.getLogger(RPCProcessMethod.class);
	
	public static String generateResultMessage(int code, String message) {
		RPCMessage resultMsg = new RPCMessage();
		resultMsg.setRpccode(code);
		resultMsg.setMessage(message);
		return JsonUtils.objToJson(resultMsg);
	}

	@Override
	public String processData(String from, String data) {
		String respResult = null;
		RPCMessage response = null;
		RPCMessage request = JsonUtils.jsonToObj(data, RPCMessage.class);
		if(request == null) {
			respResult = generateResultMessage(-901, "请求数据格式错误");
		}
		else if(request.getMessage().equals("init")) {
			response = new SMUserImpl().getUserConfig(from);
		}
		else if(request.getMessage().equals("get_provinces")) {
			response = new SMUserImpl().getProvinces(from);
		}
		else if(request.getMessage().equals("get_cities")) {
			Map<String, Integer> params = JsonUtils.jsonToObj(request.getDataContent(), HashMap.class, String.class, Integer.class);
			int province = 0;
			if(params == null || (province = params.get("province")) < 1) {
				return respResult = generateResultMessage(-902, "请求参数错误");
			}
			response = new SMUserImpl().getCities(from, province);
		}
		else if(request.getMessage().equals("get_counties")) {
			Map<String, Integer> params = JsonUtils.jsonToObj(request.getDataContent(), HashMap.class, String.class, Integer.class);
			int city = 0;
			if(params == null || (city = params.get("city")) < 1) {
				return respResult = generateResultMessage(-902, "请求参数错误");
			}
			response = new SMUserImpl().getCounties(from, city);
		}
		else if(request.getMessage().equals("get_towns")) {
			Map<String, Integer> params = JsonUtils.jsonToObj(request.getDataContent(), HashMap.class, String.class, Integer.class);
			int county = 0;
			if(params == null || (county = params.get("county")) < 1) {
				return respResult = generateResultMessage(-902, "请求参数错误");
			}
			response = new SMUserImpl().getTowns(from, county);
		}
		else if(request.getMessage().equals("get_communities")) {
			Map<String, Integer> params = JsonUtils.jsonToObj(request.getDataContent(), HashMap.class, String.class, Integer.class);
			int town = 0;
			if(params == null || (town = params.get("town")) < 1) {
				return respResult = generateResultMessage(-902, "请求参数错误");
			}
			response = new SMUserImpl().getCommunities(from, town);
		}
		else if(request.getMessage().equals("get_livingareas")) {
			Map<String, Integer> params = JsonUtils.jsonToObj(request.getDataContent(), HashMap.class, String.class, Integer.class);
			int community = 0;
			if(params == null || (community = params.get("community")) < 1) {
				return respResult = generateResultMessage(-902, "请求参数错误");
			}
			response = new SMUserImpl().getLivingareas(from, community);
		}
		else if(request.getMessage().equals("get_device_reg_list")) {
			Map<String, String> params = JsonUtils.jsonToObj(request.getDataContent(), HashMap.class, String.class, Integer.class);
			String identify = params.get("identify");
			int operator = ConvertUtils.parseInt(params.get("operator"));
			response = new RMDeviceImpl().getDeviceRegList(from, identify, operator);
		}
		else if(request.getMessage().equals("commit_device_reg")) {
			RMDeviceReg device = JsonUtils.jsonToObj(request.getDataContent(), RMDeviceReg.class);
			response = new RMDeviceImpl().regDevice(from, device);
		}
		else {
			respResult = generateResultMessage(-902, "未定义的接口:"+request.getMessage());
		}
		if(response != null) respResult = JsonUtils.objToJson(response);
		if(respResult == null) {
//			respResult = generateResultMessage(-999, "服务器接口返回空数据");
			respResult = "";
		}
		return respResult;
	}

}

package com.qs.screen.sm_server.rpc;

import org.apache.log4j.Logger;

import com.qs.screen.SMCommon.RPCMessage;
import com.yeild.common.JsonUtils.JsonUtils;

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
		RPCMessage request = JsonUtils.jsonToObj(data, RPCMessage.class);
		if(request == null) {
			respResult = generateResultMessage(-901, "请求数据格式错误");
		}
		if(request.getMessage().equals("ld_reg")) {
			
		} else {
			respResult = generateResultMessage(-902, "未定义的接口:"+request.getMessage());
		}
		if(respResult == null) {
			respResult = generateResultMessage(-999, "服务器接口返回空数据");
		}
		return respResult;
	}

}

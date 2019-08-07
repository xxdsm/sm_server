package com.qs.screen.sm_server.processor;

import java.io.UnsupportedEncodingException;

import org.apache.log4j.Logger;

import com.qs.screen.sm_server.Application;
import com.qs.screen.sm_server.mqtt.ReceivedMessageType;
import com.qs.screen.sm_server.rpc.RPCProcessMethod;
import com.qs.screen.sm_server.rpc.ld.LDManager;
import com.yeild.mqtt.PushMqttMessage;

public class MqttMessageProcessor implements Runnable {
	private Logger logger = Logger.getLogger(MqttMessageProcessor.class);
	private PushMqttMessage message;
	private ReceivedMessageType type;
	
	public MqttMessageProcessor() {
	}
	
	public MqttMessageProcessor(PushMqttMessage message, ReceivedMessageType type) {
		this.message = message;
		this.type = type;
	}
	
	public void pushResult(String result) {
		if(result == null || result.trim().length() < 1) return;
		PushMqttMessage resultMessage = new PushMqttMessage(message.getTopic().replaceFirst(Application.mqttServerTask.getMqttConfig().getRpcRequestName()
				, Application.mqttServerTask.getMqttConfig().getRpcResponseName()), message);
		resultMessage.setPayload(result);
		resultMessage.setRetained(false);
		int retry = 0;
		boolean responsed = false;
		while (true) {
			if(Application.mqttServerTask.pushMessage(resultMessage)){
				responsed = true;
				break;
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			if(++retry > 7) {
				break;
			}
		}
		logger.info(message.getTopic()+" processed"+(responsed?"":" but push response failed")+":\n"+result);
	}

	@Override
	public void run() {
		String result = "";
		if(message == null) {
			result = RPCProcessMethod.generateResultMessage(-98, "服务器错误：空消息");
			return;
		}
		String msgcontent = null;
		try {
			msgcontent = new String(message.getPayload(), "UTF-8");
			String []topicSplit = message.getTopic().split("/");
			String from = null;
			switch(type) {
			case Type_LD_REP:
			case Type_LD_INIT:
				result = LDManager.handle(message.getTopic(), msgcontent);
				return;
			case Type_RPC:
				from = topicSplit[topicSplit.length-2];
				break;
			default:
			}
			RPCProcessMethod processMethod = new RPCProcessMethod();
			result = processMethod.processData(from, msgcontent);
		} catch (Exception e1) {
			if(e1 instanceof UnsupportedEncodingException) {
				logger.warn("rpc request data:"+msgcontent, e1);
				result = RPCProcessMethod.generateResultMessage(-99, "服务器错误：请求数据编码错误");
			} else {
				logger.error("rpc request data:"+msgcontent, e1);
				result = RPCProcessMethod.generateResultMessage(-999, "服务器异常，请检查");
			}
		} finally {
			pushResult(result);
		}
	}
}

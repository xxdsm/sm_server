package com.qs.screen.sm_server.listener;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.concurrent.RejectedExecutionException;

import org.apache.log4j.Logger;

import com.qs.screen.sm_server.Application;
import com.qs.screen.sm_server.mqtt.ReceivedMessageType;
import com.qs.screen.sm_server.processor.MqttMessageProcessor;
import com.qs.screen.sm_server.rpc.RPCProcessMethod;
import com.yeild.common.Utils.CommonUtils;
import com.yeild.mqtt.PushMqttMessage;
import com.yeild.mqtt.listener.OnMqttMessageListener;

public class SMMqttMessageListener implements OnMqttMessageListener {
	private Logger logger = Logger.getLogger(SMMqttMessageListener.class);

	@Override
	public void onMqttReceiveMessage(PushMqttMessage pmessage) {
		ReceivedMessageType type = ReceivedMessageType.Type_UNKOWN;
		if(pmessage.getTopic().startsWith(Application.mqttServerTask.getMqttConfig().getRpcTopicPrefix())) {
			type = ReceivedMessageType.Type_RPC;
		} else if(pmessage.getTopic().startsWith(Application.Topic_LD_rep)) {
			type = ReceivedMessageType.Type_LD_REP;
		} else if(pmessage.getTopic().startsWith(Application.Topic_LD_init)) {
			type = ReceivedMessageType.Type_LD_INIT;
		}
		boolean requireResp = true;
		switch(type) {
		case Type_LD_REP:
			requireResp = false;
		case Type_RPC:
		case Type_LD_INIT:
			MqttMessageProcessor tProcessor = new MqttMessageProcessor(pmessage, type);
			try {
				Application.clientProcessorPool.execute(tProcessor);
			} catch (RejectedExecutionException e) {
				logger.error("there are not enough system resources available to run\n"+CommonUtils.getExceptionInfo(e));
				if(requireResp) {
					PushMqttMessage errorRespMsg = new PushMqttMessage(pmessage.getTopic().replaceFirst(Application.mqttServerTask.getMqttConfig().getRpcRequestName()
							, Application.mqttServerTask.getMqttConfig().getRpcResponseName()), pmessage);
					errorRespMsg.setPayload(RPCProcessMethod.generateResultMessage(-99, "服务器繁忙，请稍后重试"));
					errorRespMsg.setRetained(false);
					Application.mqttServerTask.pushMessage(errorRespMsg);
				}
			}
			break;
		case Type_UNKOWN:
		default:
			String msgcontent;
			try {
				msgcontent = new String(pmessage.getPayload(), "UTF-8");
			} catch (UnsupportedEncodingException e) {
				msgcontent = Arrays.toString(pmessage.getPayload());
			}
			logger.debug("unprocessed message:"+msgcontent);
		}
	}

	@Override
	public void pushMessageResult(PushMqttMessage message, Error error) {
		
	}

}

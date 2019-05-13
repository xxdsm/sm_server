package com.qs.screen.sm_server.mqtt;

import org.eclipse.paho.client.mqttv3.MqttException;

import com.qs.screen.sm_server.Application;
import com.yeild.common.Utils.CommonUtils;
import com.yeild.mqtt.MqttConfig;
import com.yeild.mqtt.MqttConnector;

public class SMMqttConnector extends MqttConnector {

	public SMMqttConnector(MqttConfig config) {
		super(config);
	}

	public SMMqttConnector(String confPath) {
		super(confPath);
	}
	
	@Override
	protected void initAfterConnect() {
		try {
			mqttClient.subscribeWithResponse(Application.Topic_LD_init+mqttConfig.getRpcRequestName()+"#", 0).waitForCompletion();
			mqttClient.subscribeWithResponse(Application.Topic_LD_rep+"/#", 0).waitForCompletion();
			super.initAfterConnect();
		} catch (MqttException e) {
			logger.error(CommonUtils.getExceptionInfo(e));
			lastException = e;
		}
	}
}

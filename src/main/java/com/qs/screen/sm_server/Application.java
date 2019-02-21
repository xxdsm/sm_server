package com.qs.screen.sm_server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import com.qs.screen.sm_server.listener.SMMqttMessageListener;
import com.yeild.common.Utils.ConvertUtils;
import com.yeild.mqtt.MqttConnector;

public class Application {
	private static Logger logger = Logger.getLogger(Application.class);
	public static String appHomePath;
	public static String appConfPath;
	private static Properties appConfig = null;
	
    /** 运行服务端主要进程 */
	public static ExecutorService serverCachePool = Executors.newCachedThreadPool();
	/** 运行客户端消息处理进程，并发数受限 */
	public static ExecutorService clientProcessorPool = null;
	
	public static MqttConnector mqttServerTask = null;
	public static SMMqttMessageListener mqttListener = new SMMqttMessageListener();
	
	/** 设备注册 */
	public static String Topic_LD_init;
	/** 设备上报信息 */
	public static String Topic_LD_rep;
	/** 下发控制指令 */
	public static String Topic_LD_ctl;

	public static boolean loadAppConfig(String confPath) throws IOException {
		InputStream confInputStream = null;
		try {
			File confFile = new File(confPath);
			confInputStream = new FileInputStream(confFile);
			appConfig = new Properties();
			appConfig.load(confInputStream);
			Topic_LD_init = getAppConf("topic.ld.init");
			Topic_LD_rep = getAppConf("topic.ld.rep");
			Topic_LD_ctl = getAppConf("topic.ld.ctl");
			clientProcessorPool = Executors.newFixedThreadPool(getAppConf("maxProcessorCount", 100));
			return true;
		} catch (IOException e) {
			logger.debug("the app conf file does not exits");
			 throw e;
		} finally {
			if (confInputStream != null) {
				try {
					confInputStream.close();
				} catch (IOException e) {
				}
			}
		}
	}
	
	public static String getAppConf(String key) {
		return appConfig.getProperty(key);
	}
	
	public static String getAppConf(String key, String defaultValue) {
		return appConfig.getProperty(key, defaultValue);
	}
	
	public static String getAppHomePath() {
		String appPath = System.getProperty("workHome");
		File appHomeFile = null;
		if(appPath != null) {
			appHomeFile = new File(appPath);
			if(appHomeFile.exists()) {
				return appPath;
			}
		}
		appHomeFile = new File("../");
		try {
			return appHomeFile.getCanonicalFile().getAbsolutePath();
		} catch (IOException e) {
		}
		return null;
	}
	
	public static int getAppConf(String key, int defaultValue) {
		return ConvertUtils.parseInt(appConfig.getProperty(key, ""), defaultValue);
	}
}

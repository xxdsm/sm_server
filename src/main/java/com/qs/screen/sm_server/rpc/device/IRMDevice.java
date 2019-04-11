package com.qs.screen.sm_server.rpc.device;

import com.qs.screen.SMCommon.RPCMessage;
import com.qs.screen.SMCommon.bean.RMDeviceListFilter;
import com.qs.screen.SMCommon.bean.RMDeviceReg;

public interface IRMDevice {
	RPCMessage getDeviceList(String from, RMDeviceListFilter filter);
	RPCMessage getDeviceRegList(String from, String identify, int operator);
	RPCMessage getDeviceDetail(String from, int rmd_id);
	RPCMessage regDevice(String from, RMDeviceReg device);
}

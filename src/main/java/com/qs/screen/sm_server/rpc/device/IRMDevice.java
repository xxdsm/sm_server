package com.qs.screen.sm_server.rpc.device;

import com.qs.screen.SMCommon.RPCMessage;
import com.qs.screen.SMCommon.bean.OpsRMDeviceListFilter;
import com.qs.screen.SMCommon.bean.RMDeviceListFilter;
import com.qs.screen.SMCommon.bean.RMDeviceReg;

public interface IRMDevice {
	RPCMessage getDeviceList(String from, RMDeviceListFilter filter);
	RPCMessage getDeviceRegList(String from, OpsRMDeviceListFilter filter);
	RPCMessage getDeviceDetail(String from, int rmd_id);
	RPCMessage regDevice(String from, RMDeviceReg device);
}

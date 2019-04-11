package com.qs.screen.sm_server.rpc.user;

import com.qs.screen.SMCommon.RPCMessage;

public interface ISMUser {
	public RPCMessage getUserConfig(String from);
	public RPCMessage getProvinces(String from);
	public RPCMessage getCities(String from, int province);
	public RPCMessage getCounties(String from, int city);
	public RPCMessage getTowns(String from, int county);
	public RPCMessage getCommunities(String from, int town);
	public RPCMessage getLivingareas(String from, int community);
}

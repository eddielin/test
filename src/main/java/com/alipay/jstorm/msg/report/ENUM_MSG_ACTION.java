package com.alipay.jstorm.msg.report;

public enum ENUM_MSG_ACTION {
	AD_JSON_LOAD_SUC(995),
	AD_JSON_LOAD_FAIL(996),
	MSG_RECEIVED(999),
	CLICK(1000),
	RESOURCE_REQUIRED_PRELOAD_FAILED(1014),
	INVALID_PARAM(1100),
	CLCIK_APPLIST(1019),
	APN_AT_ACTIVE(2001),
	APN_AT_INACTIVE(2002),
	APN_AT_LAUNCH(2003),
	APN_BACKGROUND(2004);

	private int code;

	private ENUM_MSG_ACTION(int c) {
		code = c;
	}
	public int getCode() {
		return code;
	}
}

/**
 * 
 */
package com.alipay.jstorm.msg.report;

import java.io.Serializable;

/**
 * @author eddie
 * SDK Report Info
 */
public class UdpReportPkg implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4023731368275663569L;
	String ip;
	Long ts=0L;
	String senderid;
	Long uid;
	int result;
	Long messageid;
	String sdk_ver;
	
//	{"senderid":"c23bce612b37be38e1168518","uid":41621516,"result":999,"messageid":"198528754","sdk_ver":"1.3.8"}
	public String getIp() {
		return ip;
	}
	public String getSenderid() {
		return senderid;
	}
	public void setSenderid(String senderid) {
		this.senderid = senderid;
	}
	public Long getUid() {
		return uid;
	}
	public void setUid(Long uid) {
		this.uid = uid;
	}
	public int getResult() {
		return result;
	}
	public void setResult(int result) {
		this.result = result;
	}
	public Long getMessageid() {
		return messageid;
	}
	public void setMessageid(Long messageid) {
		this.messageid = messageid;
	}
	public String getSdk_ver() {
		return sdk_ver;
	}
	public void setSdk_ver(String sdk_ver) {
		this.sdk_ver = sdk_ver;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public Long getTs() {
		return ts;
	}
	public void setTs(Long ts) {
		this.ts = ts;
	}
}

package com.alipay.jstorm.msg.report;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class ActionFilter extends BaseFilter {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8868870446997507412L;
	private int _act;
	
	public ActionFilter(int action) {
		this._act = action;
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		return tuple.getIntegerByField("action")==this._act;
	}

}

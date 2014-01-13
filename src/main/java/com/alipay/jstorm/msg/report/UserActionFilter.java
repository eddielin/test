/**
 * 
 */
package com.alipay.jstorm.msg.report;

import java.util.HashSet;
import java.util.Set;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 * @author eddie
 * filter data with action
 */
public class UserActionFilter extends BaseFilter {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 8649305840220939001L;
	private Set<ENUM_MSG_ACTION> _filtedAction = new HashSet<ENUM_MSG_ACTION>();
	public UserActionFilter(ENUM_MSG_ACTION action) {
		this._filtedAction.add(action);
	}
	public UserActionFilter(Set<ENUM_MSG_ACTION> actions) {
		this._filtedAction.addAll(actions);
	}

	public boolean isKeep(TridentTuple tuple) {
		return this._filtedAction.contains(tuple.getIntegerByField("action"));
	}


}

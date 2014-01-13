package com.alipay.jstorm.test.trident;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class ActionFilter extends BaseFilter {
	private static Logger LOG= LoggerFactory.getLogger(ActionFilter.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = -8868870446997507412L;

	@Override
	public boolean isKeep(TridentTuple tuple) {
		int id = tuple.getIntegerByField("id");
		LOG.info( "tuple fit - " + String.valueOf(id<100));
		return id<100;
	}

}

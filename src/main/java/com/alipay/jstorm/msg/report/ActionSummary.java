package com.alipay.jstorm.msg.report;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class ActionSummary implements Aggregator<Map<Long, Long>> {
	private static Logger LOG = LoggerFactory.getLogger(ActionSummary.class);
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -7645716759546949487L;

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<Long, Long> init(Object batchId, TridentCollector collector) {
		return new HashMap<Long, Long>();
	}

	@Override
	public void aggregate(Map<Long, Long> result, TridentTuple tuple,
			TridentCollector collector) {
		Long ts = tuple.getLongByField("itime");
		if ( result.containsKey(ts) ){
			result.put( ts, result.get(ts)+1 );
		} else {
			result.put( ts, 1L );
		}
	}

	@Override
	public void complete(Map<Long, Long> result, TridentCollector collector) {
		LOG.info("===========================================");
		LOG.info("===========================================");
		LOG.info("===========================================");
		LOG.info("===========================================");
		Iterator<Long> keys = result.keySet().iterator();
		while ( keys.hasNext() ){
			Long ts = keys.next();
			LOG.info( ts + " - " + result.get(ts) );	
		}
		LOG.info("===========================================");
		LOG.info("===========================================");
		LOG.info("===========================================");
	}

}

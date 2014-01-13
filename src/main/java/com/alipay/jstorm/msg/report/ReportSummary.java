/**
 * 
 */
package com.alipay.jstorm.msg.report;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.jstorm.util.data.JedisFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/**
 * @author eddie
 *
 */
public class ReportSummary extends BaseAggregator<Map<Long, Long>> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3099894261479994467L;
	private static Logger LOG = LoggerFactory.getLogger(ReportSummary.class);
	
	private static String CACHE_KEY = "realtime-stat";
	
	private static String REDIS_KEY = "recv-stat";
	
	@Override
	public Map<Long, Long> init(Object batchId, TridentCollector collector) {
		return new HashMap<Long, Long>();
	}

	@Override
	public void aggregate(Map<Long, Long> result, TridentTuple tuple, TridentCollector collector) {
		try{
			Long ts = tuple.getLongByField("itime");
			Long shortTs = ts / 1000;
			
			if ( result.containsKey(shortTs) ){
				result.put( shortTs, result.get(shortTs)+1 );
			} else {
				result.put( shortTs, 1L );
			}
		} catch (Exception e){
			LOG.error("summary action data error", e);
		}
	}

	@Override
	public void complete(Map<Long, Long> result, TridentCollector collector) {
		Set<Long> keys = result.keySet();
		LOG.info("summary result: size=" + keys.size());
		
		long sum = 0L;
		Jedis cli = JedisFactory.getJedisInstance( CACHE_KEY );
		Pipeline pip = cli.pipelined();
		if ( keys.size()>0 ){
			Iterator<Long> itors = keys.iterator();
			
			while( itors.hasNext() ){
				Long ts = itors.next();
				Long count = result.get(ts);
				sum += count;
				pip.hincrBy( REDIS_KEY, String.valueOf(ts), count );
			}
		}
		pip.exec();
		collector.emit( new Values(sum) );
	}

	static class CountState {
		long result = 0;
	}
}

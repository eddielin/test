/**
 * 
 */
package com.alipay.jstorm.msg.report;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.jstorm.util.data.JedisFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import storm.trident.operation.TridentCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author eddie
 *
 */
public class RecvSummary implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3099894261479994467L;
	private static Logger LOG = LoggerFactory.getLogger(RecvSummary.class);
	
	private static String CACHE_KEY = "realtime-stat";
	
	private static String REDIS_KEY = "recv-stat";
	private static Map<Long, Long> result = null;
	private OutputCollector _collector = null;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this._collector = collector;
		result = new HashMap<Long, Long>();
	}

	@Override
	@SuppressWarnings("unchecked")
	public void execute(Tuple input) {
		List<UdpReportPkg> buffer = (List<UdpReportPkg>)input.getValue(0);
		Iterator<UdpReportPkg> itors = buffer.iterator();
		while( itors.hasNext() ){
			UdpReportPkg urp = itors.next();
			

			Long ts = urp.getTs();
			Long shortTs = ts / 1000;
			
			if ( result.containsKey(shortTs) ){
				result.put( shortTs, result.get(shortTs)+1 );
			} else {
				result.put( shortTs, 1L );
			}
		}
		
		Set<Long> keys = result.keySet();
		LOG.info("summary ts size - " + keys.size() );
		Jedis cli = JedisFactory.getJedisInstance( CACHE_KEY );
		Pipeline pip = cli.pipelined();
		Long sum = 0L;
		if ( keys.size()>0 ){
			Iterator<Long> keyItors = keys.iterator();
			
			while( itors.hasNext() ){
				Long ts = keyItors.next();
				Long count = result.get(ts);
				sum += count;
				pip.hincrBy( REDIS_KEY, String.valueOf(ts), count );
			}
		}
		pip.exec();
		JedisFactory.release(CACHE_KEY, cli);
		this._collector.emit( new Values(sum) );
	}

	@Override
	public void cleanup() {
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}

package com.alipay.jstorm.test.trident;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.jstorm.util.data.JedisFactory;

import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class BatchGet implements IBatchSpout {
	private static Logger LOG= LoggerFactory.getLogger(BatchGet.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = 883734110670872450L;

	private String _key;
	private Long _batch = 0L;
	

	public BatchGet(String key, Long batch){
		this._key = key;
		this._batch = batch;
	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void open(Map conf, TopologyContext context) {
		// TODO Auto-generated method stub
	}

	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
		Jedis jedis = JedisFactory.getJedisInstance(this._key);

		String response = null;
		for (int i = 0; i < this._batch; i++) {
			if ((response = jedis.rpop(this._key)) != null) {
				LOG.info( "MQ item - " + response );
				
				collector.emit( new Values( batchId, response ) );
			}
		}
		
		JedisFactory.release( this._key, jedis );
	}

	@Override
	public void ack(long batchId) {
		LOG.info("ack rec -" + batchId);
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("id","splitter");
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}

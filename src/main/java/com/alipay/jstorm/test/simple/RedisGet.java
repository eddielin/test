package com.alipay.jstorm.test.simple;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.jstorm.util.data.JedisFactory;

import redis.clients.jedis.Jedis;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class RedisGet implements IRichSpout {
	private static Logger LOG = LoggerFactory.getLogger(RedisGet.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = 883734110670872450L;
	private SpoutOutputCollector _collector;
	
	private String _key;
	private Long _batchId = 0L;
	
	public RedisGet(String key){
		this._key = key;
	}
	
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this._collector = collector;
	}

	public void nextTuple() {
		LOG.info("get -");
		Jedis jedis = JedisFactory.getJedisInstance(this._key);
		String response = jedis.rpop(this._key);
		JedisFactory.release( this._key, jedis);
		LOG.info("get -" + response);
		
		if (response != null) {
			this._collector.emit(new Values(_batchId, response), _batchId);
		} else {
			LOG.info("get null");
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id","splitter"));
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void ack(Object msgId) {
		_batchId++;
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}

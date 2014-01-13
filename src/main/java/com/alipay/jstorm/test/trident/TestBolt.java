package com.alipay.jstorm.test.trident;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TestBolt implements IRichBolt {

	private static Logger LOG = LoggerFactory.getLogger(TestBolt.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = 2919778203192761223L;
	private OutputCollector _collector = null;
	private static String STREAM_KEY = "storm";

	@Override
	public void execute(Tuple input) {
		Long id = input.getLongByField("id");
		String val = input.getString(1);
		
		LOG.info(id + " - " + val);
		this._collector.ack(input);
		this._collector.emit( new Values(id + "-" + val) );
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this._collector = collector;

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields(STREAM_KEY));

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}

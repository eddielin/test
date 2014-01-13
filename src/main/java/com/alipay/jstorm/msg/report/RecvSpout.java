package com.alipay.jstorm.msg.report;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.alipay.jstorm.util.data.JedisFactory;
import com.google.gson.Gson;

public class RecvSpout implements IRichSpout {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2944650839784306650L;

	private static Logger LOG = LoggerFactory.getLogger(RecvSpout.class);

	private SpoutOutputCollector _collector;
	private String _key = null;
	private int _batch = 1;

	public RecvSpout(String key, int batch) {
		LOG.info(String.format("init redis spout[key=%s, batch=%s]", new Object[]{key, batch}));
		this._key = key;
		this._batch = batch;
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		LOG.info(String.format("open redis spout[key=%s, batch=%s]", new Object[]{this._key, this._batch}));
		this._collector = collector;
	}

	@Override
	public void close() {
	}

	@Override
	public void activate() {
	}

	@Override
	public void deactivate() {
	}

	@Override
	public void nextTuple() {
		Jedis cli = JedisFactory.getJedisInstance( this._key );
		
		List<UdpReportPkg> buffer = new ArrayList<UdpReportPkg>(this._batch);
		boolean isEmpty = false;
		for (int i=0; i<this._batch; i++){
			String response = null;
			if ( (response=cli.rpop(this._key))==null ){
				LOG.info( "mq is empty. fresh buffer&sleep" );
				isEmpty = true;
				break;
			}else{
				LOG.info( "MQ item - " + response );
				
				String[] data = response.split("|");
				if (data.length!=3){
					LOG.warn("Illegal data, ignore!");
				} else {
					String ip = data[0];
					Long ts = Long.valueOf(data[1]);
					
					UdpReportPkg urp = new Gson().fromJson(data[3], UdpReportPkg.class);
					urp.setIp(ip);
					urp.setTs(ts);
					buffer.add(urp);
					
//					buffer.add( new Values( ip, ts, urp.getUid(), urp.getMessageid(), urp.getSenderid(), urp.getResult(), urp.getSdk_ver() ) );						
				}
			}
		}
		this._collector.emit( new Values(buffer) );
		if ( isEmpty ){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				LOG.error("thread sleep error", e);
			}
		}
	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("data"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}


}

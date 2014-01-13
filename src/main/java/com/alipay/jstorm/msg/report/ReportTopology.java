/**
 * 
 */
package com.alipay.jstorm.msg.report;

import java.util.HashSet;
import java.util.Set;

import com.alipay.jstorm.test.simple.RedisGet;
import com.alipay.jstorm.test.simple.TestBolt;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.TopologyAssignException;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;


/**
 * @author eddie
 * Use for computing message action
 */
public class ReportTopology {
	private final static String TOPOLOGY_SPOUT_PARALLELISM_HINT = "topology_spout_parallelism_hint";
	private final static String TOPOLOGY_BOLT_PARALLELISM_HINT = "topology_bolt_parallelism_hint";


	/**
	 * @param args
	 * @throws TopologyAssignException 
	 * @throws InvalidTopologyException 
	 * @throws AlreadyAliveException 
	 */
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, TopologyAssignException {
		TopologyBuilder builder = new TopologyBuilder();
		
		final int PARALLEL = 1;
		IRichSpout ibs = new RecvSpout( "msg", 1 );

		builder.setSpout("recv-redis", ibs, 1);

		builder.setBolt("recv-stat", new RecvSummary(), 1).shuffleGrouping("recv-redis");
		Config conf = new Config();

		conf.put(Config.STORM_CLUSTER_MODE, "distributed");
		conf.put(Config.STORM_MESSAGING_TRANSPORT, "com.alibaba.jstorm.message.netty.NettyContext");
		conf.put(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE, 1024);
		conf.put(Config.STORM_MESSAGING_NETTY_MAX_RETRIES, 10);
		conf.put(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS, 1000);
		conf.put(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS, 1000);
		conf.put(Config.STORM_MESSAGING_NETTY_SERVER_WORKER_THREADS, 1);
		conf.put(Config.STORM_MESSAGING_NETTY_CLIENT_WORKER_THREADS, 1);

		conf.put(Config.TOPOLOGY_DEBUG, true);

		Config.setNumAckers(conf, 1);

		conf.put(Config.TOPOLOGY_WORKERS, 1);
		StormSubmitter.submitTopology("recv-stat", conf,
				builder.createTopology());
	}

}

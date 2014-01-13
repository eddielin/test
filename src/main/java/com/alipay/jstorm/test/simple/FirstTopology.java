package com.alipay.jstorm.test.simple;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.TopologyAssignException;
import backtype.storm.topology.TopologyBuilder;

public class FirstTopology {

	private final static String TOPOLOGY_SPOUT_PARALLELISM_HINT = "topology_spout_parallelism_hint";
	private final static String TOPOLOGY_BOLT_PARALLELISM_HINT = "topology_bolt_parallelism_hint";

	public static void SetBuilder(TopologyBuilder builder, Map<String, Object> conf) {

		builder.setSpout("test-spout", new RedisGet("msg_report"), 1);

		builder.setBolt("test-bolt", new TestBolt(), 1).shuffleGrouping(
				"test-spout");

		conf.put(Config.TOPOLOGY_DEBUG, false);
		// conf.put(ConfigExtension.TOPOLOGY_DEBUG_RECV_TUPLE, false);
		// conf.put(Config.STORM_LOCAL_MODE_ZMQ, false);

		Config.setNumAckers(conf, 1);
		// conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 6);
		// conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 20);
		// conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

		conf.put(Config.TOPOLOGY_WORKERS, 20);

	}

	public static void SetRemoteTopology(String streamName,
			Integer spout_parallelism_hint, Integer bolt_parallelism_hint)
			throws AlreadyAliveException, InvalidTopologyException,
			TopologyAssignException {
		TopologyBuilder builder = new TopologyBuilder();

		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(TOPOLOGY_SPOUT_PARALLELISM_HINT, spout_parallelism_hint);
		conf.put(TOPOLOGY_BOLT_PARALLELISM_HINT, bolt_parallelism_hint);

		SetBuilder(builder, conf);

		conf.put(Config.STORM_CLUSTER_MODE, "distributed");
		conf.put(Config.STORM_MESSAGING_TRANSPORT, "com.alibaba.jstorm.message.netty.NettyContext");
		conf.put(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE, 1024);
		conf.put(Config.STORM_MESSAGING_NETTY_MAX_RETRIES, 10);
		conf.put(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS, 1000);
		conf.put(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS, 1000);
		conf.put(Config.STORM_MESSAGING_NETTY_SERVER_WORKER_THREADS, 1);
		conf.put(Config.STORM_MESSAGING_NETTY_CLIENT_WORKER_THREADS, 1);

		StormSubmitter.submitTopology(streamName, conf,
				builder.createTopology());

	}

	public static void main(String[] args) throws Exception {
			// args: 0-topologyName, 1-spoutParallelism, 2-boltParallelism
			Integer spout_parallelism_hint = 1;
			Integer bolt_parallelism_hint = 2;
			SetRemoteTopology(args[0], spout_parallelism_hint,
					bolt_parallelism_hint);

	}
}

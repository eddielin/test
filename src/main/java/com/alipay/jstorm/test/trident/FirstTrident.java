package com.alipay.jstorm.test.trident;

import java.util.HashMap;
import java.util.Map;

import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.TopologyAssignException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class FirstTrident {

	private final static String TOPOLOGY_SPOUT_PARALLELISM_HINT = "topology_spout_parallelism_hint";
	private final static String TOPOLOGY_BOLT_PARALLELISM_HINT = "topology_bolt_parallelism_hint";

	public static void SetBuilder(TopologyBuilder builder, Map<String, Object> conf) {


		conf.put(Config.TOPOLOGY_DEBUG, false);

		Config.setNumAckers(conf, 1);

		conf.put(Config.TOPOLOGY_WORKERS, 20);

	}

	public static void SetRemoteTopology(String streamName,
			Integer spout_parallelism_hint, Integer bolt_parallelism_hint,
			Long batch_size)
			throws AlreadyAliveException, InvalidTopologyException,
			TopologyAssignException {

		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(TOPOLOGY_SPOUT_PARALLELISM_HINT, spout_parallelism_hint);
		conf.put(TOPOLOGY_BOLT_PARALLELISM_HINT, bolt_parallelism_hint);

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

		conf.put(Config.TOPOLOGY_WORKERS, 20);

		TridentTopology topology = new TridentTopology();
		topology.newStream("trident-test", new BatchGet("msg_report", batch_size))
				.each( new Fields("id", "splitter"), new ActionFilter() );
		StormSubmitter.submitTopology(streamName, conf,
				topology.build());

	}

	public static void main(String[] args) throws Exception {
			// args: 0-topologyName, 1-spoutParallelism, 2-boltParallelism
			Integer spout_parallelism_hint = 1;
			Integer bolt_parallelism_hint = 2;
			Long batch_size = Long.valueOf(args[1]);
			SetRemoteTopology(args[0], spout_parallelism_hint,
					bolt_parallelism_hint, batch_size);

	}
}

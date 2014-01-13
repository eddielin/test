package com.alipay.jstorm.util.data;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.jstorm.util.SystemConfig;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisFactory {
	private static Logger LOG = LoggerFactory.getLogger(JedisFactory.class);
	
	private static Map<String, JedisPool> jedisPools = new HashMap<String, JedisPool>();

	public static JedisPool initJedisPool(String jedisName){
		JedisPool jPool = jedisPools.get(jedisName);
		if (jPool == null){
			String host = SystemConfig.getProperty(jedisName + ".redis.host");
			int port = SystemConfig.getIntProperty(jedisName + ".redis.port");

			jPool = newJeisPool(host, port);
			jedisPools.put(jedisName, jPool);
		}
		return jPool;
	}
 

	public static Jedis getJedisInstance(String jedisName) {
		LOG.debug("get jedis[name=" + jedisName + "]");
		JedisPool jedisPool = jedisPools.get(jedisName);
		if (jedisPool==null){
			jedisPool = initJedisPool(jedisName);
		}
		
		Jedis jedis = null;
		for (int i=0; i<10; i++){
			try{
				jedis = jedisPool.getResource();
				
				break;
			}catch(Exception e){
				LOG.error("get resource from jedis pool error. times " + (i+1) + ". retry...", e);
				jedisPool.returnBrokenResource(jedis);
				try {
					Thread.sleep(500);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
		return jedis;
	}
	
	private static JedisPool newJeisPool(String host, int port){
		LOG.debug("init jedis pool[" + host + ":" + port + "]");
		
		JedisPoolConfig jpCfg = new JedisPoolConfig();
		jpCfg.setMaxIdle(500);
		jpCfg.setMaxActive(500);
		jpCfg.setMaxWait(1000);
		return new JedisPool(jpCfg, host, port, 30000);
	}

	/** 
	 * 配合使用getJedisInstance方法后将jedis对象释放回连接池中 
	 * 
	 * @param jedis 使用完毕的Jedis对象 
	 * @return true 释放成功；否则返回false 
	 */
	public static boolean release(String poolName, Jedis jedis) { 
		LOG.debug("release jedis pool[name=" + poolName + "]");
		
		JedisPool jedisPool = jedisPools.get(poolName);
		if ( jedisPool!=null && jedis!=null ) { 
			try{
				jedisPool.returnResource(jedis);
			}catch(Exception e){
				jedisPool.returnBrokenResource(jedis);
			}
			return true;
		}
		return false; 
	}
	
	public static void destroy(){
		LOG.debug("destroy all pool");
		for(Iterator<JedisPool> itors = jedisPools.values().iterator(); itors.hasNext(); ){
			try{
				JedisPool jedisPool = itors.next();
				jedisPool.destroy();
			}finally{}
		}
	}
	
}

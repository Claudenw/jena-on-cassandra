/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jena.cassandra.graph;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;

/**
 * Class to perform bulk operations on the Cassandra database.
 *
 */
public class BulkExecutor {

	private Log log;

	/*
	 * the Cassandra session to use.
	 */
	private Session session;

	/*
	 * this is a map of all ResultSetFutures and the Runnables that are
	 * registered as their listener. when the future completes the runnable
	 * removes the future from the map.
	 */
	private ConcurrentHashMap<Runnable, ResultSetFuture> map = new ConcurrentHashMap<>();

	/*
	 * The service that executest the runnables.
	 */
	private ExecutorService executor = Executors.newSingleThreadExecutor();

	/**
	 * Constructor.
	 * 
	 * @param session
	 *            The Cassandra session to use.
	 */
	public BulkExecutor(Session session) {
		this.session = session;
		this.log = LogFactory.getLog(BulkExecutor.class.getName() + "." + hashCode());
	}

	/**
	 * Set the logging. Used when another class uses the bulk executor and wants
	 * to capture the logging.
	 * 
	 * @param log
	 *            The log for this BulkExecutor to log to.
	 */
	public void setLog(Log log) {
		this.log = log;
	}

	/**
	 * Execute a number of statements.
	 * 
	 * All output from the statements are discarded.
	 * 
	 * Statement order is not guaranteed.
	 * 
	 * May be called multiple times. May not be called after awaitFinish().
	 * 
	 * @see awaitFinish
	 * 
	 * @param statements
	 *            An iterator of statements to execute.
	 */
	public void execute(Iterator<String> statements) {
		while (statements.hasNext()) {
			String statement = statements.next();

			/*
			 * runner runs when future is complete. It simply removes the future
			 * from the map to show that it is complete.
			 */
			Runnable runner = new Runnable() {
				@Override
				public void run() {
					if (log.isDebugEnabled()) {
						log.debug("finished executing statement: " + statement);
					}
					map.remove(this);
				}
			};
			if (log.isDebugEnabled()) {
				log.debug("executing statement: " + statement);
			}
			ResultSetFuture rsf = session.executeAsync(statement);
			/*
			 * the map keeps a reference to the ResultSetFuture so we can track
			 * when all futures have executed
			 */
			map.put(runner, rsf);
			// the ResultSetFuture will execute the runner on the executor.
			rsf.addListener(runner, executor);
		}
	}

	/**
	 * Wait for the executor to complete all executions.
	 */
	public void awaitFinish() {
		/* if the map is not empty we need to wait until it is */
		if (!map.isEmpty()) {
			/*
			 * this runnable will check the map and if it is empty notify this
			 * (calling) thread so it can continue. Otherwise it places itself
			 * back on the executor queue again.
			 */
			Runnable r = new Runnable() {
				@Override
				public void run() {
					synchronized (this) {
						if (map.isEmpty()) {
							notify();
						} else {
							if (log.isDebugEnabled()) {
								log.debug("Requeueing for finish");
							}
							executor.execute(this);
						}
					}
				}
			};
			synchronized (r) {
				try {
					/*
					 * place the runnable on the executor thread and then wait
					 * to be notified. We will be notified when all the
					 * statements have been executed
					 */
					executor.execute(r);
					r.wait();
				} catch (InterruptedException e) {
					log.error("Interrupted waiting for map to clear", e);
				}
			}
		}
		executor.shutdown();
	}

}

package com.anvizent.elt.core.listener.common.connection;

/**
 * @author Hareen Bejjanki
 *
 */
public class RethinkDBConnectionByTaskId extends ConnectionByTaskId<RethinkDBConnection, Object> {

	public RethinkDBConnectionByTaskId() {
	}

	public RethinkDBConnectionByTaskId(RethinkDBConnection connection, Object query, int partitionId) {
		super(connection, query, partitionId);
	}

}

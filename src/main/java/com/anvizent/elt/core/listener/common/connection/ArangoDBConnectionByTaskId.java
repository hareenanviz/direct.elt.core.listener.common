package com.anvizent.elt.core.listener.common.connection;

/**
 * @author Hareen Bejjanki
 *
 */
public class ArangoDBConnectionByTaskId extends ConnectionByTaskId<ArangoDBConnection, Object> {

	public ArangoDBConnectionByTaskId() {
	}

	public ArangoDBConnectionByTaskId(ArangoDBConnection connection, Object query, int partitionId) {
		super(connection, query, partitionId);
	}

}

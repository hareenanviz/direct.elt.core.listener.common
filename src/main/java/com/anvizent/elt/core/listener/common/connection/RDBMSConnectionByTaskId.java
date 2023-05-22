package com.anvizent.elt.core.listener.common.connection;

/**
 * @author Hareen Bejjanki
 *
 */
public class RDBMSConnectionByTaskId extends ConnectionByTaskId<RDBMSConnection, String> {

	public RDBMSConnectionByTaskId() {
	}

	public RDBMSConnectionByTaskId(RDBMSConnection rdbmsConnection, String query, int partitionId) {
		super(rdbmsConnection, query, partitionId);
	}
}

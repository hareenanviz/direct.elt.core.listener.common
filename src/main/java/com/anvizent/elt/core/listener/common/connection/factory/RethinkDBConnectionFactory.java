package com.anvizent.elt.core.listener.common.connection.factory;

import java.util.concurrent.TimeoutException;

import com.anvizent.elt.core.listener.common.connection.RethinkDBConnection;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Connection.Builder;

/**
 * @author Hareen Bejjanki
 *
 */
public class RethinkDBConnectionFactory {
	public static Connection createConnection(RethinkDBConnection connection, int index) throws TimeoutException {
		index = index % connection.getHost().size();
		Builder builder = RethinkDB.r.connection().hostname(connection.getHost().get(index));

		index = index % connection.getPortNumber().size();
		if (connection.getPortNumber() != null) {
			builder = builder.port(connection.getPortNumber().get(index));
		}

		if (connection.getDBName() != null && !connection.getDBName().isEmpty()) {
			builder = builder.db(connection.getDBName());
		}

		if (connection.getUserName() != null && !connection.getUserName().isEmpty()) {
			builder = builder.user(connection.getUserName(), connection.getPassword());
		}

		if (connection.getTimeout() != null) {
			builder = builder.timeout(connection.getTimeout());
		}

		return builder.connect();
	}
}
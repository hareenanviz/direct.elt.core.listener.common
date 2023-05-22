package com.anvizent.elt.core.listener.common.connection;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;

import org.apache.spark.TaskContext;

import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.listener.common.connection.factory.ArangoDBConnectionFactory;
import com.anvizent.elt.core.listener.common.connection.factory.RethinkDBConnectionFactory;
import com.anvizent.elt.core.listener.common.connection.util.SQLConnectionUtil;
import com.arangodb.ArangoDB;

/**
 * @author Hareen Bejjanki
 *
 */
public class ApplicationConnectionBean {

	private static ApplicationConnectionBean applicationConnectionBeanInstance = null;

	private HashMap<ConnectionByTaskId<?, ?>, Object> connections = new HashMap<>();

	private ApplicationConnectionBean() {
	}

	public static ApplicationConnectionBean getInstance() {
		if (applicationConnectionBeanInstance == null) {
			applicationConnectionBeanInstance = new ApplicationConnectionBean();
		}

		return applicationConnectionBeanInstance;
	}

	@SuppressWarnings("rawtypes")
	public Object[] get(ConnectionByTaskId connectionByTaskId, boolean reconnect)
	        throws ImproperValidationException, UnimplementedException, SQLException, TimeoutException {
		if (connectionByTaskId instanceof RDBMSConnectionByTaskId) {
			return getOrCreateSQLConnection((RDBMSConnectionByTaskId) connectionByTaskId, reconnect);
		} else if (connectionByTaskId instanceof RethinkDBConnectionByTaskId) {
			return getOrCreateRethinkConnection((RethinkDBConnectionByTaskId) connectionByTaskId, reconnect);
		} else if (connectionByTaskId instanceof ArangoDBConnectionByTaskId) {
			return getOrCreateArangoConnection((ArangoDBConnectionByTaskId) connectionByTaskId, reconnect);
		} else {
			throw new UnimplementedException();
		}
	}

	private Object[] getOrCreateSQLConnection(RDBMSConnectionByTaskId rdbmsConnection, boolean reconnect) throws ImproperValidationException, SQLException {
		boolean reConnected = false;
		java.sql.Connection connection = (java.sql.Connection) connections.get(rdbmsConnection);

		if (reconnect && (connection == null || connection.isClosed())) {
			synchronized (connections) {
				connection = (java.sql.Connection) connections.get(rdbmsConnection);
				if (reconnect && (connection == null || connection.isClosed())) {
					connection = SQLConnectionUtil.getConnection(rdbmsConnection.getConnection());
					reConnected = true;
					if (rdbmsConnection.getQuery() != null && !rdbmsConnection.getQuery().isEmpty()) {
						connection.createStatement().execute(rdbmsConnection.getQuery());
					}
				}
			}
		}

		if (connection != null) {
			connections.put(rdbmsConnection, connection);
		}

		return new Object[] { connection, reConnected };
	}

	private synchronized Object[] getOrCreateRethinkConnection(RethinkDBConnectionByTaskId rethinkDBConnection, boolean reconnect) throws TimeoutException {
		boolean reConnected = false;
		com.rethinkdb.net.Connection connection = (com.rethinkdb.net.Connection) connections.get(rethinkDBConnection);

		if (reconnect && (connection == null || !connection.isOpen())) {
			synchronized (connections) {
				connection = (com.rethinkdb.net.Connection) connections.get(rethinkDBConnection);
				if (reconnect && (connection == null || !connection.isOpen())) {
					connection = RethinkDBConnectionFactory.createConnection(rethinkDBConnection.getConnection(), TaskContext.getPartitionId());
					reConnected = true;
				}
			}
		}

		connections.put(rethinkDBConnection, connection);

		return new Object[] { connection, reConnected };
	}

	private synchronized Object[] getOrCreateArangoConnection(ArangoDBConnectionByTaskId arangoDBConnection, boolean reconnect) throws TimeoutException {
		boolean reConnected = false;
		ArangoDB connection = (ArangoDB) connections.get(arangoDBConnection);

		if (reconnect && connection == null) {
			synchronized (connections) {
				connection = (ArangoDB) connections.get(arangoDBConnection);
				if (reconnect && connection == null) {
					connection = ArangoDBConnectionFactory.createConnection(arangoDBConnection.getConnection(), TaskContext.getPartitionId());
					reConnected = true;
				}
			}
		}

		connections.put(arangoDBConnection, connection);

		return new Object[] { connection, reConnected };
	}

	public synchronized void closeAll() throws SQLException, UnimplementedException {
		if (connections != null && !connections.isEmpty()) {
			ArrayList<ConnectionByTaskId<? extends Connection, ?>> connectionBeans = new ArrayList<>();

			Iterator<Entry<ConnectionByTaskId<? extends Connection, ?>, Object>> iterator = connections.entrySet().iterator();
			while (iterator.hasNext()) {
				Entry<ConnectionByTaskId<? extends Connection, ?>, Object> entry = iterator.next();
				if (entry.getKey() instanceof RDBMSConnectionByTaskId) {
					java.sql.Connection sqlConnection = (java.sql.Connection) connections.get(entry.getKey());
					SQLConnectionUtil.closeConnectionObjects(sqlConnection);
					connectionBeans.add(entry.getKey());
				} else if (entry.getKey() instanceof RethinkDBConnectionByTaskId) {
					com.rethinkdb.net.Connection rethinkDBConnection = (com.rethinkdb.net.Connection) connections.get(entry.getKey());
					if (rethinkDBConnection != null && rethinkDBConnection.isOpen()) {
						rethinkDBConnection.close();
						connectionBeans.add(entry.getKey());
					}
				} else if (entry.getKey() instanceof ArangoDBConnectionByTaskId) {
					ArangoDB arangoDBConnection = (ArangoDB) connections.get(entry.getKey());
					if (arangoDBConnection != null) {
						arangoDBConnection.shutdown();
						connectionBeans.add(entry.getKey());
					}
				} else {
					throw new UnimplementedException();
				}
			}

			removeAll(connectionBeans);
		}
	}

	private void removeAll(ArrayList<ConnectionByTaskId<? extends Connection, ?>> connectionBeans) {
		if (connectionBeans != null) {
			for (int i = 0; i < connectionBeans.size(); i++) {
				connections.remove(connectionBeans.get(i));
			}
		}
	}
}

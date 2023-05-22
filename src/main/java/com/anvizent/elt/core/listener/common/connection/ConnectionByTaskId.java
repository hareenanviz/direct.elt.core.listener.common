package com.anvizent.elt.core.listener.common.connection;

/**
 * @author Hareen Bejjanki
 *
 */
public abstract class ConnectionByTaskId<T extends Connection, Q> {

	private T connection;
	private Q query;
	private int partitionId;

	public T getConnection() {
		return connection;
	}

	public void setConnection(T connection) {
		this.connection = connection;
	}

	public Q getQuery() {
		return query;
	}

	public void setQuery(Q query) {
		this.query = query;
	}

	public int getPartitionId() {
		return partitionId;
	}

	public void setPartitionId(int partitionId) {
		this.partitionId = partitionId;
	}

	public ConnectionByTaskId() {
	}

	public ConnectionByTaskId(T connection, Q query, int partitionId) {
		this.connection = connection;
		this.query = query;
		this.partitionId = partitionId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;

		result = prime * result + partitionId;
		result = prime * result + ((connection == null) ? 0 : connection.hashCode());

		return result;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (obj == null) {
			return false;
		}

		if (getClass() != obj.getClass()) {
			return false;
		}

		ConnectionByTaskId<T, Q> other = (ConnectionByTaskId<T, Q>) obj;

		if (partitionId != other.partitionId) {
			return false;
		}

		if (connection == null) {
			if (other.connection != null) {
				return false;
			}
		} else if (!connection.equals(other.connection)) {
			if (query == null) {
				if (other.query != null) {
					return false;
				}
			} else if (!query.equals(other.query)) {
				return false;
			}
		}

		return true;
	}

}

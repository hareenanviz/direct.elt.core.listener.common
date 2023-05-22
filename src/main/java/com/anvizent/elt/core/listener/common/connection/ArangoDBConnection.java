package com.anvizent.elt.core.listener.common.connection;

import java.util.ArrayList;

/**
 * @author Hareen Bejjanki
 *
 */
public class ArangoDBConnection implements Connection {

	private static final long serialVersionUID = 1L;

	private ArrayList<String> host;
	private ArrayList<Integer> portNumber;
	private String dbName;
	private String userName;
	private String password;
	private Integer timeout;

	public ArrayList<String> getHost() {
		return host;
	}

	public void setHost(ArrayList<String> host) {
		this.host = host;
	}

	public ArrayList<Integer> getPortNumber() {
		return portNumber;
	}

	public void setPortNumber(ArrayList<Integer> portNumber) {
		this.portNumber = portNumber;
	}

	public String getDBName() {
		return dbName;
	}

	public void setDBName(String dbName) {
		this.dbName = dbName;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		if (password == null) {
			password = "";
		}

		return password;
	}

	public void setPassword(String password) {
		if (password == null) {
			password = "";
		}

		this.password = password;
	}

	public Integer getTimeout() {
		return timeout;
	}

	public void setTimeout(Integer timeout) {
		this.timeout = timeout;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((dbName == null) ? 0 : dbName.hashCode());
		result = prime * result + ((host == null) ? 0 : host.hashCode());
		result = prime * result + ((password == null) ? 0 : password.hashCode());
		result = prime * result + ((portNumber == null) ? 0 : portNumber.hashCode());
		result = prime * result + ((timeout == null) ? 0 : timeout.hashCode());
		result = prime * result + ((userName == null) ? 0 : userName.hashCode());
		return result;
	}

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

		ArangoDBConnection other = (ArangoDBConnection) obj;

		if (dbName == null) {
			if (other.dbName != null) {
				return false;
			}
		} else if (!dbName.equals(other.dbName)) {
			return false;
		}

		if (host == null) {
			if (other.host != null) {
				return false;
			}
		} else if (!host.equals(other.host)) {
			return false;
		}

		if (password == null) {
			if (other.password != null) {
				return false;
			}
		} else if (!password.equals(other.password)) {
			return false;
		}

		if (portNumber == null) {
			if (other.portNumber != null) {
				return false;
			}
		} else if (!portNumber.equals(other.portNumber)) {
			return false;
		}

		if (timeout == null) {
			if (other.timeout != null) {
				return false;
			}
		} else if (!timeout.equals(other.timeout)) {
			return false;
		}

		if (userName == null) {
			if (other.userName != null) {
				return false;
			}
		} else if (!userName.equals(other.userName)) {
			return false;
		}

		return true;
	}
}

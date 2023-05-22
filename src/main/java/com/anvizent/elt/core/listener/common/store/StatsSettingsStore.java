package com.anvizent.elt.core.listener.common.store;

import java.util.ArrayList;

import com.anvizent.elt.core.listener.common.connection.RethinkDBConnection;
import com.anvizent.elt.core.listener.common.constant.StatsType;

/**
 * @author Hareen Bejjanki
 *
 */
public class StatsSettingsStore {

	private RethinkDBConnection rethinkDBConnection;
	private String tableName;
	private String endPoint;
	private ArrayList<String> constantNames;
	private ArrayList<String> constantValues;
	private ArrayList<Class<?>> constantTypes;
	private StatsType statsType;
	private Integer retryCount;
	private Long retryDelay;

	public RethinkDBConnection getRethinkDBConnection() {
		return rethinkDBConnection;
	}

	public void setRethinkDBConnection(RethinkDBConnection rethinkDBConnection) {
		this.rethinkDBConnection = rethinkDBConnection;
	}

	public void setRethinkDBConnection(ArrayList<String> host) {
		if (this.rethinkDBConnection == null) {
			this.rethinkDBConnection = new RethinkDBConnection();
		}
		this.rethinkDBConnection.setHost(host);
	}

	public void setRethinkDBConnection(ArrayList<String> host, ArrayList<Integer> portNumber, String dbName, String userName, String password, Long timeout) {
		if (this.rethinkDBConnection == null) {
			this.rethinkDBConnection = new RethinkDBConnection();
		}
		this.rethinkDBConnection.setHost(host);
		this.rethinkDBConnection.setPortNumber(portNumber);
		this.rethinkDBConnection.setDBName(dbName);
		this.rethinkDBConnection.setUserName(userName);
		this.rethinkDBConnection.setPassword(password);
		this.rethinkDBConnection.setTimeout(timeout);
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getEndPoint() {
		return endPoint;
	}

	public void setEndPoint(String endPoint) {
		this.endPoint = endPoint;
	}

	public ArrayList<String> getConstantNames() {
		return constantNames;
	}

	public void setConstantNames(ArrayList<String> constantNames) {
		this.constantNames = constantNames;
	}

	public ArrayList<String> getConstantValues() {
		return constantValues;
	}

	public void setConstantValues(ArrayList<String> constantValues) {
		this.constantValues = constantValues;
	}

	public ArrayList<Class<?>> getConstantTypes() {
		return constantTypes;
	}

	public void setConstantTypes(ArrayList<Class<?>> constantTypes) {
		this.constantTypes = constantTypes;
	}

	public StatsType getStatsType() {
		return statsType;
	}

	public void setStatsType(StatsType statsType) {
		this.statsType = statsType;
	}

	public Integer getRetryCount() {
		return retryCount;
	}

	public void setRetryCount(Integer retryCount) {
		this.retryCount = retryCount;
	}

	public Long getRetryDelay() {
		return retryDelay;
	}

	public void setRetryDelay(Long retryDelay) {
		this.retryDelay = retryDelay;
	}

	public StatsSettingsStore(String tableName, String endPoint, ArrayList<String> constantNames, ArrayList<String> constantValues,
	        ArrayList<Class<?>> constantTypes, StatsType type, Integer retryCount, Long retryDelay) {
		this.tableName = tableName;
		this.endPoint = endPoint;
		this.constantNames = constantNames;
		this.constantValues = constantValues;
		this.constantTypes = constantTypes;
		this.statsType = type;
		this.retryCount = retryCount;
		this.retryDelay = retryDelay;
	}
}

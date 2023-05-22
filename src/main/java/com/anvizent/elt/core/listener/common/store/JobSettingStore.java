package com.anvizent.elt.core.listener.common.store;

import java.io.Serializable;

/**
 * @author Hareen Bejjanki
 *
 */
public class JobSettingStore implements Serializable {

	private static final long serialVersionUID = 1L;

	private Long jobDetailsId;
	private String jobDetailsURL;
	private String executorDetailsURL;
	private String applicationEndTimeURL;
	private String appDBName;
	private String clientId;
	private String hostName;
	private String portNumber;
	private String userName;
	private String password;
	private String privateKey;
	private String iv;
	private Integer retryCount;
	private Long retryDelay;
	private String initiatorTimeZone;

	public Long getJobDetailsId() {
		return jobDetailsId;
	}

	public void setJobDetailsId(Long jobDetailsId) {
		this.jobDetailsId = jobDetailsId;
	}

	public String getJobDetailsURL() {
		return jobDetailsURL;
	}

	public void setJobDetailsURL(String jobDetailsURL) {
		this.jobDetailsURL = jobDetailsURL;
	}

	public String getExecutorDetailsURL() {
		return executorDetailsURL;
	}

	public void setExecutorDetailsURL(String executorDetailsURL) {
		this.executorDetailsURL = executorDetailsURL;
	}

	public String getApplicationEndTimeURL() {
		return applicationEndTimeURL;
	}

	public void setApplicationEndTimeURL(String applicationEndTimeURL) {
		this.applicationEndTimeURL = applicationEndTimeURL;
	}

	public String getAppDBName() {
		return appDBName;
	}

	public void setAppDBName(String appDBName) {
		this.appDBName = appDBName;
	}

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public String getPortNumber() {
		return portNumber;
	}

	public void setPortNumber(String portNumber) {
		this.portNumber = portNumber;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getPrivateKey() {
		return privateKey;
	}

	public void setPrivateKey(String privateKey) {
		this.privateKey = privateKey;
	}

	public String getIv() {
		return iv;
	}

	public void setIv(String iv) {
		this.iv = iv;
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

	public String getInitiatorTimeZone() {
		return initiatorTimeZone;
	}

	public void setInitiatorTimeZone(String initiatorTimeZone) {
		this.initiatorTimeZone = initiatorTimeZone;
	}

	public JobSettingStore(Long jobDetailsId, String jobDetailsURL, String executorDetailsURL, String applicationEndTimeURL, String appDBName, String clientId,
	        String hostName, String portNumber, String userName, String password, String privateKey, String iv, Integer retryCount, Long retryDelay,
	        String initiatorTimeZone) {
		this.jobDetailsId = jobDetailsId;
		this.jobDetailsURL = jobDetailsURL;
		this.executorDetailsURL = executorDetailsURL;
		this.applicationEndTimeURL = applicationEndTimeURL;
		this.appDBName = appDBName;
		this.clientId = clientId;
		this.hostName = hostName;
		this.portNumber = portNumber;
		this.userName = userName;
		this.password = password;
		this.privateKey = privateKey;
		this.iv = iv;
		this.retryCount = retryCount;
		this.retryDelay = retryDelay;
		this.initiatorTimeZone = initiatorTimeZone;
	}
}

package com.anvizent.elt.core.listener.common.bean;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.spark.sql.SparkSession;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.listener.common.store.ErrorHandlerStore;
import com.anvizent.elt.core.listener.common.store.JobSettingStore;
import com.anvizent.elt.core.listener.common.store.ResourceConfig;
import com.anvizent.elt.core.listener.common.store.StatsSettingsStore;
import com.anvizent.rest.util.RestUtil;

/**
 * @author Hareen Bejjanki
 *
 */
public class ApplicationBean {
	private static ApplicationBean applicationBeanInstance = null;

	private String sparkAppName;
	private SparkSession sparkSession;
	private LinkedHashMap<String, LinkedHashMap<String, ArrayList<AnvizentAccumulator>>> accumulators = new LinkedHashMap<>();
	private StatsSettingsStore statsSettingsStore;
	private ErrorHandlerStore errorHandlerStore;
	private JobSettingStore jobSettingStore;
	private ResourceConfig resourceConfig;
	private RestUtil restUtil;
	private boolean sentToInitiator;

	private ApplicationBean() {
		restUtil = new RestUtil();
	}

	public static ApplicationBean getInstance() {
		if (applicationBeanInstance == null) {
			applicationBeanInstance = new ApplicationBean();
		}

		return applicationBeanInstance;
	}

	public String getSparkAppName() {
		return sparkAppName;
	}

	public void setSparkAppName(String sparkAppName) {
		this.sparkAppName = sparkAppName;
	}

	public SparkSession getSparkSession() {
		return sparkSession;
	}

	public void setSparkSession(SparkSession sparkSession) {
		this.sparkSession = sparkSession;
	}

	public void addAccumulator(String componentName, LinkedHashMap<String, ArrayList<AnvizentAccumulator>> accumulators) {
		this.accumulators.put(componentName, accumulators);
	}

	public void addAccumulator(String componentName, String internalRDDName, AnvizentAccumulator... anvizentAccumulators) {
		LinkedHashMap<String, ArrayList<AnvizentAccumulator>> componentAccumulators = getComponentAccumulators(componentName);
		ArrayList<AnvizentAccumulator> accumulators = getAccumulators(componentAccumulators, internalRDDName);

		addAccumulators(accumulators, anvizentAccumulators);
	}

	private ArrayList<AnvizentAccumulator> getAccumulators(LinkedHashMap<String, ArrayList<AnvizentAccumulator>> componentAccumulators,
	        String internalRDDName) {
		ArrayList<AnvizentAccumulator> accumulators = componentAccumulators.get(internalRDDName);

		if (accumulators == null) {
			accumulators = new ArrayList<>();
			componentAccumulators.put(internalRDDName, accumulators);
		}

		return accumulators;
	}

	private LinkedHashMap<String, ArrayList<AnvizentAccumulator>> getComponentAccumulators(String componentName) {
		LinkedHashMap<String, ArrayList<AnvizentAccumulator>> existingComponentAccumulators = this.accumulators.get(componentName);

		if (existingComponentAccumulators == null) {
			existingComponentAccumulators = new LinkedHashMap<>();
			this.accumulators.put(componentName, existingComponentAccumulators);
		}

		return existingComponentAccumulators;
	}

	private void addAccumulators(ArrayList<AnvizentAccumulator> newStatsCategoryAccumulators, AnvizentAccumulator[] anvizentAccumulators) {
		for (AnvizentAccumulator anvizentAccumulator : anvizentAccumulators) {
			newStatsCategoryAccumulators.add(anvizentAccumulator);
		}
	}

	public LinkedHashMap<String, LinkedHashMap<String, ArrayList<AnvizentAccumulator>>> getAccumulators() {
		return accumulators;
	}

	public LinkedHashMap<String, ArrayList<AnvizentAccumulator>> getAccumulators(String componentName) {
		return accumulators.get(componentName);
	}

	public ArrayList<AnvizentAccumulator> getAccumulators(String componentName, String internalRDDName) {
		if (accumulators.get(componentName) == null) {
			return null;
		}

		return accumulators.get(componentName).get(internalRDDName);
	}

	public StatsSettingsStore getStatsSettingsStore() {
		return statsSettingsStore;
	}

	public void setStatsSettingsStore(StatsSettingsStore statsSettingsStore) {
		this.statsSettingsStore = statsSettingsStore;
	}

	public ErrorHandlerStore getErrorHandlerStore() {
		return errorHandlerStore;
	}

	public void setErrorHandlerStore(ErrorHandlerStore errorHandlerStore) {
		this.errorHandlerStore = errorHandlerStore;
	}

	public JobSettingStore getJobSettingStore() {
		return jobSettingStore;
	}

	public void setJobSettingStore(JobSettingStore jobSettingStore) {
		this.jobSettingStore = jobSettingStore;
	}

	public ResourceConfig getResourceConfig() {
		return resourceConfig;
	}

	public void setResourceConfig(ResourceConfig resourceConfig) {
		this.resourceConfig = resourceConfig;
	}

	public boolean isSentToInitiator() {
		return sentToInitiator;
	}

	public void setSentToInitiator(boolean sentToInitiator) {
		this.sentToInitiator = sentToInitiator;
	}

	public void storeApplicationEndTime(ApplicationStatusDetails applicationStatusDetails) {
		JobSettingStore jobSettingStore = getJobSettingStore();

		Map<String, Object> requestBody = new HashMap<>();
		putJobDetails(requestBody, jobSettingStore);

		requestBody.put("applicationEndTime", new Date());
		requestBody.put("applicationStatus", applicationStatusDetails.getApplicationStatus().name());
		requestBody.put("message", applicationStatusDetails.getMessage());

		storeApplicationEndTime(jobSettingStore.getApplicationEndTimeURL(), requestBody, HttpMethod.PUT, jobSettingStore.getRetryCount(),
		        jobSettingStore.getRetryDelay());
	}

	private void storeApplicationEndTime(String engineURL, Map<String, Object> requestBody, HttpMethod httpMethodType, int retryCount, Long retryDelay) {
		System.out.println("storeApplicationEndTime - engineURL: " + engineURL + ", requestBody: " + requestBody);

		ResponseEntity<String> response = restUtil.exchange(engineURL, requestBody, httpMethodType, null, "");
		System.out.println("storeApplicationEndTime - engineURL: " + engineURL + ", requestBody: " + requestBody + ", resposeStatus: "
		        + response.getStatusCode() + ", response: " + response.getBody());

		if (!response.getStatusCode().equals(HttpStatus.OK)) {
			if (retryCount > 0) {
				if (retryDelay != null && retryDelay > 0) {
					try {
						Thread.sleep(retryDelay);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

				storeApplicationEndTime(engineURL, requestBody, httpMethodType, retryCount - 1, retryDelay);
			}
		}
	}

	public void putJobDetails(Map<String, Object> requestBody, JobSettingStore jobSettingStore) {
		requestBody.put("id", jobSettingStore.getJobDetailsId());
		requestBody.put("clientDBDetails", getClientDBDetails(jobSettingStore));
	}

	public Map<String, Object> getClientDBDetails(JobSettingStore jobSettingStore) {
		Map<String, Object> rdbmsRequestBody = new HashMap<>();
		rdbmsRequestBody.put("host", jobSettingStore.getHostName());
		rdbmsRequestBody.put("portNumber", jobSettingStore.getPortNumber());
		rdbmsRequestBody.put("userName", jobSettingStore.getUserName());
		rdbmsRequestBody.put("password", jobSettingStore.getPassword());
		rdbmsRequestBody.put("privateKey", jobSettingStore.getPrivateKey());
		rdbmsRequestBody.put("iv", jobSettingStore.getIv());

		Map<String, Object> clientDBDetailsRequestBody = new HashMap<>();
		clientDBDetailsRequestBody.put("rdbmsConnection", rdbmsRequestBody);
		clientDBDetailsRequestBody.put("appDBName", jobSettingStore.getAppDBName());
		clientDBDetailsRequestBody.put("clientId", jobSettingStore.getClientId());

		return clientDBDetailsRequestBody;
	}

}

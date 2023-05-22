package com.anvizent.elt.core.listener.common;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerExecutorAdded;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.constant.Constants.General;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.util.TypeConversionUtil;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.ApplicationStatusDetails;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.bean.ELTCoreJobListener;
import com.anvizent.elt.core.listener.common.connection.ApplicationConnectionBean;
import com.anvizent.elt.core.listener.common.constant.ApplicationStatus;
import com.anvizent.elt.core.listener.common.constant.Constants.ConfigConstants.StatsSettings;
import com.anvizent.elt.core.listener.common.constant.Constants.SQL;
import com.anvizent.elt.core.listener.common.store.JobSettingStore;
import com.anvizent.elt.core.listener.common.store.StatsSettingsStore;
import com.anvizent.rest.util.RestUtil;

/**
 * @author Hareen Bejjanki
 *
 */
public class ApplicationListenerUtil {

	public static void onApplicationEnd(RestUtil restUtil, LinkedHashMap<String, Component> components) {
		try {
			if (ApplicationBean.getInstance().getStatsSettingsStore() != null) {
				storeAccumulatedStats(ApplicationBean.getInstance().getStatsSettingsStore().getRetryCount(),
				        ApplicationBean.getInstance().getStatsSettingsStore().getRetryDelay(), restUtil);
			}

			if (!ApplicationBean.getInstance().isSentToInitiator() && ApplicationBean.getInstance().getJobSettingStore() != null) {
				ApplicationStatusDetails applicationStatusDetails = new ApplicationStatusDetails();
				applicationStatusDetails.setApplicationStatus(ApplicationStatus.SUCCESS);

				ApplicationBean.getInstance().storeApplicationEndTime(applicationStatusDetails);
			}

			afterShutDownJobs(components);
		} catch (Exception exception) {
			throw new RuntimeException(exception.getMessage(), exception);
		}
	}

	public static void onApplicationStart(SparkListenerApplicationStart arg0, RestUtil restUtil) {
		if (ApplicationBean.getInstance().getJobSettingStore() != null) {
			storeJobApplicationId(arg0.appId().get(), restUtil);
		}
	}

	public static void onExecutorAdded(SparkListenerExecutorAdded arg0, RestUtil restUtil) {
		if (ApplicationBean.getInstance().getJobSettingStore() != null) {
			storeJobExecutorDetails(arg0, restUtil);
		}
	}

	public static void onJobEnd(LinkedHashMap<String, Component> components) {
		try {
			afterShutDownJobs(components);
		} catch (Exception exception) {
			throw new RuntimeException(exception.getMessage(), exception);
		}
	}

	public static void onTaskEnd(RestUtil restUtil) {
		if (ApplicationBean.getInstance().getStatsSettingsStore() == null) {
			return;
		}

		try {
			storeAccumulatedStats(ApplicationBean.getInstance().getStatsSettingsStore().getRetryCount(),
			        ApplicationBean.getInstance().getStatsSettingsStore().getRetryDelay(), restUtil);
		} catch (Exception exception) {
			throw new RuntimeException(exception.getMessage(), exception);
		}
	}

	private static void afterShutDownJobs(LinkedHashMap<String, Component> components) throws Exception {
		for (Entry<String, Component> entry : components.entrySet()) {
			Component component = entry.getValue();
			for (ELTCoreJobListener eltCoreJobListener : component.getELTCoreJobListeners().values()) {
				eltCoreJobListener.afterStop();
			}
		}

		closeDBConnections();
	}

	private static void closeDBConnections() throws UnimplementedException, SQLException {
		ApplicationConnectionBean.getInstance().closeAll();
	}

	private static void storeAccumulatedStats(int retryCount, Long retryDelay, RestUtil restUtil) throws Exception {
		LinkedHashSet<String> componentNames = new LinkedHashSet<>(ApplicationBean.getInstance().getAccumulators().keySet());
		ArrayList<Map<String, Object>> accumulatedStats = new ArrayList<>();

		for (String componentName : componentNames) {
			LinkedHashMap<String, ArrayList<AnvizentAccumulator>> internalRDDsMap = ApplicationBean.getInstance().getAccumulators().get(componentName);
			LinkedHashSet<String> internalRDDNames = new LinkedHashSet<>(internalRDDsMap.keySet());

			for (String internalRDDName : internalRDDNames) {
				LinkedHashSet<AnvizentAccumulator> anvizentAccumulators = new LinkedHashSet<>(internalRDDsMap.get(internalRDDName));

				for (AnvizentAccumulator anvizentAccumulator : anvizentAccumulators) {
					accumulatedStats.add(
					        getAccumulatedStats(componentName, internalRDDName, anvizentAccumulator, ApplicationBean.getInstance().getStatsSettingsStore()));
				}
			}
		}

		storeAccumulatedStats(accumulatedStats, retryCount, retryDelay, restUtil);
	}

	private static Map<String, Object> getAccumulatedStats(String componentName, String internalRDDName, AnvizentAccumulator anvizentAccumulator,
	        StatsSettingsStore statsSettingsStore) throws Exception {
		Map<String, Object> requestBody = new HashMap<>();

		Map<String, Object> statsDetailsRow = new HashMap<>();
		statsDetailsRow.put(StatsSettings.JOB_NAME, ApplicationBean.getInstance().getSparkAppName());
		statsDetailsRow.put(StatsSettings.COMPONENT_NAME, componentName);
		statsDetailsRow.put(StatsSettings.INTERNAL_RDD_NAME, internalRDDName);
		statsDetailsRow.put(StatsSettings.SPECIAL_NAME, anvizentAccumulator.getSpecialName());
		statsDetailsRow.put(StatsSettings.STATS_NAME, anvizentAccumulator.getStatsName());
		statsDetailsRow.put(StatsSettings.STATS_VALUE, anvizentAccumulator.getDoubleAccumulator().value());
		statsDetailsRow.put(StatsSettings.COMPONENT_LEVEL, anvizentAccumulator.isComponentLevel());

		if (statsSettingsStore.getConstantNames() != null && !statsSettingsStore.getConstantNames().isEmpty()) {
			for (int i = 0; i < statsSettingsStore.getConstantNames().size(); i++) {
				Object value = TypeConversionUtil.dataTypeConversion(statsSettingsStore.getConstantValues().get(i), String.class,
				        statsSettingsStore.getConstantTypes().get(i), null, null, null, OffsetDateTime.now().getOffset());
				statsDetailsRow.put(StatsSettings.EXTERNAL_DATA + "_" + statsSettingsStore.getConstantNames().get(i), value);
			}
		}

		Map<String, Object> rethinkDBDetails = new HashMap<>();
		rethinkDBDetails.put("host", statsSettingsStore.getRethinkDBConnection().getHost().get(0));
		rethinkDBDetails.put("portNumber", statsSettingsStore.getRethinkDBConnection().getPortNumber() == null ? null
		        : statsSettingsStore.getRethinkDBConnection().getPortNumber().get(0).toString());
		rethinkDBDetails.put("userName", statsSettingsStore.getRethinkDBConnection().getUserName());
		rethinkDBDetails.put("password", statsSettingsStore.getRethinkDBConnection().getPassword());
		rethinkDBDetails.put("dbName", statsSettingsStore.getRethinkDBConnection().getDBName());
		rethinkDBDetails.put("timeout",
		        statsSettingsStore.getRethinkDBConnection().getTimeout() == null ? null : statsSettingsStore.getRethinkDBConnection().getTimeout().toString());

		requestBody.put("statsDetails", statsDetailsRow);
		requestBody.put(SQL.TABLE, statsSettingsStore.getTableName());
		requestBody.put("rethinkDBConnection", rethinkDBDetails);

		return requestBody;
	}

	private static void storeAccumulatedStats(ArrayList<Map<String, Object>> accumulatedStats, int retryCount, Long retryDelay, RestUtil restUtil) {
		StatsSettingsStore statsSettingsStore = ApplicationBean.getInstance().getStatsSettingsStore();

		String url = statsSettingsStore.getEndPoint();

		url = url + General.QUERY_STRING + StatsSettings.JOB_NAME + General.Operator.EQUAL_TO + ApplicationBean.getInstance().getSparkAppName();

		ResponseEntity<String> response = restUtil.exchange(url, accumulatedStats, HttpMethod.POST, null, "");
		if (!response.getStatusCode().equals(HttpStatus.OK)) {
			if (retryCount > 0) {
				if (retryDelay != null && retryDelay > 0) {
					try {
						Thread.sleep(retryDelay);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

				storeAccumulatedStats(accumulatedStats, retryCount - 1, retryDelay, restUtil);
			}
		}
	}

	private static void storeJobApplicationId(String applicationId, RestUtil restUtil) {
		JobSettingStore jobSettingStore = ApplicationBean.getInstance().getJobSettingStore();

		Map<String, Object> requestBody = new HashMap<>();
		ApplicationBean.getInstance().putJobDetails(requestBody, jobSettingStore);
		requestBody.put("applicationId", applicationId);
		requestBody.put("applicationStartTime", new Timestamp(System.currentTimeMillis()));

		storeJobApplicationId(jobSettingStore.getJobDetailsURL(), requestBody, HttpMethod.PUT, jobSettingStore.getRetryCount(), jobSettingStore.getRetryDelay(),
		        restUtil);
	}

	private static void storeJobApplicationId(String engineURL, Map<String, Object> requestBody, HttpMethod httpMethodType, int retryCount, Long retryDelay,
	        RestUtil restUtil) {
		ResponseEntity<String> response = restUtil.exchange(engineURL, requestBody, httpMethodType, null, "");
		if (!response.getStatusCode().equals(HttpStatus.OK)) {
			if (retryCount > 0) {
				if (retryDelay != null && retryDelay > 0) {
					try {
						Thread.sleep(retryDelay);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

				storeJobApplicationId(engineURL, requestBody, httpMethodType, retryCount - 1, retryDelay, restUtil);
			}
		}
	}

	private static void storeJobExecutorDetails(SparkListenerExecutorAdded arg0, RestUtil restUtil) {
		JobSettingStore jobSettingStore = ApplicationBean.getInstance().getJobSettingStore();

		Map<String, Object> requestBody = ApplicationBean.getInstance().getClientDBDetails(jobSettingStore);
		requestBody.put("executorId", arg0.executorId());
		requestBody.put("host", arg0.executorInfo().executorHost());
		requestBody.put("cores", arg0.executorInfo().totalCores());

		String engineURL = jobSettingStore.getExecutorDetailsURL().replace("{job_details_id}", jobSettingStore.getJobDetailsId() + "");

		storeJobExecutorDetails(engineURL, requestBody, HttpMethod.PUT, jobSettingStore.getRetryCount(), jobSettingStore.getRetryDelay(), restUtil);
	}

	private static void storeJobExecutorDetails(String engineURL, Map<String, Object> requestBody, HttpMethod httpMethodType, int retryCount, Long retryDelay,
	        RestUtil restUtil) {
		ResponseEntity<String> response = restUtil.exchange(engineURL, requestBody, httpMethodType, null, "");
		if (!response.getStatusCode().equals(HttpStatus.OK)) {
			if (retryCount > 0) {
				if (retryDelay != null && retryDelay > 0) {
					try {
						Thread.sleep(retryDelay);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

				storeJobExecutorDetails(engineURL, requestBody, httpMethodType, retryCount - 1, retryDelay, restUtil);
			}
		}
	}
}

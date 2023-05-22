package com.anvizent.elt.core.listener.common.bean;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.function.ConvertFromRowFunction;
import com.anvizent.elt.core.lib.function.ConvertToRowFunction;
import com.anvizent.elt.core.listener.common.constant.Constants.ConfigConstants;
import com.anvizent.elt.core.listener.common.exception.ListenerAlreadyExistsException;;

/**
 * @author Hareen Bejjanki
 *
 */
public class Component implements Serializable {
	private static final long serialVersionUID = 1L;

	private String name;
	private final LinkedHashMap<String, JavaRDD<HashMap<String, Object>>> rdds;
	private final LinkedHashMap<String, AnvizentDataType> structure;
	private final SparkSession sparkSession;
	private LinkedHashMap<String, ELTCoreJobListener> eltCoreJobListeners = new LinkedHashMap<>();

	public String getName() {
		return name;
	}

	public LinkedHashMap<String, AnvizentDataType> getStructure() {
		return structure;
	}

	public void persistAll() {
		for (Entry<String, JavaRDD<HashMap<String, Object>>> rdd : rdds.entrySet()) {
			rdd.getValue().persist(StorageLevel.MEMORY_AND_DISK());
		}
	}

	public LinkedHashMap<String, JavaRDD<HashMap<String, Object>>> getRdds() {
		return rdds;
	}

	public SparkSession getSparkSession() {
		return sparkSession;
	}

	private Component(SparkSession sparkSession, String name, String streamName, Dataset<Row> dataset, StructType structType, ConfigBean configBean,
	        ArrayList<AnvizentAccumulator> anvizentAccumulators, AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
	        throws UnsupportedException, InvalidArgumentsException {
		this.name = name;
		this.sparkSession = sparkSession;
		this.structure = getStructure(structType);
		rdds = new LinkedHashMap<String, JavaRDD<HashMap<String, Object>>>();
		JavaRDD<HashMap<String, Object>> rdd = dataset.javaRDD()
		        .flatMap(new ConvertFromRowFunction(configBean, structure, anvizentAccumulators, errorHandlerSinkFunction, jobDetails));
		if (streamName == null || streamName.isEmpty()) {
			streamName = ConfigConstants.DEFAULT_STREAM;
		}
		rdds.put(streamName, rdd);
	}

	private Component(SparkSession sparkSession, String name, String streamName, JavaRDD<HashMap<String, Object>> rdd, StructType structType)
	        throws UnsupportedException {
		this.name = name;
		this.sparkSession = sparkSession;
		this.structure = getStructure(structType);
		rdds = new LinkedHashMap<String, JavaRDD<HashMap<String, Object>>>();
		if (streamName == null || streamName.isEmpty()) {
			streamName = ConfigConstants.DEFAULT_STREAM;
		}
		rdds.put(streamName, rdd);
	}

	private Component(SparkSession sparkSession, String name, String streamName, JavaRDD<HashMap<String, Object>> rdd,
	        LinkedHashMap<String, AnvizentDataType> structure) {
		this.name = name;
		this.sparkSession = sparkSession;
		this.structure = structure;
		rdds = new LinkedHashMap<String, JavaRDD<HashMap<String, Object>>>();
		if (streamName == null || streamName.isEmpty()) {
			streamName = ConfigConstants.DEFAULT_STREAM;
		}
		rdds.put(streamName, rdd);
	}

	private StructType getStructType() {
		return getStructType(structure);
	}

	public static StructType getStructType(LinkedHashMap<String, AnvizentDataType> structure) {
		StructField[] fields = new StructField[structure.size()];
		int i = 0;

		for (Entry<String, AnvizentDataType> key : structure.entrySet()) {
			fields[i++] = DataTypes.createStructField(key.getKey(), key.getValue().getSparkType(), true);
		}

		return DataTypes.createStructType(fields);
	}

	public static LinkedHashMap<String, AnvizentDataType> getStructure(StructType structType) throws UnsupportedException {
		StructField[] fields = structType.fields();
		LinkedHashMap<String, AnvizentDataType> structure = new LinkedHashMap<String, AnvizentDataType>();

		for (StructField field : fields) {
			structure.put(field.name(), new AnvizentDataType(field.dataType()));
		}

		return structure;
	}

	public void addRDD(String streamName, JavaRDD<HashMap<String, Object>> rdd) {
		if (streamName == null || streamName.isEmpty()) {
			streamName = ConfigConstants.DEFAULT_STREAM;
		}
		rdds.put(streamName, rdd);
	}

	public JavaRDD<HashMap<String, Object>> getRDD() {
		return getRDD(ConfigConstants.DEFAULT_STREAM);
	}

	public JavaRDD<HashMap<String, Object>> getRDD(String streamName) {
		if (streamName == null || streamName.isEmpty()) {
			streamName = ConfigConstants.DEFAULT_STREAM;
		}
		return rdds.get(streamName);
	}

	public JavaRDD<HashMap<String, Object>> removeRDD(String streamName) {
		if (streamName == null || streamName.isEmpty()) {
			streamName = ConfigConstants.DEFAULT_STREAM;
		}
		return rdds.remove(streamName);
	}

	public Dataset<Row> getRDDAsDataset(ConfigBean configBean, ArrayList<AnvizentAccumulator> anvizentAccumulators,
	        AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails) throws InvalidArgumentsException {
		return getRDDAsDataset(ConfigConstants.DEFAULT_STREAM, configBean, anvizentAccumulators, errorHandlerSinkFunction, jobDetails);
	}

	public Dataset<Row> getRDDAsDataset(String streamName, ConfigBean configBean, ArrayList<AnvizentAccumulator> anvizentAccumulators,
	        AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails) throws InvalidArgumentsException {
		if (streamName == null || streamName.isEmpty()) {
			streamName = ConfigConstants.DEFAULT_STREAM;
		}

		JavaRDD<Row> rowRDD = rdds.get(streamName)
		        .flatMap(new ConvertToRowFunction(configBean, structure, anvizentAccumulators, errorHandlerSinkFunction, jobDetails));
		Dataset<Row> dataset = sparkSession.createDataFrame(rowRDD, getStructType());

		if (configBean.isPersist()) {
			dataset.persist(StorageLevel.MEMORY_AND_DISK());
		}

		return dataset;
	}

	public LinkedHashMap<String, ELTCoreJobListener> getELTCoreJobListeners() {
		return eltCoreJobListeners;
	}

	public void addELTCoreJobListener(String name, ELTCoreJobListener eltCoreJobListener) throws ListenerAlreadyExistsException {
		if (this.eltCoreJobListeners.containsKey(name)) {
			throw new ListenerAlreadyExistsException(ELTCoreJobListener.class, name);
		} else {
			this.eltCoreJobListeners.put(name, eltCoreJobListener);
		}
	}

	public void addELTCoreJobListenerIfNotExists(String name, ELTCoreJobListener eltCoreJobListener) throws ListenerAlreadyExistsException {
		if (!this.eltCoreJobListeners.containsKey(name)) {
			this.eltCoreJobListeners.put(name, eltCoreJobListener);
		}
	}

	public void forceAddELTCoreJobListener(String name, ELTCoreJobListener eltCoreJobListener) {
		this.eltCoreJobListeners.put(name, eltCoreJobListener);
	}

	public void forceAddELTCoreJobListeners(Map<String, ELTCoreJobListener> eltCoreJobListeners) {
		this.eltCoreJobListeners.putAll(eltCoreJobListeners);
	}

	public static Component createComponent(SparkSession sparkSession, String name, JavaRDD<HashMap<String, Object>> rdd, StructType structType)
	        throws UnsupportedException {
		return createComponent(sparkSession, name, ConfigConstants.DEFAULT_STREAM, rdd, structType);
	}

	public static Component createComponent(SparkSession sparkSession, String name, String streamName, JavaRDD<HashMap<String, Object>> rdd,
	        StructType structType) throws UnsupportedException {
		if (streamName == null || streamName.isEmpty()) {
			streamName = ConfigConstants.DEFAULT_STREAM;
		}
		return new Component(sparkSession, name, streamName, rdd, structType);
	}

	public static Component createComponent(SparkSession sparkSession, String name, JavaRDD<HashMap<String, Object>> rdd,
	        LinkedHashMap<String, AnvizentDataType> structure) {
		return createComponent(sparkSession, name, ConfigConstants.DEFAULT_STREAM, rdd, structure);
	}

	public static Component createComponent(SparkSession sparkSession, String name, String streamName, JavaRDD<HashMap<String, Object>> rdd,
	        LinkedHashMap<String, AnvizentDataType> structure) {
		if (streamName == null || streamName.isEmpty()) {
			streamName = ConfigConstants.DEFAULT_STREAM;
		}
		return new Component(sparkSession, name, streamName, rdd, structure);
	}

	public static Component createComponent(SparkSession sparkSession, String name, Dataset<Row> dataset, StructType structType, ConfigBean configBean,
	        ArrayList<AnvizentAccumulator> anvizentAccumulators, AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
	        throws UnsupportedException, InvalidArgumentsException {
		return createComponent(sparkSession, name, ConfigConstants.DEFAULT_STREAM, dataset, structType, configBean, anvizentAccumulators,
		        errorHandlerSinkFunction, jobDetails);
	}

	public static Component createComponent(SparkSession sparkSession, String name, String streamName, Dataset<Row> dataset, StructType structType,
	        ConfigBean configBean, ArrayList<AnvizentAccumulator> anvizentAccumulators, AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
	        throws UnsupportedException, InvalidArgumentsException {
		if (streamName == null || streamName.isEmpty()) {
			streamName = ConfigConstants.DEFAULT_STREAM;
		}
		return new Component(sparkSession, name, streamName, dataset, structType, configBean, anvizentAccumulators, errorHandlerSinkFunction, jobDetails);
	}
}
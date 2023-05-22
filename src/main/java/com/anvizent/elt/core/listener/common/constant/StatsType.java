package com.anvizent.elt.core.listener.common.constant;

/**
 * @author Hareen Bejjanki
 *
 */
public enum StatsType {

	ALL("ALL"), NONE("NONE"), COMPONENT_LEVEL("COMPONENT_LEVEL");

	private String value;

	private StatsType(String value) {
		this.value = value;
	}

	public static StatsType getInstance(String statsType) {
		return getInstance(statsType, COMPONENT_LEVEL);
	}

	private static StatsType getInstance(String statsType, StatsType defaultValue) {
		if (statsType == null || statsType.isEmpty()) {
			return defaultValue;
		} else if (statsType.equalsIgnoreCase(ALL.value)) {
			return ALL;
		} else if (statsType.equalsIgnoreCase(NONE.value)) {
			return NONE;
		} else if (statsType.equalsIgnoreCase(COMPONENT_LEVEL.value)) {
			return COMPONENT_LEVEL;
		} else {
			return null;
		}
	}
}

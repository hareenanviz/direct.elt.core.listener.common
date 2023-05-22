package com.anvizent.elt.core.listener.common.store;

import java.io.Serializable;

import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;

/**
 * @author Hareen Bejjanki
 *
 */
public class ResourceConfig implements Serializable {
	private static final long serialVersionUID = 1L;

	private String rdbmsConfigLocation;
	private InvalidConfigException exception;

	public String getRdbmsConfigLocation() {
		return rdbmsConfigLocation;
	}

	public void setRdbmsConfigLocation(String rdbmsConfigLocation) {
		this.rdbmsConfigLocation = rdbmsConfigLocation;
	}

	public InvalidConfigException getException() {
		return exception;
	}

	public void setException(InvalidConfigException exception) {
		this.exception = exception;
	}
}

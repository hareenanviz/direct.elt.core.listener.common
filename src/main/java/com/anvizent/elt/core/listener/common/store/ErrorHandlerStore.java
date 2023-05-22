package com.anvizent.elt.core.listener.common.store;

import java.io.Serializable;
import java.util.HashMap;

import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;

/**
 * @author Hareen Bejjanki
 *
 */
public class ErrorHandlerStore implements Serializable {
	private static final long serialVersionUID = 1L;

	private HashMap<String, ErrorHandlerSink> ehSinks = new HashMap<>();
	String defaultEHResourceName;

	public HashMap<String, ErrorHandlerSink> getEHSinks() {
		return ehSinks;
	}

	public ErrorHandlerSink getEHSink(String name) {
		return ehSinks.get(name);
	}

	public void putEHSink(String name, ErrorHandlerSink ehSink) {
		this.ehSinks.put(name, ehSink);
	}

	public String getDefaultEHResourceName() {
		return defaultEHResourceName;
	}

	public void setDefaultEHResourceName(String defaultEHResourceName) {
		this.defaultEHResourceName = defaultEHResourceName;
	}

}

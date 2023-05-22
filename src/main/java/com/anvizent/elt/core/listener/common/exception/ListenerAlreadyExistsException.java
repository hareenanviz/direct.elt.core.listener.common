package com.anvizent.elt.core.listener.common.exception;

/**
 * @author Hareen Bejjanki
 *
 */
public class ListenerAlreadyExistsException extends Exception {
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("rawtypes")
	public ListenerAlreadyExistsException(Class c, String name) {
		super(c.getCanonicalName() + " with name '" + name + "' already exists!");
	}
}

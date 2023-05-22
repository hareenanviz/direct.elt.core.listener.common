package com.anvizent.elt.core.listener.common.exception;

/**
 * @author Hareen Bejjanki
 *
 */
public class ListenerInstanceAlreadyExistsException extends Exception {

	private static final long serialVersionUID = 1L;

	public ListenerInstanceAlreadyExistsException() {
		super("Application Listener instance already exists, cannot be created again.");
	}
}

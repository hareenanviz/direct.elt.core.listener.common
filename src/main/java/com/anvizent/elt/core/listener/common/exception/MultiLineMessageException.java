package com.anvizent.elt.core.listener.common.exception;

import java.text.MessageFormat;

import com.anvizent.elt.core.lib.constant.Constants.General;
import com.anvizent.elt.core.lib.util.ExceptionUtil;

/**
 * @author Hareen Bejjanki
 *
 */
public abstract class MultiLineMessageException extends Exception {

	private static final long serialVersionUID = 1L;
	protected String baseMessage;
	private String messages = "";
	private int numberOfExceptions;

	public void add(String message, Object... values) {
		this.numberOfExceptions++;
		messages += General.NEW_LINE + General.TAB + numberOfExceptions + General.KEY_SEPARATOR + " " + MessageFormat.format(message, values);
	}

	public void add(String message, Throwable cause, Object... values) {
		this.numberOfExceptions++;
		messages += General.NEW_LINE + General.TAB + numberOfExceptions + General.KEY_SEPARATOR + " " + MessageFormat.format(message, values);
	}

	public void add(String message) {
		this.numberOfExceptions++;
		messages += General.NEW_LINE + General.TAB + numberOfExceptions + General.KEY_SEPARATOR + " " + message;
	}

	public void add(String message, Throwable cause) {
		this.numberOfExceptions++;
		messages += General.NEW_LINE + General.TAB + numberOfExceptions + General.KEY_SEPARATOR + " " + message;
		ExceptionUtil.addCause(this, cause);
	}

	public int getNumberOfExceptions() {
		return numberOfExceptions;
	}

	@Override
	public String getMessage() {
		return baseMessage + messages;
	}
}

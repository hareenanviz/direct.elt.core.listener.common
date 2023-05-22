package com.anvizent.elt.core.listener.common.bean;

import com.anvizent.elt.core.listener.common.constant.ApplicationStatus;

/**
 * @author Hareen Bejjanki
 *
 */
public class ApplicationStatusDetails {

	private ApplicationStatus applicationStatus;
	private String message;

	public ApplicationStatus getApplicationStatus() {
		return applicationStatus;
	}

	public void setApplicationStatus(ApplicationStatus applicationStatus) {
		this.applicationStatus = applicationStatus;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

}

package com.anvizent.elt.core.listener.common.bean;

/**
 * @author Hareen Bejjanki
 *
 */
public interface ELTCoreJobListener {
	void beforeStart();

	void afterStop() throws Exception;
}

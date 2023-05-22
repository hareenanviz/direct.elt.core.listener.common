package com.anvizent.elt.core.listener.common.exception;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.SeekDetails;
import com.anvizent.elt.core.lib.constant.Constants;
import com.anvizent.elt.core.listener.common.constant.ExceptionMessage;

/**
 * @author Hareen Bejjanki
 *
 */
public class InvalidConfigException extends MultiLineMessageException {

	private static final long serialVersionUID = 1L;
	private String component = Constants.UNKNOWN;
	private String componentName = Constants.UNKNOWN;
	private SeekDetails seekDetails;

	public void setComponent(String component) {
		this.component = component;
		resetBaseMessage();
	}

	private void resetBaseMessage() {
		baseMessage = ExceptionMessage.INVALID_CONFIG_EXCEPTION_PREFIX + component + ExceptionMessage.INVALID_CONFIG_WITH_NAME + componentName + "'"
		        + seekDetails + ExceptionMessage.INVALID_CONFIG_EXCEPTION_SUFIX;
	}

	public void setComponentName(String componentName) {
		this.componentName = componentName;
		resetBaseMessage();
	}

	public void setSeekDetails(SeekDetails seekDetails) {
		this.seekDetails = seekDetails;
		resetBaseMessage();
	}

	public void setDetails(ConfigBean configBean) {
		this.component = configBean.getConfigName();
		this.componentName = configBean.getName();
		this.seekDetails = configBean.getSeekDetails();
		resetBaseMessage();
	}
}

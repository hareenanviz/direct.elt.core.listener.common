package com.anvizent.elt.core.listener.common.connection;

import com.anvizent.elt.core.lib.constant.Constants.General;

/**
 * @author Hareen Bejjanki
 *
 */
public class RDBMSConnection implements Connection {

	private static final long serialVersionUID = 1L;

	private static final String MS_SQL_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	private static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";

	private String jdbcURL;
	private String userName;
	private String password;
	private String driver;

	public String getJdbcURL() {
		return jdbcURL;
	}

	public void setJdbcUrl(String jdbcURL) {
		this.jdbcURL = addCharsetParameters(jdbcURL);
	}

	private String addCharsetParameters(String jdbcURL) {
		String initialChar = "?";
		String paramSeperatorChar = "&";

		if (driver != null && driver.equals(MS_SQL_DRIVER)) {
			initialChar = ";";
			paramSeperatorChar = ";";
		}
		if (driver != null && driver.equals(ORACLE_DRIVER)) {
			return jdbcURL;
		}

		if (jdbcURL == null) {
			return null;
		}

		if (!jdbcURL.contains(initialChar) && !jdbcURL.endsWith(initialChar)) {
			jdbcURL += initialChar;
		}

		jdbcURL = checkAndAddParam(jdbcURL, "useUnicode", General.USE_UNICODE, initialChar, paramSeperatorChar);
		jdbcURL = checkAndAddParam(jdbcURL, "characterEncoding", General.CHARSET_ENCODING, initialChar, paramSeperatorChar);

		return jdbcURL;
	}

	public String checkAndAddParam(String jdbcURL, String param, String constant, String initialChar, String paramSeperatorChar) {
		if (!jdbcURL.toLowerCase().contains(param.toLowerCase())) {
			jdbcURL = addParametersSeperator(jdbcURL, initialChar, paramSeperatorChar);
			jdbcURL += constant;
		}

		return jdbcURL;
	}

	private String addParametersSeperator(String jdbcURL, String initialChar, String paramSeperatorChar) {
		if (!jdbcURL.endsWith(paramSeperatorChar) && !jdbcURL.endsWith(initialChar)) {
			jdbcURL += paramSeperatorChar;
		}

		return jdbcURL;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	public RDBMSConnection() {
	}

	public RDBMSConnection(String jdbcURL, String userName, String password, String driver) {
		this.jdbcURL = jdbcURL;
		this.userName = userName;
		this.password = password;
		this.driver = driver;
	}

	public boolean isNull() {
		return jdbcURL == null || jdbcURL.isEmpty() || userName == null || userName.isEmpty() || password == null || password.isEmpty() || driver == null
		        || driver.isEmpty();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((driver == null) ? 0 : driver.hashCode());
		result = prime * result + ((jdbcURL == null) ? 0 : jdbcURL.hashCode());
		result = prime * result + ((password == null) ? 0 : password.hashCode());
		result = prime * result + ((userName == null) ? 0 : userName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (obj == null) {
			return false;
		}

		if (getClass() != obj.getClass()) {
			return false;
		}

		RDBMSConnection other = (RDBMSConnection) obj;
		if (driver == null) {
			if (other.driver != null)
				return false;
		} else if (!driver.equals(other.driver))
			return false;
		if (jdbcURL == null) {
			if (other.jdbcURL != null)
				return false;
		} else if (!jdbcURL.equals(other.jdbcURL)) {
			return false;
		}

		if (password == null) {
			if (other.password != null) {
				return false;
			}
		} else if (!password.equals(other.password)) {
			return false;
		}

		if (userName == null) {
			if (other.userName != null)
				return false;
		} else if (!userName.equals(other.userName)) {
			return false;
		}

		return true;
	}
}
package com.anvizent.elt.core.listener.common.connection.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.listener.common.connection.RDBMSConnection;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLConnectionUtil {

	public static void closeConnectionObjects(Connection connection) throws SQLException {
		if (connection != null && !connection.isClosed()) {
			connection.close();
		}
	}

	public static Connection getConnection(RDBMSConnection rdbmsConnection) throws SQLException, ImproperValidationException {
		return getConnection(rdbmsConnection.getJdbcURL(), rdbmsConnection.getUserName(), rdbmsConnection.getPassword(), rdbmsConnection.getDriver());
	}

	public static Connection getConnection(String jdbcUrl, String userName, String password, String driver) throws SQLException, ImproperValidationException {
		try {
			Class.forName(driver);
			Connection connection = DriverManager.getConnection(jdbcUrl, userName, password);
			return connection;
		} catch (ClassNotFoundException exception) {
			throw new ImproperValidationException(exception);
		}
	}
}

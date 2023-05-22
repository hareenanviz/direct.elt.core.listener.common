package com.anvizent.elt.core.listener.common.connection.factory;

import java.util.concurrent.TimeoutException;

import com.anvizent.elt.core.listener.common.connection.ArangoDBConnection;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDB.Builder;

/**
 * @author Hareen Bejjanki
 *
 */
public class ArangoDBConnectionFactory {

	public static ArangoDB createConnection(ArangoDBConnection arangoDBConnection, int index) throws TimeoutException {

		Builder builder = new ArangoDB.Builder();

		int hostIndex = index % arangoDBConnection.getHost().size();
		int portIndex = index % arangoDBConnection.getPortNumber().size();
		if (arangoDBConnection.getHost() != null && !arangoDBConnection.getHost().isEmpty() && arangoDBConnection.getPortNumber() != null
		        && !arangoDBConnection.getPortNumber().isEmpty()) {
			builder = builder.host(arangoDBConnection.getHost().get(hostIndex), arangoDBConnection.getPortNumber().get(portIndex));
		}

		if (arangoDBConnection.getUserName() != null && !arangoDBConnection.getUserName().isEmpty()) {
			builder = builder.user(arangoDBConnection.getUserName());
		}

		if (arangoDBConnection.getPassword() != null && !arangoDBConnection.getPassword().isEmpty()) {
			builder = builder.password(arangoDBConnection.getPassword());
		}

		if (arangoDBConnection.getTimeout() >= 0) {
			builder = builder.timeout(arangoDBConnection.getTimeout());
		}

		return builder.build();
	}
}
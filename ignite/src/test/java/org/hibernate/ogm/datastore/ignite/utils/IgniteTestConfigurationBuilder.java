/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.utils;

import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.hibernate.ogm.datastore.ignite.IgniteConfigurationBuilder;

/**
 * Ignite cache configuration for tests
 *
 * @author Victor Kadachigov
 */
public class IgniteTestConfigurationBuilder implements IgniteConfigurationBuilder {

	@Override
	public IgniteConfiguration build() {
		//disable check for new versions
		System.setProperty( IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, Boolean.FALSE.toString() );
		//disable show Ignite logo
		System.setProperty( IgniteSystemProperties.IGNITE_NO_ASCII, Boolean.TRUE.toString() );
		return createConfig();
	}

	private IgniteConfiguration createConfig() {
		IgniteConfiguration config = new IgniteConfiguration();
		BinaryConfiguration binaryConfiguration = new BinaryConfiguration();
		binaryConfiguration.setCompactFooter( false );		// it is necessary only for embedded collections (@ElementCollection)
		config.setBinaryConfiguration( binaryConfiguration );
		TransactionConfiguration transactionConfiguration = new TransactionConfiguration();
		transactionConfiguration.setDefaultTxConcurrency( TransactionConcurrency.PESSIMISTIC );
		transactionConfiguration.setDefaultTxIsolation( TransactionIsolation.READ_COMMITTED );
		config.setTransactionConfiguration( transactionConfiguration );
		return config;
	}
}

/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.test.cfg;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.fest.assertions.Assertions;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.ogm.OgmSessionFactory;
import org.hibernate.ogm.boot.OgmSessionFactoryBuilder;
import org.hibernate.ogm.cfg.OgmProperties;
import org.hibernate.ogm.datastore.ignite.IgniteProperties;
import org.junit.Assert;
import org.junit.Test;

/**
 * Start Ignite dialect with different sets of initial parameters
 *
 * @author Victor Kadachigov
 */
public class ConfigProperiesTest {

	private static final String INSTANCE_NAME = "MyLittleGrid";

	/**
	 * Connect to existing Ignite instance.
	 */
	@Test
	public void startWithInstanceAndConfigBuilder() {
		IgniteConfiguration config = createConfig();
		try ( Ignite ignite = Ignition.start( config ) ) {

			StandardServiceRegistry registry = new StandardServiceRegistryBuilder()
													.applySetting( OgmProperties.ENABLED, true )
													.applySetting( AvailableSettings.TRANSACTION_COORDINATOR_STRATEGY, "jta" )
													.applySetting( AvailableSettings.JTA_PLATFORM, "JBossTS" )
													.applySetting( OgmProperties.DATASTORE_PROVIDER, "org.hibernate.ogm.datastore.ignite.impl.IgniteDatastoreProvider" )
													.applySetting( IgniteProperties.IGNITE_INSTANCE_NAME, INSTANCE_NAME )
													.applySetting( IgniteProperties.CONFIGURATION_CLASS_NAME, "org.hibernate.ogm.datastore.ignite.test.cfg.MyTinyGridConfigBuilder" )
													.build();

			try ( OgmSessionFactory sessionFactory = new MetadataSources( registry )
															.buildMetadata()
															.getSessionFactoryBuilder()
															.unwrap( OgmSessionFactoryBuilder.class )
															.build();
					) {
				Assertions.assertThat( Ignition.allGrids() ).hasSize( 1 );
				Assert.assertEquals( INSTANCE_NAME, Ignition.allGrids().get( 0 ).name() );
			}
		}
	}

	@Test
	public void startWithConfigBuilder() {
		StandardServiceRegistry registry = new StandardServiceRegistryBuilder()
													.applySetting( OgmProperties.ENABLED, true )
													.applySetting( AvailableSettings.TRANSACTION_COORDINATOR_STRATEGY, "jta" )
													.applySetting( AvailableSettings.JTA_PLATFORM, "JBossTS" )
													.applySetting( OgmProperties.DATASTORE_PROVIDER, "org.hibernate.ogm.datastore.ignite.impl.IgniteDatastoreProvider" )
													.applySetting( IgniteProperties.CONFIGURATION_CLASS_NAME, "org.hibernate.ogm.datastore.ignite.test.cfg.MyTinyGridConfigBuilder" )
													.build();

		try ( OgmSessionFactory sessionFactory = new MetadataSources( registry )
														.buildMetadata()
														.getSessionFactoryBuilder()
														.unwrap( OgmSessionFactoryBuilder.class )
														.build();
		) {
		Assertions.assertThat( Ignition.allGrids() ).hasSize( 1 );
		Assert.assertEquals( MyTinyGridConfigBuilder.GRID_NAME, Ignition.allGrids().get( 0 ).name() );
		}

	}

	private IgniteConfiguration createConfig() {
		IgniteConfiguration result = new IgniteConfiguration();
		result.setIgniteInstanceName( INSTANCE_NAME );
		return result;
	}
}

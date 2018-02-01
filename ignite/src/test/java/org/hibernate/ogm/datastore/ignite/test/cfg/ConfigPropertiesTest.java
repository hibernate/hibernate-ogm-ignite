/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.test.cfg;

import static org.fest.assertions.Assertions.assertThat;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.ogm.OgmSessionFactory;
import org.hibernate.ogm.boot.OgmSessionFactoryBuilder;
import org.hibernate.ogm.cfg.OgmProperties;
import org.hibernate.ogm.datastore.ignite.IgniteProperties;
import org.hibernate.ogm.datastore.ignite.impl.IgniteDatastoreProvider;
import org.junit.Test;

/**
 * Start Ignite dialect with different sets of initial parameters
 *
 * @author Victor Kadachigov
 */
public class ConfigPropertiesTest {

	private static final String CUSTOM_GRID_NAME = "AnotherGridName";

	@Test
	public void testGridNameOverrideConfigBuilder() {
		IgniteConfiguration config = createConfig( CUSTOM_GRID_NAME );
		try ( Ignite ignite = Ignition.start( config ) ) {

			StandardServiceRegistry registry = registryBuilder()
					.applySetting( IgniteProperties.CONFIGURATION_CLASS_NAME, MyTinyGridConfigBuilder.class.getName() )
					.applySetting( IgniteProperties.IGNITE_INSTANCE_NAME, CUSTOM_GRID_NAME )
					.build();

			try ( OgmSessionFactory sessionFactory = createFactory( registry ) ) {
				assertThat( Ignition.allGrids() ).hasSize( 1 );
				assertThat( Ignition.allGrids().get( 0 ).name() ).isEqualTo( CUSTOM_GRID_NAME );
			}
		}
	}

	@Test
	public void testConfigBuilder() {
		StandardServiceRegistry registry = registryBuilder()
				.applySetting( IgniteProperties.CONFIGURATION_CLASS_NAME, MyTinyGridConfigBuilder.class.getName() )
				.build();

		try ( OgmSessionFactory sessionFactory = createFactory( registry ) ) {

			assertThat( Ignition.allGrids() ).hasSize( 1 );
			assertThat( Ignition.allGrids().get( 0 ).name() ).isEqualTo( MyTinyGridConfigBuilder.GRID_NAME );
		}
	}

	private OgmSessionFactory createFactory(StandardServiceRegistry registry) {
		return new MetadataSources( registry )
				.buildMetadata()
				.getSessionFactoryBuilder()
				.unwrap( OgmSessionFactoryBuilder.class )
				.build();
	}

	private StandardServiceRegistryBuilder registryBuilder() {
		return new StandardServiceRegistryBuilder()
				.applySetting( OgmProperties.ENABLED, true )
				.applySetting( AvailableSettings.TRANSACTION_COORDINATOR_STRATEGY, "jta" )
				.applySetting( AvailableSettings.JTA_PLATFORM, "JBossTS" )
				.applySetting( OgmProperties.DATASTORE_PROVIDER, IgniteDatastoreProvider.class.getName() );
	}

	private IgniteConfiguration createConfig(String gridName) {
		IgniteConfiguration result = new IgniteConfiguration();
		result.setIgniteInstanceName( gridName );
		return result;
	}
}

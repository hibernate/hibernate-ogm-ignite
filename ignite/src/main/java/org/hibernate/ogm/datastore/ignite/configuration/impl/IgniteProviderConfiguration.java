/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.configuration.impl;

import java.net.URL;
import java.util.Map;

import org.hibernate.boot.registry.classloading.spi.ClassLoaderService;
import org.hibernate.ogm.datastore.ignite.IgniteConfigurationBuilder;
import org.hibernate.ogm.datastore.ignite.IgniteProperties;
import org.hibernate.ogm.datastore.ignite.impl.IgniteDatastoreProvider;
import org.hibernate.ogm.util.configurationreader.spi.ConfigurationPropertyReader;

/**
 * Configuration for {@link IgniteDatastoreProvider}.
 * @author Dmitriy Kozlov
 * @author Victor Kadachigov
 */
public class IgniteProviderConfiguration {

	/**
	 * Name of the default Ignite configuration file
	 */
	private static final String DEFAULT_CONFIG = "ignite-config.xml";

	private URL url;
	private String instanceName;
	private IgniteConfigurationBuilder configBuilder;

	/**
	 * Initialize the internal values from the given {@link Map}.
	 *
	 * @param configurationMap The values to use as configuration
	 */
	public void initialize(Map configurationMap, ClassLoaderService classLoaderService) {
		ConfigurationPropertyReader configurationPropertyReader = new ConfigurationPropertyReader( configurationMap, classLoaderService );

		this.url = configurationPropertyReader
			.property( IgniteProperties.CONFIGURATION_RESOURCE_NAME, URL.class )
			.withDefault( IgniteProviderConfiguration.class.getClassLoader().getResource( DEFAULT_CONFIG ) )
			.getValue();

		String configBuilderClassName = configurationPropertyReader
				.property( IgniteProperties.CONFIGURATION_CLASS_NAME, String.class )
				.getValue();

		if ( configBuilderClassName != null ) {
			this.configBuilder = configurationPropertyReader
					.property( IgniteProperties.CONFIGURATION_CLASS_NAME, IgniteConfigurationBuilder.class )
					.instantiate()
					.getValue();
		}

		this.instanceName = configurationPropertyReader
				.property( IgniteProperties.IGNITE_INSTANCE_NAME, String.class )
				.getValue();
	}

	/**
	 * @see IgniteProperties#CONFIGURATION_RESOURCE_NAME
	 * @return An URL to Ignite configuration file
	 */
	public URL getUrl() {
		return url;
	}

	/**
	 * @see IgniteProperties#IGNITE_INSTANCE_NAME
	 * @return the name of existing Ignite instance
	 */
	public String getInstanceName() {
		return instanceName;
	}

	public IgniteConfigurationBuilder getConfigBuilder() {
		return configBuilder;
	}

}

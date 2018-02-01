/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.test.cfg;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.hibernate.ogm.datastore.ignite.IgniteConfigurationBuilder;

/**
 * @author Victor Kadachigov
 */
public class MyTinyGridConfigBuilder implements IgniteConfigurationBuilder {

	public static final String GRID_NAME = "MyTinyGrid";

	@Override
	public IgniteConfiguration build() {
		IgniteConfiguration config = new IgniteConfiguration();
		config.setIgniteInstanceName( GRID_NAME );
		return config;
	}

}

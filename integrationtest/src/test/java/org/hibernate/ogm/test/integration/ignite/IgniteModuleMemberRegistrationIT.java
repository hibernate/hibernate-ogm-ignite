/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.test.integration.ignite;

import javax.inject.Inject;

import org.hibernate.ogm.cfg.OgmProperties;
import org.hibernate.ogm.datastore.ignite.Ignite;
import org.hibernate.ogm.datastore.ignite.IgniteConfigurationBuilder;
import org.hibernate.ogm.datastore.ignite.IgniteProperties;
import org.hibernate.ogm.test.integration.ignite.errorhandler.TestErrorHandler;
import org.hibernate.ogm.test.integration.ignite.model.EmailAddress;
import org.hibernate.ogm.test.integration.ignite.model.PhoneNumber;
import org.hibernate.ogm.test.integration.ignite.service.ContactManagementService;
import org.hibernate.ogm.test.integration.ignite.service.PhoneNumberService;
import org.hibernate.ogm.test.integration.testcase.ModuleMemberRegistrationScenario;
import org.hibernate.ogm.test.integration.testcase.util.ModuleMemberRegistrationDeployment;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.descriptor.api.Descriptors;
import org.jboss.shrinkwrap.descriptor.api.persistence20.PersistenceDescriptor;
import org.jboss.shrinkwrap.descriptor.api.persistence20.PersistenceUnit;
import org.jboss.shrinkwrap.descriptor.api.persistence20.Properties;

import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.binary.BinaryBasicNameMapper;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * Test for the Hibernate OGM module in WildFly using Apache Ignite
 *
 * @author Sergey Chernolyas &amp;sergey_chernolyas@gmail.com&amp;
 */
@RunWith(Arquillian.class)
public class IgniteModuleMemberRegistrationIT extends ModuleMemberRegistrationScenario {

	@Inject
	private PhoneNumberService phoneNumberService;

	@Inject
	private ContactManagementService contactManager;

	@Deployment
	public static Archive<?> createTestArchive() {
		return new ModuleMemberRegistrationDeployment
				.Builder( IgniteModuleMemberRegistrationIT.class )
				.addClasses( PhoneNumber.class, PhoneNumberService.class, EmailAddress.class, ContactManagementService.class, TestErrorHandler.class )
				.persistenceXml( persistenceXml() )
				.manifestDependencies( "org.hibernate.ogm:${hibernate-ogm.module.slot} services, org.hibernate.ogm.ignite:${hibernate-ogm.module.slot} services, org.apache.ignite:2.3.0 export" )
				.createDeployment();
	}

	private static PersistenceDescriptor persistenceXml() {

		Properties<PersistenceUnit<PersistenceDescriptor>> propertiesContext = Descriptors.create( PersistenceDescriptor.class )
				.version( "2.0" )
				.createPersistenceUnit()
				.name( "primary" )
				.provider( "org.hibernate.ogm.jpa.HibernateOgmPersistence" )
				.getOrCreateProperties();

		return propertiesContext
				.createProperty().name( OgmProperties.DATASTORE_PROVIDER ).value( Ignite.DATASTORE_PROVIDER_NAME ).up()
				.createProperty().name( IgniteProperties.CONFIGURATION_CLASS_NAME ).value( IgniteTestConfigurationBuilder.class.getName() ).up()
				.createProperty().name( OgmProperties.ERROR_HANDLER ).value( TestErrorHandler.class.getName() ).up()
				.createProperty().name( "hibernate.search.default.directory_provider" ).value( "ram" ).up()
				.createProperty().name( "wildfly.jpa.hibernate.search.module" ).value( "org.hibernate.search.orm:${hibernate-search.module.slot}" ).up()
				.up().up();
	}



	@Test
	public void shouldFindPersistedPhoneByIdWithNativeQuery() throws Exception {
		PhoneNumber phoneNumber = phoneNumberService.createPhoneNumber( "name1","1112233" );
		phoneNumberService.getPhoneNumber( "name1" );
	}

	public static class IgniteTestConfigurationBuilder implements IgniteConfigurationBuilder {

		@Override
		public IgniteConfiguration build() {
			// disable check for new versions
			System.setProperty( IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, Boolean.FALSE.toString() );
			// disable show Ignite logo
			System.setProperty( IgniteSystemProperties.IGNITE_NO_ASCII, Boolean.TRUE.toString() );

			return createConfig();
		}

		private IgniteConfiguration createConfig() {
			IgniteConfiguration config = new IgniteConfiguration();
			config.setIgniteInstanceName( "OgmTestGrid" );
			config.setClientMode( false );
			BinaryConfiguration binaryConfiguration = new BinaryConfiguration();
			binaryConfiguration.setNameMapper( new BinaryBasicNameMapper( true ) );
			binaryConfiguration.setCompactFooter( false ); // it is necessary only for embedded collections (@ElementCollection)
			config.setBinaryConfiguration( binaryConfiguration );
			TransactionConfiguration transactionConfiguration = new TransactionConfiguration();
			// I'm going to use PESSIMISTIC here because some people had problem with it and it would be nice if tests
			// can highlight the issue. Ideally, we would want to test the different concurrency and isolation level.
			transactionConfiguration.setDefaultTxConcurrency( TransactionConcurrency.PESSIMISTIC );
			transactionConfiguration.setDefaultTxIsolation( TransactionIsolation.READ_COMMITTED );
			config.setTransactionConfiguration( transactionConfiguration );

			return config;
		}
	}
}

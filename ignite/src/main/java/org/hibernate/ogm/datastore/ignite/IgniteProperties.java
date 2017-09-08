/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite;

import org.hibernate.ogm.datastore.keyvalue.cfg.KeyValueStoreProperties;

import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * Properties for configuring the Ignite datastore
 *
 * @author Dmitriy Kozlov
 *
 */
public final class IgniteProperties implements KeyValueStoreProperties {

	/**
	 * Configuration property for specifying the name of the Ignite configuration file
	 */
	public static final String CONFIGURATION_RESOURCE_NAME = "hibernate.ogm.ignite.configuration_resource_name";
	/**
	 * Configuration property for specifying class name. Class must implements {@link IgniteConfigurationBuilder}
	 */
	public static final String CONFIGURATION_CLASS_NAME = "hibernate.ogm.ignite.configuration_class_name";
	/**
	 * Configuration property for specifying the name existing Ignite instance
	 */
	public static final String IGNITE_INSTANCE_NAME = "hibernate.ogm.ignite.instance_name";

	/**
	 * Configuration property templates for manage read-through and write-through for each entity.
	 * Example of full string "hibernate.ogm.ignite.persistent_store.Person.read_through=true"
	 * @see <a href="https://apacheignite.readme.io/docs/persistent-store#read-through-and-write-through">Persistent Store</a>
	 */

	public static final String IGNITE_CACHE_STORE_CLASS_TEMPLATE = "hibernate.ogm.ignite.persistent_store.%s.cache_store_class";
	public static final String IGNITE_CACHE_STORE_FACTORY_TEMPLATE = "hibernate.ogm.ignite.persistent_store.%s.cache_store_factory";

	public static final String IGNITE_ALLOWS_TRANSACTION_EMULATION = "hibernate.ogm.ignite.allows_transaction_emulation";
	/**
	 * Configuration property 'concurrency' for {@link org.apache.ignite.IgniteTransactions#txStart(TransactionConcurrency, TransactionIsolation, long, int)}
	 * @see org.apache.ignite.transactions.TransactionConcurrency
	 */
	public static final String IGNITE_TRANSACTION_CONCURRENCY = "hibernate.ogm.ignite.transaction.concurrency";
	/**
	 * Configuration property 'isolation' for {@link org.apache.ignite.IgniteTransactions#txStart(TransactionConcurrency, TransactionIsolation, long, int)}
	 * @see org.apache.ignite.transactions.TransactionIsolation
	 */
	public static final String IGNITE_TRANSACTION_ISOLATION = "hibernate.ogm.ignite.transaction.isolation";
	/**
	 * Configuration property 'timeout' for {@link org.apache.ignite.IgniteTransactions#txStart(TransactionConcurrency, TransactionIsolation, long, int)}
	 */
	public static final String IGNITE_TRANSACTION_TIMEOUT = "hibernate.ogm.ignite.transaction.timeout";
	/**
	 * Configuration property 'txsize' for {@link org.apache.ignite.IgniteTransactions#txStart(TransactionConcurrency, TransactionIsolation, long, int)}
	 */
	public static final String IGNITE_TRANSACTION_TXSIZE = "hibernate.ogm.ignite.transaction.txsize";
	/**
	 * Configuration property 'support_persistence' activate Ignite Native Persistence feature
	 * @see <a href="https://apacheignite.readme.io/docs/distributed-persistent-store">Ignite Native Persistence</a>
	 * */
	public static final String IGNITE_SUPPORT_PERSISTENCE = "hibernate.ogm.ignite.support_persistence";
	public static final String IGNITE_WORK_DIRECTORY = "hibernate.ogm.ignite.work.directory.path";

	private IgniteProperties() {

	}

}

/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.transaction.impl;

import org.hibernate.ogm.datastore.ignite.IgniteProperties;
import org.hibernate.ogm.datastore.ignite.impl.IgniteDatastoreProvider;
import org.hibernate.ogm.datastore.ignite.logging.impl.Log;
import org.hibernate.ogm.datastore.ignite.logging.impl.LoggerFactory;
import org.hibernate.ogm.transaction.impl.ForwardingTransactionCoordinator;
import org.hibernate.ogm.transaction.impl.ForwardingTransactionDriver;
import org.hibernate.ogm.util.configurationreader.spi.ConfigurationPropertyReader;
import org.hibernate.resource.transaction.TransactionCoordinator;

import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * Coordinator for local transactions
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class IgniteLocalTransactionCoordinator extends ForwardingTransactionCoordinator {

	private static Log log = LoggerFactory.getLogger();
	private final IgniteDatastoreProvider datastoreProvider;
	private final TransactionConcurrency concurrency;
	private final TransactionIsolation isolation;
	private final long timeout;
	private final int txSize;

	private IgniteTransactions igniteTransactions;

	/**
	 * Constructor
	 *
	 * @param delegate transaction coordinator delegate
	 * @param datastoreProvider Ignite Datastore provider
	 */
	public IgniteLocalTransactionCoordinator(
			TransactionCoordinator delegate,
			IgniteDatastoreProvider datastoreProvider) {
		super( delegate );
		this.datastoreProvider = datastoreProvider;
		this.igniteTransactions = datastoreProvider.getCacheManager().transactions();

		ConfigurationPropertyReader propertyReader = datastoreProvider.getPropertyReader();
		this.concurrency = propertyReader.property( IgniteProperties.IGNITE_TRANSACTION_CONCURRENCY,
				TransactionConcurrency.class ).withDefault( TransactionConfiguration.DFLT_TX_CONCURRENCY ).getValue();
		this.isolation = propertyReader.property( IgniteProperties.IGNITE_TRANSACTION_ISOLATION, TransactionIsolation.class )
				.withDefault( TransactionConfiguration.DFLT_TX_ISOLATION ).getValue();
		this.timeout = propertyReader.property( IgniteProperties.IGNITE_TRANSACTION_TIMEOUT, Long.class )
				.withDefault( TransactionConfiguration.DFLT_TRANSACTION_TIMEOUT ).getValue();
		this.txSize = propertyReader.property( IgniteProperties.IGNITE_TRANSACTION_TXSIZE, Integer.class ).withDefault( 0 ).getValue();
	}

	@Override
	public TransactionDriver getTransactionDriverControl() {
		TransactionDriver driver = super.getTransactionDriverControl();
		return new IgniteTransactionDriver( driver );
	}

	private void success() {
		if ( igniteTransactions.tx() != null ) {
			log.debugf( "commit transaction (Id: %s)", igniteTransactions.tx().xid() );
			igniteTransactions.tx().commit();
			close();
		}
		else {
			log.warnf( "No transactions for thread id %s ! ", Thread.currentThread().getId() );
		}
	}

	private void failure() {
		if ( igniteTransactions.tx() != null ) {
			log.debugf( "rollback transaction (Id: %s)", igniteTransactions.tx().xid() );
			igniteTransactions.tx().rollback();
			close();
		}
		else {
			log.warnf( "No transactions for thread id %s ! ", Thread.currentThread().getId() );
		}
	}

	private void close() {
		if ( igniteTransactions.tx() != null ) {
			log.debugf( "close transaction %s", igniteTransactions.tx().xid() );
			igniteTransactions.tx().close();
		}
	}

	private class IgniteTransactionDriver extends ForwardingTransactionDriver {

		public IgniteTransactionDriver(TransactionDriver delegate) {
			super( delegate );
		}

		@Override
		public void begin() {
			Transaction currentIgniteTransaction = igniteTransactions.txStart( concurrency, isolation, timeout, txSize );
			log.debugf( "begin new transaction: %s", currentIgniteTransaction );
			super.begin();
		}

		@Override
		public void commit() {
			try {
				if ( igniteTransactions.tx() != null ) {
					log.debugf( "commit transaction with ID %s ", igniteTransactions.tx().xid() );
					super.commit();
					success();
				}
				else {
					log.warnf( "No transactions for thread id %s ! ", Thread.currentThread().getId() );
					// @todo ... Throw exception?
				}
			}
			catch (Exception e) {
				log.error( "Cannot commit transaction!", e );
				try {
					rollback();
				}
				catch (Exception re) {
				}
				throw e;
			}
		}

		@Override
		public void rollback() {
			try {
				if ( igniteTransactions.tx() != null ) {
					log.debugf( "rollback transaction with ID %s ", igniteTransactions.tx().xid() );
					super.rollback();
				}
			}
			finally {
				failure();
			}
		}
	}

}

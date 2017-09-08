/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.transaction.impl;

import org.hibernate.ConnectionAcquisitionMode;
import org.hibernate.ConnectionReleaseMode;
import org.hibernate.ogm.datastore.ignite.impl.IgniteDatastoreProvider;
import org.hibernate.resource.transaction.TransactionCoordinator;
import org.hibernate.resource.transaction.TransactionCoordinatorBuilder;
import org.hibernate.resource.transaction.spi.TransactionCoordinatorOwner;

/**
 * @author Sergey Chernolyas &amp;sergey_chernolyas@gmail.com&amp;
 */
public class IgniteTransactionCoordinatorBuilder implements TransactionCoordinatorBuilder {
	private final TransactionCoordinatorBuilder delegate;
	private final IgniteDatastoreProvider datastoreProvider;

	/**
	 * @param delegate transaction delegate
	 * @param datastoreProvider instance of IgniteDatastoreProvider
	 */
	public IgniteTransactionCoordinatorBuilder(
			TransactionCoordinatorBuilder delegate, IgniteDatastoreProvider datastoreProvider) {
		super();
		this.delegate = delegate;
		this.datastoreProvider = datastoreProvider;
	}

	@Override
	public TransactionCoordinator buildTransactionCoordinator(
			TransactionCoordinatorOwner owner,
			TransactionCoordinatorOptions options) {
		TransactionCoordinator coordinator = delegate.buildTransactionCoordinator( owner, options );
		if ( delegate.isJta() ) {
			//return new IgniteJtaTransactionCoordinator( coordinator, datastoreProvider );
			//@todo add currect code of Victor
			throw new UnsupportedOperationException( "Not supported yet" );
		}
		else {


			return new IgniteLocalTransactionCoordinator( coordinator, datastoreProvider );
		}
	}

	@Override
	public boolean isJta() {
		return delegate.isJta();
	}

	@Override
	public ConnectionReleaseMode getDefaultConnectionReleaseMode() {
		return ConnectionReleaseMode.AFTER_TRANSACTION;
	}

	@Override
	public ConnectionAcquisitionMode getDefaultConnectionAcquisitionMode() {
		return ConnectionAcquisitionMode.IMMEDIATELY;
	}
}

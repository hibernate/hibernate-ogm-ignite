/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.transaction.impl;

import org.hibernate.ogm.datastore.ignite.impl.IgniteDatastoreProvider;
import org.hibernate.ogm.transaction.impl.ForwardingTransactionCoordinator;
import org.hibernate.resource.transaction.TransactionCoordinator;

/**
 * Coordinator for JTA transctions
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 * @see <a href="https://apacheignite.readme.io/docs/transactions#section-integration-with-jta">Integration With JTA</a>
 */
public class IgniteJtaTransactionCoordinator extends ForwardingTransactionCoordinator {

	private final IgniteDatastoreProvider datastoreProvider;

	/**
	 * Constructor
	 *
	 * @param delegate transaction coordinator delegate
	 * @param datastoreProvider Ignite Datastore provider
	 */
	public IgniteJtaTransactionCoordinator(TransactionCoordinator delegate, IgniteDatastoreProvider datastoreProvider) {
		super( delegate );
		this.datastoreProvider = datastoreProvider;
	}

	@Override
	public void explicitJoin() {
		throw new UnsupportedOperationException( "Emulation of JTA is not supported!" );
	}

	@Override
	public void pulse() {
		throw new UnsupportedOperationException( "Emulation of JTA is not supported!" );
	}
}

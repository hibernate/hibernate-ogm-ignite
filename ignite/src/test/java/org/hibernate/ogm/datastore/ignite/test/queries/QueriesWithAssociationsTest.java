/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.test.queries;

import java.util.List;

import javax.persistence.EntityManager;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Copied from {@link org.hibernate.ogm.backendtck.queries.QueriesWithAssociationsTest} but Ignite queries are not
 * transactional so we need another algorithm in {@code removeEntities()}
 *
 * @author Victor Kadachigov
 */
public class QueriesWithAssociationsTest extends org.hibernate.ogm.backendtck.queries.QueriesWithAssociationsTest {

	@Override
	protected void removeEntities() throws Exception {
		EntityManager em = getFactory().createEntityManager();
		for ( Class<?> each : getAnnotatedClasses() ) {
			em.getTransaction().begin();
			List<?> entities = em.createQuery( "FROM " + each.getSimpleName() ).getResultList();
			for ( Object object : entities ) {
				em.remove( object );
			}
			em.getTransaction().commit();
		}
		em.close();
	}

	@Test
	@Override
	@Ignore
	public void testGetWithJoinOnAssociations() throws Exception {
		// Ignite does not support JOIN right now
	}
}

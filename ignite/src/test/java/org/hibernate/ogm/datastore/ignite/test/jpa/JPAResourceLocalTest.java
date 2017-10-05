/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.test.jpa;

import static org.fest.assertions.Assertions.assertThat;
import static org.hibernate.ogm.utils.TestHelper.dropSchemaAndDatabase;

import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;

import org.apache.ignite.binary.BinaryObject;
import org.hibernate.ogm.backendtck.jpa.Poem;
import org.hibernate.ogm.datastore.ignite.utils.IgniteTestHelper;
import org.hibernate.ogm.utils.PackagingRule;
import org.hibernate.ogm.utils.RequiresTransactionalCapabilitiesRule;
import org.hibernate.ogm.utils.TestHelper;
import org.junit.Rule;
import org.junit.Test;

/**
 * This mimic {@link org.hibernate.ogm.backendtck.jpa.JPAResourceLocalTest} in core.
 * <p>
 * We look for the single entities because we cannot rely on the size operation outside the transaction.
 * <p>
 * The size operation in Ignite is not transactional, meaning that if you check how many entities
 * there are in the cache before committing you will have unreliable results.
 *
 * @see org.hibernate.ogm.backendtck.jpa.JPAResourceLocalTest
 *
 * @author Davide D'Alto
 */
public class JPAResourceLocalTest {

	@Rule
	public PackagingRule packaging = new PackagingRule( "persistencexml/transaction-type-resource-local.xml", Poem.class );

	@Rule
	public RequiresTransactionalCapabilitiesRule transactions = new RequiresTransactionalCapabilitiesRule();

	@Test
	public void testBootstrapAndCRUD() throws Exception {
		final EntityManagerFactory emf = Persistence.createEntityManagerFactory( "transaction-type-resource-local", TestHelper.getDefaultTestSettings() );
		try {

			final EntityManager em = emf.createEntityManager();
			try {
				Poem albatrosPoem = null;
				em.getTransaction().begin();
				albatrosPoem = new Poem();
				albatrosPoem.setName( "L'albatros" );
				em.persist( albatrosPoem );
				em.getTransaction().commit();

				em.clear();

				Poem wazaaPoem = null;
				em.getTransaction().begin();
				wazaaPoem = new Poem();
				wazaaPoem.setName( "Wazaaaaa" );
				em.persist( wazaaPoem );
				em.flush();
				{
					Map<String, BinaryObject> result = IgniteTestHelper.find( em, Poem.class, albatrosPoem.getId(), wazaaPoem.getId() );
					assertThat( result.size() ).isEqualTo( 2 );
					assertThatCacheContainsEntity( albatrosPoem, result );
					assertThatCacheContainsEntity( wazaaPoem, result );
				}
				em.getTransaction().rollback();

				Map<String, BinaryObject> result = IgniteTestHelper.find( em, Poem.class, albatrosPoem.getId(), wazaaPoem.getId() );
				assertThat( result.size() ).isEqualTo( 1 );
				assertThatCacheContainsEntity( albatrosPoem, result );

				em.getTransaction().begin();
				albatrosPoem = em.find( Poem.class, albatrosPoem.getId() );
				assertThat( albatrosPoem ).isNotNull();
				assertThat( albatrosPoem.getName() ).isEqualTo( "L'albatros" );
				em.remove( albatrosPoem );
				wazaaPoem = em.find( Poem.class, wazaaPoem.getId() );
				assertThat( wazaaPoem ).isNull();
				em.getTransaction().commit();

			}
			finally {
				EntityTransaction transaction = em.getTransaction();
				if ( transaction != null && transaction.isActive() ) {
					transaction.rollback();
				}
				em.close();
			}
		}
		finally {
			dropSchemaAndDatabase( emf );
			emf.close();
		}
	}

	private void assertThatCacheContainsEntity(Poem entity, Map<String, BinaryObject> result) {
		assertThat( result.get( entity.getId() ) ).isNotNull();
		assertThat( (String) result.get( entity.getId() ).field( "name" ) ).isEqualTo( entity.getName() );
	}
}

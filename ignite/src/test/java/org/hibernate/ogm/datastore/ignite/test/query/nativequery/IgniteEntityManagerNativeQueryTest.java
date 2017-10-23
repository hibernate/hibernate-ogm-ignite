/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.test.query.nativequery;

import static org.fest.assertions.Assertions.assertThat;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.hibernate.ogm.utils.PackagingRule;
import org.hibernate.ogm.utils.jpa.OgmJpaTestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Sergey Chernolyas &amp;sergey_chernolyas@gmail.com&amp;
 * @see <a href="https://docs.jboss.org/hibernate/orm/5.1/userguide/html_single/chapters/query/native/Native.html">Native Queries</a>
 */
public class IgniteEntityManagerNativeQueryTest extends OgmJpaTestCase {

	@Rule
	public PackagingRule packaging = new PackagingRule( "persistencexml/ogm.xml", org.hibernate.ogm.backendtck.jpa.Poem.class );

	private final OscarWildePoem portia = new OscarWildePoem( 1L, "Portia", "Oscar Wilde" );
	private final OscarWildePoem athanasia = new OscarWildePoem( 2L, "Athanasia", "Oscar Wilde", (byte) 5 );
	private final Poet christian = new Poet( "christian", "Christian Abendsonne" );
	private final Poet james = new Poet( "james", "James Krass" );
	private final LiteratureSociety stencilClub = new LiteratureSociety( "stencil-club", "Stencil Club Germany", christian, james );
	private final Critic critic = new Critic( new CriticId( "de", "764" ), "Roger" );

	private EntityManager em;

	@Before
	public void init() throws Exception {
		// prepare test data
		em = createEntityManager();
		begin();
		em = persist( portia, athanasia, stencilClub, critic );
		commit();
		em.close();

		em = createEntityManager();
	}

	@After
	public void tearDown() throws Exception {
		rollback();
		begin();
		delete( portia, athanasia, stencilClub, critic );
		commit();
		close( em );
	}



	@Test
	public void testSingleResultQuery() throws Exception {
		begin();

		String nativeQuery = "select _key,_val from OscarWildePoem  where name='Portia' and author='Oscar Wilde'";
		OscarWildePoem poem = (OscarWildePoem) em.createNativeQuery( nativeQuery, OscarWildePoem.class ).getSingleResult();

		assertAreEquals( portia, poem );
		commit();
	}

	@Test
	public void testNamedQueryResultQuery() throws Exception {
		begin();

		OscarWildePoem poem = em.createNamedQuery( "PortiaOscarWildePoem", OscarWildePoem.class ).getSingleResult();
		assertAreEquals( portia, poem );

		commit();
	}
	@Test
	public void testNamedQueryResultQueryWithParameters() throws Exception {
		begin();

		Query poemQuery = em.createNamedQuery( "OscarWildePoemWithParameters", OscarWildePoem.class );
		poemQuery.setParameter( 1, "Portia" );
		poemQuery.setParameter( 2, "Oscar Wilde" );
		OscarWildePoem poem = (OscarWildePoem) poemQuery.getSingleResult();
		assertAreEquals( portia, poem );


		commit();
	}


	@Test
	public void testSingleResultQueryWithParameters() throws Exception {
		begin();
		String nativeQuery = "select _key,_val from OscarWildePoem  where name=?1 and author=?2";
		Query poemQuery = em.createNativeQuery( nativeQuery, OscarWildePoem.class );
		poemQuery.setParameter( 1, "Portia" );
		poemQuery.setParameter( 2, "Oscar Wilde" );
		OscarWildePoem poem = (OscarWildePoem) poemQuery.getSingleResult();

		assertAreEquals( portia, poem );

		commit();
	}


	private void assertAreEquals(OscarWildePoem expectedPoem, OscarWildePoem poem) {
		assertThat( poem ).isNotNull();
		assertThat( poem.getId() ).as( "Wrong Id" ).isEqualTo( expectedPoem.getId() );
		assertThat( poem.getName() ).as( "Wrong Name" ).isEqualTo( expectedPoem.getName() );
		assertThat( poem.getAuthor() ).as( "Wrong Author" ).isEqualTo( expectedPoem.getAuthor() );
	}

	private void close(EntityManager em) {
		em.clear();
		em.close();
	}

	private EntityManager persist(Object... entities) {
		for ( Object object : entities ) {
			em.persist( object );
		}
		return em;
	}

	private EntityManager delete(Object... entities) {
		for ( Object object : entities ) {
			Object entity = em.merge( object );
			em.remove( entity );
		}
		return em;
	}

	private void begin() throws Exception {
		em.getTransaction().begin();
	}

	private void commit() throws Exception {
		em.getTransaction().commit();
	}

	private void rollback() throws Exception {
		if ( em.getTransaction().isActive() ) {
			em.getTransaction().rollback();
		}
	}

	private EntityManager createEntityManager() {
		return getFactory().createEntityManager();
	}
	@Override
	public Class<?>[] getAnnotatedClasses() {
		return new Class<?>[] { OscarWildePoem.class, LiteratureSociety.class, Poet.class, Critic.class };
	}
}

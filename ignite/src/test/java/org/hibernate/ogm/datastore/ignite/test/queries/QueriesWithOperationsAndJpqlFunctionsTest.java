/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.test.queries;

import java.util.GregorianCalendar;
import java.util.List;

import javax.persistence.EntityManager;
import static org.fest.assertions.Assertions.assertThat;
import org.hibernate.ogm.backendtck.queries.parameters.Genre;
import org.hibernate.ogm.backendtck.queries.parameters.Movie;
import org.hibernate.ogm.utils.jpa.OgmJpaTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * <p>Created on 25.01.2018
 *
 * @author Aliaksandr Salauyou (sbt-solovev-an@mail.ca.sbrf.ru)
 */
public class QueriesWithOperationsAndJpqlFunctionsTest extends OgmJpaTestCase {

	@Test
	public void testUpperLower() {
		EntityManager em = getFactory().createEntityManager();
		em.getTransaction().begin();

		List<Movie> movies = em.createQuery( "from Movie where upper(title) = :title", Movie.class )
			.setParameter( "title", "FRONT DOOR" )
			.getResultList();
		assertThat( movies ).onProperty( "title" ).containsOnly( "Front Door" );

		movies = em.createQuery( "from Movie m where lower(m.title) = :title", Movie.class )
			.setParameter( "title", "barnie" )
			.getResultList();
		assertThat( movies ).onProperty( "title" ).containsOnly( "Barnie" );

		em.getTransaction().commit();
		em.close();
	}


	@Test
	public void testConcat() {
		EntityManager em = getFactory().createEntityManager();
		em.getTransaction().begin();
		List<Movie> movies;

		movies = em.createQuery( "from Movie where concat(title, ' 2') = :partTwo", Movie.class )
			.setParameter( "partTwo", "Front Door 2" )
			.getResultList();
		assertThat( movies ).onProperty( "title" ).containsOnly( "Front Door" );

		movies = em.createQuery( "from Movie m where concat(m.id, ' : ', m.title) = :idTitle", Movie.class )
			.setParameter( "idTitle", "movie-4 : Barnie" )
			.getResultList();
		assertThat( movies ).onProperty( "id" ).containsOnly( "movie-4" );

		em.getTransaction().commit();
		em.close();
	}

	@Test
	public void testWithNestedFunction() {
		EntityManager em = getFactory().createEntityManager();
		em.getTransaction().begin();
		List<Movie> movies;

		movies = em.createQuery( "from Movie where upper(concat(title, concat('/', id))) = concat('BARNIE', '/', 'MOVIE-4')", Movie.class )
			.getResultList();
		assertThat( movies ).onProperty( "id" ).containsOnly( "movie-4" );

		em.getTransaction().commit();
		em.close();
	}

	@Test
	public void testWithArithmeticOperations() {
		List<Movie> movies;
		EntityManager em = getFactory().createEntityManager();
		em.getTransaction().begin();

		movies = em.createQuery( "from Movie where -viewerRating < -8 ", Movie.class ).getResultList();
		assertThat( movies ).onProperty( "viewerRating" ).containsOnly( (byte) 9 );

		movies = em.createQuery( "from Movie m where m.viewerRating - 1 + -2 = 3 + 4 * 5 / 10", Movie.class ).getResultList();
		assertThat( movies ).onProperty( "viewerRating" ).containsOnly( (byte) 8 );

		em.getTransaction().commit();
		em.close();
	}

	@Before
	public void populateDb() {
		EntityManager entityManager = getFactory().createEntityManager();
		entityManager.getTransaction().begin();
		entityManager.persist( new Movie( "movie-1", Genre.COMEDY, "To thatch a roof", true, ( new GregorianCalendar( 1955, 5, 10 ) ).getTime(), (byte) 8 ) );
		entityManager.persist( new Movie( "movie-2", Genre.THRILLER, "South by Southeast", true, ( new GregorianCalendar( 1958, 3, 2 ) ).getTime(), (byte) 9 ) );
		entityManager.persist( new Movie( "movie-3", Genre.THRILLER, "Front Door", false, ( new GregorianCalendar( 1961, 2, 23 ) ).getTime(), (byte) 7 ) );
		entityManager.persist( new Movie( "movie-4", Genre.DRAMA, "Barnie", false, ( new GregorianCalendar( 1962, 11, 2 ) ).getTime(), (byte) 7 ) );
		entityManager.getTransaction().commit();
		entityManager.close();
	}

	@After
	public void removeTestEntities() throws Exception {
		removeEntities();
	}

	protected Class<?>[] getAnnotatedClasses() {
		return new Class[]{ Movie.class };
	}
}

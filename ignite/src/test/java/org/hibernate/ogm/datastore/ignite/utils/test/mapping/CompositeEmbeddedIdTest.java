/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.utils.test.mapping;

import java.util.Map;

import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.ogm.datastore.ignite.IgniteProperties;
import org.hibernate.ogm.exception.EntityAlreadyExistsException;
import org.hibernate.ogm.utils.OgmTestCase;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Sergey Chernolyas &amp;sergey_chernolyas@gmail.com&amp;
 */
public class CompositeEmbeddedIdTest extends OgmTestCase {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void insertEmbeddedIds() {
		Session session = openSession();
		Transaction transaction = session.beginTransaction();
		//insert news 1
		NewsId id1 = new NewsId( "author1", "topic1" );
		News n1 = new News();
		n1.setContent( "content1" );
		n1.setId( id1 );
		session.persist( n1 );
		transaction.commit();
		transaction = session.beginTransaction();
		//insert news 2
		NewsId id2 = new NewsId( "author2", "topic2" );
		News n2 = new News();
		n2.setContent( "content2" );
		n2.setId( id2 );
		session.persist( n2 );
		transaction.commit();
		session.clear();

		// checking
		Transaction transaction1 = session.beginTransaction();

		assertThat( session.get( News.class, id1 ) ).isNotNull();
		assertThat( session.get( News.class, id1 ).getContent() ).isEqualToIgnoringCase( "content1" );
		assertThat( session.get( News.class, id2 ) ).isNotNull();
		assertThat( session.get( News.class, id2 ).getContent() ).isEqualToIgnoringCase( "content2" );

		transaction1.commit();
		session.close();


		thrown.expect( EntityAlreadyExistsException.class );
		session = openSession();
		// try dublicate id
		Transaction transaction2 = session.beginTransaction();
		News n3 = new News();
		n3.setContent( "content3" );
		n3.setId( new NewsId( "author1", "topic1" ) );
		session.persist( n3 );
		transaction2.commit();
	}


	@Override
	protected Class<?>[] getAnnotatedClasses() {
		return new Class[] { News.class };
	}

	@Override
	protected void configure(Map<String, Object> settings) {
		settings.put( IgniteProperties.IGNITE_ALLOWS_TRANSACTION_EMULATION, Boolean.TRUE );
	}
}

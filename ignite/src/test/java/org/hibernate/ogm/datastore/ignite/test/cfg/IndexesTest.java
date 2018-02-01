/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.test.cfg;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Set;

import org.apache.ignite.cache.QueryIndex;
import org.hibernate.ogm.OgmSession;
import org.hibernate.ogm.datastore.ignite.utils.IgniteTestHelper;
import org.hibernate.ogm.utils.OgmTestCase;
import org.junit.Test;

/**
 * Some tests for generating indexes with {@code @Index} annotation
 *
 * @author Victor Kadachigov
 */
public class IndexesTest extends OgmTestCase {

	@Test
	public void testSuccessfulTextIndexCreation() throws Exception {
		try ( OgmSession session = openSession() ) {
			Set<QueryIndex> indexes = IgniteTestHelper.getIndexes( session.getSessionFactory(), Poem.class );
			assertThat( indexes.size() ).isEqualTo( 5 );
		}
	}

	@Override
	protected Class<?>[] getAnnotatedClasses() {
		return new Class<?>[]{ Poem.class };
	}
}

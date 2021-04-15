/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.test.cfg;

import org.apache.ignite.cache.QueryIndex;
import org.hibernate.ogm.OgmSession;
import org.hibernate.ogm.datastore.ignite.utils.IgniteTestHelper;
import org.hibernate.ogm.utils.OgmTestCase;
import org.junit.Test;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import java.util.HashSet;
import java.util.Set;

import static org.fest.assertions.Assertions.assertThat;

/**
 * Entities with long names that results in an index with greater than 255 chars is invalid in Ignite.
 * This ensures we raise an error and provide useful information to the user
 */
public class LongIndexNameWithIndexAnnotationTest extends OgmTestCase {

	@Test
	public void testLongEntityIndexName() throws Exception {
		try ( OgmSession session = openSession() ) {
			Set<QueryIndex> indexes = IgniteTestHelper.getIndexes( session.getSessionFactory(), EntityWithCustomIndex.class );
			assertThat( indexes.size() ).isEqualTo( 1 );
			assertThat( indexes.iterator().next().getName() ).isEqualToIgnoringCase( "SimpleIndexName" );
		}
	}

	@Override
	protected Class<?>[] getAnnotatedClasses() {
		return new Class<?>[] {
				EntityWithCustomIndex.class,
				EntityWithCustomContainer.class
		};
	}

	@Entity
	public static class EntityWithCustomContainer {
		@Id
		private String id;

		@OneToMany(targetEntity = EntityWithCustomIndex.class)
		private Set<EntityWithCustomIndex> entityWithCustomIndices = new HashSet<>();

		@javax.persistence.JoinTable(name = "joinLongEntityName")
		public Set<EntityWithCustomIndex> getEntityWithCustomIndices() {
			return entityWithCustomIndices;
		}
	}

	@Entity(name = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec dapibus cursus vestibulum. Quisque eu justo non mi tincidunt sagittis. Donec tincidunt facilisis placerat. Sed placerat urna eget tristique faucibus. Curabitur maximus gravida enim, vitae sed")
	@Table(indexes = {@Index(name = "SimpleIndexName", columnList = "id")})
	public static class EntityWithCustomIndex {
		@Id
		private String id;
	}
}

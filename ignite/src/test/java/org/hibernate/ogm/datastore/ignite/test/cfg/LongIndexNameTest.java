/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.test.cfg;

import org.hibernate.HibernateException;
import org.hibernate.ogm.utils.OgmTestCase;
import org.junit.Test;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import java.util.HashSet;
import java.util.Set;

import static junit.framework.TestCase.fail;

/**
 * Entities with long names that results in an index with greater than 255 chars is invalid in Ignite.
 * This ensures we raise an error and provide useful information to the user
 */
public class LongIndexNameTest extends OgmTestCase {

	@Test(expected = HibernateException.class)
	public void testLongEntityIndexName() throws Exception {
		fail( "The length of the registered entity's cache name should've failed this already" );
	}

	@Override
	protected Class<?>[] getAnnotatedClasses() {
		return new Class<?>[] {
				LongEntityName.class,
				LongEntityContainer.class
		};
	}

	@Entity
	public static class LongEntityContainer {
		@Id
		private String id;

		@OneToMany(targetEntity = LongEntityName.class)
		private Set<LongEntityName> longEntityNames = new HashSet<>();

		@javax.persistence.JoinTable(name = "joinLongEntityName")
		public Set<LongEntityName> getLongEntityNames() {
			return longEntityNames;
		}
	}

	@Entity(name = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec dapibus cursus vestibulum. Quisque eu justo non mi tincidunt sagittis. Donec tincidunt facilisis placerat. Sed placerat urna eget tristique faucibus. Curabitur maximus gravida enim, vitae sed")
	public static class LongEntityName {
		@Id
		private String id;
	}
}

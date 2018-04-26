/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.test.cfg;

import java.util.Map;

import static org.fest.assertions.Assertions.assertThat;
import org.hibernate.ogm.OgmSession;
import org.hibernate.ogm.OgmSessionFactory;
import org.hibernate.ogm.backendtck.associations.manytoone.Court;
import org.hibernate.ogm.backendtck.associations.manytoone.Game;
import org.hibernate.ogm.datastore.ignite.utils.IgniteTestHelper;
import org.hibernate.ogm.utils.OgmTestCase;
import org.junit.Test;

/**
 * @author Aliaksandr Salauyou
 */
public class ConfigWithAssociationsAndEmbeddedTest extends OgmTestCase {

	static final String STRING_TYPE = "java.lang.String";
	static final String INT_TYPE = "java.lang.Integer";

	@Test
	public void testCacheConfig() {
		try ( OgmSession session = openSession() ) {
			OgmSessionFactory sf = session.getSessionFactory();

			Map<String, String> fields = IgniteTestHelper.getFields( sf, Court.class );
			assertThat( fields.get( "id_countryCode" ) ).isEqualTo( STRING_TYPE );
			assertThat( fields.get( "id_sequenceNo" ) ).isEqualTo( INT_TYPE );
			assertThat( fields.get( "name" ) ).isEqualTo( STRING_TYPE );
			assertThat( fields.get( "playedOn" ) ).isNull();

			fields = IgniteTestHelper.getFields( sf, Game.class );
			assertThat( fields.get( "id_category" ) ).isEqualTo( STRING_TYPE );
			assertThat( fields.get( "id_gameSequenceNo" ) ).isEqualTo( INT_TYPE );
			assertThat( fields.get( "name" ) ).isEqualTo( STRING_TYPE );
			assertThat( fields.get( "playedOn_id_countryCode" ) ).isEqualTo( STRING_TYPE );
			assertThat( fields.get( "playedOn_id_sequenceNo" ) ).isEqualTo( INT_TYPE );
		}
	}

	@Override
	protected Class<?>[] getAnnotatedClasses() {
		return new Class<?>[]{ Court.class, Game.class };
	}
}

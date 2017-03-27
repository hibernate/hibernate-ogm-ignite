/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.redis.test.associations;

import java.util.Map;

import org.hibernate.ogm.backendtck.associations.manytoone.ManyToOneTest;
import org.hibernate.ogm.datastore.document.cfg.DocumentStoreProperties;
import org.hibernate.ogm.datastore.document.options.AssociationStorageType;
import org.hibernate.ogm.datastore.redis.RedisHashDialect;
import org.hibernate.ogm.utils.SkipByGridDialect;

/**
 * @author Emmanuel Bernard &lt;emmanuel@hibernate.org&gt;
 */
@SkipByGridDialect(dialects = RedisHashDialect.class)
public class ManyToOneInEntityTest extends ManyToOneTest {

	@Override
	protected void configure(Map<String, Object> cfg) {
		super.configure( cfg );
		cfg.put(
				DocumentStoreProperties.ASSOCIATIONS_STORE,
				AssociationStorageType.IN_ENTITY
		);
	}
}

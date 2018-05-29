/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.query.impl;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import org.hibernate.loader.custom.ScalarReturn;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;

/**
 * Describes a Ignite SQL query
 *
 * @author Victor Kadachigov
 */
public class IgniteQueryDescriptor implements Serializable {

	private final String sql;
	private final List<Object> indexedParameters;
	private final EntityKeyMetadata rootKeyMetadata;
	private final List<ScalarReturn> queryReturns;
	private final boolean hasScalar;
//	private final Set<String> querySpaces;

	public IgniteQueryDescriptor(String sql, List<Object> indexedParameters, boolean hasScalar) {
		this( sql, indexedParameters, hasScalar, null, Collections.emptyList() );
	}

	public IgniteQueryDescriptor(String sql, List<Object> indexedParameters,
			boolean hasScalar, EntityKeyMetadata rootKeyMetadata,
			List<ScalarReturn> queryReturns) {
		this.sql = sql;
		this.indexedParameters = indexedParameters;
		this.hasScalar = hasScalar;
		this.rootKeyMetadata = rootKeyMetadata;
		this.queryReturns = queryReturns;
	}

	public List<Object> getIndexedParameters() {
		return indexedParameters;
	}

	public String getSql() {
		return sql;
	}

	public boolean hasScalar() {
		return hasScalar;
	}

	public EntityKeyMetadata getRootKeyMetadata() {
		return rootKeyMetadata;
	}

	public List<ScalarReturn> getQueryReturns() {
		return queryReturns;
	}

}

/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.query.impl;

import java.io.Serializable;
import java.util.List;

/**
 * Describes a Ignite SQL query
 *
 * @author Victor Kadachigov
 */
public class IgniteQueryDescriptor implements Serializable {

	private static final long serialVersionUID = 1L;

	private final String sql;
	private final List<Object> indexedParameters;
	private final String table;
	private final boolean hasScalar;

	public IgniteQueryDescriptor(String sql, String table, List<Object> indexedParameters, boolean hasScalar) {
		this.sql = sql;
		this.indexedParameters = indexedParameters;
		this.hasScalar = hasScalar;
		this.table = table;
	}

	public List<Object> getIndexedParameters() {
		return indexedParameters;
	}

	public String getSql() {
		return sql;
	}

	public boolean isHasScalar() {
		return hasScalar;
	}

	public String getTable() {
		return table;
	}
}

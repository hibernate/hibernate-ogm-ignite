/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.query.parsing.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.hibernate.hql.ast.origin.hql.resolve.path.PropertyPath;
import org.hibernate.ogm.datastore.ignite.query.impl.IgniteQueryDescriptor;
import org.hibernate.ogm.datastore.ignite.util.StringHelper;

/**
 * @author Victor Kadachigov
 */
public class IgniteQueryRenderer {

	private static final List<String> ENTITY_COLUMN_NAMES = Collections.unmodifiableList( Arrays.asList( "_KEY", "_VALUE" ) );

	// values set by IgniteTreeRenderer
	String where;
	String orderBy;
	String from;  // additional expressions in FROM

	private final IgnitePropertyHelper propertyHelper;

	private final Map<String, Object> namedParameterValues;
	private final List<Object> indexedParameters;


	IgniteQueryRenderer(
			IgnitePropertyHelper propertyHelper,
			Map<String, Object> namedParameters ) {

		this.propertyHelper = propertyHelper;
		this.namedParameterValues = namedParameters;
		this.indexedParameters = new ArrayList<>( namedParameters.size() );
	}


	private void select(StringBuilder queryBuilder) {
		queryBuilder.append( "SELECT " );
		String selectionAlias = ( propertyHelper.getSelectionPath() == null )
			? propertyHelper.findAliasForType( propertyHelper.getRootEntity() )
			: propertyHelper.getSelectionPath().getFirstNode().getName();
		queryBuilder
			.append( selectionAlias ).append( "._KEY, " )
			.append( selectionAlias ).append( "._VAL " );
	}


	private void from(StringBuilder queryBuilder) {
		String tableAlias = propertyHelper.findAliasForType( propertyHelper.getRootEntity() );
		String tableName = propertyHelper.getTableName( propertyHelper.getRootEntity() );
		queryBuilder.append( " FROM " ).append( tableName ).append( ' ' ).append( tableAlias ).append( ' ' );
		// append joins here
	}


	public IgniteQueryParsingResult getResult() {
		StringBuilder queryBuilder = new StringBuilder();
		select( queryBuilder );
		from( queryBuilder );
		if ( !StringHelper.isEmpty( from ) ) {
			queryBuilder.append( ' ' ).append( from );
		}
		if ( !StringHelper.isEmpty( where ) ) {
			queryBuilder.append( " WHERE " ).append( where );
		}
		if ( !StringHelper.isEmpty( orderBy ) ) {
			queryBuilder.append( " ORDER BY " ).append( orderBy );
		}

		boolean hasScalar = false; // no projections for now
		IgniteQueryDescriptor queryDescriptor = new IgniteQueryDescriptor(
			queryBuilder.toString(), indexedParameters, hasScalar );
		return new IgniteQueryParsingResult( queryDescriptor, ENTITY_COLUMN_NAMES );
	}


	// In SQL we use positional parameters ('?x'),
	// thus we must collect parameter values in correct order.
	// `typeDefiningPath` - some path that defines type of parameter
	// (e.g. left part of current comparison predicate). If null,
	// value is passed unchanged
	int addParameterValue( String param, PropertyPath typeDefiningPath ) {
		Object paramValue = namedParameterValues.get( param );
		if ( paramValue != null ) {
			// May be enum or association type - needs conversion
			if ( typeDefiningPath != null ) {
				String entityType = typeDefiningPath.getFirstNode().isAlias()
					? propertyHelper.getEntityNameByAlias( typeDefiningPath.getFirstNode().getName() )
					: propertyHelper.getRootEntity();
				paramValue = propertyHelper.convertToBackendType(
					entityType, typeDefiningPath.getNodeNamesWithoutAlias(), paramValue );
			}
		}
		indexedParameters.add( paramValue );
		return indexedParameters.size();
	}

}

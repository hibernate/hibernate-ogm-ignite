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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import org.hibernate.cfg.NotYetImplementedException;
import org.hibernate.hql.ast.origin.hql.resolve.path.PropertyPath;
import org.hibernate.loader.custom.ScalarReturn;
import org.hibernate.ogm.datastore.ignite.query.impl.IgniteQueryDescriptor;
import org.hibernate.ogm.datastore.ignite.util.StringHelper;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.type.Type;

/**
 * @author Victor Kadachigov
 */
public class IgniteQueryRenderer {

	private static final List<String> ENTITY_COLUMN_NAMES = Collections.unmodifiableList( Arrays.asList( "_KEY", "_VAL" ) );

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


	/**
	 * Appends SELECT clause in query builder and returns either
	 * list of selections if a query is a projection query, or empty
	 * list if a single entity query
	 */
	private List<ScalarReturn> select(StringBuilder queryBuilder) {
		queryBuilder.append( "SELECT " );
		String rootAlias = propertyHelper.findAliasForType( propertyHelper.getRootEntity() );

		// is selected unqualified root entity (e.g. "from Hypothesis"),
		// or a single entity defined by alias (e.g. "select h from Hypothesis h")
		if ( propertyHelper.getSelections().isEmpty()
			|| ( propertyHelper.getSelections().size() == 1
			&& propertyHelper.getSelections().get( 0 ).getNodeNamesWithoutAlias().isEmpty() ) ) {
			String selectionAlias = propertyHelper.getSelections().isEmpty()
				? rootAlias
				: propertyHelper.getSelections().get( 0 ).getFirstNode().getName();
			queryBuilder
				.append( selectionAlias ).append( "._KEY, " )
				.append( selectionAlias ).append( "._VAL" );
			return Collections.emptyList();
		}

		// else, treat as projection selection
		List<ScalarReturn> selections = new ArrayList<>();
		int columnNumber = 0;
		Iterator<PropertyPath> i = propertyHelper.getSelections().iterator();
		while ( i.hasNext() ) {
			PropertyPath path = i.next();
			String alias = path.getFirstNode().isAlias()
				? path.getFirstNode().getName() : rootAlias;

			String columnName;
			List<String> propertyPath = path.getNodeNamesWithoutAlias();
			String entityType = propertyHelper.getEntityNameByAlias( alias );
			Type type = propertyHelper.getPropertyType( entityType, propertyPath );
			if ( type.isEntityType() ) {
				// though it may be better to load both key and value
				// in one query, OgmQueryLoader requires only key
				columnName = "_KEY";
			}
			else if ( type.isComponentType() ) {
				throw new NotYetImplementedException( "Embeddables in projection selection" );
			}
			else {
				columnName = propertyHelper.getColumnName( entityType, propertyPath );
				EntityKeyMetadata entityKey = propertyHelper.getKeyMetaData( entityType );
				if ( entityKey.getColumnNames().length == 1
					&& entityKey.getColumnNames()[0].equals( columnName ) ) {
					columnName = "_KEY";
				}
			}
			String columnAlias = "col_" + ( columnNumber++ );
			queryBuilder
				.append( alias ).append( '.' ).append( columnName )
				.append( " as " ).append( columnAlias );
			selections.add( new ScalarReturn( type, columnAlias ) );

			if ( i.hasNext() ) {
				queryBuilder.append( ',' ).append( ' ' );
			}
		}
		return selections;
	}


	private void from(StringBuilder queryBuilder) {
		String tableAlias = propertyHelper.findAliasForType( propertyHelper.getRootEntity() );
		String tableName = propertyHelper.getTableName( propertyHelper.getRootEntity() );
		queryBuilder.append( " FROM " ).append( tableName ).append( ' ' ).append( tableAlias ).append( ' ' );
		// append joins here
	}


	public IgniteQueryParsingResult getResult() {
		StringBuilder queryBuilder = new StringBuilder();
		List<ScalarReturn> selections = select( queryBuilder );
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

		IgniteQueryDescriptor queryDescriptor = new IgniteQueryDescriptor(
			queryBuilder.toString(), indexedParameters, !selections.isEmpty(),
			propertyHelper.getKeyMetaData( propertyHelper.getRootEntity() ), selections );

		List<String> selectionAliases = selections.isEmpty()
			? ENTITY_COLUMN_NAMES
			: selections.stream().map( ScalarReturn::getColumnAlias ).collect( toList() );

		return new IgniteQueryParsingResult( queryDescriptor, selectionAliases );
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

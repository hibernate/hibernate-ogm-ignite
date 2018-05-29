/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.query.parsing.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hibernate.cfg.NotYetImplementedException;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.hql.ast.origin.hql.resolve.path.PropertyPath;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.hql.ast.spi.SingleEntityQueryBuilder;
import org.hibernate.hql.ast.spi.SingleEntityQueryRendererDelegate;
import org.hibernate.loader.custom.ScalarReturn;
import org.hibernate.ogm.datastore.ignite.query.impl.IgniteQueryDescriptor;
import org.hibernate.ogm.datastore.ignite.query.parsing.predicate.impl.IgnitePredicateFactory;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.persister.impl.OgmEntityPersister;
import org.hibernate.type.Type;

import static java.util.stream.Collectors.toList;

/**
 * Parser delegate which creates Ignite SQL queries in form of {@link StringBuilder}s.
 *
 * @author Victor Kadachigov
 */
public class IgniteQueryRendererDelegate extends SingleEntityQueryRendererDelegate<StringBuilder, IgniteQueryParsingResult> {

	private static final List<String> ENTITY_COLUMN_NAMES = Collections.unmodifiableList( Arrays.asList( "_KEY", "_VAL" ) );

	private final IgnitePropertyHelper propertyHelper;
	private final SessionFactoryImplementor sessionFactory;
	private final Map<String, Object> namedParamsWithValues;
	private List<Object> indexedParameters;
	private List<OrderByClause> orderByExpressions;

	public IgniteQueryRendererDelegate(SessionFactoryImplementor sessionFactory, IgnitePropertyHelper propertyHelper, EntityNamesResolver entityNamesResolver, Map<String, Object> namedParameters) {
		super(
				propertyHelper,
				entityNamesResolver,
				SingleEntityQueryBuilder.getInstance( new IgnitePredicateFactory( propertyHelper ), propertyHelper ),
				namedParameters != null ? NamedParametersMap.INSTANCE : null /* we put '?' in query instead of parameter value */
		);
		this.propertyHelper = propertyHelper;
		this.sessionFactory = sessionFactory;
		this.namedParamsWithValues = namedParameters;
	}

	@Override
	public void setPropertyPath(PropertyPath propertyPath) {
		this.propertyPath = propertyPath;
	}

	private void where( StringBuilder queryBuilder ) {
		StringBuilder where = builder.build();
		if ( where != null && where.length() > 0 ) {
			queryBuilder.append( " WHERE " ).append( where );
		}
	}

	private void orderBy(StringBuilder queryBuilder) {
		if ( orderByExpressions != null && !orderByExpressions.isEmpty() ) {
			queryBuilder.append( " ORDER BY " );
			int counter = 1;
			for ( OrderByClause orderBy : orderByExpressions ) {
				orderBy.asString( queryBuilder );
				if ( counter++ < orderByExpressions.size() ) {
					queryBuilder.append( ", " );
				}
			}
		}
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
		String tableAlias = propertyHelper.findAliasForType( targetTypeName );
		OgmEntityPersister persister = (OgmEntityPersister) ( sessionFactory ).getEntityPersister( targetType.getName() );
		String tableName = propertyHelper.getKeyMetaData( targetType.getName() ).getTable();
		queryBuilder.append( " FROM " ).append( tableName ).append( ' ' ).append( tableAlias ).append( ' ' );
	}

	@Override
	public IgniteQueryParsingResult getResult() {
		StringBuilder queryBuilder = new StringBuilder();
		List<ScalarReturn> selections = select( queryBuilder );
		from( queryBuilder );
		where( queryBuilder );
		orderBy( queryBuilder );

		IgniteQueryDescriptor queryDescriptor = new IgniteQueryDescriptor(
				queryBuilder.toString(), indexedParameters, !selections.isEmpty(),
				propertyHelper.getKeyMetaData( propertyHelper.getRootEntity() ), selections );

		List<String> selectionAliases = selections.isEmpty()
			? ENTITY_COLUMN_NAMES
			: selections.stream().map( ScalarReturn::getColumnAlias ).collect( toList() );

		return new IgniteQueryParsingResult( queryDescriptor, selectionAliases );
	}

	@Override
	protected void addSortField(PropertyPath propertyPath, String collateName, boolean isAscending) {
		if ( orderByExpressions == null ) {
			orderByExpressions = new ArrayList<OrderByClause>();
		}

		List<String> propertyPathWithoutAlias = resolveAlias( propertyPath );
		PropertyIdentifier identifier = propertyHelper.getPropertyIdentifier( targetTypeName, propertyPathWithoutAlias );

		OrderByClause order = new OrderByClause( identifier.getAlias(), identifier.getPropertyName(), isAscending );
		orderByExpressions.add( order );
	}

	private void fillIndexedParams(String param) {
		if ( param.startsWith( ":" ) ) {
			if ( indexedParameters == null ) {
				indexedParameters = new ArrayList<>();
			}
			Object paramValue = namedParamsWithValues.get( param.substring( 1 ) );
			if ( paramValue != null && paramValue.getClass().isEnum() ) {
				//vk: for now I work only with @Enumerated(EnumType.ORDINAL) field params
				//    How to determite corresponding field to this param and check it's annotation?
				paramValue = ( (Enum) paramValue ).ordinal();
			}
			indexedParameters.add( paramValue );
		}
	}

	@Override
	public void predicateLess(String comparativePredicate) {
		fillIndexedParams( comparativePredicate );
		super.predicateLess( comparativePredicate );
	}

	@Override
	public void predicateLessOrEqual(String comparativePredicate) {
		fillIndexedParams( comparativePredicate );
		super.predicateLessOrEqual( comparativePredicate );
	}

	@Override
	public void predicateEquals(final String comparativePredicate) {
		fillIndexedParams( comparativePredicate );
		super.predicateEquals( comparativePredicate );
	}

	@Override
	public void predicateNotEquals(String comparativePredicate) {
		fillIndexedParams( comparativePredicate );
		super.predicateNotEquals( comparativePredicate );
	}

	@Override
	public void predicateGreaterOrEqual(String comparativePredicate) {
		fillIndexedParams( comparativePredicate );
		super.predicateGreaterOrEqual( comparativePredicate );
	}

	@Override
	public void predicateGreater(String comparativePredicate) {
		fillIndexedParams( comparativePredicate );
		super.predicateGreater( comparativePredicate );
	}

	@Override
	public void predicateBetween(String lower, String upper) {
		fillIndexedParams( lower );
		fillIndexedParams( upper );
		super.predicateBetween( lower, upper );
	}

	@Override
	public void predicateLike(String patternValue, Character escapeCharacter) {
		fillIndexedParams( patternValue );
		super.predicateLike( patternValue, escapeCharacter );
	}

	@Override
	public void predicateIn(List<String> list) {
		for ( String s : list ) {
			fillIndexedParams( s );
		}
		super.predicateIn( list );
	}

	private static class NamedParametersMap implements Map<String, Object> {

		public static final NamedParametersMap INSTANCE = new NamedParametersMap();

		@Override
		public int size() {
			throw new UnsupportedOperationException( "Not supported" );
		}
		@Override
		public boolean isEmpty() {
			throw new UnsupportedOperationException( "Not supported" );
		}
		@Override
		public boolean containsKey(Object key) {
			throw new UnsupportedOperationException( "Not supported" );
		}
		@Override
		public boolean containsValue(Object value) {
			throw new UnsupportedOperationException( "Not supported" );
		}
		@Override
		public Object get(Object key) {
			return PropertyIdentifier.PARAM_INSTANCE;
		}
		@Override
		public Object put(String key, Object value) {
			throw new UnsupportedOperationException( "Not supported" );
		}
		@Override
		public Object remove(Object key) {
			throw new UnsupportedOperationException( "Not supported" );
		}
		@Override
		public void putAll(Map<? extends String, ? extends Object> m) {
			throw new UnsupportedOperationException( "Not supported" );
		}
		@Override
		public void clear() {
			throw new UnsupportedOperationException( "Not supported" );
		}
		@Override
		public Set<String> keySet() {
			throw new UnsupportedOperationException( "Not supported" );
		}
		@Override
		public Collection<Object> values() {
			throw new UnsupportedOperationException( "Not supported" );
		}
		@Override
		public Set<java.util.Map.Entry<String, Object>> entrySet() {
			throw new UnsupportedOperationException( "Not supported" );
		}
	}

}

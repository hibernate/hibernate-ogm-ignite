/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.query.parsing.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.cfg.NotYetImplementedException;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.hql.ast.origin.hql.resolve.path.PropertyPath;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.ogm.datastore.ignite.logging.impl.Log;
import org.hibernate.ogm.datastore.ignite.logging.impl.LoggerFactory;
import org.hibernate.ogm.datastore.ignite.util.StringHelper;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.persister.impl.OgmCollectionPersister;
import org.hibernate.ogm.persister.impl.OgmEntityPersister;
import org.hibernate.ogm.query.parsing.impl.ParserPropertyHelper;
import org.hibernate.ogm.util.impl.ArrayHelper;
import org.hibernate.persister.entity.Joinable;
import org.hibernate.type.AssociationType;
import org.hibernate.type.Type;

/**
 * @author Victor Kadachigov
 */
public class IgnitePropertyHelper extends ParserPropertyHelper {
	private static final Log log = LoggerFactory.getLogger();

	private final Map<String, String> aliasByEntityName = new HashMap<>();
	private final Map<String, String> entityNameByAlias = new HashMap<>();

	private String rootEntityType;
	private PropertyPath selectionPath;

	public IgnitePropertyHelper(SessionFactoryImplementor sessionFactory, EntityNamesResolver entityNames) {
		super( sessionFactory, entityNames );
	}

	@Override
	public Object convertToBackendType(String entityType, List<String> propertyPath, Object value) {
		return value == PropertyIdentifier.PARAM_INSTANCE
					? value : super.convertToBackendType( entityType, propertyPath, value );
	}

	@Override
	protected Type getPropertyType(String entityType, List<String> propertyPath) {
		return super.getPropertyType( entityType, propertyPath );
	}


	void setRootEntity(String entityName) {
		if ( rootEntityType != null && !rootEntityType.equals( entityName ) ) {
			throw new NotYetImplementedException( "Multiple root entities" );
		}
		rootEntityType = entityName;
	}

	String getRootEntity() {
		return rootEntityType;
	}


	/**
	 * Sets the selection path. We should do this explicitly,
	 * because selected entity may differ from root entity
	 */
	void setSelectionPath(PropertyPath path) {
		if ( selectionPath != null ) {
			throw new NotYetImplementedException( "Multiple selections" );
		}
		if ( path.getNodes().size() > 1 || !path.getFirstNode().isAlias() ) {
			throw new NotYetImplementedException( "Projection selection" );
		}
		selectionPath = path;
	}

	PropertyPath getSelectionPath() {
		return selectionPath;
	}

	String getEntityNameByAlias(String alias) {
		return entityNameByAlias.get( StringHelper.sqlNormalize( alias ) );
	}


	/**
	 * Returns the {@link PropertyIdentifier} for the given property path.
	 *
	 * In passing, it creates all the necessary aliases for embedded/associations.
	 *
	 * @param path the path to the property
	 * @param targetEntityType the type of the entity
	 * @return the {@link PropertyIdentifier}
	 */
	PropertyIdentifier getPropertyIdentifier(PropertyPath path, String targetEntityType) {
		// we analyze the property path to find all the associations/embedded
		// which are in the way and create proper aliases for them

		List<String> propertyPath = path.getNodeNamesWithoutAlias();
		String entityAlias;
		boolean isLastElementAssociation = true;
		if ( path.getFirstNode().isAlias() ) {
			entityAlias = path.getFirstNode().getName();
		}
		else {
			entityAlias = findAliasForType( targetEntityType );
		}
		String propertyEntityType = entityNameByAlias.get( entityAlias );
		if ( propertyEntityType == null ) {
			propertyEntityType = targetEntityType;
		}

		String propertyAlias = entityAlias;
		String propertyName;

		List<String> currentPropertyPath = new ArrayList<>();
		List<String> lastAssociationPath = new ArrayList<>();

		OgmEntityPersister currentPersister = getPersister( propertyEntityType );
		OgmEntityPersister predPersister = currentPersister;
		String predJoinAlias = entityAlias;

		for ( String property : propertyPath ) {
			currentPropertyPath.add( property );
			Type currentPropertyType = getPropertyType( propertyEntityType, Collections.singletonList( property ) );

			if ( currentPropertyType.isAssociationType() ) {
				propertyEntityType = currentPropertyType.getName();
				currentPersister = getPersister( propertyEntityType );
				AssociationType associationPropertyType = (AssociationType) currentPropertyType;
				Joinable associatedJoinable = associationPropertyType.getAssociatedJoinable( getSessionFactory() );
				if ( associatedJoinable.isCollection()
					&& ( (OgmCollectionPersister) associatedJoinable ).getType().isComponentType() ) {
					// we have a collection of embedded
					throw new NotYetImplementedException( "Query with collection of embeddables" );
//					propertyAlias = aliasResolver.createAliasForEmbedded( entityAlias, currentPropertyPath, optionalMatch );
				}
				else {
					// last in path? - no need for join
					if ( currentPropertyPath.size() == propertyPath.size() ) {
						propertyName = getColumnName( predPersister.getEntityType().getName(),
							Collections.singletonList( property ) );
						return new PropertyIdentifier( predJoinAlias, propertyName );
					}
					// else, we register an implicit join
					lastAssociationPath.add( property );
					throw new NotYetImplementedException( "Query on associated property" );
					// predPersister = currentPersister;
					// isLastElementAssociation = true;
				}
			}
			else if ( currentPropertyType.isComponentType()
				&& !isIdProperty( currentPersister, propertyPath.subList( lastAssociationPath.size(), propertyPath.size() ) ) ) {
				// we are in the embedded case and the embedded is not the id of the entity (the id is stored as normal
				// properties)
				String embeddedProperty = String.join( ".", propertyPath.subList( lastAssociationPath.size(), propertyPath.size() ) );
				String[] columns = currentPersister.getPropertyColumnNames( embeddedProperty );
				if ( columns.length > 1 ) {
					throw new NotYetImplementedException( "Query with composite-ID association" );
				}
				return new PropertyIdentifier( propertyAlias, columns[0] );
			}
			else {
				isLastElementAssociation = false;
			}
		}

		if ( isLastElementAssociation ) {
			// even the last element is an association, we need to find a suitable identifier property
			propertyName = getPersister( propertyEntityType ).getIdentifierPropertyName();
		}
		else {
			// the last element is a property so we can build the rest with this property
			propertyName = getColumnName( propertyEntityType, propertyPath.subList( lastAssociationPath.size(), propertyPath.size() ) );
		}
		return new PropertyIdentifier( predJoinAlias, propertyName );
	}


	public String getColumnName(String entityType, List<String> propertyPathWithoutAlias) {
		String columnName = getColumn( getPersister( entityType ), propertyPathWithoutAlias );
		return StringHelper.realColumnName( columnName );
	}


	public String getTableName(String entityType) {
		return getPersister( entityType ).getEntityKeyMetadata().getTable();
	}

	/**
	 * Check if the property is part of the identifier of the entity.
	 *
	 * @param persister the {@link OgmEntityPersister} of the entity with the property
	 * @param namesWithoutAlias the path to the property with all the aliases resolved
	 * @return {@code true} if the property is part of the id, {@code false} otherwise.
	 */
	public boolean isIdProperty(OgmEntityPersister persister, List<String> namesWithoutAlias) {
		String join = String.join( ".", namesWithoutAlias );
		Type propertyType = persister.getPropertyType( namesWithoutAlias.get( 0 ) );
		String[] identifierColumnNames = persister.getIdentifierColumnNames();
		if ( propertyType.isComponentType() ) {
			String[] embeddedColumnNames = persister.getPropertyColumnNames( join );
			for ( String embeddedColumn : embeddedColumnNames ) {
				if ( !ArrayHelper.contains( identifierColumnNames, embeddedColumn ) ) {
					return false;
				}
			}
			return true;
		}
		return ArrayHelper.contains( identifierColumnNames, join );
	}

	public EntityKeyMetadata getKeyMetaData(String entityType) {
		OgmEntityPersister persister = (OgmEntityPersister) getSessionFactory().getEntityPersister( entityType );
		return persister.getEntityKeyMetadata();
	}

	public void registerEntityAlias(String entityName, String alias) {
		alias = StringHelper.sqlNormalize( alias );
		aliasByEntityName.put( entityName, alias );
		entityNameByAlias.put( alias, entityName );
	}

	public String findAliasForType(String entityType) {
		return aliasByEntityName.get( entityType );
	}

}

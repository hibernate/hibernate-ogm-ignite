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

	private final Map<String, String> aliasByEntityName = new HashMap<String, String>();

	public IgnitePropertyHelper(SessionFactoryImplementor sessionFactory, EntityNamesResolver entityNames) {
		super( sessionFactory, entityNames );
	}

	@Override
	public Object convertToBackendType(String entityType, List<String> propertyPath, Object value) {
		return value == PropertyIdentifier.PARAM_INSTANCE
					? value : super.convertToBackendType( entityType, propertyPath, value );
	}

	/**
	 * Returns the {@link PropertyIdentifier} for the given property path.
	 *
	 * In passing, it creates all the necessary aliases for embedded/associations.
	 *
	 * @param entityType the type of the entity
	 * @param propertyPath the path to the property without aliases
	 * @return the {@link PropertyIdentifier}
	 */
	public PropertyIdentifier getPropertyIdentifier(String entityType, List<String> propertyPath) {
		// we analyze the property path to find all the associations/embedded which are in the way and create proper
		// aliases for them
		String entityAlias = findAliasForType( entityType );

		String propertyEntityType = entityType;
		String propertyAlias = entityAlias;

		List<String> currentPropertyPath = new ArrayList<>();
		List<String> lastAssociationPath = Collections.emptyList();
		OgmEntityPersister currentPersister = getPersister( entityType );

		int requiredDepth = propertyPath.size();
		boolean isLastElementAssociation = false;
		int depth = 1;
		for ( String property : propertyPath ) {
			currentPropertyPath.add( property );
			Type currentPropertyType = getPropertyType( entityType, currentPropertyPath );

			// determine if the current property path is still part of requiredPropertyMatch
			boolean optionalMatch = depth > requiredDepth;

			if ( currentPropertyType.isAssociationType() ) {
				AssociationType associationPropertyType = (AssociationType) currentPropertyType;
				Joinable associatedJoinable = associationPropertyType.getAssociatedJoinable( getSessionFactory() );
				if ( associatedJoinable.isCollection()
						&& ( (OgmCollectionPersister) associatedJoinable ).getType().isComponentType() ) {
					// we have a collection of embedded
					throw new NotYetImplementedException();
//					propertyAlias = aliasResolver.createAliasForEmbedded( entityAlias, currentPropertyPath, optionalMatch );
				}
				else {
					isLastElementAssociation = false;
				}
			}
			else if ( currentPropertyType.isComponentType() ) {
				isLastElementAssociation = false;
				break;
			}
			else {
				isLastElementAssociation = false;
			}
			depth++;
		}

		String propertyName;
		if ( isLastElementAssociation ) {
			// even the last element is an association, we need to find a suitable identifier property
			propertyName = getSessionFactory().getEntityPersister( propertyEntityType ).getIdentifierPropertyName();
		}
		else {
			// the last element is a property so we can build the rest with this property
			propertyName = getColumnName( propertyEntityType, propertyPath.subList( lastAssociationPath.size(), propertyPath.size() ) );
		}
		return new PropertyIdentifier( propertyAlias, propertyName );
	}


	public String getColumnName(String entityType, List<String> propertyPathWithoutAlias) {
		String columnName = getColumn( getPersister( entityType ), propertyPathWithoutAlias );
		return StringHelper.realColumnName( columnName );
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

	/**
	 * Checks whether the supplied character is a letter.
	 */
	private boolean isLetter(int c) {
		return isUpperCaseLetter( c ) || isLowerCaseLetter( c );
	}
	/**
	 * Checks whether the supplied character is an upper-case letter.
	 */
	private boolean isUpperCaseLetter(int c) {
		return ( c >= 65 && c <= 90 ); // A - Z
	}
	/**
	 * Checks whether the supplied character is an lower-case letter.
	 */
	private boolean isLowerCaseLetter(int c) {
		return ( c >= 97 && c <= 122 ); // a - z
	}
	/**
	 * Checks whether the supplied character is a number
	 */
	private boolean isNumber(int c) {
		return ( c >= 48 && c <= 57 ); // 0 - 9
	}

	public void registerEntityAlias(String entityName, String alias) {
		StringBuilder sb = new StringBuilder( alias );
		for ( int i = 0; i < sb.length(); i++ ) {
			char c = sb.charAt( i );
			if ( c == '_' || isLetter( c ) ||  ( i > 0 && isNumber( c ) ) ) {
				continue;
			}
			sb.setCharAt( i, '_' );
		}
		aliasByEntityName.put( entityName, sb.toString() );
	}

	public String findAliasForType(String entityType) {
		return aliasByEntityName.get( entityType );
	}
}

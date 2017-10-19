/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.impl;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.hibernate.HibernateException;
import org.hibernate.boot.model.relational.Namespace;
import org.hibernate.boot.model.relational.Sequence;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.Table;
import org.hibernate.ogm.datastore.ignite.logging.impl.Log;
import org.hibernate.ogm.datastore.ignite.logging.impl.LoggerFactory;
import org.hibernate.ogm.datastore.ignite.util.StringHelper;
import org.hibernate.ogm.datastore.spi.BaseSchemaDefiner;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.model.key.spi.AssociationKeyMetadata;
import org.hibernate.ogm.model.key.spi.AssociationKind;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.model.key.spi.IdSourceKeyMetadata;
import org.hibernate.ogm.type.impl.EnumType;
import org.hibernate.ogm.type.impl.NumericBooleanType;
import org.hibernate.ogm.type.impl.YesNoType;
import org.hibernate.ogm.type.spi.GridType;
import org.hibernate.ogm.type.spi.TypeTranslator;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.type.Type;

/**
 * @author Victor Kadachigov
 */
public class IgniteCacheInitializer extends BaseSchemaDefiner {

	private static final Log log = LoggerFactory.getLogger();
	private static final String STRING_CLASS_NAME = String.class.getName();
	private static final String INTEGER_CLASS_NAME = Integer.class.getName();

	private ServiceRegistry serviceRegistry;

	@Override
	public void initializeSchema(SchemaDefinitionContext context) {
		serviceRegistry = context.getSessionFactory().getServiceRegistry();
		DatastoreProvider provider = serviceRegistry.getService( DatastoreProvider.class );
		if ( provider instanceof IgniteDatastoreProvider ) {
			IgniteDatastoreProvider igniteDatastoreProvider = (IgniteDatastoreProvider) provider;
			initializeEntities( context, igniteDatastoreProvider );
			initializeAssociations( context, igniteDatastoreProvider );
			initializeIdSources( context, igniteDatastoreProvider );
		}
		else {
			throw log.unexpectedDatastoreProvider( provider.getClass(), IgniteDatastoreProvider.class );
		}
	}

	private void initializeEntities(SchemaDefinitionContext context, final IgniteDatastoreProvider igniteDatastoreProvider) {
		for ( EntityKeyMetadata entityKeyMetadata : context.getAllEntityKeyMetadata() ) {
			try {
				try {
					igniteDatastoreProvider.getEntityCache( entityKeyMetadata );
				}
				catch (HibernateException ex) {
					CacheConfiguration config = createEntityCacheConfiguration( entityKeyMetadata, context );
					igniteDatastoreProvider.initializeCache( config );
				}
			}
			catch (Exception ex) {
				// just write error to log
				throw log.unableToInitializeCache( entityKeyMetadata.getTable(), ex );
			}
		}
	}

	private void initializeAssociations(SchemaDefinitionContext context, IgniteDatastoreProvider igniteDatastoreProvider) {
		for ( AssociationKeyMetadata associationKeyMetadata : context.getAllAssociationKeyMetadata() ) {
			log.debugf( "initializeAssociations. associationKeyMetadata: %s", associationKeyMetadata );
			if ( associationKeyMetadata.getAssociationKind() != AssociationKind.EMBEDDED_COLLECTION
					&& IgniteAssociationSnapshot.isThirdTableAssociation( associationKeyMetadata ) ) {
				try {
					try {
						igniteDatastoreProvider.getAssociationCache( associationKeyMetadata );
					}
					catch (HibernateException ex) {
						CacheConfiguration config = createCacheConfiguration( associationKeyMetadata, context );
						if ( config != null ) {
							igniteDatastoreProvider.initializeCache( config );
						}
					}
				}
				catch (Exception ex) {
					// just write error to log
					throw log.unableToInitializeCache( associationKeyMetadata.getTable(), ex );
				}

			}
		}
	}

	private void initializeIdSources(SchemaDefinitionContext context, IgniteDatastoreProvider igniteDatastoreProvider) {
		// generate tables
		for ( IdSourceKeyMetadata idSourceKeyMetadata : context.getAllIdSourceKeyMetadata() ) {
			if ( idSourceKeyMetadata.getType() == IdSourceKeyMetadata.IdSourceType.TABLE ) {
				try {
					try {
						igniteDatastoreProvider.getIdSourceCache( idSourceKeyMetadata );
					}
					catch (HibernateException ex) {
						CacheConfiguration config = createCacheConfiguration( idSourceKeyMetadata );
						igniteDatastoreProvider.initializeCache( config );
					}
				}
				catch (Exception ex) {
					// just write error to log
					throw log.unableToInitializeCache( idSourceKeyMetadata.getName(), ex );
				}
			}
		}
		Set<String> generatedSequences = new HashSet<>();
		// generate sequences
		for ( Namespace namespace : context.getDatabase().getNamespaces() ) {
			for ( Sequence sequence : namespace.getSequences() ) {
				generatedSequences.add( sequence.getName().getSequenceName().getText() );
				igniteDatastoreProvider.atomicSequence( sequence.getName().getSequenceName().getText(), sequence.getInitialValue(), true );
			}
		}
		for ( IdSourceKeyMetadata idSourceKeyMetadata : context.getAllIdSourceKeyMetadata() ) {
			if ( idSourceKeyMetadata.getType() == IdSourceKeyMetadata.IdSourceType.SEQUENCE ) {
				if ( idSourceKeyMetadata.getName() != null && !generatedSequences.contains( idSourceKeyMetadata.getName() ) ) {
					igniteDatastoreProvider.atomicSequence( idSourceKeyMetadata.getName(), 1, true );
				}
			}
		}
	}

	private CacheConfiguration createCacheConfiguration(IdSourceKeyMetadata idSourceKeyMetadata) {
		CacheConfiguration result = new CacheConfiguration();
		result.setName( StringHelper.stringBeforePoint( idSourceKeyMetadata.getName() ) );
		return result;
	}

	private CacheConfiguration createCacheConfiguration(AssociationKeyMetadata associationKeyMetadata, SchemaDefinitionContext context) {
		QueryEntity queryEntity = new QueryEntity();
		queryEntity.setTableName( associationKeyMetadata.getTable() );
		queryEntity.setValueType( StringHelper.stringAfterPoint( associationKeyMetadata.getTable() ) );
		appendIndex( queryEntity, associationKeyMetadata, context );

		CacheConfiguration result = new CacheConfiguration();
		result.setName( StringHelper.stringBeforePoint( associationKeyMetadata.getTable() ) );
		result.setQueryEntities( Arrays.asList( queryEntity ) );
		return result;
	}

	private void appendIndex(QueryEntity queryEntity, AssociationKeyMetadata associationKeyMetadata, SchemaDefinitionContext context) {
		for ( String idFieldName : associationKeyMetadata.getRowKeyColumnNames() ) {
			queryEntity.addQueryField( generateIndexName( idFieldName ), STRING_CLASS_NAME, null );
			queryEntity.setIndexes( Arrays.asList( new QueryIndex( generateIndexName( idFieldName ), QueryIndexType.SORTED  ) ) );
		}
	}

	private String generateIndexName(String fieldName) {
		return fieldName.replace( '.','_' );
	}

	private Class getEntityIdClassName( String table, SchemaDefinitionContext context ) {
		Class<?> entityClass = context.getTableEntityTypeMapping().get( table );
		EntityPersister entityPersister = context.getSessionFactory().getEntityPersister( entityClass.getName() );
		return entityPersister.getIdentifierType().getReturnedClass();
	}

	private CacheConfiguration<?,?> createEntityCacheConfiguration(EntityKeyMetadata entityKeyMetadata, SchemaDefinitionContext context) {
		CacheConfiguration<?,?> cacheConfiguration = new CacheConfiguration<>();
		cacheConfiguration.setStoreKeepBinary( true );
		cacheConfiguration.setSqlSchema( QueryUtils.DFLT_SCHEMA );
		cacheConfiguration.setBackups( 1 );
		cacheConfiguration.setName( StringHelper.stringBeforePoint( entityKeyMetadata.getTable() ) );
		cacheConfiguration.setAtomicityMode( CacheAtomicityMode.TRANSACTIONAL );

		QueryEntity queryEntity = new QueryEntity();
		queryEntity.setTableName( entityKeyMetadata.getTable() );
		queryEntity.setKeyType( getEntityIdClassName( entityKeyMetadata.getTable(), context ).getSimpleName() );
		queryEntity.setValueType( StringHelper.stringAfterPoint( entityKeyMetadata.getTable() ) );

		addTableInfo( queryEntity, context, entityKeyMetadata.getTable() );
		for ( AssociationKeyMetadata associationKeyMetadata : context.getAllAssociationKeyMetadata() ) {
			if ( associationKeyMetadata.getAssociationKind() != AssociationKind.EMBEDDED_COLLECTION
					&& associationKeyMetadata.getTable().equals( entityKeyMetadata.getTable() )
					&& !IgniteAssociationSnapshot.isThirdTableAssociation( associationKeyMetadata ) ) {
				appendIndex( queryEntity, associationKeyMetadata, context );
			}
		}
		log.debugf( "queryEntity: %s", queryEntity );
		cacheConfiguration.setQueryEntities( Arrays.asList( queryEntity ) );
		return cacheConfiguration;
	}

	@SuppressWarnings("unchecked")
	private void addTableInfo(QueryEntity queryEntity, SchemaDefinitionContext context, String tableName) {
		Namespace namespace = context.getDatabase().getDefaultNamespace();
		Optional<Table> tableOptional = namespace.getTables().stream().filter( currentTable -> currentTable.getName().equals( tableName ) ).findFirst();
		if ( tableOptional.isPresent() ) {
			Table table = tableOptional.get();
			for ( Iterator<Column> columnIterator = table.getColumnIterator(); columnIterator.hasNext(); ) {
				Column currentColumn = columnIterator.next();
				String fieldType = fieldType( currentColumn );
				queryEntity.addQueryField( currentColumn.getName(), fieldType, null );
			}
		}
	}

	private String fieldType(Column currentColumn) {
		TypeTranslator translator = serviceRegistry.getService( TypeTranslator.class );
		Type valueType = currentColumn.getValue().getType();
		GridType gridType = translator.getType( valueType );
		if ( gridType instanceof EnumType ) {
			return enumFieldType( (EnumType) gridType );
		}
		if ( gridType instanceof YesNoType ) {
			return STRING_CLASS_NAME;
		}
		if ( gridType instanceof NumericBooleanType ) {
			return INTEGER_CLASS_NAME;
		}
		Class<?> returnedClass = valueType.getReturnedClass();
		if ( Character.class.equals( returnedClass ) ) {
			return STRING_CLASS_NAME;
		}
		return returnedClass.getName();
	}

	private String enumFieldType(EnumType enumType) {
		return enumType.isOrdinal()
				? INTEGER_CLASS_NAME
				: STRING_CLASS_NAME;
	}
}

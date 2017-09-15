/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.configuration.CacheConfiguration;
import org.hibernate.HibernateException;
import org.hibernate.boot.model.relational.Namespace;
import org.hibernate.boot.model.relational.Sequence;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.SimpleValue;
import org.hibernate.mapping.Table;
import org.hibernate.mapping.Value;
import org.hibernate.ogm.datastore.ignite.logging.impl.Log;
import org.hibernate.ogm.datastore.ignite.logging.impl.LoggerFactory;
import org.hibernate.ogm.datastore.ignite.util.StringHelper;
import org.hibernate.ogm.datastore.spi.BaseSchemaDefiner;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.model.key.spi.AssociationKeyMetadata;
import org.hibernate.ogm.model.key.spi.AssociationKind;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.model.key.spi.IdSourceKeyMetadata;
import org.hibernate.persister.entity.EntityPersister;

/**
 * @author Victor Kadachigov
 */
public class IgniteCacheInitializer extends BaseSchemaDefiner {

	private static final Log log = LoggerFactory.getLogger();
	private static Map<Class<?>, Class<?>> h2TypeMapping;

	static {
		Map<Class<?>, Class<?>> map = new HashMap<>(  );
		map.put( Character.class, String.class );

		h2TypeMapping = Collections.unmodifiableMap( map );
	}



	@Override
	public void initializeSchema(SchemaDefinitionContext context) {
		context.getTableEntityTypeMapping();

		DatastoreProvider provider = context.getSessionFactory().getServiceRegistry().getService( DatastoreProvider.class );
		if ( provider instanceof IgniteDatastoreProvider ) {
			IgniteDatastoreProvider igniteDatastoreProvider = (IgniteDatastoreProvider) provider;
			initializeEntities( context, igniteDatastoreProvider );
			initializeAssociations( context, igniteDatastoreProvider );
			initializeIdSources( context, igniteDatastoreProvider );
		}
		else {
			log.unexpectedDatastoreProvider( provider.getClass(), IgniteDatastoreProvider.class );
		}
	}

	private void initializeEntities(SchemaDefinitionContext context, final IgniteDatastoreProvider igniteDatastoreProvider) {
		for ( EntityKeyMetadata entityKeyMetadata : context.getAllEntityKeyMetadata() ) {
			try {
				try {
					igniteDatastoreProvider.getEntityCache( entityKeyMetadata );
				}
				catch (HibernateException ex) {
					igniteDatastoreProvider.initializeCache( createEntityCacheConfiguration( entityKeyMetadata, context ) );
				}
			}
			catch (Exception ex) {
				// just write error to log
				log.unableToInitializeCache( entityKeyMetadata.getTable(), ex );
			}
		}
	}

	private void initializeAssociations(SchemaDefinitionContext context, IgniteDatastoreProvider igniteDatastoreProvider) {
		for ( AssociationKeyMetadata associationKeyMetadata : context.getAllAssociationKeyMetadata() ) {
			if ( associationKeyMetadata.getAssociationKind() != AssociationKind.EMBEDDED_COLLECTION
					&& IgniteAssociationSnapshot.isThirdTableAssociation( associationKeyMetadata ) ) {
				try {
					try {
						igniteDatastoreProvider.getAssociationCache( associationKeyMetadata );
					}
					catch (HibernateException ex) {
						igniteDatastoreProvider.initializeCache( createCacheConfiguration( associationKeyMetadata, context ) );
					}
				}
				catch (Exception ex) {
					// just write error to log
					log.unableToInitializeCache( associationKeyMetadata.getTable(), ex );
				}
			}
		}
	}

	private void initializeIdSources(SchemaDefinitionContext context, IgniteDatastoreProvider igniteDatastoreProvider) {
		for ( IdSourceKeyMetadata idSourceKeyMetadata : context.getAllIdSourceKeyMetadata() ) {
			if ( idSourceKeyMetadata.getType() == IdSourceKeyMetadata.IdSourceType.TABLE ) {
				try {
					try {
						igniteDatastoreProvider.getIdSourceCache( idSourceKeyMetadata );
					}
					catch (HibernateException ex) {
						igniteDatastoreProvider.initializeCache( createCacheConfiguration( idSourceKeyMetadata ) );
					}
				}
				catch (Exception ex) {
					// just write error to log
					throw log.unableToInitializeCache( idSourceKeyMetadata.getName(), ex );
				}
			}
			else if ( idSourceKeyMetadata.getType() == IdSourceKeyMetadata.IdSourceType.SEQUENCE ) {
				if ( idSourceKeyMetadata.getName() != null ) {
					igniteDatastoreProvider.atomicSequence( idSourceKeyMetadata.getName(),  1, true );
				}
			}
		}
		//generate sequences
		for ( Namespace namespace : context.getDatabase().getNamespaces() ) {
			for ( Sequence sequence : namespace.getSequences() ) {
				igniteDatastoreProvider.atomicSequence( sequence.getName().getSequenceName().getCanonicalName(),  sequence.getInitialValue(), true );
			}
		}
	}

	private CacheConfiguration<?,?> createCacheConfiguration(IdSourceKeyMetadata idSourceKeyMetadata) {
		CacheConfiguration<Object, Object> result = new CacheConfiguration<>();
		result.setName( StringHelper.stringBeforePoint( idSourceKeyMetadata.getName() ) );
		return result;
	}

	private CacheConfiguration<?,?> createCacheConfiguration(AssociationKeyMetadata associationKeyMetadata, SchemaDefinitionContext context) {

		CacheConfiguration<Object, Object> result = new CacheConfiguration<>();
		result.setName( StringHelper.stringBeforePoint( associationKeyMetadata.getTable() ) );

		QueryEntity queryEntity = new QueryEntity();
		queryEntity.setTableName( associationKeyMetadata.getTable() );
		queryEntity.setValueType( StringHelper.stringAfterPoint( associationKeyMetadata.getTable() ) );
		appendIndex( queryEntity, associationKeyMetadata, context );

		result.setQueryEntities( Arrays.asList( queryEntity ) );

		return result;
	}

	private void appendIndex(QueryEntity queryEntity, AssociationKeyMetadata associationKeyMetadata, SchemaDefinitionContext context) {

		for ( String idFieldName : associationKeyMetadata.getRowKeyColumnNames() ) {
			queryEntity.addQueryField( generateIndexName( idFieldName ), String.class.getName(),null );
			queryEntity.setIndexes( Arrays.asList( new QueryIndex( generateIndexName( idFieldName ), QueryIndexType.SORTED  ) ) );
		}
	}

	private String generateIndexName(String fieldName) {
		return fieldName.replace( '.','_' );
	}

	@SuppressWarnings("rawtypes")
	private Class getEntityIdClassName( String table, SchemaDefinitionContext context ) {
		Class<?> entityClass = context.getTableEntityTypeMapping().get( table );
		EntityPersister entityPersister = context.getSessionFactory().getEntityPersister( entityClass.getName() );
		return entityPersister.getIdentifierType().getReturnedClass();
	}

	private CacheConfiguration<?, ?> createEntityCacheConfiguration(EntityKeyMetadata entityKeyMetadata, SchemaDefinitionContext context) {

		CacheConfiguration<?, ?> cacheConfiguration = new CacheConfiguration<>();
		cacheConfiguration.setStoreKeepBinary( true );

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
		log.debugf( "QueryEntity info :%s;", queryEntity );
		cacheConfiguration.setQueryEntities( Arrays.asList( queryEntity ) );
		return cacheConfiguration;
	}

	@SuppressWarnings("unchecked")
	private void addTableInfo(QueryEntity queryEntity, SchemaDefinitionContext context, String tableName) {
		Namespace namespace = context.getDatabase().getDefaultNamespace();
		Optional<Table> tableOptional = namespace.getTables().stream().filter( currentTable -> currentTable.getName().equals( tableName ) ).findFirst();
		if ( tableOptional.isPresent() ) {
			Table table = tableOptional.get();
			for ( Iterator<Column> columnIterator = table.getColumnIterator(); columnIterator.hasNext();) {
				Column currentColumn = columnIterator.next();
				Value value = currentColumn.getValue();
				if ( value.getClass() == SimpleValue.class ) {
					// it is simple type. add the field
					SimpleValue simpleValue = (SimpleValue) value;
					Class<?> returnValue = simpleValue.getType().getReturnedClass();
					returnValue = h2TypeMapping.getOrDefault( returnValue , returnValue  );
					queryEntity.addQueryField( currentColumn.getName(),returnValue.getName(),null );
				}
			}
		}

	}
}

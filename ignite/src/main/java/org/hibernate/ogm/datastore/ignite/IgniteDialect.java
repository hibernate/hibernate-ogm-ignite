/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.cfg.NotYetImplementedException;
import org.hibernate.dialect.lock.LockingStrategy;
import org.hibernate.dialect.lock.OptimisticForceIncrementLockingStrategy;
import org.hibernate.dialect.lock.OptimisticLockingStrategy;
import org.hibernate.dialect.lock.PessimisticForceIncrementLockingStrategy;
import org.hibernate.loader.custom.Return;
import org.hibernate.loader.custom.ScalarReturn;
import org.hibernate.ogm.datastore.ignite.impl.IgniteAssociationRowSnapshot;
import org.hibernate.ogm.datastore.ignite.impl.IgniteAssociationSnapshot;
import org.hibernate.ogm.datastore.ignite.impl.IgniteDatastoreProvider;
import org.hibernate.ogm.datastore.ignite.impl.IgniteEmbeddedAssociationSnapshot;
import org.hibernate.ogm.datastore.ignite.impl.IgniteTupleSnapshot;
import org.hibernate.ogm.datastore.ignite.logging.impl.Log;
import org.hibernate.ogm.datastore.ignite.logging.impl.LoggerFactory;
import org.hibernate.ogm.datastore.ignite.options.impl.CollocatedAssociationOption;
import org.hibernate.ogm.datastore.ignite.options.impl.ReadThroughOption;
import org.hibernate.ogm.datastore.ignite.query.impl.IgniteParameterMetadataBuilder;
import org.hibernate.ogm.datastore.ignite.query.impl.IgniteQueryDescriptor;
import org.hibernate.ogm.datastore.ignite.query.impl.IgniteSqlQueryParser;
import org.hibernate.ogm.datastore.ignite.query.impl.QueryHints;
import org.hibernate.ogm.datastore.ignite.type.impl.IgniteGridTypeMapper;
import org.hibernate.ogm.datastore.ignite.util.StringHelper;
import org.hibernate.ogm.datastore.map.impl.MapTupleSnapshot;
import org.hibernate.ogm.dialect.multiget.spi.MultigetGridDialect;
import org.hibernate.ogm.dialect.query.spi.BackendQuery;
import org.hibernate.ogm.dialect.query.spi.ClosableIterator;
import org.hibernate.ogm.dialect.query.spi.ParameterMetadataBuilder;
import org.hibernate.ogm.dialect.query.spi.QueryParameters;
import org.hibernate.ogm.dialect.query.spi.QueryableGridDialect;
import org.hibernate.ogm.dialect.query.spi.RowSelection;
import org.hibernate.ogm.dialect.spi.AssociationContext;
import org.hibernate.ogm.dialect.spi.AssociationTypeContext;
import org.hibernate.ogm.dialect.spi.BaseGridDialect;
import org.hibernate.ogm.dialect.spi.GridDialect;
import org.hibernate.ogm.dialect.spi.ModelConsumer;
import org.hibernate.ogm.dialect.spi.NextValueRequest;
import org.hibernate.ogm.dialect.spi.OperationContext;
import org.hibernate.ogm.dialect.spi.TupleAlreadyExistsException;
import org.hibernate.ogm.dialect.spi.TupleContext;
import org.hibernate.ogm.dialect.spi.TupleTypeContext;
import org.hibernate.ogm.entityentry.impl.TuplePointer;
import org.hibernate.ogm.model.key.spi.AssociationKey;
import org.hibernate.ogm.model.key.spi.AssociationKeyMetadata;
import org.hibernate.ogm.model.key.spi.AssociationKind;
import org.hibernate.ogm.model.key.spi.AssociationType;
import org.hibernate.ogm.model.key.spi.EntityKey;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.model.key.spi.RowKey;
import org.hibernate.ogm.model.spi.Association;
import org.hibernate.ogm.model.spi.AssociationOperation;
import org.hibernate.ogm.model.spi.AssociationOperationType;
import org.hibernate.ogm.model.spi.AssociationSnapshot;
import org.hibernate.ogm.model.spi.Tuple;
import org.hibernate.ogm.model.spi.Tuple.SnapshotType;
import org.hibernate.ogm.model.spi.TupleSnapshot;
import org.hibernate.ogm.type.spi.GridType;
import org.hibernate.ogm.util.impl.Contracts;
import org.hibernate.persister.entity.Lockable;
import org.hibernate.type.Type;

import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.SqlFieldsQuery;

public class IgniteDialect extends BaseGridDialect implements GridDialect, QueryableGridDialect<IgniteQueryDescriptor>,MultigetGridDialect {

	private static final long serialVersionUID = -4347702430400562694L;
	private static final Log log = LoggerFactory.getLogger();

	private IgniteDatastoreProvider provider;

	public IgniteDialect(IgniteDatastoreProvider provider) {
		this.provider = provider;
	}

	@Override
	public LockingStrategy getLockingStrategy(Lockable lockable, LockMode lockMode) {
		if ( lockMode == LockMode.PESSIMISTIC_FORCE_INCREMENT ) {
			return new PessimisticForceIncrementLockingStrategy( lockable, lockMode );
		}
		// else if ( lockMode==LockMode.PESSIMISTIC_WRITE ) {
		// return new PessimisticWriteLockingStrategy( lockable, lockMode );
		// }
		else if ( lockMode == LockMode.PESSIMISTIC_READ ) {
			return new IgnitePessimisticReadLockingStrategy( lockable, lockMode, provider );
		}
		else if ( lockMode == LockMode.OPTIMISTIC ) {
			return new OptimisticLockingStrategy( lockable, lockMode );
		}
		else if ( lockMode == LockMode.OPTIMISTIC_FORCE_INCREMENT ) {
			return new OptimisticForceIncrementLockingStrategy( lockable, lockMode );
		}
		else {
			return null;
		}
	}

	@Override
	public Tuple getTuple(EntityKey key, OperationContext operationContext) {
		IgniteCache<Object, BinaryObject> entityCache = provider.getEntityCache( key.getMetadata() );
		if ( entityCache == null ) {
			throw log.cacheNotFound( key.getMetadata().getTable() );
		}
		Object id = provider.createKeyObject( key );
		BinaryObject po = entityCache.get( id );
		if ( po != null ) {
			return new Tuple( new IgniteTupleSnapshot( id, po, key.getMetadata() ), SnapshotType.UPDATE );
		}
		else {
			return null;
		}
	}

	@Override
	public List<Tuple> getTuples(EntityKey[] keys, TupleContext tupleContext) {
		Map<Object, EntityKey> idKeyMap = Stream.of( keys ).collect(
				Collectors.toMap( ( EntityKey key ) -> provider.createKeyObject( key ), ( EntityKey key ) -> key ) );

		// all keys from one cache
		IgniteCache<Object, BinaryObject> entityCache = provider.getEntityCache( keys[0].getMetadata() );
		return entityCache.getAll( idKeyMap.keySet() ).entrySet().stream()
				.map( ( Map.Entry<Object, BinaryObject> entry ) -> new Tuple(
						new IgniteTupleSnapshot( entry.getKey(), entry.getValue(),
								idKeyMap.get( entry.getKey() ).getMetadata() ),
						SnapshotType.UPDATE ) )
				.collect( Collectors.toList() );
	}

	@Override
	public Tuple createTuple(EntityKey key, OperationContext operationContext) {
		IgniteCache<Object, BinaryObject> entityCache = provider.getEntityCache( key.getMetadata() );
		if ( entityCache == null ) {
			throw log.cacheNotFound( key.getMetadata().getTable() );
		}
		Object id = provider.createKeyObject( key );
		return new Tuple( new IgniteTupleSnapshot( id, null, key.getMetadata() ), SnapshotType.INSERT );
	}

	private boolean isIndexField(Field[] searchableFields, String fieldName) {
		for ( int i = 0; i < searchableFields.length; i++ ) {
			if ( searchableFields[i].getName().equals( fieldName ) ) {
				return true;
			}
		}
		return false;
	}

	@Override
	public void insertOrUpdateTuple(EntityKey key, TuplePointer tuplePointer, TupleContext tupleContext) throws TupleAlreadyExistsException {
		IgniteCache<Object, BinaryObject> entityCache = provider.getEntityCache( key.getMetadata() );

		Tuple tuple = tuplePointer.getTuple();

		Object keyObject = null;
		BinaryObjectBuilder builder = null;
		if ( tuple.getSnapshotType() == SnapshotType.UPDATE ) {
			IgniteTupleSnapshot tupleSnapshot = (IgniteTupleSnapshot) tuple.getSnapshot();
			keyObject = tupleSnapshot.getCacheKey();
			builder = provider.createBinaryObjectBuilder( entityCache.get( keyObject ) );
		}
		else {
			builder = provider.createBinaryObjectBuilder( provider.getEntityTypeName( key.getMetadata().getTable() ) );
		}

		for ( String columnName : tuple.getColumnNames() ) {
			if ( key.getMetadata().isKeyColumn( columnName ) ) {
				continue;
			}
			Object value = tuple.get( columnName );
			if ( value != null ) {
				builder.setField( StringHelper.realColumnName( columnName ), value );
			}
			else {
				builder.removeField( StringHelper.realColumnName( columnName ) );
			}
		}
		BinaryObject valueObject = builder.build();
		entityCache.put( keyObject, valueObject );
		tuplePointer.setTuple( new Tuple( new IgniteTupleSnapshot( keyObject, valueObject, key.getMetadata() ), SnapshotType.UPDATE ) );
	}

	@Override
	public void removeTuple(EntityKey key, TupleContext tupleContext) {
		IgniteCache<Object, BinaryObject> entityCache = provider.getEntityCache( key.getMetadata() );
		entityCache.remove( provider.createKeyObject( key ) );
	}

	@Override
	public Association getAssociation(AssociationKey associationKey, AssociationContext associationContext) {
		log.debugf( "getAssociation: association key : %s; associationContext: %s",
				associationKey,associationContext );
		Association result = null;
		IgniteCache<Object, BinaryObject> associationCache = provider.getAssociationCache( associationKey.getMetadata() );

		if ( associationCache == null ) {
			throw log.cacheNotFound( associationKey.getMetadata().getTable() );
		}

		if ( associationKey.getMetadata().getAssociationKind() == AssociationKind.ASSOCIATION ) {
			Boolean isReadThrough = associationContext.getAssociationTypeContext().getOptionsContext().getUnique( ReadThroughOption.class );
			if ( isReadThrough ) {

				TuplePointer tuplePointer = getOwnerEntityTuplePointer( associationKey, associationContext );
				IgniteTupleSnapshot igniteTupleSnapshot = (IgniteTupleSnapshot) tuplePointer.getTuple().getSnapshot();
				//need to add link info
				IgniteCache<Object, BinaryObject> entityCache = provider.getEntityCache( igniteTupleSnapshot.getEntityKeyMetadata() );

				Object key = igniteTupleSnapshot.getCacheKey();
				BinaryObject lastEntityVertion = entityCache.get( key );
				String notOwnerLinkFieldName = associationKey.getMetadata().getCollectionRole();

				Map<Object, BinaryObject> associationMap = new LinkedHashMap<>();
				log.debugf( "getAssociation: lastEntityVertion has field %s. Result: %b", notOwnerLinkFieldName, (lastEntityVertion.hasField( notOwnerLinkFieldName )) );
				if ( lastEntityVertion.hasField( notOwnerLinkFieldName ) ) {
					Collection associatedEntityKeys = lastEntityVertion.field( notOwnerLinkFieldName );
					String associatedEntityName = associationKey.getTable();
					IgniteCache<Object, BinaryObject> associatedEntityCache = provider.getEntityCache( associatedEntityName );

					for ( Object associatedKey : associatedEntityKeys ) {
						associationMap.put( associatedKey, associatedEntityCache.get( associatedKey ) );
					}
				}
				else {
					log.warnf( "Entity %s with id %s not have field %s", igniteTupleSnapshot.getEntityKeyMetadata().getTable() , key , notOwnerLinkFieldName );
				}
				result = new Association( new IgniteAssociationSnapshot( associationKey, associationMap ) );
			}
			else {
				QueryHints.Builder hintsBuilder = new QueryHints.Builder();
				Boolean isCollocated = associationContext.getAssociationTypeContext().getOptionsContext().getUnique( CollocatedAssociationOption.class );

				if ( isCollocated ) {
					hintsBuilder.setAffinityRun( true );
					hintsBuilder.setAffinityKey( provider.createParentKeyObject( associationKey ) );
				}
				QueryHints hints = hintsBuilder.build();

				SqlFieldsQuery sqlQuery = provider.createSqlFieldsQueryWithLog( createAssociationQuery( associationKey,true ),
						hints, associationKey.getColumnValues() );
				Iterable<List<?>> list = executeWithHints( associationCache, sqlQuery, hints );

				Iterator<List<?>> iterator = list.iterator();
				if ( iterator.hasNext() ) {
					Map<Object, BinaryObject> associationMap = new HashMap<>();
					while ( iterator.hasNext() ) {
						List<?> item = iterator.next();
						Object id = item.get( 0 );
						BinaryObject bo = (BinaryObject) item.get( 1 );
						associationMap.put( id, bo );
					}
					result = new Association( new IgniteAssociationSnapshot( associationKey, associationMap ) );
				}
			}
		}
		else if ( associationKey.getMetadata().getAssociationKind() == AssociationKind.EMBEDDED_COLLECTION ) {
			result = new Association( new IgniteEmbeddedAssociationSnapshot( associationKey, associationContext.getEntityTuplePointer().getTuple() ) );
		}
		else {
			throw new UnsupportedOperationException( "Unknown association kind " + associationKey.getMetadata().getAssociationKind() );
		}

		return result;
	}

	private String createAssociationQuery(AssociationKey key, boolean selectObjects) {
		StringBuilder sb = new StringBuilder();
		if ( selectObjects ) {
			sb.append( "SELECT _KEY, _VAL FROM " );
		}
		else {
			sb.append( "SELECT _KEY FROM " );
		}
		sb.append( key.getMetadata().getTable() ).append( " WHERE " );
		boolean first = true;
		for ( String columnName : key.getColumnNames() ) {
			if ( !first ) {
				sb.append( " AND " );
			}
			else {
				first = false;
			}
			sb.append( StringHelper.realColumnName( columnName ) ).append( "=?" );
		}
		return sb.toString();
	}

	@Override
	public Association createAssociation(AssociationKey associationKey, AssociationContext associationContext) {
		log.debugf( "createAssociation: association key : %s; associationContext: %s",
				associationKey,associationContext );
		log.debugf( "createAssociation: key.getMetadata().getAssociationKind(): %s",
				associationKey.getMetadata().getAssociationKind() );
		if ( associationKey.getMetadata().getAssociationKind() == AssociationKind.ASSOCIATION ) {
			return new Association( new IgniteAssociationSnapshot( associationKey ) );
		}
		else if ( associationKey.getMetadata().getAssociationKind() == AssociationKind.EMBEDDED_COLLECTION ) {
			return new Association( new IgniteEmbeddedAssociationSnapshot( associationKey, associationContext.getEntityTuplePointer().getTuple() ) );
		}
		else {
			throw new UnsupportedOperationException( "Unknown association kind " + associationKey.getMetadata().getAssociationKind() );
		}
	}

	@Override
	public void insertOrUpdateAssociation(AssociationKey associationKey, Association association, AssociationContext associationContext) {
		log.debugf( "insertOrUpdateAssociation: association key : %s; association : %s; associationContext: %s",
				associationKey,association,associationContext );
		log.debugf( "insertOrUpdateAssociation: key.getMetadata().isInverse(): %b; associationKey.getMetadata().getAssociationKind(): %s ",
				associationKey.getMetadata().isInverse(), associationKey.getMetadata().getAssociationKind() );

		//@todo refactor it. Method is too big and complex!
		if ( associationKey.getMetadata().isInverse() ) {
			if ( associationKey.getMetadata().getAssociationKind() == AssociationKind.ASSOCIATION ) {
				insertInverseRelationship( associationKey, association, associationContext );
				return;
			}
			else {
				return;
			}
		}

		IgniteCache<Object, BinaryObject> associationCache = provider.getAssociationCache( associationKey.getMetadata() );

		if ( associationKey.getMetadata().getAssociationKind() == AssociationKind.ASSOCIATION ) {
			Map<Object, BinaryObject> changedObjects = new HashMap<>();
			Set<Object> removedObjects = new HashSet<>();
			boolean thirdTableAssociation = IgniteAssociationSnapshot.isThirdTableAssociation( associationKey.getMetadata() );

			for ( AssociationOperation op : association.getOperations() ) {
				AssociationSnapshot associationSnapshot = association.getSnapshot();
				Tuple previousStateTuple = associationSnapshot.get( op.getKey() );
				Tuple currentStateTuple = op.getValue();
				Object previousId = previousStateTuple != null
						? ( (IgniteAssociationRowSnapshot) previousStateTuple.getSnapshot() ).getCacheKey()
								: null;
						if ( op.getType() == AssociationOperationType.CLEAR
								|| op.getType() == AssociationOperationType.REMOVE && !thirdTableAssociation ) {
							BinaryObject clearBo = associationCache.get( previousId );
							if ( clearBo != null ) {
								BinaryObjectBuilder clearBoBuilder = provider.createBinaryObjectBuilder( clearBo );
								for ( String columnName : associationKey.getColumnNames() ) {
									clearBoBuilder.removeField( columnName );
								}
								for ( String columnName : associationKey.getMetadata().getRowKeyIndexColumnNames() ) {
									clearBoBuilder.removeField( columnName );
								}
								changedObjects.put( previousId, clearBoBuilder.build() );
							}
						}
						else if ( op.getType() == AssociationOperationType.PUT ) {
							Object currentId = null;
							if ( currentStateTuple.getSnapshot().isEmpty() ) {
								currentId = provider.createAssociationKeyObject( op.getKey(), associationKey.getMetadata() );
							}
							else {
								currentId = ( (IgniteAssociationRowSnapshot) currentStateTuple.getSnapshot() ).getCacheKey();
							}
							BinaryObject putBo = previousId != null ? associationCache.get( previousId ) : null;
							BinaryObjectBuilder putBoBuilder = null;
							if ( putBo != null ) {
								boolean hasChanges = false;
								for ( String columnName : currentStateTuple.getColumnNames() ) {
									if ( associationKey.getMetadata().getAssociatedEntityKeyMetadata().getEntityKeyMetadata().isKeyColumn( columnName ) ) {
										continue;
									}
									hasChanges = Objects.equals( currentStateTuple.get( columnName ), putBo.field( columnName ) );
									if ( hasChanges ) {
										break;
									}
								}
								if ( !hasChanges ) { //vk: all changes already set. nothing to update
									continue;
								}
								putBoBuilder = provider.createBinaryObjectBuilder( putBo );
							}
							else {
								putBoBuilder = provider.createBinaryObjectBuilder( provider.getEntityTypeName( associationKey.getMetadata().getTable() ) );
							}
							for ( String columnName : currentStateTuple.getColumnNames() ) {
								if ( associationKey.getMetadata().getAssociatedEntityKeyMetadata().getEntityKeyMetadata().isKeyColumn( columnName ) ) {
									continue;
								}
								Object value = currentStateTuple.get( columnName );
								if ( value != null ) {
									putBoBuilder.setField( StringHelper.realColumnName( columnName ), value );
								}
								else {
									putBoBuilder.removeField( columnName );
								}
							}
							if ( previousId != null && !previousId.equals( currentId ) ) {
								removedObjects.add( previousId );
							}
							changedObjects.put( currentId, putBoBuilder.build() );
						}
						else if ( op.getType() == AssociationOperationType.REMOVE ) {
							removedObjects.add( previousId );
						}
						else {
							throw new UnsupportedOperationException( "AssociationOperation not supported: " + op.getType() );
						}
			}

			if ( !changedObjects.isEmpty() ) {
				associationCache.putAll( changedObjects );
			}
			if ( !removedObjects.isEmpty() ) {
				associationCache.removeAll( removedObjects );
			}
		}
		else if ( associationKey.getMetadata().getAssociationKind() == AssociationKind.EMBEDDED_COLLECTION ) {
			String indexColumnName = findIndexColumnName( associationKey.getMetadata() );
			boolean searchByValue = indexColumnName == null;
			Object id = ( (IgniteTupleSnapshot) associationContext.getEntityTuplePointer().getTuple().getSnapshot() ).getCacheKey();
			BinaryObject binaryObject = associationCache.get( id );
			Contracts.assertNotNull( binaryObject, "binaryObject" );
			String column = StringHelper.realColumnName( associationKey.getMetadata().getCollectionRole() );

			Object binaryObjects[] = binaryObject.field( column );
			List<BinaryObject> associationObjects = new ArrayList<>();
			if ( binaryObjects != null ) {
				for ( int i = 0; i < binaryObjects.length; i++ ) {
					associationObjects.add( (BinaryObject) binaryObjects[i] );
				}
			}

			EntityKeyMetadata itemMetadata = associationKey.getMetadata().getAssociatedEntityKeyMetadata().getEntityKeyMetadata();
			for ( AssociationOperation op : association.getOperations() ) {
				int index = findIndexByRowKey( associationObjects, op.getKey(), indexColumnName );
				switch ( op.getType() ) {
					case PUT:
						Tuple currentStateTuple = op.getValue();
						BinaryObjectBuilder putBoBuilder = provider.createBinaryObjectBuilder(
								provider.getEntityTypeName( itemMetadata.getTable() )
								);
						for ( String columnName : op.getKey().getColumnNames() ) {
							Object value = op.getKey().getColumnValue( columnName );
							if ( value != null ) {
								putBoBuilder.setField( StringHelper.stringAfterPoint( columnName ), value );
							}
						}
						for ( String columnName : itemMetadata.getColumnNames() ) {
							Object value = currentStateTuple.get( columnName );
							if ( value != null ) {
								putBoBuilder.setField( StringHelper.stringAfterPoint( columnName ), value );
							}
						}
						BinaryObject itemObject = putBoBuilder.build();
						if ( index >= 0 ) {
							associationObjects.set( index, itemObject );
						}
						else {
							associationObjects.add( itemObject );
						}
						break;
					case REMOVE:
						if ( index >= 0 ) {
							associationObjects.remove( index );
						}
						break;
					default:
						throw new HibernateException( "AssociationOperation not supported: " + op.getType() );
				}
			}

			BinaryObjectBuilder binaryObjectBuilder = provider.createBinaryObjectBuilder( binaryObject );
			binaryObjectBuilder.setField( column, associationObjects.toArray( new BinaryObject[ associationObjects.size() ] ) );
			binaryObject = binaryObjectBuilder.build();
			associationCache.put( id, binaryObject );
		}
	}
	private void removeInverseRelationship( AssociationKey associationKey, AssociationContext associationContext) {
		TuplePointer tuplePointer = getOwnerEntityTuplePointer( associationKey, associationContext );
		IgniteTupleSnapshot igniteTupleSnapshot = (IgniteTupleSnapshot) tuplePointer.getTuple().getSnapshot();
		boolean isReadThrough = associationContext.getAssociationTypeContext().getOptionsContext().getUnique( ReadThroughOption.class );
		if ( !isReadThrough ) {
			//not needs to add association info to link non owner entity
			return;
		}
		//need to remove link info
		IgniteCache<Object, BinaryObject> entityCache = provider.getEntityCache( igniteTupleSnapshot.getEntityKeyMetadata() );
		Object key = igniteTupleSnapshot.getCacheKey();
		BinaryObject lastEntityVersion = entityCache.get( key );
		String notOwnerLinkFieldName = associationKey.getMetadata().getCollectionRole();
		BinaryObjectBuilder builder = lastEntityVersion.toBuilder();
		LinkedHashSet associationIds = new LinkedHashSet();
		if ( lastEntityVersion.hasField( notOwnerLinkFieldName  ) ) {
			associationIds.addAll( lastEntityVersion.field( notOwnerLinkFieldName  ) );
			builder.removeField( notOwnerLinkFieldName );
			lastEntityVersion = builder.build();
			entityCache.put( key, lastEntityVersion );
		}
	}

	private void insertInverseRelationship( AssociationKey associationKey, Association association, AssociationContext associationContext) {
		TuplePointer tuplePointer = getOwnerEntityTuplePointer( associationKey, associationContext );
		IgniteTupleSnapshot igniteTupleSnapshot = (IgniteTupleSnapshot) tuplePointer.getTuple().getSnapshot();
		boolean isReadThrough = associationContext.getAssociationTypeContext().getOptionsContext().getUnique( ReadThroughOption.class );
		if ( !isReadThrough ) {
			//not needs to add association info to link non owner entity
			return;
		}
		//need to add link info
		IgniteCache<Object, BinaryObject> entityCache = provider.getEntityCache( igniteTupleSnapshot.getEntityKeyMetadata() );
		Object key = igniteTupleSnapshot.getCacheKey();
		BinaryObject lastEntityVersion = entityCache.get( key );
		String notOwnerLinkFieldName = associationKey.getMetadata().getCollectionRole();
		BinaryObjectBuilder builder = lastEntityVersion.toBuilder();
		LinkedHashSet associationIds = new LinkedHashSet();
		if ( lastEntityVersion.hasField( notOwnerLinkFieldName  ) ) {
			associationIds.addAll( lastEntityVersion.field( notOwnerLinkFieldName  ) );
		}
		for ( AssociationOperation op : association.getOperations() ) {
			if ( op.getType().equals( AssociationOperationType.PUT  ) ) {
				for ( RowKey rowKey : association.getKeys() ) {
					//@todo correct the magic number
					String fieldNameFromOwnerSide = rowKey.getColumnNames()[1];
					Object keyValueFromOwnerSide = rowKey.getColumnValue( fieldNameFromOwnerSide );
					associationIds.add( keyValueFromOwnerSide );
				}
			}
			else if ( op.getType().equals( AssociationOperationType.CLEAR  ) ) {
				associationIds.clear();
			}
			else if ( op.getType().equals( AssociationOperationType.REMOVE  ) ) {
				associationIds.clear();
				for ( RowKey rowKey : association.getKeys() ) {
					//RowKey[memberOf_jug_id=summer_camp, member_id=emmanuel]
					//@todo correct the magic number
					String fieldNameFromOwnerSide = rowKey.getColumnNames()[1];
					Object keyValueFromOwnerSide = rowKey.getColumnValue( fieldNameFromOwnerSide );
					associationIds.add( keyValueFromOwnerSide );
				}
			}
		}

		switch ( associationKey.getMetadata().getAssociationType() ) {
			case SET:
				lastEntityVersion = builder.setField( notOwnerLinkFieldName, associationIds ).build();
				break;
			case BAG:
				lastEntityVersion = builder.setField( notOwnerLinkFieldName, new ArrayList<>( associationIds ) )
				.build();
				break;
			default:
				throw new NotYetImplementedException( "Association Type " + associationKey.getMetadata()
				.getAssociationType() + " not implemented yet!" );
		}
		entityCache.put( key, lastEntityVersion );
	}

	/**
	 * Retrieve entity that contains the association, do not enhance with entity key
	 */
	protected TuplePointer getOwnerEntityTuplePointer(AssociationKey key, AssociationContext associationContext) {
		TuplePointer tuplePointer = associationContext.getEntityTuplePointer();

		if ( tuplePointer.getTuple() == null ) {
			tuplePointer.setTuple( getTuple( key.getEntityKey(), associationContext ) );
		}

		return tuplePointer;
	}


	private int findIndexByRowKey(List<BinaryObject> objects, RowKey rowKey, String indexColumnName) {
		int result = -1;

		if ( !objects.isEmpty() ) {
			String columnNames[] = indexColumnName == null ? rowKey.getColumnNames() : new String[] { indexColumnName };
			String fieldNames[] = new String[columnNames.length];
			for ( int i = 0; i < columnNames.length; i++ ) {
				fieldNames[i] = StringHelper.stringAfterPoint( columnNames[i] );
			}

			for ( int i = 0; i < objects.size() && result < 0; i++ ) {
				BinaryObject bo = objects.get( i );
				boolean thisIsIt = true;
				for ( int j = 0; j < columnNames.length; j++ ) {
					if ( !Objects.equals( rowKey.getColumnValue( columnNames[j] ), bo.field( fieldNames[j] ) ) ) {
						thisIsIt = false;
						break;
					}
				}
				if ( thisIsIt ) {
					result = i;
				}
			}
		}

		return result;
	}

	/**
	 * @param associationMetadata
	 * @return index column name for indexed embedded collections or null for collections without index
	 */
	private String findIndexColumnName(AssociationKeyMetadata associationMetadata) {
		String indexColumnName = null;
		if ( associationMetadata.getAssociationType() == AssociationType.SET
				|| associationMetadata.getAssociationType() == AssociationType.BAG ) {
			//			String cols[] =  associationMetadata.getColumnsWithoutKeyColumns(
			//									Arrays.asList( associationMetadata.getRowKeyColumnNames() )
			//							);
		}
		else {
			if ( associationMetadata.getRowKeyIndexColumnNames().length > 1 ) {
				throw new UnsupportedOperationException( "Multiple index columns not implemented yet" );
			}
			indexColumnName = associationMetadata.getRowKeyIndexColumnNames()[0];
		}

		return indexColumnName;
	}

	@Override
	public void removeAssociation(AssociationKey associationKey, AssociationContext associationContext) {
		log.debugf( "removeAssociation: associationKey: %s",associationKey );

		if ( associationKey.getMetadata().isInverse() ) {
			if ( associationKey.getMetadata().getAssociationKind() == AssociationKind.ASSOCIATION ) {
				removeInverseRelationship( associationKey,  associationContext );
				return;
			}
			else {
				return;
			}
		}

		IgniteCache<Object, BinaryObject> associationCache = provider.getAssociationCache( associationKey.getMetadata() );

		if ( associationKey.getMetadata().getAssociationKind() == AssociationKind.ASSOCIATION ) {
			QueryHints.Builder hintsBuilder = new QueryHints.Builder();
			Boolean isCollocated = associationContext.getAssociationTypeContext().getOptionsContext().getUnique( CollocatedAssociationOption.class );
			if ( isCollocated ) {
				throw new NotYetImplementedException();
				//				hintsBuilder.setAffinityRun( true );
				//				hintsBuilder.setAffinityKey( provider.createKeyObject( key ) );
			}
			QueryHints hints = hintsBuilder.build();

			if ( !IgniteAssociationSnapshot.isThirdTableAssociation( associationKey.getMetadata() ) ) {
				// clear reference
				Map<Object, BinaryObject> changedObjects = new HashMap<>();

				SqlFieldsQuery sqlQuery = provider.createSqlFieldsQueryWithLog( createAssociationQuery( associationKey, true ), hints, associationKey.getColumnValues() );
				Iterable<List<?>> list = executeWithHints( associationCache, sqlQuery, hints );
				for ( List<?> item : list ) {
					Object id = item.get( /* _KEY */ 0 );
					BinaryObject clearBo = (BinaryObject) item.get( /* _VALUE */ 1 );
					if ( clearBo != null ) {
						BinaryObjectBuilder clearBoBuilder = provider.createBinaryObjectBuilder( clearBo );
						for ( String columnName : associationKey.getMetadata().getRowKeyColumnNames() ) {
							clearBoBuilder.removeField( StringHelper.realColumnName( columnName ) );
						}
						changedObjects.put( id, clearBoBuilder.build() );
					}
				}

				if ( !changedObjects.isEmpty() ) {
					associationCache.putAll( changedObjects );
				}
			}
			else {
				// remove objects
				Set<Object> removedObjects = new HashSet<>();

				SqlFieldsQuery sqlQuery = provider.createSqlFieldsQueryWithLog( createAssociationQuery( associationKey, false ), hints, associationKey.getColumnValues() );
				Iterable<List<?>> list = executeWithHints( associationCache, sqlQuery, hints );
				for ( List<?> item : list ) {
					removedObjects.add( /* _KEY */ item.get( 0 ) );
				}

				if ( !removedObjects.isEmpty() ) {
					associationCache.removeAll( removedObjects );
				}
			}
		}
		else if ( associationKey.getMetadata().getAssociationKind() == AssociationKind.EMBEDDED_COLLECTION ) {
			Object id = ( (IgniteTupleSnapshot) associationContext.getEntityTuplePointer().getTuple().getSnapshot() ).getCacheKey();
			BinaryObject binaryObject = associationCache.get( id );
			if ( binaryObject != null ) {
				BinaryObjectBuilder binaryObjectBuilder = provider.createBinaryObjectBuilder( binaryObject );
				binaryObjectBuilder.removeField( associationKey.getMetadata().getCollectionRole() );
				binaryObject = binaryObjectBuilder.build();
				associationCache.put( id, binaryObject );
			}
			else {
				log.warnf( "Binary object with id %s from cache %s not exists", id, associationCache.getName() );
			}
		}
	}

	@Override
	public boolean isStoredInEntityStructure(AssociationKeyMetadata associationKeyMetadata, AssociationTypeContext associationTypeContext) {
		return false;
	}

	@Override
	public Number nextValue(NextValueRequest request) {
		log.debugf( "generate next value: %s",request );
		Long result = null;
		switch ( request.getKey().getMetadata().getType() ) {
			case TABLE:
				IgniteCache<String, Long> cache = provider.getIdSourceCache( request.getKey().getMetadata() );
				String idSourceKey = request.getKey().getColumnValue();
				Long previousValue = cache.get( idSourceKey );
				if ( previousValue == null ) {
					result = (long) request.getInitialValue();
					if ( !cache.putIfAbsent( idSourceKey, result ) ) {
						previousValue = (long) request.getInitialValue();
					}
				}
				if ( previousValue != null ) {
					while ( true ) {
						result = previousValue + request.getIncrement();
						if ( cache.replace( idSourceKey, previousValue, result ) ) {
							break;
						}
						else {
							previousValue = cache.get( idSourceKey );
						}
					}
				}
				break;
			case SEQUENCE:

				IgniteAtomicSequence seq = provider.atomicSequence( request.getKey().getMetadata().getName(), request.getInitialValue(), false );
				result = seq.getAndAdd( request.getIncrement() );
				break;
		}
		return result;
	}

	@Override
	public boolean supportsSequences() {
		return true;
	}

	@Override
	public void forEachTuple(ModelConsumer consumer, TupleTypeContext tupleTypeContext, EntityKeyMetadata entityKeyMetadata) {
		throw new UnsupportedOperationException( "forEachTuple() is not implemented" );
	}

	@Override
	public int executeBackendUpdateQuery(BackendQuery<IgniteQueryDescriptor> query, QueryParameters queryParameters, TupleContext tupleContext) {
		throw new UnsupportedOperationException( "executeBackendUpdateQuery() is not implemented" );
	}

	@Override
	public ClosableIterator<Tuple> executeBackendQuery(BackendQuery<IgniteQueryDescriptor> backendQuery, QueryParameters queryParameters,
			TupleContext tupleContext) {
		IgniteCache<Object, BinaryObject> cache;
		if ( backendQuery.getSingleEntityMetadataInformationOrNull() != null ) {
			cache = provider.getEntityCache( backendQuery.getSingleEntityMetadataInformationOrNull().getEntityKeyMetadata() );
		}
		else {
			throw new UnsupportedOperationException( "Not implemented. Can't find cache name" );
		}

		log.debugf( "execute query: %s", backendQuery.getQuery().getSql() );

		QueryHints hints = ( new QueryHints.Builder( queryParameters.getQueryHints() ) ).build();
		SqlFieldsQuery sqlQuery = provider.createSqlFieldsQueryWithLog(
				backendQuery.getQuery().getSql(),
				hints,
				backendQuery.getQuery().getIndexedParameters() != null ? backendQuery.getQuery().getIndexedParameters().toArray() : null );
		Iterable<List<?>> result = executeWithHints( cache, sqlQuery, hints );

		if ( backendQuery.getSingleEntityMetadataInformationOrNull() != null ) {
			return new IgnitePortableFromProjectionResultCursor( result, queryParameters.getRowSelection(),
					backendQuery.getSingleEntityMetadataInformationOrNull().getEntityKeyMetadata() );
		}
		else {
			throw new UnsupportedOperationException( "Not implemented yet" );
		}
	}

	private Iterable<List<?>> executeWithHints(IgniteCache<Object, BinaryObject> cache, SqlFieldsQuery sqlQuery, QueryHints hints) {
		Iterable<List<?>> result;

		if ( hints.isLocal() ) {
			if ( !provider.isClientMode() ) {
				sqlQuery.setLocal( true );
			}
		}
		if ( hints.isAffinityRun() ) {
			result = provider.affinityCall( cache.getName(), hints.getAffinityKey(), sqlQuery );
		}
		else {
			result = cache.query( sqlQuery );
		}

		return result;
	}

	@Override
	public ParameterMetadataBuilder getParameterMetadataBuilder() {
		return IgniteParameterMetadataBuilder.INSTANCE;
	}

	@Override
	public IgniteQueryDescriptor parseNativeQuery(String nativeQuery) {
		IgniteSqlQueryParser parser = new IgniteSqlQueryParser( nativeQuery );
		return parser.buildQueryDescriptor();
	}

	@Override
	public GridType overrideType(Type type) {
		return IgniteGridTypeMapper.INSTANCE.overrideType( type );
	}

	public org.apache.ignite.Ignite getIgnite() {
		return provider.getCacheManager();
	}

	private abstract class BaseResultCursor<T> implements ClosableIterator<Tuple> {

		private final Iterator<T> resultIterator;
		private final Integer maxRows;
		private int rowNum = 0;

		public BaseResultCursor(Iterable<T> resultCursor, RowSelection rowSelection) {
			this.resultIterator = resultCursor.iterator();
			this.maxRows = rowSelection.getMaxRows();
			iterateToFirst( rowSelection );
		}

		private void iterateToFirst(RowSelection rowSelection) {
			int firstRow = rowSelection.getFirstRow() != null ? rowSelection.getFirstRow() : 0;
			for ( int i = 0; i < firstRow && resultIterator.hasNext(); i++ ) {
				resultIterator.next();
			}
		}

		@Override
		public boolean hasNext() {
			return ( maxRows == null || rowNum < maxRows ) && resultIterator.hasNext();
		}

		@Override
		public Tuple next() {
			T value = resultIterator.next();
			rowNum++;
			return new Tuple( createTupleSnapshot( value ), SnapshotType.UPDATE );
		}

		abstract TupleSnapshot createTupleSnapshot(T value);

		@Override
		public void remove() {
			resultIterator.remove();
		}

		@Override
		public void close() {
		}
	}

	private class IgniteProjectionResultCursor extends BaseResultCursor<List<?>> {

		private final List<Return> queryReturns;

		public IgniteProjectionResultCursor(Iterable<List<?>> resultCursor, List<Return> queryReturns, RowSelection rowSelection) {
			super( resultCursor, rowSelection );
			this.queryReturns = queryReturns;
		}

		@Override
		TupleSnapshot createTupleSnapshot(List<?> value) {
			Map<String, Object> map = new HashMap<>();
			for ( int i = 0; i < value.size(); i++ ) {
				ScalarReturn ret = (ScalarReturn) queryReturns.get( i );
				map.put( ret.getColumnAlias(), value.get( i ) );
			}
			return new MapTupleSnapshot( map );
		}
	}

	private class IgnitePortableFromProjectionResultCursor extends BaseResultCursor<List<?>> {
		private final EntityKeyMetadata keyMetadata;

		public IgnitePortableFromProjectionResultCursor(Iterable<List<?>> resultCursor, RowSelection rowSelection, EntityKeyMetadata keyMetadata) {
			super( resultCursor, rowSelection );
			this.keyMetadata = keyMetadata;
		}

		@Override
		TupleSnapshot createTupleSnapshot(List<?> value) {
			return new IgniteTupleSnapshot( /* _KEY */ value.get( 0 ), /* _VAL */ (BinaryObject) value.get( 1 ), keyMetadata );
		}
	}
}

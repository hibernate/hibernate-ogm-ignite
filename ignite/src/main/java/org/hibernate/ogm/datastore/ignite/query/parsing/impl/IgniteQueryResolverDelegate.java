/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.query.parsing.impl;

import org.antlr.runtime.tree.Tree;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.hql.ast.common.JoinType;
import org.hibernate.hql.ast.origin.hql.resolve.path.PathedPropertyReference;
import org.hibernate.hql.ast.origin.hql.resolve.path.PathedPropertyReferenceSource;
import org.hibernate.hql.ast.origin.hql.resolve.path.PropertyPath;
import org.hibernate.hql.ast.spi.QueryResolverDelegate;
import org.hibernate.ogm.datastore.ignite.logging.impl.Log;
import org.hibernate.ogm.datastore.ignite.logging.impl.LoggerFactory;
import org.hibernate.type.AssociationType;
import org.hibernate.type.Type;

/**
 * Query resolver delegate targeting Ignite queries.
 *
 * @author Victor Kadachigov
 */
public class IgniteQueryResolverDelegate implements QueryResolverDelegate {

	private static final Log log = LoggerFactory.getLogger();

	private final IgnitePropertyHelper propertyHelper;
	private final SessionFactoryImplementor sessionFactory;

	private String currentAlias;
	private boolean definingSelect = false;


	IgniteQueryResolverDelegate(SessionFactoryImplementor sessionFactory, IgnitePropertyHelper propertyHelper) {
		this.sessionFactory = sessionFactory;
		this.propertyHelper = propertyHelper;
	}


	@Override
	public void registerPersisterSpace(Tree entityName, Tree alias) {
		propertyHelper.setRootEntity( entityName.getText() );
		propertyHelper.registerEntityAlias( entityName.getText(), alias.getText() );
	}


	@Override
	public void registerJoinAlias(Tree aliasNode, PropertyPath path) {
		String alias = aliasNode.getText();
		Type type = propertyHelper.getPropertyType(
			propertyHelper.getEntityNameByAlias( path.getFirstNode().getName() ),
			path.getNodeNamesWithoutAlias() );
		if ( type.isEntityType() ) {
			propertyHelper.registerEntityAlias( type.getName(), alias );
		}
		else if ( type.isAssociationType() ) {
			propertyHelper.registerEntityAlias(
				( (AssociationType) type ).getAssociatedEntityName( sessionFactory ), alias );
		}
		else {
			throw new IllegalArgumentException( "Failed to determine type for alias '" + alias + "'" );
		}
	}

	@Override
	public boolean isUnqualifiedPropertyReference() {
		return true;
	}

	@Override
	public PathedPropertyReferenceSource normalizeUnqualifiedPropertyReference(Tree property) {
		return normalizeUnqualifiedRoot( property );
	}

	@Override
	public boolean isPersisterReferenceAlias() {
		return currentAlias != null && isAlias( currentAlias );
	}

	@Override
	public PathedPropertyReferenceSource normalizeUnqualifiedRoot(Tree root) {
		String identifier = StringHelper.sqlNormalize( root.getText() );
		boolean isAlias = isAlias( identifier );
		return new PathedPropertyReference( isAlias ? identifier : root.getText(), null, isAlias );
	}

	private boolean isAlias(String identifier) {
		return propertyHelper.getEntityNameByAlias( identifier ) != null;
	}

	@Override
	public PathedPropertyReferenceSource normalizeQualifiedRoot(Tree root) {
		String entityNameForAlias = propertyHelper.getEntityNameByAlias( root.getText() );

		if ( entityNameForAlias == null ) {
			throw log.getUnknownAliasException( root.getText() );
		}

		return new PathedPropertyReference( root.getText(), null, true );
	}

	@Override
	public PathedPropertyReferenceSource normalizePropertyPathIntermediary(PropertyPath path, Tree propertyName) {
		return new PathedPropertyReference( propertyName.getText(), null, false );
	}

	@Override
	public PathedPropertyReferenceSource normalizeIntermediateIndexOperation(PathedPropertyReferenceSource propertyReferenceSource, Tree collectionProperty,
			Tree selector) {
		throw new UnsupportedOperationException( "Not implemented yet" );
	}

	@Override
	public void normalizeTerminalIndexOperation(PathedPropertyReferenceSource propertyReferenceSource, Tree collectionProperty, Tree selector) {
		throw new UnsupportedOperationException( "Not implemented yet" );
	}

	@Override
	public PathedPropertyReferenceSource normalizeUnqualifiedPropertyReferenceSource(Tree identifier394) {
		throw new UnsupportedOperationException( "Not implemented yet" );
	}

	@Override
	public PathedPropertyReferenceSource normalizePropertyPathTerminus(PropertyPath path, Tree propertyNameNode) {
		return new PathedPropertyReference( propertyNameNode.getText(), null, false );
	}

	@Override
	public void pushFromStrategy(JoinType joinType, Tree assosiationFetchTree, Tree propertyFetchTree, Tree alias) {
		this.currentAlias = alias.getText();
	}

	@Override
	public void pushSelectStrategy() {
		definingSelect = true;
	}

	@Override
	public void popStrategy() {
		definingSelect = false;
	}

	@Override
	public void propertyPathCompleted(PropertyPath path) {
		private final List<PropertyPath> selections = new ArrayList<>();
		// TODO: resolve selection path(s) in IgniteQueryTreeRenderer
		if ( definingSelect ) {
			propertyHelper.addSelectionPath( path );
		}
	}

}

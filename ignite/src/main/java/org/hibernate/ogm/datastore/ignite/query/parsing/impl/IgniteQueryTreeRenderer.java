/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.query.parsing.impl;

import java.util.ArrayDeque;
import java.util.Deque;

import org.antlr.runtime.TokenStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.hibernate.cfg.NotYetImplementedException;
import org.hibernate.hql.ast.origin.hql.parse.HQLParser;
import org.hibernate.hql.ast.origin.hql.resolve.path.PropertyPath;
import org.hibernate.hql.ast.spi.AstProcessor;
import org.hibernate.hql.ast.tree.PropertyPathTree;
import org.hibernate.ogm.datastore.ignite.util.StringHelper;

/**
 * Component which renders parts of Ignite SQL query, given JPQL query tree
 *
 * @author Aliaksandr Salauyou
 */
public class IgniteQueryTreeRenderer implements AstProcessor {

	private final IgnitePropertyHelper propertyHelper;
	private final IgniteQueryRenderer queryRenderer;

	// some property path near current position which
	// helps to determine parameter value type properly
	private PropertyPath typeDefiningPath;


	IgniteQueryTreeRenderer(IgnitePropertyHelper propertyHelper,
			IgniteQueryRenderer queryRenderer) {
		this.propertyHelper = propertyHelper;
		this.queryRenderer = queryRenderer;
	}


	private static Tree firstChildOfType(Tree parent, int type) {
		Tree node;
		for ( int i = 0; i < parent.getChildCount(); ++i ) {
			if ( ( node = parent.getChild( i ) ).getType() == type ) {
				return node;
			}
		}
		return null;
	}


	/**
	 * Flattens a subtree of kind "op[A, op[op[B, C], ...]]"
	 * (or similar) into "op[A, B, C, ...]". Useful for nodes
	 * representing multipart dis/conjunctions and arithmetic
	 * expressions to skip unnecessary parentheses
	 */
	private static void flattenSubtree(Tree node) {
		if ( firstChildOfType( node, node.getType() ) == null ) {
			return;
		}
		Deque<Tree> s = new ArrayDeque<>();
		Deque<Tree> m = new ArrayDeque<>();
		s.add( node );
		Tree n;
		while ( ( n = s.pollLast() ) != null ) {
			if ( n.getType() == node.getType() ) {
				for ( int i = 0; i < n.getChildCount(); ++i ) {
					s.add( n.getChild( i ) );
				}
			}
			else {
				m.add( n );
			}
		}
		for ( int i = 0; i < node.getChildCount(); ++i ) {
			node.setChild( i, m.pollLast() );
		}
		while ( ( n = m.pollLast() ) != null ) {
			node.addChild( n );
		}
	}


	@Override
	public CommonTree process(TokenStream tokens, CommonTree tree) {
		Tree querySpec = firstChildOfType( tree, HQLParser.QUERY_SPEC );
		StringBuilder from = new StringBuilder();
		Tree node = firstChildOfType( querySpec, HQLParser.SELECT_FROM );
		if ( node != null && ( node = firstChildOfType( node, HQLParser.FROM ) ) != null ) {
			processSubtree( from, node );
		}
		StringBuilder where = new StringBuilder();
		node = firstChildOfType( querySpec, HQLParser.WHERE );
		if ( node != null && node.getChildCount() > 0 ) {
			processSubtree( where, node.getChild( 0 ) );
		}
		StringBuilder orderBy = new StringBuilder();
		node = firstChildOfType( tree, HQLParser.ORDER_BY );
		if ( node != null ) {
			processSubtree( orderBy, node );
		}
		queryRenderer.from = from.toString();
		queryRenderer.where = where.toString();
		queryRenderer.orderBy = orderBy.toString();
		return tree;
	}


	private void processSubtree(StringBuilder builder, Tree node) {

		int type = node.getType();
		String operator = node.getText();

		switch ( type ) {
			case HQLParser.PATH:
				PropertyPath p = ( (PropertyPathTree) node ).getPropertyPath();
				String entityType;
				if ( p.getFirstNode().isAlias() ) {
					entityType = propertyHelper.getEntityNameByAlias( p.getFirstNode().getName() );
				}
				else {
					entityType = propertyHelper.getRootEntity();
				}
				PropertyIdentifier identifier = propertyHelper.getPropertyIdentifier( p, entityType );
				String columnName = StringHelper.realColumnName( identifier.getPropertyName() );
				builder.append( identifier.getAlias() ).append( '.' ).append( columnName );
				break;

			case HQLParser.NAMED_PARAM:  // :param
			case HQLParser.JPA_PARAM:    // ?1
				// TODO: add mappings parameter->type, not values, to allow query caching
				int n = queryRenderer.addParameterValue( node.getText(), typeDefiningPath );
				builder.append( '?' ).append( n );
				break;

			case HQLParser.PARAM:        // ?
				builder.append( '?' );
				break;

			case HQLParser.CONST_STRING_VALUE:
				processSubtree( builder, node.getChild( 0 ) );
				break;

			case HQLParser.CHARACTER_LITERAL:
			case HQLParser.STRING_LITERAL:
				StringHelper.escapeString( builder, operator, '\'' );
				break;

			case HQLParser.DECIMAL_LITERAL:
			case HQLParser.FLOATING_POINT_LITERAL:
			case HQLParser.HEX_LITERAL:
			case HQLParser.INTEGER_LITERAL:
			case HQLParser.OCTAL_LITERAL:
				builder.append( node.getText() );
				break;

			case HQLParser.UNARY_PLUS:
			case HQLParser.UNARY_MINUS:
				builder.append( operator );
				processSubtree( builder, node.getChild( 0 ) );
				break;

			case HQLParser.PLUS:
			case HQLParser.ASTERISK:
				flattenSubtree( node );
			case HQLParser.MINUS:
			case HQLParser.SOLIDUS:
				combineChildren( builder, node, operator, true );
				break;

			case HQLParser.EQUALS:
			case HQLParser.LESS:
			case HQLParser.GREATER:
			case HQLParser.LESS_EQUAL:
			case HQLParser.GREATER_EQUAL:
			case HQLParser.NOT_EQUAL:
				Tree path = firstChildOfType( node, HQLParser.PATH );
				if ( path != null ) {
					typeDefiningPath = ( (PropertyPathTree) path ).getPropertyPath();
				}
				processSubtree( builder, node.getChild( 0 ) );
				builder.append( operator );
				processSubtree( builder, node.getChild( 1 ) );
				typeDefiningPath = null;
				break;

			case HQLParser.IN:
				processIn( builder, node, false );
				break;

			case HQLParser.NOT_IN:
				processIn( builder, node, true );
				break;

			case HQLParser.LIKE:
				processLike( builder, node, false );
				break;

			case HQLParser.NOT_LIKE:
				processLike( builder, node, true );
				break;

			case HQLParser.BETWEEN:
				processBetween( builder, node, false );
				break;

			case HQLParser.NOT_BETWEEN:
				processBetween( builder, node, true );
				break;

			case HQLParser.IS_NULL:
				processIsNull( builder, node, false );
				break;

			case HQLParser.IS_NOT_NULL:
				processIsNull( builder, node, true );
				break;

			case HQLParser.OR:
				flattenSubtree( node );
				combineChildren( builder, node, " OR ", true );
				break;

			case HQLParser.AND:
				flattenSubtree( node );
				combineChildren( builder, node, " AND ", true );
				break;

			case HQLParser.UPPER:
			case HQLParser.LOWER:
			case HQLParser.CONCAT:
			case HQLParser.LENGTH:
			case HQLParser.SUBSTRING:
			case HQLParser.TRIM:
			case HQLParser.LOCATE:
			case HQLParser.ABS:
			case HQLParser.MOD:
			case HQLParser.SQRT:
			case HQLParser.COALESCE:
				// TODO: other functions supported by Ignite SQL
				builder.append( operator );
				combineChildren( builder, node, ", ", true );
				break;

			case HQLParser.NOT:
				node = node.getChild( 0 );
				switch ( node.getType() ) {
					case HQLParser.BETWEEN:
						processBetween( builder, node, true );
						break;
					case HQLParser.IS_NULL:
						processIsNull( builder, node, true );
						break;
					case HQLParser.IN:
						processIn( builder, node, true );
						break;
					case HQLParser.LIKE:
						processLike( builder, node, true );
						break;
					default:
						builder.append( " NOT (" );
						processSubtree( builder, node );
						builder.append( ')' );
				}
				break;

			case HQLParser.ORDER_BY:
				combineChildren( builder, node, ", ", false );
				break;

			case HQLParser.SORT_SPEC:
				processSubtree( builder, node.getChild( 0 ) );
				node = firstChildOfType( node, HQLParser.ORDER_SPEC );
				if ( node != null ) {
					builder.append( ' ' ).append( node.getText() );
				}
				break;

			case HQLParser.FROM:
				combineChildren( builder, node, ", ", false );
				break;

			case HQLParser.PERSISTER_SPACE:
				combineChildren( builder, node, "", false );
				break;

			case HQLParser.ENTITY_PERSISTER_REF:
				break;

			case HQLParser.PROPERTY_JOIN:
				throw new NotYetImplementedException( "Joins in JPQL queries" );

			case HQLParser.PERSISTER_JOIN:
				throw new NotYetImplementedException( "Conditional joins" );

			default:
				throw new NotYetImplementedException( node.getText() + " (type=" + node.getType() + ')' );
		}
	}


	private void processBetween(StringBuilder builder, Tree node, boolean negated) {
		Tree path = firstChildOfType( node, HQLParser.PATH );
		if ( path != null ) {
			typeDefiningPath = ( (PropertyPathTree) path ).getPropertyPath();
		}
		if ( negated ) {
			builder.append( '(' );
			isNullOrNot( builder, node.getChild( 0 ) );
		}
		else {
			processSubtree( builder, node.getChild( 0 ) );
		}
		builder.append( " BETWEEN " );
		node = node.getChild( 1 );
		processSubtree( builder, node.getChild( 0 ) );
		builder.append( " AND " );
		processSubtree( builder, node.getChild( 1 ) );
		if ( negated ) {
			builder.append( ')' );
		}
		typeDefiningPath = null;
	}


	private void processIn(StringBuilder builder, Tree node, boolean negated) {
		Tree path = firstChildOfType( node, HQLParser.PATH );
		if ( path != null ) {
			typeDefiningPath = ( (PropertyPathTree) path ).getPropertyPath();
		}
		if ( negated ) {
			builder.append( '(' );
			isNullOrNot( builder, node.getChild( 0 ) );
		}
		else {
			processSubtree( builder, node.getChild( 0 ) );
		}
		builder.append( " IN " );
		combineChildren( builder, node.getChild( 1 ), ",", true );
		if ( negated ) {
			builder.append( ')' );
		}
		typeDefiningPath = null;
	}


	private void processLike(StringBuilder builder, Tree node, boolean negated) {
		if ( negated ) {
			builder.append( '(' );
			isNullOrNot( builder, node.getChild( 0 ) );
		}
		else {
			processSubtree( builder, node.getChild( 0 ) );
		}
		builder.append( " LIKE " );
		processSubtree( builder, node.getChild( 1 ) );
		Tree escape = firstChildOfType( node, HQLParser.ESCAPE );
		if ( escape != null ) {
			builder.append( " ESCAPE " );
			processSubtree( builder, escape.getChild( 0 ) );
		}
		if ( negated ) {
			builder.append( ')' );
		}
	}


	private void processIsNull(StringBuilder builder, Tree node, boolean negated) {
		processSubtree( builder, node.getChild( 0 ) );
		builder.append( negated ? " IS NOT NULL" : " IS NULL" );
	}


	private void isNullOrNot(StringBuilder builder, Tree node) {
		StringBuilder buff = new StringBuilder();
		processSubtree( buff, node );
		builder.append( buff ).append( " IS NULL OR " ).append( buff ).append( " NOT" );
	}


	private void combineChildren(StringBuilder builder, Tree parent,
			String operator, boolean parentheses) {

		if ( parentheses ) {
			builder.append( '(' );
		}
		for ( int i = 0; i < parent.getChildCount(); ++i ) {
			if ( i > 0 ) {
				builder.append( operator );
			}
			processSubtree( builder, parent.getChild( i ) );
		}
		if ( parentheses ) {
			builder.append( ')' );
		}
	}

}

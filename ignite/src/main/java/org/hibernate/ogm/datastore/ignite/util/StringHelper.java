/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.util;

/**
 * Some string transformation methods
 *
 * @author Victor Kadachigov
 */
public class StringHelper {
	public static String realColumnName(String fieldName) {
		return fieldName.replace( '.', '_' );
	}

	public static String stringBeforePoint(String value) {
		String result = value;
		int index = result.indexOf( '.' );
		if ( index >= 0 ) {
			result = result.substring( 0, index );
		}
		return result;
	}

	public static String stringAfterPoint(String value) {
		String result = value;
		int index = result.indexOf( '.' );
		if ( index >= 0 ) {
			result = result.substring( index + 1 );
		}
		return result;
	}

	public static boolean isNotEmpty(String value) {
		return !isEmpty( value );
	}

	public static boolean isEmpty(String value) {
		return value == null || value.length() == 0;
	}

	public static void escapeString(StringBuilder builder, String input, char escapeChar) {
		builder.append( escapeChar );
		if ( input.contains( String.valueOf( escapeChar ) ) ) {
			for ( char c : input.toCharArray() ) {
				if ( c == escapeChar ) {
					builder.append( c );
				}
				builder.append( c );
			}
		}
		else {
			builder.append( input );
		}
		builder.append( escapeChar );
	}

	/**
	 * Normalizes an identifier to make it SQL-safe,
	 * e. g. "&lt;gen_0&gt;" -&gt; "_gen_0_"
	 * @param identifier - identifier
	 * @return normalized identifier
	 */
	public static String sqlNormalize(String identifier) {
		StringBuilder sb = new StringBuilder( identifier );
		for ( int i = 0; i < sb.length(); i++ ) {
			char c = sb.charAt( i );
			if ( c == '_'
				|| ( c >= 'A' && c <= 'Z') || ( c >= 'a' && c <= 'z' )
				|| ( i > 0 && c >= '0' && c <= '9' ) ) {
				continue;
			}
			sb.setCharAt( i, '_' );
		}
		return sb.toString();
	}
}

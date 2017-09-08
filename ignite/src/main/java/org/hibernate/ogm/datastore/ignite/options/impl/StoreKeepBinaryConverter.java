/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.options.impl;

import org.hibernate.ogm.datastore.ignite.options.StoreKeepBinary;
import org.hibernate.ogm.options.spi.AnnotationConverter;
import org.hibernate.ogm.options.spi.OptionValuePair;

/**
 * @author Sergey Chernolyas &amp;sergey_chernolyas@gmail.com&amp;
 */
public class StoreKeepBinaryConverter implements AnnotationConverter<StoreKeepBinary> {

	@Override
	public OptionValuePair<?> convert(StoreKeepBinary annotation) {
		Boolean value = annotation.value();
		return OptionValuePair.getInstance( new StoreKeepBinaryOption(), value );
	}
}

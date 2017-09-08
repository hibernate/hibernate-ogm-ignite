/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.options;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.hibernate.ogm.datastore.ignite.options.impl.ReadThroughConverter;
import org.hibernate.ogm.options.spi.MappingOption;

/**
 * Specified that the entity supports paradigm "read/write through".
 * @author Sergey Chernolyas &amp;sergey_chernolyas@gmail.com&amp;
 */
@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
@MappingOption(ReadThroughConverter.class)
public @interface ReadThrough {
	boolean value() default true;
}

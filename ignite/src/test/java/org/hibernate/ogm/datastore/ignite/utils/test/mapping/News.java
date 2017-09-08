/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.utils.test.mapping;

import java.util.Objects;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;

/**
 * @author Sergey Chernolyas &amp;sergey_chernolyas@gmail.com&amp;
 */
@Entity
public class News {
	@EmbeddedId
	private NewsId id;

	private  String content;

	public NewsId getId() {
		return id;
	}

	public void setId(NewsId id) {
		this.id = id;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	@Override
	public boolean equals(Object o) {
		if ( this == o ) {
			return true;
		}
		if ( o == null || getClass() != o.getClass() ) {
			return false;
		}
		News news = (News) o;
		return Objects.equals( id, news.id ) &&
				Objects.equals( content, news.content );
	}

	@Override
	public int hashCode() {
		return Objects.hash( id, content );
	}
}

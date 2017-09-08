/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.utils.test.associations;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.ManyToOne;

import org.hibernate.ogm.datastore.ignite.options.CacheStoreFactory;
import org.hibernate.ogm.datastore.ignite.options.ReadThrough;
import org.hibernate.ogm.datastore.ignite.options.StoreKeepBinary;
import org.hibernate.ogm.datastore.ignite.options.WriteThrough;

/**
 * @author Sergey Chernolyas &amp;sergey_chernolyas@gmail.com&amp;
 */
@Entity
@ReadThrough
@WriteThrough
@StoreKeepBinary
@CacheStoreFactory(HallBinaryStore.class)
public class CacheStoreHall {
	private String id;
	private CacheStoreJUG jug;

	public CacheStoreHall() {
	}

	public CacheStoreHall(String id) {
		this.id = id;
	}

	@Id
	@Column(name = "hall_id")
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@ManyToOne
	public CacheStoreJUG getJug() {
		return jug;
	}

	public void setJug(CacheStoreJUG jug) {
		this.jug = jug;
	}
}

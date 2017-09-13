/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.utils.test.transaction;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.ogm.datastore.ignite.IgniteProperties;
import org.hibernate.ogm.utils.OgmTestCase;

import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

/**
 * Local transaction testing
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class LocalTransactionTest extends OgmTestCase {

	@Test
	public void insertAssociations() {
		Session session = openSession();
		Transaction transaction = session.beginTransaction();
		//insert garage
		Garage garage = new Garage();
		garage.setId( 1L );
		garage.setTitle( "Test garage" );
		session.persist( garage );
		//insert cars
		Car car1 = new Car();
		car1.setId( 1L );
		car1.setTitle( "Maserati Birdcage" );
		session.persist( car1 );

		Car car2 = new Car();
		car2.setId( 2L );
		car2.setTitle( "Ferrari 488 Spider" );
		session.persist( car2 );
		//add associations

		List<Car> cars = new LinkedList<>();
		cars.add( car1 );
		cars.add( car2 );
		garage.setCars( cars );
		for ( Car car : cars ) {
			car.setGarage( garage );
			session.update( car );
		}
		session.update( garage );

		transaction.commit();
		session.clear();

		// checking
		Transaction transaction1 = session.beginTransaction();
		garage = (Garage) session.get( Garage.class, 1L );
		assertThat( garage ).isNotNull();
		assertThat( garage.getCars() ).isNotNull();
		assertThat( garage.getCars().size() ).isEqualTo( 2 );
		assertThat( garage.getCars() ).contains( car1, car2 );
		transaction1.commit();
	}


	/* (non-Javadoc)
	 * @see org.hibernate.ogm.utils.OgmTestCase#getAnnotatedClasses()
	 */
	@Override
	protected Class<?>[] getAnnotatedClasses() {
		// TODO Auto-generated method stub
		return new Class[] { Car.class, Garage.class };
	}

	/* (non-Javadoc)
	 * @see org.hibernate.ogm.utils.OgmTestCase#configure(java.util.Map)
	 */
	@Override
	protected void configure(Map<String, Object> settings) {
		settings.put( IgniteProperties.IGNITE_ALLOWS_TRANSACTION_EMULATION, Boolean.TRUE );
	}


}

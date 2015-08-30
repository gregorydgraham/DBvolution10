/*
 * Copyright 2015 gregorygraham.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nz.co.gregs.dbvolution.reflection;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author gregorygraham
 */
public class DataModelTest {
	
	public DataModelTest() {
	}

	@Test
	public void testGetDatabases() {
		System.out.println("getDatabases");
		Set<Class<? extends DBDatabase>> result = DataModel.getUseableDBDatabaseClasses();
		for (Class<? extends DBDatabase> result1 : result) {
			System.out.println("DBDatabase: "+result1.getName());
		}
		Assert.assertThat(result.size(), is(9));
	}
	
	@Test
	public void testGetDBDatabaseConstructors() {
		System.out.println("getDBDatabaseConstructors");
		Set<Constructor<DBDatabase>> result = DataModel.getDBDatabaseConstructors();
		for (Constructor<DBDatabase> result1 : result) {
			System.out.println("Constructor: "+result1.getName());
		}
		Assert.assertThat(result.size(), is(9));
	}

	@Test
	public void testGetDBDatabaseConstructorsPublicWithoutParameters() {
		System.out.println("getDBDatabaseConstructorsPublicWithoutParameters");
		Set<Constructor<DBDatabase>> result = DataModel.getDBDatabaseConstructorsPublicWithoutParameters();
		for (Constructor<DBDatabase> constr : result) {
			try {
				constr.setAccessible(true);
				System.out.println("DBDatabase Constructor: "+constr.getName());
				DBDatabase newInstance = constr.newInstance();
				Assert.assertThat(newInstance, instanceOf(DBDatabase.class));
				System.out.println("DBDatabase created: "+newInstance.getClass().getCanonicalName());
			} catch (InstantiationException ex) {
				Logger.getLogger(DataModelTest.class.getName()).log(Level.SEVERE, null, ex);
			} catch (IllegalAccessException ex) {
				Logger.getLogger(DataModelTest.class.getName()).log(Level.SEVERE, null, ex);
			} catch (IllegalArgumentException ex) {
				Logger.getLogger(DataModelTest.class.getName()).log(Level.SEVERE, null, ex);
			} catch (InvocationTargetException ex) {
				Logger.getLogger(DataModelTest.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
		Assert.assertThat(result.size(), is(8));
	}

	@Test
	public void testGetDBRowClasses() {
		System.out.println("getDBRowClasses");
		Set<Class<? extends DBRow>> result = DataModel.getDBRowClasses();
		Assert.assertThat(result.size(), is(184));
	}
	
	@Test
	public void testGetDBDatabaseCreationMethodsStaticWithoutParameters(){
		System.out.println("getDBDatabaseCreationMethodsWithoutParameters");
		List<Method> dbDatabaseCreationMethods = DataModel.getDBDatabaseCreationMethodsStaticWithoutParameters();
		for (Method creator : dbDatabaseCreationMethods) {
			System.out.println("Creator: "+creator.getDeclaringClass().getCanonicalName()+"."+creator.getName()+"()");
			creator.setAccessible(true);
			try {
				DBDatabase database = (DBDatabase)creator.invoke(null);
				System.out.println("DBDatabase Created: "+database);
			} catch (IllegalAccessException ex) {
				Assert.fail("Unable to invoke "+creator.getDeclaringClass().getCanonicalName()+"."+creator.getName()+"()");
			} catch (IllegalArgumentException ex) {
				Assert.fail("Unable to invoke "+creator.getDeclaringClass().getCanonicalName()+"."+creator.getName()+"()");
			} catch (InvocationTargetException ex) {
				Assert.fail("Unable to invoke "+creator.getDeclaringClass().getCanonicalName()+"."+creator.getName()+"()");
			}
		}
		Assert.assertThat(dbDatabaseCreationMethods.size(), is(2));
	}
	
}

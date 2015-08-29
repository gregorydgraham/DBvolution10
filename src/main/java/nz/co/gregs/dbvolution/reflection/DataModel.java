/*
 * Copyright 2015 gregory.graham.
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javassist.Modifier;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import org.reflections.Reflections;

/**
 *
 * @author gregory.graham
 */
public class DataModel {

	private DataModel() {
	}

	protected static Set<Class<? extends DBDatabase>> getUseableDBDatabaseClasses() {
		Reflections reflections = new Reflections("");
		final Set<Class<? extends DBDatabase>> allKnownDBDatabases = reflections.getSubTypesOf(DBDatabase.class);

		final Set<Class<? extends DBDatabase>> usefulDBDatabases = new HashSet<Class<? extends DBDatabase>>();
		for (Class<? extends DBDatabase> known : allKnownDBDatabases) {
			if (!known.getPackage().getName().equals("nz.co.gregs.dbvolution.databases")) {
				usefulDBDatabases.add(known);
			}
		}
		return usefulDBDatabases;
	}

	public static HashSet<Constructor<DBDatabase>> getDBDatabaseConstructors() {
		HashSet<Constructor<DBDatabase>> constructors = new HashSet<Constructor<DBDatabase>>();
		final Set<Class<? extends DBDatabase>> allKnownDBDatabases = getUseableDBDatabaseClasses();
		for (Class<? extends DBDatabase> aDatabase : allKnownDBDatabases) {
			@SuppressWarnings("unchecked")
			Constructor<DBDatabase>[] cons = (Constructor<DBDatabase>[]) aDatabase.getDeclaredConstructors();
			constructors.addAll(Arrays.asList(cons));
		}
		return constructors;
	}

	public static List<Method> getDBDatabaseCreationMethods() {
		List<Method> allMethods = new ArrayList<Method>();
		final Set<Class<? extends DBDatabase>> allKnownDBDatabases = getUseableDBDatabaseClasses();
		for (Class<? extends DBDatabase> aDatabase : allKnownDBDatabases) {
			@SuppressWarnings("unchecked")
			Method[] meths = aDatabase.getDeclaredMethods();
			for (Method meth : meths) {
				// weed out the clone methods, as they copy not create DBDatabases
				if (!(meth.getName().equals("clone") && getMethodParameterCount(meth) == 0)) {
					allMethods.add(meth);
				}
			}
		}
		return allMethods;
	}

	private static int getMethodParameterCount(Method meth) {
		return meth.getParameterTypes().length;
	}

	public static List<Method> getDBDatabaseCreationMethodsStaticWithoutParameters() {
		List<Method> creationMethods = new ArrayList<Method>();
		for (Method meth : getDBDatabaseCreationMethods()) {
			if (DBDatabase.class.isAssignableFrom(meth.getReturnType())) {
				if ((getMethodParameterCount(meth) == 0) && (Modifier.isStatic(meth.getModifiers()))) {
					creationMethods.add(meth);
				}
			}
		}
		return creationMethods;
	}

	public static Set<Constructor<DBDatabase>> getDBDatabaseConstructorsPublicWithoutParameters() {
		HashSet<Constructor<DBDatabase>> constructors = getDBDatabaseConstructors();
		Set<Constructor<DBDatabase>> parameterlessConstructors = new HashSet<Constructor<DBDatabase>>();
		for (Constructor<DBDatabase> constructor : constructors) {
			if (constructor.getParameterTypes().length == 0 && Modifier.isPublic(constructor.getModifiers())) {
				parameterlessConstructors.add(constructor);
			}
		}
		return parameterlessConstructors;
	}

	public static List<DBDatabase> getDBDatabaseInstancesWithoutParameters() {
		ArrayList<DBDatabase> databaseInstances = new ArrayList<DBDatabase>();
		Set<Constructor<DBDatabase>> constructors = getDBDatabaseConstructorsPublicWithoutParameters();
		for (Constructor<DBDatabase> constr : constructors) {
			constr.setAccessible(true);
			try {
				DBDatabase newInstance = constr.newInstance();
				databaseInstances.add(newInstance);
			} catch (InstantiationException ex) {
				Logger.getLogger(DataModel.class.getName()).log(Level.SEVERE, null, ex);
			} catch (IllegalAccessException ex) {
				Logger.getLogger(DataModel.class.getName()).log(Level.SEVERE, null, ex);
			} catch (IllegalArgumentException ex) {
				Logger.getLogger(DataModel.class.getName()).log(Level.SEVERE, null, ex);
			} catch (InvocationTargetException ex) {
				Logger.getLogger(DataModel.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
		return databaseInstances;
	}

	public static Set<Class<? extends DBRow>> getDBRowClasses() {
		Reflections reflections = new Reflections("");
		return reflections.getSubTypesOf(DBRow.class);
	}

	public static List<DBRow> getDBRowInstances() {
		List<DBRow> dbrows = new ArrayList<DBRow>();
		Set<Class<? extends DBRow>> dbRowClasses = getDBRowClasses();
		for (Class<? extends DBRow> dbRowClass : dbRowClasses) {
			try {
				DBRow newInstance = dbRowClass.newInstance();
				dbrows.add(newInstance);
			} catch (InstantiationException ex) {
				Logger.getLogger(DataModel.class.getName()).log(Level.SEVERE, null, ex);
			} catch (IllegalAccessException ex) {
				Logger.getLogger(DataModel.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
		return dbrows;
	}

}

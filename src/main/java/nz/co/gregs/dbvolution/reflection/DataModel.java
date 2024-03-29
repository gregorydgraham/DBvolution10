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
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import javassist.Modifier;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.internal.properties.RowDefinitionWrapperFactory;
import org.reflections.Reflections;

/**
 * Provides convenient access to classes, instances, and methods that may be of
 * use during reflection.
 *
 * <p>
 * This is very experimental at the moment, use with caution.
 *
 * <p>
 * The intent of this class is to provide access to methods that might define
 * database connections, the schema of those databases, and methods to
 * manipulate the schema objects in a generic way.
 *
 * @author gregory.graham
 */
public class DataModel {

	private static Set<DBRow> storedRequiredTables;

	private DataModel() {
	}

	/**
	 * Scans all known classes and returns a set of all non-builtin DBDatabase
	 * instances that could be found.
	 *
	 * <p>
	 * Excludes the standard DBDatabases in nz.co.gregs.dbvolution.databases.
	 *
	 * <p>
	 * The intent of this method is to help provide access to classes that might
	 * define database connections.
	 *
	 * @return a set of {@link DBDatabase} classes.
	 */
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

	/**
	 * Scans all known classes and returns a set of all builtin DBDatabase
	 * instances that could be found.
	 *
	 * <p>
	 * Only includes the standard DBDatabases in nz.co.gregs.dbvolution.databases.
	 *
	 * <p>
	 * The intent of this method is to help provide access to classes that might
	 * define database connections.
	 *
	 * @return a set of {@link DBDatabase} classes.
	 */
	protected static Set<Class<? extends DBDatabase>> getBuiltinDBDatabaseClasses() {
		Reflections reflections = new Reflections("");
		final Set<Class<? extends DBDatabase>> allKnownDBDatabases = reflections.getSubTypesOf(DBDatabase.class);

		final Set<Class<? extends DBDatabase>> usefulDBDatabases = new HashSet<Class<? extends DBDatabase>>();
		for (Class<? extends DBDatabase> known : allKnownDBDatabases) {
			if (known.getPackage().getName().equals("nz.co.gregs.dbvolution.databases")) {
				usefulDBDatabases.add(known);
			}
		}
		return usefulDBDatabases;
	}

	/**
	 * Scans all known classes and returns a set of all builtin DBDatabase
	 * instances that could be found.
	 *
	 * <p>
	 * Only includes the standard DBDatabases in nz.co.gregs.dbvolution.databases.
	 *
	 * <p>
	 * The intent of this method is to help provide access to classes that might
	 * define database connections.
	 *
	 * @return a set of {@link DBDatabase} classes.
	 */
	protected static Set<Class<? extends DBDatabase>> getAllDBDatabaseClasses() {
		Reflections reflections = new Reflections("");
		final Set<Class<? extends DBDatabase>> allKnownDBDatabases = reflections.getSubTypesOf(DBDatabase.class);

		final Set<Class<? extends DBDatabase>> usefulDBDatabases = new HashSet<Class<? extends DBDatabase>>();
		allKnownDBDatabases.forEach(known -> {
			usefulDBDatabases.add(known);
		});
		return usefulDBDatabases;
	}

	/**
	 * Finds the constructors for
	 * {@link #getUseableDBDatabaseClasses() all known databases}.
	 *
	 * @return a set of all constructors for all the known databases.
	 */
	public static Set<Constructor<DBDatabase>> getDBDatabaseConstructors() {
		Set<Constructor<DBDatabase>> constructors = new HashSet<Constructor<DBDatabase>>();
		final Set<Class<? extends DBDatabase>> allKnownDBDatabases = getUseableDBDatabaseClasses();
		for (Class<? extends DBDatabase> aDatabase : allKnownDBDatabases) {
			@SuppressWarnings("unchecked")
			Constructor<DBDatabase>[] cons = (Constructor<DBDatabase>[]) aDatabase.getDeclaredConstructors();
			constructors.addAll(Arrays.asList(cons));
		}
		return constructors;
	}

	/**
	 * Scans across {@link #getUseableDBDatabaseClasses() all known databases} and
	 * finds no-parameter methods that return a DBDatabase object.
	 *
	 * <p>
	 * The intent of this method is to find DBDatabase subclasses that define
	 * standard DBDatabase factory methods.
	 *
	 * <p>
	 * For instance a useful pattern for your databases during development might
	 * be something like this:
	 * <p>
	 * <code>
	 * private static class TestPostgreSQL extends PostgresDB {<br>
	 * <br>
	 * protected static PostgresDB getTestDatabaseOnHost(String hostname) {<br>
	 * return new PostgresDB(hostname, 5432, "testdb", "developer", "developer
	 * password");<br>
	 * }<br>
	 * <br>
	 * protected static PostgresDB getTestDatabase() {<br>
	 * return new PostgresDB("testdb.mycompany.com", 5432, "testdb", "developer",
	 * "developer password");<br>
	 * }<br>
	 * <br>
	 * protected static PostgresDB getLocalTestDatabase() {<br>
	 * return new PostgresDB("localhost", 5432, "testdb", "developer", "developer
	 * password");<br>
	 * }<br>
	 * }<br>
	 * <br>
	 * </code>
	 *
	 * <p>
	 * This method aims to find the getTestDatabase and getLocalTestDatabase
	 * methods. The method getTestDatabaseOnHost(String) is excluded as it
	 * requires a parameter to be supplied.
	 *
	 * @return a list of methods defined in DBDatabase classes that return a
	 * DBDatabase and require no parameters.
	 */
	public static List<Method> getDBDatabaseSimpleCreationMethods() {
		List<Method> allMethods = new ArrayList<Method>();
		List<Method> meths = getDBDatabaseCreationMethods();
		for (Method meth : meths) {
			// weed out the clone methods, as they copy not create DBDatabases
			if (getMethodParameterCount(meth) == 0) {
				allMethods.add(meth);
			}
		}
		return allMethods;
	}

	/**
	 * Scans across {@link #getUseableDBDatabaseClasses() all known databases} and
	 * finds methods that return a DBDatabase object.
	 *
	 * <p>
	 * The intent of this method is to find DBDatabase subclasses that define
	 * standard DBDatabase factory methods.
	 *
	 * <p>
	 * For instance a useful pattern for your databases during development might
	 * be something like this:
	 * <p>
	 * <code>
	 * private static class TestPostgreSQL extends PostgresDB {<br>
	 * <br>
	 * protected static PostgresDB getTestDatabaseOnHost(String hostname) {<br>
	 * return new PostgresDB(hostname, 5432, "testdb", "developer", "developer
	 * password");<br>
	 * }<br>
	 * <br>
	 * protected PostgresDB getTestDatabase() {<br>
	 * return new PostgresDB("testdb.mycompany.com", 5432, "testdb", "developer",
	 * "developer password");<br>
	 * }<br>
	 * <br>
	 * protected static PostgresDB getLocalTestDatabase() {<br>
	 * return new PostgresDB("localhost", 5432, "testdb", "developer", "developer
	 * password");<br>
	 * }<br>
	 * }<br>
	 * <br>
	 * </code>
	 *
	 * <p>
	 * This method aims to find the getTestDatabaseOnHost(String),
	 * getTestDatabase(), getLocalTestDatabase() methods.
	 *
	 * @return a list of methods defined in DBDatabase classes that return a
	 * DBDatabase.
	 */
	public static List<Method> getDBDatabaseCreationMethods() {
		List<Method> allMethods = new ArrayList<Method>();
		final Set<Class<? extends DBDatabase>> allKnownDBDatabases = getUseableDBDatabaseClasses();
		for (Class<? extends DBDatabase> aDatabase : allKnownDBDatabases) {
			@SuppressWarnings("unchecked")
			Method[] meths = aDatabase.getDeclaredMethods();
			for (Method meth : meths) {
				// weed out the clone methods, as they copy not create DBDatabases
				if (!(meth.getName().equals("clone"))
						&& DBDatabase.class.isAssignableFrom(meth.getReturnType())) {
					allMethods.add(meth);
				}
			}
		}
		return allMethods;
	}

	private static int getMethodParameterCount(Method meth) {
		return meth.getParameterTypes().length;
	}

	/**
	 * Scans the classpath for DBDatabase subclasses and returns all static
	 * parameter-less methods that produce DBDatabase objects.
	 *
	 * <p>
	 * The intent of this method is to find the easiest to use DBDatabase creation
	 * methods. That is DBDatabase creating methods that require no state or
	 * parameters.
	 *
	 * <p>
	 * For instance a useful pattern for your databases during development might
	 * be something like this:
	 * <p>
	 * <code>
	 * private static class TestPostgreSQL extends PostgresDB {<br>
	 * <br>
	 * protected static PostgresDB getTestDatabaseOnHost(String hostname) {<br>
	 * return new PostgresDB(hostname, 5432, "testdb", "developer", "developer
	 * password");<br>
	 * }<br>
	 * <br>
	 * protected PostgresDB getTestDatabase() {<br>
	 * return new PostgresDB("testdb.mycompany.com", 5432, "testdb", "developer",
	 * "developer password");<br>
	 * }<br>
	 * <br>
	 * protected static PostgresDB getLocalTestDatabase() {<br>
	 * return new PostgresDB("localhost", 5432, "testdb", "developer", "developer
	 * password");<br>
	 * }<br>
	 * }<br>
	 * <br>
	 * </code>
	 *
	 * <p>
	 * This method aims to find the getLocalTestDatabase() method.
	 *
	 * @return a list of static methods with no parameters defined in DBDatabase
	 * classes that return a DBDatabase.
	 */
	public static List<Method> getDBDatabaseCreationMethodsStaticWithoutParameters() {
		List<Method> creationMethods = new ArrayList<Method>();
		for (Method meth : getDBDatabaseSimpleCreationMethods()) {
			if (DBDatabase.class.isAssignableFrom(meth.getReturnType())) {
				if ((getMethodParameterCount(meth) == 0) && (Modifier.isStatic(meth.getModifiers()))) {
					creationMethods.add(meth);
				}
			}
		}
		return creationMethods;
	}

	/**
	 * Scan the classpath for DBDatabase subclasses with public parameterless
	 * constructors and return those constructors.
	 *
	 * <p>
	 * The intent of this method is to find easy to use constructors that MAY
	 * directly create a usable DBDatabase object.
	 *
	 * @return a list of easy to invoke DBDatabase constructors
	 */
	public static Set<Constructor<DBDatabase>> getDBDatabaseConstructorsPublicWithoutParameters() {
		Set<Constructor<DBDatabase>> constructors = getDBDatabaseConstructors();
		Set<Constructor<DBDatabase>> parameterlessConstructors = new HashSet<Constructor<DBDatabase>>();
		for (Constructor<DBDatabase> constructor : constructors) {
			if (constructor.getParameterTypes().length == 0 && Modifier.isPublic(constructor.getModifiers())) {
				parameterlessConstructors.add(constructor);
			}
		}
		return parameterlessConstructors;
	}

	/**
	 * Using {@link #getDBDatabaseConstructorsPublicWithoutParameters() } creates
	 * instances of the accessible DBDatabases.
	 *
	 * @return all the instances that can be created from the constructors found
	 * by {@link #getDBDatabaseConstructorsPublicWithoutParameters() }.
	 */
	public static List<DBDatabase> getDBDatabaseInstancesWithoutParameters() {
		ArrayList<DBDatabase> databaseInstances = new ArrayList<DBDatabase>();
		Set<Constructor<DBDatabase>> constructors = getDBDatabaseConstructorsPublicWithoutParameters();
		for (Constructor<DBDatabase> constr : constructors) {
			constr.setAccessible(true);
			try {
				DBDatabase newInstance = constr.newInstance();
				databaseInstances.add(newInstance);
			} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
				Logger.getLogger(DataModel.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
		return databaseInstances;
	}

	/**
	 * Find all DBRow subclasses on the current classpath.
	 *
	 * @return all the subclasses of DBRow in the current classpath.
	 */
	public static Set<Class<? extends DBRow>> getDBRowSubclasses() {
		Reflections reflections = new Reflections("");
		return reflections.getSubTypesOf(DBRow.class);
	}

	/**
	 * Find all DBRow subclasses on the current classpath.
	 *
	 * @return all the subclasses of DBRow in the current classpath.
	 */
	public static Set<Class<? extends DBRow>> getDBRowDirectSubclasses() {
		Set<Class<? extends DBRow>> result = new HashSet<Class<? extends DBRow>>();
		Reflections reflections = new Reflections("");
		Set<Class<? extends DBRow>> subTypesOf = reflections.getSubTypesOf(DBRow.class);
		for (Class<? extends DBRow> clzz : subTypesOf) {
			try {
				clzz.getConstructor();// checking that there an appropriate constructor
				if (clzz.getGenericSuperclass().equals(DBRow.class)) {
					result.add(clzz);
				}
			} catch (NoSuchMethodException | SecurityException ex) {
				;// no constructor
			}
		}
		return result;
	}

	/**
	 * Find all DBRow subclasses on the current classpath.
	 *
	 * @return all the subclasses of DBRow in the current classpath.
	 */
	public synchronized static Set< DBRow> getRequiredTables() {
		if (storedRequiredTables == null || storedRequiredTables.isEmpty()) {
			Set< DBRow> result = new HashSet<>(0);
			Set<Class<? extends DBRow>> dbRowDirectSubclasses = getDBRowDirectSubclasses();
			for (Class<? extends DBRow> clzz : dbRowDirectSubclasses) {
				DBRow dbRow = DBRow.getDBRow(clzz);
				if (dbRow.isRequiredTable()) {
					result.add(dbRow);
				}
			}
			storedRequiredTables = result;
		}
		return new HashSet<DBRow>(storedRequiredTables);
	}

	/**
	 * Find all DBRow subclasses on the current classpath, minus the example
	 * classes found in DBvolution.
	 *
	 * @return all the subclasses of DBRow in the current classpath, except DBV's
	 * examples.
	 */
	public static Set<Class<? extends DBRow>> getDBRowClassesExcludingDBvolutionExamples() {
		Set<Class<? extends DBRow>> dbRowClasses = getDBRowSubclasses();
		HashSet<Class<? extends DBRow>> returnSet = new HashSet<Class<? extends DBRow>>();
		for (Class<? extends DBRow> dbrowClass : dbRowClasses) {
			if (!dbrowClass.getPackage().getName().startsWith("nz.co.gregs.dbvolution")) {
				returnSet.add(dbrowClass);
			}
		}
		return returnSet;
	}

	/**
	 * Using the classes found by {@link #getDBRowSubclasses() }, creates an
	 * instance of as many classes as possible.
	 *
	 * @return an instance of every DBRow class that can be found and created
	 * easily.
	 */
	public static List<DBRow> getDBRowInstances() {
		return getDBRowInstances(getDBRowSubclasses());
	}

	/**
	 * Using the classes supplied creates an instance of as many classes as
	 * possible.
	 *
	 * @param dbRowClasses the classes to instantiate
	 * @return an instance of every DBRow class that can be found and created
	 * easily.
	 */
	public static List<DBRow> getDBRowInstances(Collection<Class<? extends DBRow>> dbRowClasses) {
		List<DBRow> dbrows = new ArrayList<DBRow>();
		for (Class<? extends DBRow> dbRowClass : dbRowClasses) {
			DBRow newInstance = DBRow.getDBRow(dbRowClass);
			dbrows.add(newInstance);
		}
		return dbrows;
	}

	/**
	 * Creates a query from a string of tables,fields, and values separated by 3
	 * different separators.
	 *
	 * <p>
	 * Intended to create a DBQuery from a string like
	 * {@code carcompany.name=toyota&marque.name=toyota} using the separators
	 * "&amp;", ".", and "=".
	 *
	 * <p>
	 * The greater intent is an extensible mechanism for parsing web links and
	 * creating queries from them.
	 *
	 * @param db the database to query
	 * @param encodedTablesPropertiesAndValues a string encoding of a query
	 * @param interpreter the interpreter to use to understand the encoded query
	 *
	 * @return a DBQuery.
	 * @throws ClassNotFoundException if Class.forName() cannot find the table
	 * provided by the interpreter
	 * @throws InstantiationException if the class cannot be instantiated
	 * @throws IllegalAccessException if the class cannot be accessed
	 */
	public static DBQuery createDBQueryFromEncodedTablesPropertiesAndValues(DBDatabase db, String encodedTablesPropertiesAndValues, EncodingInterpreter interpreter) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		final RowDefinitionWrapperFactory rowDefinitionWrapperFactory = new RowDefinitionWrapperFactory();
		final Map<String, DBRow> foundAlready = new HashMap<String, DBRow>();

		String[] parameters = interpreter.splitParameters(encodedTablesPropertiesAndValues);
		for (String parameter : parameters) {
			String table = interpreter.getDBRowClassName(parameter);
			DBRow newInstance = foundAlready.get(table);
			if (newInstance == null) {
				Class<?> tableClass = Class.forName(table);
				if (DBRow.class.isAssignableFrom(tableClass)) {
					@SuppressWarnings("unchecked")
					final Class<? extends DBRow> rowClass = (Class<? extends DBRow>) tableClass;
					newInstance = DBRow.getDBRow(rowClass);
					foundAlready.put(table, newInstance);
				} else {
					throw new DBRuntimeException("Class Specified Is Not A DBRow Sub-Class: expected " + tableClass + "(derived from " + table + ") to be a DBRow subclass but it was not.  Please only use DBRows with this method.");
				}
			}
			if (newInstance != null) {
				String propertyName = interpreter.getPropertyName(parameter);
				if (propertyName != null) {
					String value = interpreter.getPropertyValue(parameter);
					var instanceWrapper = rowDefinitionWrapperFactory.instanceWrapperFor(newInstance);
					var propertyWrapper = instanceWrapper.getPropertyByName(propertyName);
					interpreter.setValue(propertyWrapper.getQueryableDatatype(), value);
				}
			}
		}
		final Collection<DBRow> values = foundAlready.values();
		DBRow[] allTables = values.toArray(new DBRow[]{});
		final DBQuery dbQuery = db.getDBQuery();
		dbQuery.add(allTables);
		return dbQuery;
	}

}

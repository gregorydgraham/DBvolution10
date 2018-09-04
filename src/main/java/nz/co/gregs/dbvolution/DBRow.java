package nz.co.gregs.dbvolution;

import nz.co.gregs.dbvolution.databases.DBDatabase;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import nz.co.gregs.dbvolution.actions.DBQueryable;
import nz.co.gregs.dbvolution.annotations.*;
import nz.co.gregs.dbvolution.columns.AbstractColumn;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.exceptions.AccidentalCartesianJoinException;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
import nz.co.gregs.dbvolution.exceptions.UnableToInterpolateReferencedColumnInMultiColumnPrimaryKeyException;
import nz.co.gregs.dbvolution.exceptions.UnableToInstantiateDBRowSubclassException;
import nz.co.gregs.dbvolution.exceptions.UnacceptableClassForAutoFillAnnotation;
import nz.co.gregs.dbvolution.exceptions.UndefinedPrimaryKeyException;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.internal.properties.*;
import nz.co.gregs.dbvolution.operators.DBOperator;
import nz.co.gregs.dbvolution.query.RowDefinition;
import org.reflections.Reflections;

/**
 * DBRow is the representation of a table and its structure.
 *
 * <p>
 * A fuller description of creating a DBRow subclass is at <a
 * href="https://dbvolution.gregs.co.nz/usingDBRow.html">the DBvolution
 * website</a>
 * <p>
 * A fundamental difference between Object Oriented Programming and Relational
 * Databases is that DBs are based on persistent tables that store information.
 * OOP however generates a process that creates transient information.
 *
 * <p>
 * In Java the only persistent concept is a class, so DBvolution represents each
 * table as a class. Each column of the table is represented as a Java field.
 * Connecting the class and fields to the table and columns are annotations:
 * {@link DBTableName} &amp; {@link DBColumn}
 *
 * <p>
 * Java and Relational datatypes differ considerably, not the least because each
 * DB has it's own unique version of the standard datatypes. Also DB datatypes
 * are much more weakly typed. DBvolution uses
 * {@link QueryableDatatype QueryableDatatypes (QDTs)} to bridge the gap between
 * Java and Relational. The most common QDTs are:
 * {@link DBString}, {@link DBInteger}, {@link DBNumber}, {@link DBDate}, and
 * {@link DBLargeBinary}.
 *
 * <p>
 * Relational databases deliberately eschew hierarchies and has very weak links
 * between entities with enormous number of entities. Conversely Java is filled
 * with hierarchies and strong links while having a, relatively, tiny number of
 * entities. These incompatibilities can only be resolved by avoiding a
 * hierarchy, keeping the links relatively weak, and only retrieving the
 * entities that have been requested to keep the number of entities relatively
 * low.
 *
 * <p>
 * Hierarchy is avoided by not directly using any DBRow subclass within a DBRow
 * subclass. Extending a DBRow subclass has it's uses but there is still little
 * connection between the classes.
 * <p>
 * Weak linking is achieved using the {@link DBForeignKey} annotation with the
 * <em>class</em> of the related class. Foreign keys connect to the primary key
 * (see the {@link DBPrimaryKey} annotation) of the other class to allow NATURAL
 * JOINS but don't connect the classes directly and can be ignored using
 * {@link DBRow#ignoreForeignKey(java.lang.Object)}.
 * <p>
 * Reducing the number of entities is facilitated by making it easy for you to
 * specify the tables required and add conditions to the queries. Refer to
 * {@link DBDatabase}, {@link DBQuery}, {@link QueryableDatatype}, and
 * {@link DBExpression} for more on this aspect of DBvolution.
 *
 * <p>
 * A simple DBRow subclass is shown below as an example. Please note that
 * DBvolution uses a lot of reflection and thus fields must be accessible and
 * there must be a public default constructor.
 * <p>
 * <code>
 * &#64;DBTableName("car_company")<br>
 * public class CarCompany extends DBRow {<br>
 * <br>
 * &#64;DBColumn("name")<br>
 * public DBString name = new DBString();<br>
 * <br>
 * &#64;DBPrimaryKey<br>
 * &#64;DBColumn("uid_carcompany")<br>
 * public DBInteger uidCarCompany = new DBInteger();<br>
 * <br>
 * &#64;DBForeignKey(Staff.class)<br>
 * &#64;DBColumn("fk_staff_ceo")<br>
 * public DBInteger fkCEO = new DBInteger();<br>
 * <br>
 * public CarCompany() {}<br>
 * }<br>
 * </code>
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
abstract public class DBRow extends RowDefinition implements Serializable {

	private static final long serialVersionUID = 1L;

	private boolean isDefined = false;
	private final List<PropertyWrapperDefinition> ignoredForeignKeys = Collections.synchronizedList(new ArrayList<PropertyWrapperDefinition>());
	private transient Boolean hasBlobs;
	private transient final List<PropertyWrapper> fkFields = new ArrayList<>();
	private transient final List<PropertyWrapper> blobColumns = new ArrayList<>();
	private transient final SortedSet<Class<? extends DBRow>> referencedTables = new TreeSet<>(new DBRow.ClassNameComparator());
	private Boolean emptyRow = true;

	private String recursiveTableAlias = null;
	private String tableVariantIdentifier = null;

	/**
	 * Creates a new blank DBRow of the supplied subclass.
	 *
	 * @param <T> DBRow type
	 * @param requiredDBRowClass requiredDBRowClass
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a new blank version of the specified class
	 */
	public static <T extends DBRow> T getDBRow(Class<T> requiredDBRowClass) throws UnableToInstantiateDBRowSubclassException {
		try {
			Constructor<T> constructor = requiredDBRowClass.getConstructor();
			constructor.setAccessible(true);
			return constructor.newInstance();
		} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
			throw new UnableToInstantiateDBRowSubclassException(requiredDBRowClass, ex);
		}
	}

	/**
	 * Returns a new example of the sourceRow with only the primary key set for
	 * use in a query.
	 *
	 * @param <R> DBRow type
	 * @param sourceRow sourceRow
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return Returns a new DBRow example, of the same class as the supplied row,
	 * with the same primary key as the source key
	 */
	public static <R extends DBRow> R getPrimaryKeyExample(R sourceRow) {
		@SuppressWarnings("unchecked")
		R newRow = (R) getDBRow(sourceRow.getClass());
		final List<PropertyWrapper> wrappers = sourceRow.getPrimaryKeyPropertyWrappers();
		for (PropertyWrapper wrapper : wrappers) {
			PropertyWrapperDefinition definition = wrapper.getPropertyWrapperDefinition();
			QueryableDatatype<?> sourceQDT = definition.getQueryableDatatype(sourceRow);
			QueryableDatatype<?> newQDT = definition.getQueryableDatatype(newRow);

			new InternalQueryableDatatypeProxy(newQDT).setValue(sourceQDT);
		}
		return newRow;
	}

	/**
	 * Creates a new instance of the supplied DBRow subclass and duplicates it's
	 * values.
	 *
	 * @param <T> DBRow type
	 * @param originalRow originalRow
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a new version of the specified row with values that duplicate the
	 * original.
	 */
	public static <T extends DBRow> T copyDBRow(T originalRow) {
		@SuppressWarnings("unchecked")
		T newRow = (T) DBRow.getDBRow(originalRow.getClass());
		newRow.setTableVariantIdentifier(originalRow.getTableVariantIdentifier());
		if (originalRow.getDefined()) {
			newRow.setDefined();
		} else {
			newRow.setUndefined();
		}
		for (PropertyWrapperDefinition defn : originalRow.getIgnoredForeignKeys()) {
			newRow.getIgnoredForeignKeys().add(defn);
		}
		if (originalRow.getReturnColumns() != null) {
			newRow.setReturnColumns(new ArrayList<PropertyWrapperDefinition>());
			for (PropertyWrapperDefinition defn : originalRow.getReturnColumns()) {
				newRow.getReturnColumns().add(defn);
			}
		} else {
			newRow.setReturnColumns(null);
		}

		List<PropertyWrapper> subclassFields = originalRow.getColumnPropertyWrappers();
		for (PropertyWrapper field : subclassFields) {
			try {
				Object originalValue = field.rawJavaValue();
				if (originalValue instanceof QueryableDatatype) {
					QueryableDatatype<?> originalQDT = (QueryableDatatype) originalValue;
					field.getPropertyWrapperDefinition().setRawJavaValue(newRow, originalQDT.copy());
				} else {
					field.getPropertyWrapperDefinition().setRawJavaValue(newRow, originalValue);
				}
			} catch (IllegalArgumentException ex) {
				throw new RuntimeException(ex);
			}
		}
		return newRow;
	}

	/**
	 * Returns the QueryableDatatype instance of the Primary Key of This DBRow
	 *
	 * <p>
	 * If the DBRow class has a {@link DBPrimaryKey @DBPrimaryKey} designated
	 * field, then the QueryableDatatype instance of that field is returned.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the QDT of the primary key or null if there is no primary key.
	 */
	public List<QueryableDatatype<?>> getPrimaryKeys() {
		List<PropertyWrapper> primaryKeyPropertyWrappers = getPrimaryKeyPropertyWrappers();
		if (primaryKeyPropertyWrappers == null) {
			return null;
		} else {
			List<QueryableDatatype<?>> names = new ArrayList<>();
			for (PropertyWrapper pk : primaryKeyPropertyWrappers) {
				names.add(pk.getQueryableDatatype());
			}
			return names;
		}
	}

	/**
	 * Returns the column instance of the Primary Key of This DBRow
	 *
	 * <p>
	 * If the DBRow class has a {@link DBPrimaryKey @DBPrimaryKey} designated
	 * field, then the Column instance of that field is returned.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the ColumnProvider instance of the primary key or null if there is
	 * no primary key.
	 */
	public List<ColumnProvider> getPrimaryKeysAsColumns() {
		List<PropertyWrapper> primaryKeyPropertyWrappers = getPrimaryKeyPropertyWrappers();
		if (primaryKeyPropertyWrappers == null) {
			return null;
		} else {
			List<ColumnProvider> names = new ArrayList<>();
			for (PropertyWrapper pk : primaryKeyPropertyWrappers) {
				names.add(this.column(pk.getQueryableDatatype()));
			}
			return names;
		}
	}

	/**
	 * Returns the QueryableDatatype instance of the Primary Key of This DBRow, as
	 * an array in case there a
	 *
	 * <p>
	 * If the DBRow class has a {@link DBPrimaryKey @DBPrimaryKey} designated
	 * field, then the QueryableDatatype instance of that field is returned.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the QDT of the primary key or an empty array if there is no primary
	 * key.
	 */
	public QueryableDatatype<?>[] getPrimaryKeysAsArray() {
		return getPrimaryKeys().toArray(new QueryableDatatype<?>[]{});
	}

	/**
	 * Set the value of the Primary Key field/column in this DBRow.
	 *
	 * <p>
	 * Retrieves the PK field and calls the appropriate setValue method of the QDT
	 * to set the value.
	 *
	 * <p>
	 * This method is dangerous as it doesn't enforce type-safety until runtime.
	 * However its utility is too great to ignore.
	 *
	 * @param newPKValue newPKValue
	 * @see DBPrimaryKey
	 * @see DBAutoIncrement
	 */
	public void setPrimaryKey(Object newPKValue) throws ClassCastException {
		final List<QueryableDatatype<?>> primaryKeys = getPrimaryKeys();
		if (primaryKeys == null || primaryKeys.isEmpty()) {
			throw new UndefinedPrimaryKeyException(this);
		} else if (primaryKeys.size() == 1) {
			InternalQueryableDatatypeProxy proxy = new InternalQueryableDatatypeProxy(primaryKeys.get(0));
			proxy.setValue(newPKValue);
		} else {
			throw new UnableToInterpolateReferencedColumnInMultiColumnPrimaryKeyException(this, primaryKeys);
		}
	}

	/**
	 * Returns the field index of the Primary Key of This DBRow
	 *
	 * <p>
	 * If the DBRow class has a {@link DBPrimaryKey @DBPrimaryKey} designated
	 * field, then the field index of that field is returned.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the index of the primary key or null if there is no primary key.
	 */
	public List<Integer> getPrimaryKeyIndexes() {
		final List<PropertyWrapper> primaryKeyPropertyWrappers = getPrimaryKeyPropertyWrappers();
		if (primaryKeyPropertyWrappers == null) {
			return null;
		} else {
			List<Integer> names = new ArrayList<>();
			for (PropertyWrapper pk : primaryKeyPropertyWrappers) {
				names.add(pk.getPropertyWrapperDefinition().getColumnIndex());
			}
			return names;
		}
	}

	/**
	 *
	 * Indicates whether this instance is a defined row within the database.
	 *
	 * Example objects and blank rows from an optional table are "undefined".
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if this row exists within the database, otherwise FALSE.
	 */
	public boolean getDefined() {
		return isDefined;
	}

	/**
	 * indicates the DBRow is not defined in the database.
	 *
	 * <p>
	 * Used internally, probably not the method you want.
	 * </p>
	 *
	 */
	public void setUndefined() {
		isDefined = false;
	}

	/**
	 * indicates the DBRow is defined in the database.
	 *
	 * <p>
	 * Used internally, probably not the method you want.
	 * </p>
	 *
	 */
	public void setDefined() {
		isDefined = true;
	}

	/**
	 * Returns true if any of the non-LargeObject fields has been changed.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if the simple types have changed, otherwise FALSE
	 */
	public boolean hasChangedSimpleTypes() {
		List<PropertyWrapper> propertyWrappers = getWrapper().getColumnPropertyWrappers();
		for (PropertyWrapper prop : propertyWrappers) {
			if (!(prop.getQueryableDatatype() instanceof DBLargeObject)) {
				if (prop.getQueryableDatatype().hasChanged()) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * Sets all simple types to unchanged.
	 *
	 * <p>
	 * Clears the changed flag on all the simple types. {@link DBLargeObject}
	 * objects are not affected.
	 *
	 * <p>
	 * After this method is called {@link #hasChangedSimpleTypes() } will return
	 * false.
	 *
	 */
	public void setSimpleTypesToUnchanged() {
		List<PropertyWrapper> propertyWrappers = getWrapper().getColumnPropertyWrappers();
		for (PropertyWrapper prop : propertyWrappers) {
			final QueryableDatatype<?> qdt = prop.getQueryableDatatype();
			if (!(qdt instanceof DBLargeObject)) {
				if (qdt.hasChanged()) {
					qdt.setUnchanged();

					// ensure field set when using type adaptors
					prop.setQueryableDatatype(qdt);
				}
			}
		}
	}

	/**
	 * Finds the Primary Key, if there is one, and returns its column name
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the column of the primary key
	 */
	public List<String> getPrimaryKeyColumnNames() {
		List<PropertyWrapper> primaryKeyPropertyWrappers = getPrimaryKeyPropertyWrappers();
		if (primaryKeyPropertyWrappers == null) {
			return null;
		} else {
			List<String> names = new ArrayList<>();
			for (PropertyWrapper pk : primaryKeyPropertyWrappers) {
				names.add(pk.columnName());
			}
			return names;
		}
	}

	/**
	 * Finds the Primary Key, if there is one, and returns the name of the field.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the java field name of the primary key
	 */
	public List<String> getPrimaryKeyFieldName() {
		List<PropertyWrapper> primaryKeyPropertyWrappers = getPrimaryKeyPropertyWrappers();
		if (primaryKeyPropertyWrappers == null) {
			return null;
		} else {
			List<String> names = new ArrayList<>();
			for (PropertyWrapper pk : primaryKeyPropertyWrappers) {
				names.add(pk.javaName());
			}
			return names;
		}
	}

	/**
	 * Returns the PropertyWrapper for the DBRow's primary key.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the PropertyWrapper for the primary key.
	 */
	public List<PropertyWrapper> getPrimaryKeyPropertyWrappers() {
		return getWrapper().getPrimaryKeysPropertyWrappers();
	}

	/**
	 * Change all the criteria specified on this DBRow instance into a list of
	 * strings for adding in to the WHERE clause
	 *
	 * <p>
	 * Uses table and column aliases appropriate for SELECT queries
	 *
	 * @param db The DBDatabase instance that this query is to be executed on.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the WHERE clause that will be used with the current parameters
	 *
	 */
	public List<String> getWhereClausesWithoutAliases(DBDefinition db) {
		return getWhereClauses(db, false);
	}

	/**
	 * Change all the criteria specified on this DBRow instance into a list of
	 * strings for adding in to the WHERE clause
	 *
	 * <p>
	 * Uses plain table and column names appropriate for DELETE queries
	 *
	 * @param db The DBDatabase instance that this query is to be executed on.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the WHERE clause that will be used with the current parameters
	 *
	 */
	public List<String> getWhereClausesWithAliases(DBDefinition db) {
		return getWhereClauses(db, true);
	}

	private List<String> getWhereClauses(DBDefinition db, boolean useTableAlias) //throws InstantiationException, IllegalAccessException 
	{
		List<String> whereClause = new ArrayList<>();
		List<PropertyWrapper> props = getWrapper().getColumnPropertyWrappers();
		for (PropertyWrapper prop : props) {
			if (prop.isColumn()) {
				QueryableDatatype<?> qdt = prop.getQueryableDatatype();
				String possibleWhereClause;
				ColumnProvider column;
				if (prop.isTypeAdapted()) {
					Object rawJavaValue = prop.rawJavaValue();
					if (rawJavaValue == null) {
						try {
							rawJavaValue = prop.getRawJavaType().newInstance();
						} catch (InstantiationException | IllegalAccessException ex) {
							// note: InstantiationException tends to be thrown without a message
							throw new RuntimeException("Unable to instantiate instance of " + prop.toString(), ex);
						}
						prop.setRawJavaValue(rawJavaValue);
					}
					column = this.column(rawJavaValue);
				} else {
					column = this.column(qdt);
				}
				column.setUseTableAlias(useTableAlias);
				possibleWhereClause = getQDTWhereClause(db, column, qdt);
				if (!possibleWhereClause.replaceAll(" ", "").isEmpty()) {
					whereClause.add("(" + possibleWhereClause + ")");
				}
			}
		}
		return whereClause;
	}

	private String getQDTWhereClause(DBDefinition db, ColumnProvider column, QueryableDatatype<?> qdt) {
		StringBuilder whereClause = new StringBuilder();
		DBOperator op = qdt.getOperator();
		if (op != null) {
			if (column instanceof DBExpression) {
				DBExpression requiredExpression = column;
			if (qdt.hasColumnExpression()) {
				DBExpression[] columnExpression = qdt.getColumnExpression();
				String sep = "";
				for (DBExpression dBExpression : columnExpression) {
					whereClause.append(sep).append(op.generateWhereExpression(db, dBExpression).toSQLString(db));
					sep = db.beginAndLine();
				}
			} else {
				whereClause = new StringBuilder(op.generateWhereExpression(db, requiredExpression).toSQLString(db));
			}
		}
		}
		return whereClause.toString();
	}

	/**
	 * USED INTERNALLY
	 *
	 * @param db
	 * @param useTableAlias
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of the DBExpressions that are defined for this exemplar.
	 */
	public final List<BooleanExpression> getWhereClauseExpressions(DBDefinition db, boolean useTableAlias) //throws InstantiationException, IllegalAccessException 
	{
		List<BooleanExpression> whereClause = new ArrayList<>();
		List<PropertyWrapper> props = getWrapper().getColumnPropertyWrappers();
		for (PropertyWrapper prop : props) {
			if (prop.isColumn()) {
				QueryableDatatype<?> qdt = prop.getQueryableDatatype();
				ColumnProvider column;
				if (prop.isTypeAdapted()) {
					Object rawJavaValue = prop.rawJavaValue();
					if (rawJavaValue == null) {
						try {
							rawJavaValue = prop.getRawJavaType().newInstance();
						} catch (InstantiationException | IllegalAccessException ex) {
							// note: InstantiationException tends to be thrown without a message
							throw new RuntimeException("Unable to instantiate instance of " + prop.toString(), ex);
						}
						prop.setRawJavaValue(rawJavaValue);
					}
					column = this.column(rawJavaValue);
				} else {
					column = this.column(qdt);
				}
				column.setUseTableAlias(useTableAlias);
				BooleanExpression possibleWhereClause = getQDTWhereClauseExpression(db, column, qdt);
				if (possibleWhereClause != null) {
					whereClause.add(possibleWhereClause);
				}
			}
		}
		return whereClause;
	}

	private BooleanExpression getQDTWhereClauseExpression(DBDefinition db, ColumnProvider column, QueryableDatatype<?> qdt) {
		BooleanExpression whereClause = null;
		DBOperator op = qdt.getOperator();
		if (op != null) {
			if (column instanceof DBExpression) {
				DBExpression requiredExpression = column;
				if (qdt.hasColumnExpression()) {
					DBExpression[] columnExpression = qdt.getColumnExpression();
					for (DBExpression dBExpression : columnExpression) {
						if (whereClause == null) {
							whereClause = op.generateWhereExpression(db, dBExpression);
						} else {
							whereClause = whereClause.and(op.generateWhereExpression(db, dBExpression));
						}
					}
				} else {
					whereClause = op.generateWhereExpression(db, requiredExpression);
				}
			}
		}
		return whereClause;
	}

	/**
	 * Tests whether this DBRow instance has any criteria ({@link DBNumber#permittedValues(java.lang.Number...)
	 * }, etc) set.
	 *
	 * <p>
	 * The database is not accessed and this method does not protect against
	 * functionally blank queries.
	 *
	 * @param db	db
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return true if this DBRow instance has no specified criteria and will
	 * create a blank query returning the whole table.
	 *
	 */
	public boolean willCreateBlankQuery(DBDefinition db) {
		List<String> whereClause = getWhereClausesWithoutAliases(db);
		return whereClause == null || whereClause.isEmpty();
	}

	/**
	 * Probably not needed by the programmer, this is the convenience function to
	 * find the table name specified by {@code @DBTableName} or the class name
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the name of the table in the database specified to correlate with
	 * the specified type
	 *
	 */
	public String getTableName() {
		return getWrapper().tableName();
	}

	public String getSelectQuery() {
		return getWrapper().selectQuery();
	}

	public void setRecursiveTableAlias(String alias) {
		recursiveTableAlias = alias;
	}

	/**
	 * Returns the alias to be used if this DBRow is being used in a recursive
	 * query.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the recursive alias set by {@link DBRecursiveQuery} during query
	 * execution.
	 */
	public String getRecursiveTableAlias() {
		return recursiveTableAlias;
	}

	/**
	 * Returns the same result as {@link #toString() } but omitting the Foreign
	 * Key references.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a string representation of the contents of this instance with
	 * Foreign Key fields removed
	 */
	public String toStringMinusFKs() {
		StringBuilder string = new StringBuilder();
		List<PropertyWrapper> fields = getWrapper().getColumnPropertyWrappers();

		String separator = "";

		for (PropertyWrapper field : fields) {
			if (field.isColumn()) {
				if (!field.isForeignKey()) {
					string.append(separator);
					string.append(" ");
					string.append(field.javaName());
					string.append(":");
					string.append(field.getQueryableDatatype());
					separator = ",";
				}
			}
		}
		return string.toString();
	}

	/**
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a list of all foreign keys, MINUS the ignored foreign keys
	 */
	public List<PropertyWrapper> getForeignKeyPropertyWrappers() {
		synchronized (fkFields) {
			if (fkFields.isEmpty()) {
				List<PropertyWrapper> props = getWrapper().getForeignKeyPropertyWrappers();

				for (PropertyWrapper prop : props) {
					if (prop.isColumn()) {
						if (prop.isForeignKey()) {
							if (!ignoredForeignKeys.contains(prop.getPropertyWrapperDefinition())) {
								fkFields.add(prop);
							}
						}
					}
				}
			}
			return fkFields;
		}
	}

	/**
	 * Ignores the foreign key of the property (field or method) given the
	 * property's object reference.
	 *
	 * <p>
	 * For example the following code snippet will ignore the foreign key on the
	 * fkAddress field:
	 * <pre>
	 * Customer customer = ...;
	 * customer.ignoreForeignKey(customer.fkAddress);
	 * </pre>
	 *
	 * <p>
	 * Requires the field to be from this instance to work.
	 *
	 * @param qdt	qdt
	 */
	public void ignoreForeignKey(Object qdt) throws IncorrectRowProviderInstanceSuppliedException {
		PropertyWrapper fkProp = getPropertyWrapperOf(qdt);
		if (fkProp == null) {
			throw new IncorrectRowProviderInstanceSuppliedException(this, qdt);
		}
		getIgnoredForeignKeys().add(fkProp.getPropertyWrapperDefinition());
		fkFields.clear();
	}

	/**
	 * Ignores the foreign keys of the property (field or method) given the
	 * property's object reference.
	 *
	 * <p>
	 * For example the following code snippet will ignore the foreign key on the
	 * fkAddress field:
	 * <pre>
	 * Customer customer = ...;
	 * customer.ignoreForeignKeys(customer.fkAddress, customer.fkManager);
	 * </pre>
	 *
	 * <p>
	 * Requires the field to be from this instance to work.
	 *
	 * @param qdts	qdts
	 */
	public void ignoreForeignKeys(Object... qdts) throws IncorrectRowProviderInstanceSuppliedException {
		for (Object object : qdts) {
			ignoreForeignKey(object);
		}
	}

	/**
	 * Ignores the foreign keys of the property (field or method) given the
	 * property's object reference.
	 *
	 * <p>
	 * For example the following code snippet will ignore the foreign key on the
	 * fkAddress field:
	 * <pre>
	 * Customer customer = ...;
	 * customer.ignoreForeignKeys(customer.fkAddress, customer.fkManager);
	 * </pre>
	 *
	 * <p>
	 * Requires the field to be from this instance to work.
	 *
	 * @param columns	columns
	 */
	public void ignoreForeignKeys(ColumnProvider... columns) throws IncorrectRowProviderInstanceSuppliedException {
		for (ColumnProvider col : columns) {
			ignoreForeignKey(col);
		}
	}

	/**
	 * Ignores the foreign key of the column provided.
	 * <p>
	 * Similar to {@link #ignoreForeignKey(java.lang.Object) } but uses a
	 * ColumnProvider which is portable between instances of DBRow.
	 * <p>
	 * For example the following code snippet will ignore the foreign key provided
	 * by a different instance of Customer:
	 * <pre>
	 * Customer customer = ...;
	 * IntegerColumn addressColumn = customer.column(customer.fkAddress);
	 * Customer cust2 = new Customer();
	 * cust2.ignoreForeignKey(addressColumn);
	 * </pre>
	 *
	 * @param column	column
	 */
	public void ignoreForeignKey(ColumnProvider column) {
		PropertyWrapper fkProp = column.getColumn().getPropertyWrapper();
		getIgnoredForeignKeys().add(fkProp.getPropertyWrapperDefinition());
		fkFields.clear();
	}

	/**
	 * Removes all foreign keys from the "ignore" list.
	 *
	 * @see DBRow#ignoreAllForeignKeys()
	 * @see DBRow#ignoreAllForeignKeysExceptFKsTo(nz.co.gregs.dbvolution.DBRow...)
	 * @see DBRow#ignoreForeignKey(java.lang.Object)
	 * @see DBRow#ignoreForeignKey(nz.co.gregs.dbvolution.columns.ColumnProvider)
	 *
	 */
	public void useAllForeignKeys() {
		getIgnoredForeignKeys().clear();
		fkFields.clear();
	}

	/**
	 * Adds All foreign keys to the "ignore" list.
	 *
	 * All foreign keys of this instance will be ignored. This may cause an
	 * {@link AccidentalCartesianJoinException} if no additional relationships
	 * have been added.
	 *
	 */
	public void ignoreAllForeignKeys() {
		List<PropertyWrapper> props = this.getForeignKeyPropertyWrappers();
		for (PropertyWrapper prop : props) {
			getIgnoredForeignKeys().add(prop.getPropertyWrapperDefinition());
		}
		fkFields.clear();
	}

	/**
	 * Adds All foreign keys to the "ignore" list except those specified.
	 *
	 * All foreign keys of this instance will be ignored. This may cause an
	 * {@link AccidentalCartesianJoinException} if no additional relationships
	 * have been added.
	 *
	 * @param importantForeignKeys	importantForeignKeys
	 */
	public void ignoreAllForeignKeysExcept(Object... importantForeignKeys) throws IncorrectRowProviderInstanceSuppliedException {
		ArrayList<PropertyWrapperDefinition> importantFKs = new ArrayList<>();
		for (Object object : importantForeignKeys) {
			PropertyWrapper importantProp = getPropertyWrapperOf(object);
			if (importantProp != null) {
				if (importantProp.isColumn() && importantProp.isForeignKey()) {
					importantFKs.add(importantProp.getPropertyWrapperDefinition());
				}
			} else {
				throw new IncorrectRowProviderInstanceSuppliedException(this, object);
			}
		}
		List<PropertyWrapper> props = this.getForeignKeyPropertyWrappers();
		for (PropertyWrapper prop : props) {
			final PropertyWrapperDefinition propDefn = prop.getPropertyWrapperDefinition();
			if (!importantFKs.contains(propDefn)) {
				getIgnoredForeignKeys().add(propDefn);
			}
		}
		fkFields.clear();
	}

	/**
	 * Indicates if the DBRow has {@link DBLargeObject} (BLOB) columns.
	 *
	 * If the DBrow has columns that represent BLOB, CLOB, TEXT, JAVA_OBJECT, or
	 * other large object columns, this method with indicate it.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if this DBRow has large object columns, FALSE otherwise.
	 */
	public boolean hasLargeObjects() {
		synchronized (blobColumns) {
			if (hasBlobs == null) {
				hasBlobs = Boolean.FALSE;
				for (PropertyWrapper prop : getColumnPropertyWrappers()) {
					if (prop.isInstanceOfLargeObject()) {
						blobColumns.add(prop);
						hasBlobs = Boolean.TRUE;
					}
				}
			}
			return hasBlobs;
		}
	}

	/**
	 * Removes all fields of this DBRow from the query results.
	 *
	 * <p>
	 * All fields will be removed and the returned rows will be effectively a NULL
	 * row, however the DBRow's table will still be used in the query to set
	 * conditions.
	 *
	 */
	public final void setReturnFieldsToNone() {
		setReturnColumns(new ArrayList<PropertyWrapperDefinition>());
	}

	/**
	 * Limits the returned columns by the specified properties (fields and/or
	 * methods) given the properties object references.
	 *
	 * <p>
	 * For example the following code snippet will include only the uid and name
	 * columns based on the uid and name fields:
	 * <pre>
	 * Customer customer = ...;
	 * customer.setReturnFields(customer.uid, customer.name);
	 * </pre>
	 *
	 * <p>
	 * Requires the field to be from this instance to work.
	 *
	 * @param fields a list of fields/methods from this object
	 */
	public final void setReturnFields(Object... fields) throws IncorrectRowProviderInstanceSuppliedException {
		setReturnColumns(new ArrayList<PropertyWrapperDefinition>());
		PropertyWrapper propWrapper;
		for (Object property : fields) {
			propWrapper = getPropertyWrapperOf(property);
			if (propWrapper == null) {
				throw new IncorrectRowProviderInstanceSuppliedException(this, property);
			}
			getReturnColumns().add(propWrapper.getPropertyWrapperDefinition());
		}
	}

	/**
	 * Limits the returned columns by the specified properties (fields and/or
	 * methods) given the properties object references.
	 *
	 * <p>
	 * For example the following code snippet will include only the uid and name
	 * columns based on the uid and name fields:
	 * <pre>
	 * Customer customer = ...;
	 * customer.setReturnFields(customer.uid, customer.name);
	 * </pre>
	 *
	 * <p>
	 * Requires the field to be from this instance to work.
	 *
	 * @param columns a list of fields/methods from this object
	 */
	public final void setReturnFields(ColumnProvider... columns) throws IncorrectRowProviderInstanceSuppliedException {
		setReturnFieldsToNone();
		for (ColumnProvider provider : columns) {
			final AbstractColumn column = provider.getColumn();
			Object appropriateFieldFromRow = column.getAppropriateFieldFromRow(this);
			this.setReturnFields(appropriateFieldFromRow);
		}

	}

	/**
	 * Extends the returned columns to include the specified properties (fields
	 * and/or methods) given the properties object references.
	 *
	 * <p>
	 * For example the following code snippet will include only the uid, name, and
	 * address columns based on the uid and name fields:
	 * <pre>
	 * Customer customer = ...;
	 * customer.setReturnFields(customer.uid, customer.name);
	 * customer.addReturnFields(customer.address);
	 * </pre>
	 *
	 * <p>
	 * Requires the field to be from this instance to work.
	 *
	 * @param fields a list of fields/methods from this object
	 */
	public final void addReturnFields(Object... fields) throws IncorrectRowProviderInstanceSuppliedException {
		if (getReturnColumns() == null) {
			setReturnColumns(new ArrayList<PropertyWrapperDefinition>());
		}
		PropertyWrapper propWrapper;
		for (Object property : fields) {
			propWrapper = getPropertyWrapperOf(property);
			if (propWrapper == null) {
				throw new IncorrectRowProviderInstanceSuppliedException(this, property);
			}
			getReturnColumns().add(propWrapper.getPropertyWrapperDefinition());
		}
	}

	/**
	 * Removes all builtin columns from the return list.
	 *
	 * <p>
	 * Used by DBReport to avoid returning fields that haven't been specified with
	 * an expression.
	 *
	 * <p>
	 * Probably not useful in general use.
	 *
	 */
	protected final void removeAllFieldsFromResults() {
		setReturnFields(new Object[]{});
	}

	/**
	 * Remove all limitations on the fields returned.
	 *
	 * <p>
	 * Clears the limits on returned fields set by
	 * {@code DBRow.setReturnFields(T...)}
	 *
	 *
	 */
	public void returnAllFields() {
		setReturnColumns(null);
	}

	/**
	 * List the foreign keys and ad-hoc relationships from this instance to the
	 * supplied example as DBRelationships
	 *
	 * @param otherTable otherTable
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the foreign keys and ad-hoc relationships as an SQL String or a
	 * null pointer
	 */
	public List<BooleanExpression> getRelationshipsAsBooleanExpressions(DBRow otherTable) {
		List<BooleanExpression> rels = new ArrayList<>();

		List<PropertyWrapper> fks = getForeignKeyPropertyWrappers();
		for (PropertyWrapper fk : fks) {
			Class<? extends DBRow> referencedClass = fk.referencedClass();

			if (referencedClass.isAssignableFrom(otherTable.getClass())) {
				rels.add(getRelationshipExpressionFor(this, fk, otherTable));
			}
		}

		fks = otherTable.getForeignKeyPropertyWrappers();
		for (PropertyWrapper fk : fks) {
			Class<? extends DBRow> referencedClass = fk.referencedClass();

			if (referencedClass.isAssignableFrom(this.getClass())) {
				rels.add(getRelationshipExpressionFor(otherTable, fk, this));
			}
		}

		return rels;
	}

	/**
	 * Tests whether this instance of DBRow and the otherTable instance of DBRow
	 * will be connected given the specified database and query options.
	 *
	 * @param otherTable otherTable
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return TRUE if this instance and the otherTable will be connected, FALSE
	 * otherwise.
	 */
	public boolean willBeConnectedTo(DBRow otherTable) {
		List<BooleanExpression> relationshipsAsBooleanExpressions = this.getRelationshipsAsBooleanExpressions(otherTable);
		relationshipsAsBooleanExpressions.addAll(otherTable.getRelationshipsAsBooleanExpressions(this));
		return (!relationshipsAsBooleanExpressions.isEmpty());
	}

	/**
	 * Returns all the DBRow subclasses referenced by this class with foreign keys
	 *
	 * <p>
	 * Similar to {@link #getAllConnectedTables() } but where this class directly
	 * references the external DBRow subclass with an {@code @DBForeignKey}
	 * annotation.
	 *
	 * <p>
	 * That is to say: where A is this class, returns a List of B such that A
	 * =&gt; B
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A set of DBRow subclasses referenced with {@code @DBForeignKey}
	 *
	 */
	@SuppressWarnings("unchecked")
	public SortedSet<Class<? extends DBRow>> getReferencedTables() {
		synchronized (referencedTables) {
			if (referencedTables.isEmpty()) {
				List<PropertyWrapper> props = getWrapper().getForeignKeyPropertyWrappers();
				for (PropertyWrapper prop : props) {
					referencedTables.add(prop.referencedClass());
				}
			}
		}
		final SortedSet<Class<? extends DBRow>> returnSet = new TreeSet<>(new DBRow.ClassNameComparator());
		returnSet.addAll(referencedTables);
		return returnSet;
	}

	/**
	 * Returns all the DBRow subclasses referenced by this class with foreign keys
	 *
	 * <p>
	 * Similar to {@link #getAllConnectedTables() } but where this class directly
	 * references the external DBRow subclass with an {@code @DBForeignKey}
	 * annotation.
	 *
	 * <p>
	 * That is to say: where A is this class, returns a List of B such that A
	 * =&gt; B
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A set of DBRow subclasses referenced with {@code @DBForeignKey}
	 *
	 */
	@SuppressWarnings("unchecked")
	public SortedSet<Class<? extends DBRow>> getReferencedBaseTables() {
		synchronized (referencedTables) {
			if (referencedTables.isEmpty()) {
				List<PropertyWrapper> props = getWrapper().getForeignKeyPropertyWrappers();
				for (PropertyWrapper prop : props) {
					final Class<? extends DBRow> referencedClass = prop.referencedClass();
					if (referencedClass.getSuperclass().equals(DBRow.class)) {
						referencedTables.add(referencedClass);
					}
				}
			}
		}
		final SortedSet<Class<? extends DBRow>> returnSet = new TreeSet<>(new DBRow.ClassNameComparator());
		returnSet.addAll(referencedTables);
		return returnSet;
	}

	/**
	 * Creates a set of all DBRow subclasses that are connected to this class.
	 *
	 * <p>
	 * Uses {@link #getReferencedTables() } and {@link #getRelatedTables() } to
	 * produce a complete list of tables connected by a foreign key to this DBRow
	 * class.
	 *
	 * <p>
	 * That is to say: where A is this class, returns a List of B such that B
	 * =&gt; A or A =&gt; B
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a set of classes that have a {@code @DBForeignKey} reference to or
	 * from this class
	 */
	public SortedSet<Class<? extends DBRow>> getAllConnectedTables() {
		final SortedSet<Class<? extends DBRow>> relatedTables = new TreeSet<>(new DBRow.ClassNameComparator());
		relatedTables.addAll(getRelatedTables());
		relatedTables.addAll(getReferencedTables());
		return relatedTables;
	}

	/**
	 * Creates a set of all DBRow direct subclasses that are connected to this
	 * class.
	 *
	 * <p>
	 * Uses {@link #getReferencedBaseTables() } and {@link #getRelatedBaseTables()
	 * } to produce a complete list of base tables connected by a foreign key to
	 * this DBRow class.
	 *
	 * <p>
	 * That is to say: where A is this class, returns a List of B such that B
	 * =&gt; A or A =&gt; B
	 *
	 * <p>
	 * Base tables are direct subclasses of DBRow, generally this means they
	 * represent actual database tables and not a subset of a base table.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a set of classes that have a {@code @DBForeignKey} reference to or
	 * from this class
	 */
	public SortedSet<Class<? extends DBRow>> getAllConnectedBaseTables() {
		final SortedSet<Class<? extends DBRow>> relatedTables = new TreeSet<>(new DBRow.ClassNameComparator());
		relatedTables.addAll(getRelatedBaseTables());
		relatedTables.addAll(getReferencedBaseTables());
		return relatedTables;
	}

	/**
	 * Creates a set of all DBRow subclasses that reference this class with
	 * foreign keys.
	 *
	 * <p>
	 * Similar to {@link #getReferencedTables() } but where this class is being
	 * referenced by the external DBRow subclass.
	 *
	 * <p>
	 * That is to say: where A is this class, returns a List of B such that B
	 * =&gt; A
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a set of classes that have a {@code @DBForeignKey} reference to
	 * this class
	 */
	public SortedSet<Class<? extends DBRow>> getRelatedTables() throws UnableToInstantiateDBRowSubclassException {
		SortedSet<Class<? extends DBRow>> relatedTables = new TreeSet<>(new DBRow.ClassNameComparator());
		Reflections reflections = new Reflections(this.getClass().getPackage().getName());

		Set<Class<? extends DBRow>> subTypes = reflections.getSubTypesOf(DBRow.class);
		for (Class<? extends DBRow> tableClass : subTypes) {
			try {
				if (!Modifier.isAbstract(tableClass.getModifiers())) {
					DBRow newInstance = tableClass.newInstance();
					if (newInstance.getReferencedTables().contains(this.getClass())) {
						relatedTables.add(tableClass);
					}
				}
			} catch (InstantiationException | IllegalAccessException ex) {
				throw new UnableToInstantiateDBRowSubclassException(tableClass, ex);
			}
		}
		return relatedTables;
	}

	/**
	 * Creates a set of all DBRow subclasses that reference this class with
	 * foreign keys.
	 *
	 * <p>
	 * Similar to {@link #getReferencedTables() } but where this class is being
	 * referenced by the external DBRow subclass.
	 *
	 * <p>
	 * That is to say: where A is this class, returns a List of B such that B
	 * =&gt; A
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a set of classes that have a {@code @DBForeignKey} reference to
	 * this class
	 */
	public SortedSet<Class<? extends DBRow>> getRelatedBaseTables() throws UnableToInstantiateDBRowSubclassException {
		SortedSet<Class<? extends DBRow>> relatedTables = new TreeSet<>(new DBRow.ClassNameComparator());
		Reflections reflections = new Reflections(this.getClass().getPackage().getName());

		Set<Class<? extends DBRow>> subTypes = reflections.getSubTypesOf(DBRow.class);
		for (Class<? extends DBRow> tableClass : subTypes) {
			if (tableClass.getSuperclass().equals(DBRow.class)) {
				try {
					if (!Modifier.isAbstract(tableClass.getModifiers())) {
						DBRow newInstance = tableClass.newInstance();
						if (newInstance.getReferencedTables().contains(this.getClass())) {
							relatedTables.add(tableClass);
						}
					}
				} catch (InstantiationException | IllegalAccessException ex) {
					throw new UnableToInstantiateDBRowSubclassException(tableClass, ex);
				}
			}
		}
		return relatedTables;
	}

	/**
	 * Ignores the Foreign Keys to all tables except those to the supplied DBRows
	 *
	 * @param goodTables	goodTables
	 */
	public void ignoreAllForeignKeysExceptFKsTo(DBRow... goodTables) {
		List<PropertyWrapper> props = getWrapper().getForeignKeyPropertyWrappers();
		for (PropertyWrapper prop : props) {
			boolean ignore = true;
			for (DBRow goodTable : goodTables) {
				if (prop.isForeignKeyTo(goodTable)) {
					ignore = false;
					break;
				}
			}
			if (ignore) {
				ignoreForeignKey(prop.getQueryableDatatype());
			}
		}
	}

	/**
	 * Returns all fields that represent BLOB columns such DBLargeObject or
	 * DBJavaObject.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a list of {@link QueryableDatatype} that are large objects in this
	 * object.
	 */
	public List<QueryableDatatype<?>> getLargeObjects() {
		// Initialise the blob columns list if necessary
		ArrayList<QueryableDatatype<?>> returnList = new ArrayList<>();
		if (hasLargeObjects()) {
			for (PropertyWrapper propertyWrapper : blobColumns) {
				returnList.add(propertyWrapper.getQueryableDatatype());
			}
		}
		return returnList;
	}

	/**
	 * Finds all instances of {@code example} that share a {@link DBQueryRow} with
	 * this instance.
	 *
	 * @param <R> DBRow
	 * @param query query
	 * @param example example
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return all instances of {@code example} that are connected to this
	 * instance in the {@code query} 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 */
	public <R extends DBRow> List<R> getRelatedInstancesFromQuery(DBQueryable query, R example) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		List<R> instances = new ArrayList<>();
		final List<DBQueryRow> allRows = query.getAllRows();
		for (DBQueryRow qrow : allRows) {
			DBRow versionOfThis = qrow.get(this);
			R versionOfThat = qrow.get(example);
			if (versionOfThis.equals(this) && versionOfThat != null) {
				instances.add(versionOfThat);
			}
		}
		return instances;
	}

	/**
	 * Indicates whether this instance has any values set from the database.
	 *
	 * <p>
	 * If this row is the result of the database sending back a row with NULL in
	 * every column, this method returns TRUE.
	 *
	 * <p>
	 * An empty row is probably the result an optional table not having a matching
	 * row for the query. In database parlance this row is a null row of an OUTER
	 * JOIN and this table did not have any matching rows.
	 *
	 * <p>
	 * Only used internally as DBQuery results produce NULLs for non-existent
	 * rows.
	 *
	 * <p>
	 * Please note: if the row is undefined
	 * {@link DBRow#isDefined (see isDefined)} then this is meaningless
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if the row has no non-null values or is undefined, FALSE
	 * otherwise
	 */
	public Boolean isEmptyRow() {
		return emptyRow;
	}

	/**
	 * Sets the row to be empty or not.
	 *
	 * Used within DBQuery while creating the DBRows to indicate an empty row.
	 *
	 * @param isThisRowEmpty	isThisRowEmpty
	 */
	public void setEmptyRow(Boolean isThisRowEmpty) {
		this.emptyRow = isThisRowEmpty;
	}

	public List<PropertyWrapper> getSelectedProperties() {
		if (getReturnColumns() == null) {
			return getColumnPropertyWrappers();
		} else {
			ArrayList<PropertyWrapper> selected = new ArrayList<>();
			for (PropertyWrapperDefinition proDef : getReturnColumns()) {
				for (PropertyWrapper pro : getColumnPropertyWrappers()) {
					if (pro.getPropertyWrapperDefinition().equals(proDef)) {
						selected.add(pro);
					}
				}
			}
			return selected;
		}
	}

	boolean isPeerOf(DBRow table) {
		final RowDefinitionClassWrapper thisClassWrapper = this.getWrapper().getClassWrapper();
		final RowDefinitionClassWrapper thatClassWrapper = table.getWrapper().getClassWrapper();
		return thisClassWrapper.equals(thatClassWrapper);
	}

	/**
	 * Finds all fields is this object that are foreign keys to the table
	 * represented by the supplied DBRow.
	 *
	 * <p>
	 * Returned QueryableDatatypes do not necessarily reference the supplied
	 * DBRow.
	 *
	 * <p>
	 * Values of the primary key and foreign keys are not compared.
	 *
	 * @param <R> DBRow type
	 * @param row row
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of {@link QueryableDatatype} that are foreign keys to the
	 * {@link DBRow}
	 */
	public <R extends DBRow> List<QueryableDatatype<?>> getForeignKeysTo(R row) {
		List<QueryableDatatype<?>> fksToR = new ArrayList<>();
		RowDefinitionInstanceWrapper wrapper = getWrapper();
		List<PropertyWrapper> foreignKeyPropertyWrappers = wrapper.getForeignKeyPropertyWrappers();
		for (PropertyWrapper propertyWrapper : foreignKeyPropertyWrappers) {
			if (propertyWrapper.isForeignKeyTo(row)) {
				fksToR.add(propertyWrapper.getQueryableDatatype());
			}
		}
		return fksToR;
	}

	/**
	 * Provides DBExpressions representing the FK relationship between this DBRow
	 * and the target specified.
	 *
	 * <p>
	 * Values of the primary key and foreign keys are not compared.
	 *
	 * @param <R> DBRow type
	 * @param target target
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of {@link DBExpression DBExpressions} that are foreign keys
	 * to the {@link DBRow target}
	 */
	public <R extends DBRow> List<DBExpression> getForeignKeyExpressionsTo(R target) {
		List<DBExpression> fksToR = new ArrayList<>();
		RowDefinitionInstanceWrapper wrapper = getWrapper();
		List<PropertyWrapper> foreignKeyPropertyWrappers = wrapper.getForeignKeyPropertyWrappers();
		for (PropertyWrapper propertyWrapper : foreignKeyPropertyWrappers) {
			if (propertyWrapper.isForeignKeyTo(target)) {
				RowDefinition source = propertyWrapper.getRowDefinitionInstanceWrapper().adapteeRowDefinition();
				final QueryableDatatype<?> sourceFK = propertyWrapper.getQueryableDatatype();
				PropertyWrapperDefinition targetPropertyDefinition = propertyWrapper.getPropertyWrapperDefinition().referencedPropertyDefinitionIdentity();
				PropertyWrapper targetProperty = target.getWrapper().getPropertyByName(targetPropertyDefinition.javaName());
				QueryableDatatype<?> targetPK = targetProperty.getQueryableDatatype().getQueryableDatatypeForExpressionValue();

				Object column = source.column(sourceFK);
				try {
					final Method isMethod = column.getClass().getMethod("is", targetPK.getClass());
					if (isMethod != null) {
						Object fkExpression = isMethod.invoke(column, targetPK);
						if (DBExpression.class.isAssignableFrom(fkExpression.getClass())) {
							fksToR.add((DBExpression) fkExpression);
						}
					}
				} catch (IllegalAccessException | IllegalArgumentException | NoSuchMethodException | SecurityException | InvocationTargetException ex) {
					throw new nz.co.gregs.dbvolution.exceptions.ForeignKeyCannotBeComparedToPrimaryKey(ex, source, propertyWrapper, target, targetProperty);
				}
			}
		}
		return fksToR;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the ignoredForeignKeys
	 */
	protected List<PropertyWrapperDefinition> getIgnoredForeignKeys() {
		return ignoredForeignKeys;
	}

	/**
	 * Ignores the foreign keys of the property (field or method) given the
	 * property's object reference.
	 *
	 * <p>
	 * For example the following code snippet will ignore the foreign key on the
	 * fkAddress field:
	 * <pre>
	 * Customer customer = ...;
	 * fksToIgnore.add(customer.fkAddress);
	 * customer.ignoreForeignKeyProperties(fksToIgnore);
	 * </pre>
	 *
	 * <p>
	 * Requires the field to be from this instance to work.
	 *
	 * @param ignoreTheseFKs	ignoreTheseFKs
	 * @see #ignoreForeignKey(java.lang.Object)
	 */
	public void ignoreForeignKeyProperties(Collection<Object> ignoreTheseFKs) {
		for (Object qdt : ignoreTheseFKs) {
			this.ignoreForeignKey(qdt);
		}
	}

	/**
	 * Ignores the foreign keys of the columns provided.
	 * <p>
	 * Similar to {@link #ignoreForeignKeyProperties(java.util.Collection) } but
	 * uses a ColumnProvider which is portable between instances of DBRow.
	 * <p>
	 * For example the following code snippet will ignore the foreign key provided
	 * by a different instance of Customer:
	 * <pre>
	 *
	 * Customer customer = new Customer();
	 * IntegerColumn addressColumn = customer.column(customer.fkAddress);
	 * fksToIgnore.add(addressColumn);
	 *
	 * Customer cust2 = new Customer();
	 * cust2.ignoreForeignKeyColumns(fksToIgnore);
	 *
	 * </pre>
	 *
	 * @param ignoreTheseFKColumns	ignoreTheseFKColumns
	 * @see #ignoreForeignKey(nz.co.gregs.dbvolution.columns.ColumnProvider)
	 */
	public void ignoreForeignKeyColumns(Collection<ColumnProvider> ignoreTheseFKColumns) {
		for (ColumnProvider qdt : ignoreTheseFKColumns) {
			this.ignoreForeignKey(qdt);
		}
	}

	private static BooleanExpression getRelationshipExpressionFor(DBRow fkTable, PropertyWrapper fk, DBRow otherTable) {
		BooleanExpression expr = BooleanExpression.falseExpression();
		final PropertyWrapperDefinition fkDefn = fk.getPropertyWrapperDefinition();
		QueryableDatatype<?> fkQDT = fkDefn.getQueryableDatatype(fkTable);

		PropertyWrapperDefinition targetPropertyDefinition = fkDefn.referencedPropertyDefinitionIdentity();
		PropertyWrapper targetProperty = otherTable.getWrapper().getPropertyByName(targetPropertyDefinition.javaName());
		QueryableDatatype<?> pkQDT = targetProperty.getQueryableDatatype();

		if (fkQDT.getClass().isAssignableFrom(pkQDT.getClass())
				|| pkQDT.getClass().isAssignableFrom(fkQDT.getClass())) {
			if (DBBoolean.class.isAssignableFrom(fkQDT.getClass())) {
				expr = fkTable.column((DBBoolean) fkQDT).is(otherTable.column((DBBoolean) pkQDT));
			} else if (DBDate.class.isAssignableFrom(fkQDT.getClass())) {
				expr = fkTable.column((DBDate) fkQDT).is(otherTable.column((DBDate) pkQDT));
			} else if (DBInteger.class.isAssignableFrom(fkQDT.getClass())) {
				expr = fkTable.column((DBInteger) fkQDT).is(otherTable.column((DBInteger) pkQDT));
			} else if (DBNumber.class.isAssignableFrom(fkQDT.getClass())) {
				expr = fkTable.column((DBNumber) fkQDT).is(otherTable.column((DBNumber) pkQDT));
			} else if (DBString.class.isAssignableFrom(fkQDT.getClass())) {
				expr = fkTable.column((DBString) fkQDT).is(otherTable.column((DBString) pkQDT));
			}
		}
		return expr;
	}

	/**
	 * Returns the unique values for the column in the database.
	 *
	 * <p>
	 * Creates a query that finds the distinct values that are used in the
	 * field/column supplied.
	 *
	 * <p>
	 * Some tables use repeated values instead of foreign keys or do not use all
	 * of the possible values of a foreign key. This method makes it easy to find
	 * the distinct or unique values that are used.
	 *
	 * @param <A> the field type
	 * @param database - the database to connect to.
	 * @param fieldOfThisInstance - the field/column that you need data from.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of distinct values used in the column. 1 Database exceptions
	 * may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 */
	@SuppressWarnings("unchecked")
	public <A> List<A> getDistinctValuesOfColumn(DBDatabase database, A fieldOfThisInstance) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		List<A> results = new ArrayList<>();
		final PropertyWrapper fieldProp = this.getPropertyWrapperOf(fieldOfThisInstance);
		QueryableDatatype<?> thisQDT = fieldProp.getPropertyWrapperDefinition().getQueryableDatatype(this);
		this.setReturnFields(fieldOfThisInstance);
		final ColumnProvider columnProvider = this.column(thisQDT);
		DBExpression expr = columnProvider.getColumn().asExpression();
		DBQuery dbQuery = database.getDBQuery(this).addGroupByColumn(this, expr);
		dbQuery.setSortOrder(columnProvider.getSortProvider());
		dbQuery.setBlankQueryAllowed(true);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		for (DBQueryRow row : allRows) {
			DBRow get = row.get(this);
			results.add(get == null ? null : (A) fieldProp.getPropertyWrapperDefinition().rawJavaValue(get));
		}
		return results;
	}

	/**
	 * Checks if the fields of the object have been changed
	 *
	 * <p>
	 * Checks if any of the columns has a Permitted/Excluded method set.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return true if the row has a condition set.
	 */
	public boolean hasConditionsSet() {
		List<PropertyWrapper> props = getWrapper().getColumnPropertyWrappers();
		for (PropertyWrapper prop : props) {
			if (prop.isColumn()) {
				QueryableDatatype<?> qdt = prop.getQueryableDatatype();
				if (qdt.getOperator() != null) {
					return true;
				}
			}
		}
		return false;
	}

	public void setReturnFieldsBasedOn(DBRow tableRow) {
		this.setReturnColumns(tableRow.getReturnColumns());
	}

	/**
	 * Find all the DBRow subclasses in the package.
	 *
	 * <p>
	 * Because sometimes you need everything.
	 *
	 * <p>
	 * Might be helpful for recreating the database, but I recommend
	 * triple-checking everything.
	 *
	 * @param referencePackage
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of all the direct subclasses of DBRow from the specified
	 * package.
	 */
	public static List<DBRow> getDBRowSubclassesFromPackage(Package referencePackage) throws UnableToInstantiateDBRowSubclassException {
		List<DBRow> resultList = new ArrayList<>();
		Reflections reflections = new Reflections(referencePackage);
		Set<Class<? extends DBRow>> tables = reflections.getSubTypesOf(DBRow.class);
		for (Class<? extends DBRow> tab : tables) {
			if (!Modifier.isAbstract(tab.getModifiers())
					&& tab.getSuperclass().equals(DBRow.class)
					&& tab.getPackage().equals(referencePackage)) {
				DBRow tabInstance;
				try {
					tabInstance = tab.newInstance();
				} catch (InstantiationException | IllegalAccessException ex) {
					throw new UnableToInstantiateDBRowSubclassException(tab, ex);
				}
				resultList.add(tabInstance);
			}
		}
		return resultList;
	}

	void removeConstraints() {
		RowDefinitionInstanceWrapper wrapper = getWrapper();
		List<PropertyWrapper> propertyWrappers = wrapper.getColumnPropertyWrappers();
		for (PropertyWrapper propertyWrapper : propertyWrappers) {
			propertyWrapper.getQueryableDatatype().removeConstraints();
		}
	}

	@SuppressWarnings("unchecked")
	void setAutoFilledFields(DBQueryable query) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		boolean arrayRequired = false;
		boolean listRequired = false;
		try {
			List<PropertyWrapper> fields = this.getAutoFillingPropertyWrappers();
			for (PropertyWrapper field : fields) {
				if (field.isAutoFilling()) {
					Class<?> requiredClass = field.getRawJavaType();
					if (requiredClass.isArray()) {
						requiredClass = requiredClass.getComponentType();
						arrayRequired = true;
					} else if (Collection.class.isAssignableFrom(requiredClass)) {
						listRequired = true;
						requiredClass = field.getAutoFillingClass();
						if (requiredClass.isAssignableFrom(DBRow.class)) {
							throw new nz.co.gregs.dbvolution.exceptions.UnacceptableClassForAutoFillAnnotation(field, requiredClass);
						}
					}
					if (DBRow.class.isAssignableFrom(requiredClass)) {
						DBRow fieldInstance;
						try {
							fieldInstance = (DBRow) requiredClass.newInstance();
						} catch (InstantiationException | IllegalAccessException ex) {
							throw new UnableToInstantiateDBRowSubclassException((Class<? extends DBRow>) requiredClass, ex);
						}
						List<DBRow> relatedInstancesFromQuery = this.getRelatedInstancesFromQuery(query, fieldInstance);
						if (arrayRequired) {
							Object newInstance = Array.newInstance(requiredClass, relatedInstancesFromQuery.size());
							for (int index = 0; index < relatedInstancesFromQuery.size(); index++) {
								Array.set(newInstance, index, relatedInstancesFromQuery.get(index));
							}
							field.setRawJavaValue(newInstance);
						} else if (listRequired) {
							field.setRawJavaValue(relatedInstancesFromQuery);
						} else if (relatedInstancesFromQuery.isEmpty()) {
							field.setRawJavaValue(null);
						} else {
							field.setRawJavaValue(relatedInstancesFromQuery.get(0));
						}
					}
				}
			}
		} catch (UnacceptableClassForAutoFillAnnotation | UnableToInstantiateDBRowSubclassException | SQLException | NegativeArraySizeException | IllegalArgumentException | ArrayIndexOutOfBoundsException ex) {
			throw new RuntimeException("Unable To AutoFill Field", ex);
		}
	}

	/**
	 * Returns the schema defined for this DBRow in the DBTableName annotation, if
	 * there is one.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the defined schema name.
	 */
	public String getSchemaName() {
		return getWrapper().schemaName();
	}

	public boolean isRequiredTable() {
		return getWrapper().isRequiredTable();
	}

	public String getTableVariantIdentifier() {
		return tableVariantIdentifier;
	}

	public String getTableNameOrVariantIdentifier() {
		if (tableVariantIdentifier == null) {
			return this.getTableName();
		} else {
			return tableVariantIdentifier;
		}
	}

	@Override
	public String getTableVariantAlias() {
		if (tableVariantIdentifier == null) {
			return super.getTableVariantAlias();
		} else {
			return "" + tableVariantIdentifier.hashCode();
		}
	}

	public DBRow setTableVariantIdentifier(String variantName) {
		tableVariantIdentifier = variantName;
		return this;
	}

	public boolean hasAutoIncrementField() {
		List<PropertyWrapper> columns = getColumnPropertyWrappers();
		for (PropertyWrapper column : columns) {
			if (column.isAutoIncrement()) {
				return true;
			}
		}
		return false;
	}

	public PropertyWrapper getAutoIncrementField() {
		List<PropertyWrapper> columns = getColumnPropertyWrappers();
		for (PropertyWrapper column : columns) {
			if (column.isAutoIncrement()) {
				return column;
			}
		}
		return null;
	}

	/**
	 * Default sorting for DBRow in the various collections in DBRow and DBQuery.
	 *
	 */
	private static class ClassNameComparator implements Comparator<Class<?>>, Serializable {

		private static final long serialVersionUID = 1L;

		ClassNameComparator() {
		}

		@Override
		public int compare(Class<?> first, Class<?> second) {
			String firstCanonicalName = first.getCanonicalName();
			String secondCanonicalName = second.getCanonicalName();
			if (firstCanonicalName != null && secondCanonicalName != null) {
				return firstCanonicalName.compareTo(secondCanonicalName);
			} else {
				return first.getSimpleName().compareTo(second.getSimpleName());
			}
		}
	}
}

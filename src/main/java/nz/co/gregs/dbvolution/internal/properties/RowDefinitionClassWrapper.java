package nz.co.gregs.dbvolution.internal.properties;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.DBRow;

import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.exceptions.ReferenceToUndefinedPrimaryKeyException;
import nz.co.gregs.dbvolution.internal.properties.JavaPropertyFinder.PropertyType;
import nz.co.gregs.dbvolution.internal.properties.JavaPropertyFinder.Visibility;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 * Wraps the class-type of an end-user's data model object.Generally it's
 * expected that the class is annotated with DBvolution annotations to mark the
 * table name and the fields or bean properties that map to columns, however
 * this class will work against any class type.<p>
 * To wrap a target object instance, use the
 * {@link #instanceWrapperFor(nz.co.gregs.dbvolution.query.RowDefinition) }
 * method.
 *
 * <p>
 * Note: instances of this class are expensive to create, and are intended to be
 * cached and kept long-term. Instances can be safely shared between DBDatabase
 * instances for different database types.
 *
 * <p>
 * Instances of this class are <i>thread-safe</i>.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Malcolm Lett
 * @param <ROW> the class of the DBRow (probably) that this instance wraps
 */
public class RowDefinitionClassWrapper<ROW extends RowDefinition> implements Serializable {

	private static final long serialVersionUID = 1l;

	private final Class<ROW> adapteeClass;
	private final boolean identityOnly;
	private final TableHandler tableHandler;
	/**
	 * The property that forms the primary key, null if none.
	 */
	private final PropertyWrapperDefinition<ROW, ?>[] primaryKeyProperties;
	/**
	 * All properties of which DBvolution is aware, ordered as first encountered.
	 * Properties are only included if they are columns.
	 */
	private final List<PropertyWrapperDefinition<ROW, ?>> columnProperties;
	private final List<PropertyWrapperDefinition<ROW, ?>> autoFillingProperties;
	private final List<PropertyWrapperDefinition<ROW, ?>> allProperties;
	/**
	 * Column names with original case for doing lookups on case-sensitive
	 * databases. If column names duplicated, stores only the first encountered of
	 * each column name. Assumes validation is done elsewhere in this class. Note:
	 * doesn't need to be synchronized because it's never modified once created.
	 */
	private final Map<String, PropertyWrapperDefinition<ROW, ?>> columnPropertiesByCaseSensitiveColumnName;
	/**
	 * Column names normalized to upper case for doing lookups on case-insensitive
	 * databases. If column names duplicated, stores only the first encountered of
	 * each column name. Assumes validation is done elsewhere in this class. Note:
	 * doesn't need to be synchronized because it's never modified once created.
	 */
	private final Map<String, PropertyWrapperDefinition<ROW, ?>> columnPropertiesByUpperCaseColumnName;
	/**
	 * Lists of properties that would have duplicated columns if-and-only-if using
	 * a case-insensitive database. For each duplicate upper case column name,
	 * lists all properties that have that same upper case column name.
	 *
	 * <p>
	 * We don't know in advance whether the database in use is case-insensitive or
	 * not. So we give case-different duplicates the benefit of doubt and just
	 * record until later. If this class is accessed for use on a case-insensitive
	 * database the exception will be thrown then, on first access to this class.
	 */
	private final Map<String, List<PropertyWrapperDefinition<ROW, ?>>> duplicatedColumnPropertiesByUpperCaseColumnName;
	/**
	 * Indexed by java property name.
	 */
	private final Map<String, PropertyWrapperDefinition<ROW, ?>> columnPropertiesByPropertyName;

	/**
	 * Fully constructs a wrapper for the given class, including performing all
	 * validations that can be performed up front.
	 *
	 * @param clazz the {@code DBRow} class to wrap
	 */
	public RowDefinitionClassWrapper(Class<ROW> clazz) {
		this(clazz, false);
	}

	/**
	 * Internal constructor only. Pass {@code processIdentityOnly=true} when
	 * processing a referenced class.
	 *
	 * <p>
	 * When processing identity only, only the primary key properties are
	 * identified.
	 *
	 *
	 * @param processIdentityOnly pass {@code true} to only process the set of
	 * columns and primary keys, and to ensure that the primary key columns are
	 * valid, but to exclude all other validations on non-primary key columns and
	 * types etc.
	 */
	RowDefinitionClassWrapper(Class<ROW> clazz, boolean processIdentityOnly) {
		adapteeClass = clazz;
		identityOnly = processIdentityOnly;

		// annotation handlers
		tableHandler = new TableHandler(clazz);

		// pre-calculate properties list
		// (note: skip if processing identity only, in order to avoid
		//  all the per-property validation)
		columnProperties = new ArrayList<PropertyWrapperDefinition<ROW, ?>>();
		autoFillingProperties = new ArrayList<PropertyWrapperDefinition<ROW, ?>>();
		allProperties = new ArrayList<PropertyWrapperDefinition<ROW, ?>>();
		if (processIdentityOnly) {
			// identity-only: extract only primary key properties
			JavaPropertyFinder propertyFinder = getColumnPropertyFinder();
			for (JavaProperty<?> javaProperty : propertyFinder.getPropertiesOf(clazz)) {
				ColumnHandler<?> column = new ColumnHandler<>(javaProperty);
				if (column.isColumn() && column.isPrimaryKey()) {
					PropertyWrapperDefinition<ROW, ?> property = new PropertyWrapperDefinition<>(this, javaProperty, processIdentityOnly);
					columnProperties.add(property);
					allProperties.add(property);
				}
			}
		} else {
			// extract all column properties
			int columnIndex = 0;
			JavaPropertyFinder propertyFinder = getColumnOrAutoFillablePropertyFinder();
			for (JavaProperty<?> javaProperty : propertyFinder.getPropertiesOf(clazz)) {
				var property = new PropertyWrapperDefinition<>(this, javaProperty, processIdentityOnly);
				if (property.isColumn()) {
					columnIndex++;
					property.setColumnIndex(columnIndex);
					columnProperties.add(property);
					allProperties.add(property);
				} else {
					autoFillingProperties.add(property);
					allProperties.add(property);
				}
			}
		}

		// pre-calculate primary key
		List<PropertyWrapperDefinition<ROW, ?>> pkProperties = new ArrayList<PropertyWrapperDefinition<ROW, ?>>();
		columnProperties
				.stream()
				.filter(property -> (property.isPrimaryKey()))
				.forEachOrdered(property -> pkProperties.add(property));

		@SuppressWarnings("unchecked")
		final PropertyWrapperDefinition<ROW, ?>[] genericArray = new PropertyWrapperDefinition[]{};
		this.primaryKeyProperties = pkProperties.toArray(genericArray);

		// pre-calculate properties index
		columnPropertiesByCaseSensitiveColumnName = new HashMap<String, PropertyWrapperDefinition<ROW, ?>>();
		columnPropertiesByUpperCaseColumnName = new HashMap<String, PropertyWrapperDefinition<ROW, ?>>();
		columnPropertiesByPropertyName = new HashMap<String, PropertyWrapperDefinition<ROW, ?>>();
		duplicatedColumnPropertiesByUpperCaseColumnName = new HashMap<String, List<PropertyWrapperDefinition<ROW, ?>>>();
		for (PropertyWrapperDefinition<ROW, ?> property : allProperties) {
			// add unique values for case-insensitive lookups
			// (defer erroring until actually know database is case insensitive)
			if (property.isColumn()) {
				columnPropertiesByPropertyName.put(property.javaName(), property);

				// add unique values for case-sensitive lookups
				// (error immediately on collisions)
				if (columnPropertiesByCaseSensitiveColumnName.containsKey(property.getColumnName())) {
					if (!processIdentityOnly) {
						throw new ReferenceToUndefinedPrimaryKeyException("Class " + clazz.getName() + " has multiple properties for column " + property.getColumnName());
					}
				} else {
					columnPropertiesByCaseSensitiveColumnName.put(property.getColumnName(), property);
				}

				if (columnPropertiesByUpperCaseColumnName.containsKey(property.getColumnName().toUpperCase())) {
					if (!processIdentityOnly) {
						List<PropertyWrapperDefinition<ROW, ?>> list = duplicatedColumnPropertiesByUpperCaseColumnName.get(property.getColumnName().toUpperCase());
						if (list == null) {
							list = new ArrayList<PropertyWrapperDefinition<ROW, ?>>();
							list.add(columnPropertiesByUpperCaseColumnName.get(property.getColumnName().toUpperCase()));
						}
						list.add(property);
						duplicatedColumnPropertiesByUpperCaseColumnName.put(property.getColumnName().toUpperCase(), list);
					}
				} else {
					columnPropertiesByUpperCaseColumnName.put(property.getColumnName().toUpperCase(), property);
				}
			}
		}
	}

	/**
	 * Gets a new instance of the java property finder, configured as required
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A new JavePropertyFinder for all fields, public methods, that have
	 * DBColumn annotation, and are fields or beans.
	 */
	private static JavaPropertyFinder getColumnPropertyFinder() {
		return new JavaPropertyFinder(
				Visibility.PRIVATE, Visibility.PUBLIC,
				JavaPropertyFilter.COLUMN_PROPERTY_FILTER,
				PropertyType.FIELD, PropertyType.BEAN_PROPERTY);
	}

	private static JavaPropertyFinder getColumnOrAutoFillablePropertyFinder() {
		return new JavaPropertyFinder(
				Visibility.PRIVATE, Visibility.PUBLIC,
				JavaPropertyFilter.COLUMN_OR_AUTOFILLABLE_PROPERTY_FILTER,
				PropertyType.FIELD, PropertyType.BEAN_PROPERTY);
	}

	/**
	 * Checks for errors that can't be known in advance without knowing the
	 * database being accessed.
	 *
	 * @param database active database
	 */
	private void checkForRemainingErrorsOnAcccess(DBDatabase database) {
		// check for case-differing duplicate columns
		if (database.getDefinition().isColumnNamesCaseSensitive()) {
			if (!duplicatedColumnPropertiesByUpperCaseColumnName.isEmpty()) {
				StringBuilder buf = new StringBuilder();
				for (var props : duplicatedColumnPropertiesByUpperCaseColumnName.values()) {
					for (var property : props) {
						if (buf.length() > 0) {
							buf.append(", ");
						}
						buf.append(property.getColumnName());
					}
				}

				throw new DBRuntimeException("The following columns are referenced multiple times on case-insensitive databases: " + buf.toString());
			}
		}
	}

	/**
	 * Gets an object wrapper instance for the given target object
	 *
	 * @param target the {@code DBRow} instance
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return A RowDefinitionInstanceWrapper for the supplied target.
	 */
	public RowDefinitionInstanceWrapper<ROW> instanceWrapperFor(ROW target) {
		if (identityOnly) {
			throw new AssertionError("Attempt to access non-identity information of identity-only DBRow class wrapper");
		}
//		checkForRemainingErrorsOnAcccess(database);
		return new RowDefinitionInstanceWrapper<ROW>(this, target);
	}

	/**
	 * Gets a string representation suitable for debugging.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a string representation of this object.
	 */
	@Override
	public String toString() {
		if (isTable()) {
			return getClass().getSimpleName() + "<" + tableName() + ":" + adapteeClass.getName() + ">";
		} else {
			return getClass().getSimpleName() + "<no-table:" + adapteeClass.getName() + ">";
		}
	}

	/**
	 * Two {@code RowDefinitionClassWrappers} are equal if they wrap the same
	 * classes.
	 *
	 * @param obj	obj
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return {@code true} if the two objects are equal, {@code false} otherwise.
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof RowDefinitionClassWrapper)) {
			return false;
		}
		@SuppressWarnings("unchecked")
		RowDefinitionClassWrapper<ROW> other = (RowDefinitionClassWrapper<ROW>) obj;
		if (adapteeClass == null) {
			if (other.adapteeClass != null) {
				return false;
			}
		} else if (!adapteeClass.equals(other.adapteeClass)) {
			return false;
		}
		return true;
	}

	/**
	 * Calculates the hash-code based on the hash-code of the wrapped class.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the hash-code
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((adapteeClass == null) ? 0 : adapteeClass.hashCode());
		return result;
	}

	/**
	 * Gets the underlying wrapped class.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the DBRow or Object wrapped by this instance.
	 */
	public Class<ROW> adapteeClass() {
		return adapteeClass;
	}

	/**
	 * Gets the simple name of the class being wrapped by this adaptor.
	 * <p>
	 * Use {@link #tableName()} for the name of the table mapped to this class.
	 *
	 * <p>
	 * Equivalent to {@code this.adaptee().getSimpleName();}
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the SimpleName of the class being wrapped.
	 */
	public String javaName() {
		return adapteeClass.getSimpleName();
	}

	/**
	 * Gets the fully qualified name of the class being wrapped by this adaptor.
	 * <p>
	 * Use {@link #tableName()} for the name of the table mapped to this class.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the fully qualified name of the class being wrapped.
	 */
	public String qualifiedJavaName() {
		return adapteeClass.getName();
	}

	/**
	 * Indicates whether this class maps to a database table.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if this RowDefinitionClassWrapper represents a database table
	 * or view, otherwise FALSE.
	 */
	public boolean isTable() {
		return tableHandler.isTable();
	}

	/**
	 * Gets the indicated table name. Applies defaulting if the
	 * {@link DBTableName} annotation is present but doesn't provide an explicit
	 * table name.
	 *
	 * <p>
	 * If the {@link DBTableName} annotation is missing, this method returns
	 * {@code null}.
	 *
	 * <p>
	 * Use {@link TableHandler#getDBTableNameAnnotation() } for low level access.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the table name, if specified explicitly or implicitly.
	 */
	public String tableName() {
		return tableHandler.getTableName();
	}

	/**
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the table name, if specified explicitly or implicitly.
	 */
	public String selectQuery() {
		return tableHandler.getSelectQuery();
	}

	/**
	 * Gets the property that is the primary key, if one is marked.
	 *
	 * <p>
	 * Note: multi-column primary key tables are not yet supported.</p>
	 *
	 * @return the primary key property or null if no primary key
	 */
	public PropertyWrapperDefinition<ROW, ?>[] primaryKeyDefinitions() {
		return Arrays.copyOf(primaryKeyProperties, primaryKeyProperties.length);
	}

	/**
	 * Gets the property associated with the given column. If multiple properties
	 * are annotated for the same column, this method will return only the first.
	 *
	 * <p>
	 * Only provides access to properties annotated with {@code DBColumn}.
	 *
	 * <p>
	 * Assumes validation is applied elsewhere to prohibit duplication of column
	 * names.
	 *
	 * @param database active database
	 * @param columnName columnName columnName
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the PropertyWrapperDefinition for the column name supplied. Null if
	 * no such column is found.
	 * @throws AssertionError if called when in {@code identityOnly} mode.
	 */
	public PropertyWrapperDefinition<ROW, ?> getPropertyDefinitionByColumn(DBDatabase database, String columnName) {
		if (identityOnly) {
			throw new AssertionError("Attempt to access non-identity information of identity-only DBRow class wrapper");
		}

		checkForRemainingErrorsOnAcccess(database);
		if (database.getDefinition().isColumnNamesCaseSensitive()) {
			return columnPropertiesByUpperCaseColumnName.get(columnName.toUpperCase());
		} else {
			return columnPropertiesByCaseSensitiveColumnName.get(columnName);
		}
	}

	/**
	 * Like {@link #getPropertyDefinitionByColumn(DBDatabase, String)} except that
	 * handles the case where the database definition is not yet known, and thus
	 * returns all possible matching properties by column name.
	 *
	 * <p>
	 * Assumes working in "identity-only" mode.
	 *
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the non-null list of matching property definitions, with only
	 * identity information available, empty if no such properties found
	 */
	List<PropertyWrapperDefinition<ROW, ?>> getPropertyDefinitionIdentitiesByColumnNameCaseInsensitive(String columnName) {
		List<PropertyWrapperDefinition<ROW, ?>> list = new ArrayList<PropertyWrapperDefinition<ROW, ?>>();
		JavaPropertyFinder propertyFinder = getColumnPropertyFinder();
		for (JavaProperty<?> javaProperty : propertyFinder.getPropertiesOf(adapteeClass)) {
			ColumnHandler<?> column = new ColumnHandler<>(javaProperty);
			if (column.isColumn() && column.getColumnName().equalsIgnoreCase(columnName)) {
				PropertyWrapperDefinition<ROW, ?> property = new PropertyWrapperDefinition<>(this, javaProperty, true);
				list.add(property);
			}
		}
		return list;
	}

	/**
	 * Gets the property by its java property name.
	 * <p>
	 * Only provides access to properties annotated with {@code DBColumn}.
	 *
	 * <p>
	 * It's legal for a field and bean-property to have the same name, and to both
	 * be annotated, but for different columns. This method doesn't handle that
	 * well and returns only the first one it sees.
	 *
	 * @param propertyName	propertyName
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the PropertyWrapperDefinition for the named object property Null if
	 * no such property is found.
	 * @throws AssertionError if called when in {@code identityOnly} mode.
	 */
	public PropertyWrapperDefinition<ROW, ?> getPropertyDefinitionByName(String propertyName) {
		if (identityOnly) {
			throw new AssertionError("Attempt to access non-identity information of identity-only DBRow class wrapper");
		}
		return columnPropertiesByPropertyName.get(propertyName);
	}

	/**
	 * Gets all properties annotated with {@code DBColumn}.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a List of all PropertyWrapperDefinitions for the wrapped class.
	 */
	public List<PropertyWrapperDefinition<ROW, ?>> getColumnPropertyDefinitions() {
		if (identityOnly) {
			throw new AssertionError("Attempt to access non-identity information of identity-only DBRow class wrapper");
		}
		return columnProperties;
	}

	/**
	 * Gets all properties NOT annotated with {@code DBColumn}.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a List of all PropertyWrapperDefinitions for the wrapped class.
	 */
	public List<PropertyWrapperDefinition<ROW, ?>> getAutoFillingPropertyDefinitions() {
		if (identityOnly) {
			throw new AssertionError("Attempt to access non-identity information of identity-only DBRow class wrapper");
		}
		return autoFillingProperties;
	}

	/**
	 * Gets all foreign key properties.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a list of ProperyWrapperDefinitions for all the foreign keys
	 * defined in the wrapped object
	 */
	public List<PropertyWrapperDefinition<ROW, ?>> getForeignKeyPropertyDefinitions() {
		if (identityOnly) {
			throw new AssertionError("Attempt to access non-identity information of identity-only DBRow class wrapper");
		}

		List<PropertyWrapperDefinition<ROW, ?>> list = new ArrayList<PropertyWrapperDefinition<ROW, ?>>();
		columnProperties
				.stream()
				.filter(property -> (property.isColumn() && property.isForeignKey()))
				.forEachOrdered(property -> {
					list.add(property);
				});
		return list;
	}

	/**
	 * Gets all recursive foreign key properties.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a list of ProperyWrapperDefinitions for all the recursive foreign
	 * keys defined in the wrapped object
	 */
	public List<PropertyWrapperDefinition<ROW, ?>> getRecursiveForeignKeyPropertyDefinitions() {
		if (identityOnly) {
			throw new AssertionError("Attempt to access non-identity information of identity-only DBRow class wrapper");
		}
		List<PropertyWrapperDefinition<ROW, ?>> list = new ArrayList<PropertyWrapperDefinition<ROW, ?>>();
		for (var property : columnProperties) {
			try {
				final RowDefinition newInstance = this.adapteeClass().getConstructor().newInstance();
				if (property.isColumn() && property.isForeignKey() && property.isForeignKeyTo(newInstance)) {
					list.add(property);
				}
			} catch (InstantiationException | IllegalAccessException | NoSuchMethodException | SecurityException | IllegalArgumentException | InvocationTargetException ex) {
				Logger.getLogger(RowDefinitionClassWrapper.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
		return list;
	}

	/**
	 * Gets all primary key properties.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a list of ProperyWrapperDefinitions for all the foreign keys
	 * defined in the wrapped object
	 */
	public List<PropertyWrapperDefinition<ROW, ?>> getPrimaryKeyPropertyDefinitions() {
		if (identityOnly) {
			throw new AssertionError("Attempt to access non-identity information of identity-only DBRow class wrapper");
		}

		List<PropertyWrapperDefinition<ROW, ?>> list = new ArrayList<PropertyWrapperDefinition<ROW, ?>>();
		columnProperties
				.stream()
				.filter(property -> (property.isColumn() && property.isPrimaryKey()))
				.forEachOrdered(property -> {
					list.add(property);
				});
		return list;
	}

	String schemaName() {
		return tableHandler.getSchemaName();
	}

	boolean isRequiredTable() {
		return tableHandler.isRequiredTable();
	}
}

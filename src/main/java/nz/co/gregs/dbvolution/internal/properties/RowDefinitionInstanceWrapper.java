package nz.co.gregs.dbvolution.internal.properties;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 * Wraps a specific target object according to its type's
 * {@link RowDefinitionClassWrapper}.
 *
 * <p>
 * To create instances of this type, call
 * {@link RowDefinitionWrapperFactory#instanceWrapperFor(nz.co.gregs.dbvolution.query.RowDefinition)}
 * on the appropriate {@link RowDefinition}.
 *
 * <p>
 * Instances of this class are lightweight and efficient to create, and they are
 * intended to be short lived. Instances of this class must not be shared
 * between different DBDatabase instances, however they can be safely associated
 * within a single DBDatabase instance.
 *
 * <p>
 * Instances of this class are <i>thread-safe</i>.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Malcolm Lett
 */
public class RowDefinitionInstanceWrapper implements Serializable{

	private static final long serialVersionUID = 1l;

	private final RowDefinitionClassWrapper classWrapper;
	private final RowDefinition rowDefinition;
	private final List<PropertyWrapper> allProperties;
	private final List<PropertyWrapper> columnProperties;
	private final List<PropertyWrapper> autoFillingProperties;
	private final List<PropertyWrapper> foreignKeyProperties;
	private final List<PropertyWrapper> primaryKeyProperties;

	/**
	 * Called by
	 * {@link DBRowClassWrapper#instanceAdaptorFor(DBDefinition, Object)}.
	 *
	 *
	 * @param rowDefinition the target object of the same type as analyzed by
	 * {@code classWrapper}
	 */
	RowDefinitionInstanceWrapper(RowDefinitionClassWrapper classWrapper, RowDefinition rowDefinition) {
		if (rowDefinition == null) {
			throw new DBRuntimeException("Target object is null");
		}
		if (!classWrapper.adapteeClass().isInstance(rowDefinition)) {
			throw new DBRuntimeException("Target object's type (" + rowDefinition.getClass().getName()
					+ ") is not compatible with given class adaptor for type " + classWrapper.adapteeClass().getName()
					+ " (this is probably a bug in DBvolution)");
		}

		this.rowDefinition = rowDefinition;
		this.classWrapper = classWrapper;

		// pre-cache commonly used things
		// (note: if you change this to use lazy-initialisation, you'll have to
		// add explicit synchronisation, or it won't be thread-safe anymore)
		this.allProperties = new ArrayList<PropertyWrapper>();
		this.columnProperties = new ArrayList<PropertyWrapper>();
		for (PropertyWrapperDefinition propertyDefinition : classWrapper.getColumnPropertyDefinitions()) {
			final PropertyWrapper propertyWrapper = new PropertyWrapper(this, propertyDefinition, rowDefinition);
			addPropertyWrapperToCollection(columnProperties, propertyWrapper);
//			this.columnProperties.add(propertyWrapper);
//			this.allProperties.add(propertyWrapper);
//			if (propertyWrapper.isAutoFilling()){
//				autoFillingProperties.add(propertyWrapper);
//			}
		}
		this.autoFillingProperties = new ArrayList<PropertyWrapper>();
		for (PropertyWrapperDefinition propertyDefinition : classWrapper.getAutoFillingPropertyDefinitions()) {
			final PropertyWrapper propertyWrapper = new PropertyWrapper(this, propertyDefinition, rowDefinition);
			addPropertyWrapperToCollection(autoFillingProperties, propertyWrapper);
		}

		this.foreignKeyProperties = new ArrayList<PropertyWrapper>();
		for (PropertyWrapperDefinition propertyDefinition : classWrapper.getForeignKeyPropertyDefinitions()) {
			this.foreignKeyProperties.add(new PropertyWrapper(this, propertyDefinition, rowDefinition));
		}

		this.primaryKeyProperties = new ArrayList<PropertyWrapper>();
		for (PropertyWrapperDefinition propertyDefinition : classWrapper.primaryKeyDefinitions()) {
			this.primaryKeyProperties.add(new PropertyWrapper(this, propertyDefinition, rowDefinition));
		}
	}

	private void addPropertyWrapperToCollection(List<PropertyWrapper> collection, PropertyWrapper propertyWrapper) {
		collection.add(propertyWrapper);
		this.allProperties.add(propertyWrapper);
	}

	/**
	 * Gets a string representation suitable for debugging.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a String representing this object sufficient for debugging purposes
	 */
	@Override
	public String toString() {
		if (isTable()) {
			return getClass().getSimpleName() + "<" + tableName() + ":" + classWrapper.adapteeClass().getName() + ">";
		} else {
			return getClass().getSimpleName() + "<no-table:" + classWrapper.adapteeClass().getName() + ">";
		}
	}

	/**
	 * Two {@code RowDefinitionInstanceWrappers} are equal if they wrap two
	 * {@code RowDefinition} instances that are themselves equal, and are
	 * instances of the same class.
	 *
	 * @param obj the other object to compare to.
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
		if (!(obj instanceof RowDefinitionInstanceWrapper)) {
			return false;
		}
		RowDefinitionInstanceWrapper other = (RowDefinitionInstanceWrapper) obj;
		if (classWrapper == null) {
			if (other.classWrapper != null) {
				return false;
			}
		} else if (!classWrapper.equals(other.classWrapper)) {
			return false;
		}
		if (rowDefinition == null) {
			if (other.rowDefinition != null) {
				return false;
			}
		} else if (!rowDefinition.equals(other.rowDefinition)) {
			return false;
		}
		return true;
	}

	/**
	 * Calculates the hash-code based on the hash-code of the wrapped @{code
	 * RowDefinition} instance and its class.
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
		result = prime * result + ((classWrapper == null) ? 0 : classWrapper.hashCode());
		result = prime * result + ((rowDefinition == null) ? 0 : rowDefinition.hashCode());
		return result;
	}

	/**
	 * Gets the class-wrapper for the class of wrapped {@code RowDefinition}
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the class-wrapper
	 */
	public RowDefinitionClassWrapper getClassWrapper() {
		return classWrapper;
	}

	/**
	 * Gets the wrapped object type supported by this {@code ObjectAdaptor}. Note:
	 * this should be the same as the wrapped object's actual type.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the class of the wrapped instance
	 */
	public Class<? extends RowDefinition> adapteeRowDefinitionClass() {
		return classWrapper.adapteeClass();
	}

	/**
	 * Gets the {@link RowDefinition} instance wrapped by this
	 * {@code ObjectAdaptor}.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the {@link RowDefinition} (usually a {@link DBRow} or
	 * {@link DBReport}) for this instance.
	 */
	public RowDefinition adapteeRowDefinition() {
		return rowDefinition;
	}

	/**
	 * Gets the simple name of the class being wrapped by this adaptor.
	 * <p>
	 * Use {@link #tableName()} for the name of the table mapped to this class.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the simple class name of the wrapped RowDefinition
	 */
	public String javaName() {
		return classWrapper.javaName();
	}

	/**
	 * Gets the fully qualified name of the class being wrapped by this adaptor.
	 * <p>
	 * Use {@link #tableName()} for the name of the table mapped to this class.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the full class name of the wrapped RowDefinition
	 */
	public String qualifiedJavaName() {
		return classWrapper.qualifiedJavaName();
	}

	/**
	 * Indicates whether this class maps to a database table.
	 *
	 * <p>
	 * If the wrapped {@link RowDefinition} is a {@link DBRow} and thus maps
	 * directly to a table or view, this method returns true. Other
	 * RowDefinitions, probably {@link DBReport}, will return false.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if this RowDefinition maps directly to a table or view, FALSE
	 * otherwise
	 */
	public boolean isTable() {
		return classWrapper.isTable();
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the table name, if specified explicitly or implicitly.
	 */
	public String tableName() {
		return classWrapper.tableName();
	}

	public String selectQuery() {
		return classWrapper.selectQuery();
	}

	/**
	 * Gets the property that is the primary key, if one is marked. Note:
	 * multi-column primary key tables are not yet supported.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the primary key property or null if no primary key
	 */
	public List<PropertyWrapper> getPrimaryKeysPropertyWrappers() {
		return primaryKeyProperties;
	}

	/**
	 * Gets the property associated with the given column.
	 *
	 * <p>
	 * If multiple properties are annotated for the same column, this method will
	 * return only the first.
	 *
	 * <p>
	 * Only provides access to properties annotated with {@code DBColumn}.
	 *
	 * <p>
	 * Assumes validation is applied elsewhere to prohibit duplication of column
	 * names.
	 *
	 * @param database database
	 * @param columnName columnName
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the Java property associated with the column name supplied. Null if
	 * no such column is found.
	 */
	public PropertyWrapper getPropertyByColumn(DBDatabase database, String columnName) {
		PropertyWrapperDefinition classProperty = classWrapper.getPropertyDefinitionByColumn(database, columnName);
		return (classProperty == null) ? null : new PropertyWrapper(this, classProperty, rowDefinition);
	}

	/**
	 * Gets the property by its java field name.
	 * <p>
	 * Only provides access to properties annotated with {@code DBColumn}.
	 *
	 * @param propertyName propertyName
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return property of the wrapped {@link RowDefinition} associated with the
	 * java field name supplied. Null if no such property is found.
	 */
	public PropertyWrapper getPropertyByName(String propertyName) {
		PropertyWrapperDefinition classProperty = classWrapper.getPropertyDefinitionByName(propertyName);
		return (classProperty == null) ? null : new PropertyWrapper(this, classProperty, rowDefinition);
	}

	/**
	 * Gets all properties that are annotated with {@code DBColumn}. This method
	 * is intended for where you need to get/set property values on all properties
	 * in the class.
	 *
	 * <p>
	 * Note: if you wish to iterate over the properties and only use their
	 * definitions (ie: meta-information), this method is not efficient. Use
	 * {@link #getColumnPropertyDefinitions()} instead in that case.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the non-null list of properties, empty if none
	 */
	public List<PropertyWrapper> getColumnPropertyWrappers() {
		return columnProperties;
	}

	/**
	 * Gets all properties that are NOT annotated with {@code DBColumn}. This
	 * method is intended for where you need to get/set property values on all
	 * properties in the class.
	 *
	 * <p>
	 * Note: if you wish to iterate over the properties and only use their
	 * definitions (ie: meta-information), this method is not efficient. Use
	 * {@link #getColumnPropertyDefinitions()} instead in that case.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the non-null list of properties, empty if none
	 */
	public List<PropertyWrapper> getAutoFillingPropertyWrappers() {
		return autoFillingProperties;
	}

	/**
	 * Gets all foreign key properties.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return non-null list of PropertyWrappers, empty if no foreign key
	 * properties
	 */
	public List<PropertyWrapper> getForeignKeyPropertyWrappers() {
		return foreignKeyProperties;
	}

	/**
	 * Gets all foreign key properties as property definitions.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a non-null list of PropertyWrapperDefinitions, empty if no foreign
	 * key properties
	 */
	public List<PropertyWrapperDefinition> getForeignKeyPropertyWrapperDefinitions() {
		return classWrapper.getForeignKeyPropertyDefinitions();
	}

	/**
	 * Gets all property definitions that are annotated with {@code DBColumn}.
	 * This method is intended for where you need to examine meta-information
	 * about all properties in a class.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a list of PropertyWrapperDefinitions for the PropertyWrappers of
	 * this RowDefinition
	 */
	public List<PropertyWrapperDefinition> getColumnPropertyDefinitions() {
		return classWrapper.getColumnPropertyDefinitions();
	}

	public String schemaName() {
		return classWrapper.schemaName();
	}

	public boolean isRequiredTable() {
		return classWrapper.isRequiredTable();
	}
}

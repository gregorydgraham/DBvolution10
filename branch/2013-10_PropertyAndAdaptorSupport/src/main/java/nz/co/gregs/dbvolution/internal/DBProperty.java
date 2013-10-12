package nz.co.gregs.dbvolution.internal;

import nz.co.gregs.dbvolution.DBThrownByEndUserCodeException;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 * Abstracts a java field or bean-property on a target object as a DBvolution-centric
 * property, which contains values from a specific column in a database table.
 * Transparently handles all annotations associated with the property,
 * including type adaption.
 * 
 * <p> Provides access to the meta-data defined on a single java property of a class,
 * and provides methods for reading and writing the value of the property
 * on a single bound object, given a specified database definition.
 * 
 * <p> DB properties can be seen to have the types and values in the table that follows.
 * This class provides a virtual view over the property whereby the DBv-centric type
 * and value are easily accessible via the {@link #value(Object) value()} and
 * {@link #setValue(Object, QueryableDatatype) setValue()} methods.
 * <ul>
 * <li> rawType/rawValue - the type and value actually stored on the declared java property
 * <li> dbvType/dbvValue - the type and value used within DBv (a QueryableDataType)
 * <li> databaseType/databaseValue - the type and value of the database column itself (this class doesn't deal with these) 
 * </ul>
 * 
 * <p> Note: instances of this class cheap to create and do not need to be cached.
 * 
 * <p> This class is <i>thread-safe</i>.
 */
public class DBProperty {
	private final ClassDBProperty classProperty;
	private final Object target;
	
	/**
	 * @param classProperty the class-level wrapper
	 * @param target the target object containing the given property
	 */
	public DBProperty(ClassDBProperty classProperty, Object target) {
		this.classProperty = classProperty;
		this.target = target;
	}
	
	/**
	 * Gets the name of the java property.
	 * Mainly used within error messages.
	 * 
	 * <p> Use {@link #columnName()} to determine column name.
	 * @return
	 */
	public String name() {
		return classProperty.name();
	}

	/**
	 * Gets the DBvolution-centric type of the property.
	 * If a type adaptor is present, then this is the type after conversion
	 * from the target object's actual property type.
	 * 
	 * <p> Use {@link #getRawType()} in the rare case that you need to know the underlying
	 * java property type.
	 * @return
	 */
	public Class<? extends QueryableDatatype> type() {
		return classProperty.type();
	}
	
	/**
	 * Gets the annotated column name.
	 * Applies defaulting if the {@code DBColumn} annotation is present
	 * but does not explicitly specify the column name.
	 * 
	 * <p> If the {@code DBColumn} annotation is missing, this method returns {@code null}.
	 * 
	 * <p> Use {@link #getDBColumnAnnotation} for low level access.
	 * @return the column name, if specified explicitly or implicitly
	 */
	public String columnName() {
		return classProperty.columnName();
	}

	/**
	 * Indicates whether this property is a column.
	 * @return {@code true} if this property is a column
	 */
	public boolean isColumn() {
		return classProperty.isColumn();
	}
	
	/**
	 * Indicates whether this property is a primary key.
	 * @return {@code true} if this property is a primary key
	 */
	public boolean isPrimaryKey() {
		return classProperty.isPrimaryKey();
	}
	
	/**
	 * Indicates whether this property is a foreign key.
	 * @return {@code true} if this property is a foreign key
	 */
	public boolean isForeignKey() {
		return classProperty.isForeignKey();
	}

	/**
	 * Gets the class referenced by this property, if this property
	 * is a foreign key.
	 * @return the referenced class or null if not applicable
	 */
	public Class<?> referencedClass() {
		return classProperty.referencedClass();
	}
	
	/**
	 * Gets the table referenced by this property, if this property
	 * is a foreign key.
	 * @return the referenced table name, or null if not applicable
	 */
	public String referencedTableName() {
		return classProperty.referencedTableName();
	}
	
	/**
	 * Gets the column name in the foreign table referenced by this property,
	 * if this property is a foreign key.
	 * Referenced column names may not be specified, in which case the foreign key
	 * references the primary key in the foreign class/table.
	 * 
	 * <p> Use {@link #getDBForeignKeyAnnotation} for low level access.
	 * @return the referenced column name, or null if not specified or not applicable
	 */
	public String referencedColumnName() {
		return classProperty.referencedColumnName();
	}

	/**
	 * Indicates whether the value of the property can be retrieved.
	 * Bean properties which are missing a 'getter' can not be read,
	 * but may be able to be set.
	 * @return
	 */
	public boolean isReadable() {
		return classProperty.isReadable();
	}

	/**
	 * Indicates whether the value of the property can be modified.
	 * Bean properties which are missing a 'setter' can not be written to,
	 * but may be able to be read.
	 * @return
	 */
	public boolean isWritable() {
		return classProperty.isWritable();
	}

	/**
	 * Gets the DBvolution-centric value of the property.
	 * The value returned may have undergone type conversion from the target object's
	 * actual property type, if a type adaptor is present.
	 * 
	 * <p> Use {@link #isReadable()} beforehand to check whether the property
	 * can be read.
	 * @return
	 * @throws IllegalStateException if not readable (you should have called isReadable() first)
	 * @throws DBThrownByEndUserCodeException if any user code throws an exception
	 */
	public QueryableDatatype value() {
		return classProperty.value(target);
	}
	
	/**
	 * Sets the DBvolution-centric value of the property.
	 * The value set may have undergone type conversion to the target object's
	 * actual property type, if a type adaptor is present.
	 * 
	 * <p> Use {@link #isWritable()} beforehand to check whether the property
	 * can be modified.
	 * @param value
	 * @throws IllegalStateException if not writable (you should have called isWritable() first)
	 * @throws DBThrownByEndUserCodeException if any user code throws an exception
	 */
	public void setValue(QueryableDatatype value) {
		classProperty.setValue(target, value);
	}
	
	/**
	 * Gets the value of the declared property in the end-user's target object,
	 * prior to type conversion to the DBvolution-centric type.
	 * 
	 * <p> In most cases you will not need to call this method, as type
	 * conversion is done transparently via the {@link #value(Object)} and
	 * {@link #setValue(Object, QueryableDatatype)} methods.
	 * 
	 * <p> Use {@link #isReadable()} beforehand to check whether the property
	 * can be read.
	 * @return value
	 * @throws IllegalStateException if not readable (you should have called isReadable() first)
	 * @throws DBThrownByEndUserCodeException if any user code throws an exception
	 */
	public Object rawValue() {
		return classProperty.rawValue(target);
	}
	
	/**
	 * Set the value of the declared property in the end-user's target object,
	 * without type conversion to/from the DBvolution-centric type.
	 * 
	 * <p> In most cases you will not need to call this method, as type
	 * conversion is done transparently via the {@link #value(Object)} and
	 * {@link #setValue(Object, QueryableDatatype)} methods.
	 * 
	 * <p> Use {@link #isWritable()} beforehand to check whether the property
	 * can be modified.
	 * @param value new value
	 * @throws IllegalStateException if not writable (you should have called isWritable() first)
	 * @throws DBThrownByEndUserCodeException if any user code throws an exception
	 */
	public void setRawValue(Object value) {
		classProperty.setRawValue(target, value);
	}
	
	/**
	 * Gets the declared type of the property in the end-user's target object,
	 * prior to type conversion to the DBvolution-centric type.
	 * 
	 * <p> In most cases you will not need to call this method, as type
	 * conversion is done transparently via the {@link #value(Object)} and
	 * {@link #setValue(Object, QueryableDatatype)} methods.
	 * Use the {@link #type()} method to get the DBv-centric property type,
	 * after type conversion.
	 * @return
	 */
	public Class<?> getRawType() {
		return classProperty.getRawType();
	}
}

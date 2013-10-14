package nz.co.gregs.dbvolution.internal;

import nz.co.gregs.dbvolution.DBPebkacException;
import nz.co.gregs.dbvolution.DBThrownByEndUserCodeException;
import nz.co.gregs.dbvolution.databases.definitions.DBDatabase;
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
 * and value are easily accessible via the {@link #getQueryableDatatype(Object) value()} and
 * {@link #setQueryableDatatype(Object, QueryableDatatype) setValue()} methods.
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
	private final DBPropertyDefinition propertyDefinition;
	private final DBDatabase database;
	private final Object target;
	
	/**
	 * @param classProperty the class-level wrapper
	 * @param target the target object containing the given property
	 */
	public DBProperty(DBPropertyDefinition classProperty, DBDatabase database, Object target) {
		this.propertyDefinition = classProperty;
		this.database = database;
		this.target = target;
	}
	
	/**
	 * Gets the name of the java property.
	 * Mainly used within error messages.
	 * 
	 * <p> Use {@link #columnName()} to determine column name.
	 * @return
	 */
	public String javaName() {
		return propertyDefinition.javaName();
	}

	/**
	 * Gets the qualified name of the underlying java property.
	 * Mainly used within logging and error messages.
	 * 
	 * <p> Use {@link #columnName()} to determine column name.
	 * @return
	 */
	public String qualifiedJavaName() {
		return propertyDefinition.qualifiedJavaName();
	}
	
	/**
	 * Gets the DBvolution-centric type of the property.
	 * If a type adaptor is present, then this is the type after conversion
	 * from the target object's actual property type.
	 * 
	 * <p> Use {@link #getRawJavaType()} in the rare case that you need to know the underlying
	 * java property type.
	 * @return
	 */
	public Class<? extends QueryableDatatype> type() {
		return propertyDefinition.type();
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
		return propertyDefinition.columnName();
	}

	/**
	 * Indicates whether this property is a column.
	 * @return {@code true} if this property is a column
	 */
	public boolean isColumn() {
		return propertyDefinition.isColumn();
	}
	
	/**
	 * Indicates whether this property is a primary key.
	 * @return {@code true} if this property is a primary key
	 */
	public boolean isPrimaryKey() {
		return propertyDefinition.isPrimaryKey();
	}
	
	/**
	 * Indicates whether this property is a foreign key.
	 * @return {@code true} if this property is a foreign key
	 */
	public boolean isForeignKey() {
		return propertyDefinition.isForeignKey();
	}

	/**
	 * Gets the class referenced by this property, if this property
	 * is a foreign key.
	 * @return the referenced class or null if not applicable
	 */
	public Class<?> referencedClass() {
		return propertyDefinition.referencedClass();
	}
	
	/**
	 * Gets the table referenced by this property, if this property
	 * is a foreign key.
	 * @return the referenced table name, or null if not applicable
	 */
	public String referencedTableName() {
		return propertyDefinition.referencedTableName();
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
	// TODO update javadoc for this method now that it's got more smarts
	public String referencedColumnName(DBRowClassWrapperFactory cache) {
		return propertyDefinition.referencedColumnName(database, cache);
	}

	/**
	 * Note: this returns only a single property; in the case where multiple foreign key
	 * columns are used together to reference a table with a composite primary key,
	 * each foreign key column references its respective foreign primary key.
	 * @param dbDefin the current active database definition
	 * @param cache the active class adaptor cache
	 * @return the mapped foreign key property, or null if not a foreign key
	 * @throws DBPebkacException if the foreign table has multiple primary keys and the foreign key
	 *         column doesn't identify which primary key column to target
	 */
	// An idea of what could be possible; to be decided whether we want to keep this
	public DBPropertyDefinition referencedProperty(DBRowClassWrapperFactory cache) {
		return propertyDefinition.referencedProperty(database, cache);
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
	// TODO improve javadoc
	public String declaredReferencedColumnName() {
		return propertyDefinition.declaredReferencedColumnName();
	}
	
	/**
	 * Indicates whether the value of the property can be retrieved.
	 * Bean properties which are missing a 'getter' can not be read,
	 * but may be able to be set.
	 * @return
	 */
	public boolean isReadable() {
		return propertyDefinition.isReadable();
	}

	/**
	 * Indicates whether the value of the property can be modified.
	 * Bean properties which are missing a 'setter' can not be written to,
	 * but may be able to be read.
	 * @return
	 */
	public boolean isWritable() {
		return propertyDefinition.isWritable();
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
	public QueryableDatatype getQueryableDatatype() {
		return propertyDefinition.getQueryableDatatype(target);
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
	public void setQueryableDatatype(QueryableDatatype value) {
		propertyDefinition.setQueryableDatatype(target, value);
	}
	
	/**
	 * Gets the value of the declared property in the end-user's target object,
	 * prior to type conversion to the DBvolution-centric type.
	 * 
	 * <p> In most cases you will not need to call this method, as type
	 * conversion is done transparently via the {@link #getQueryableDatatype(Object)} and
	 * {@link #setQueryableDatatype(Object, QueryableDatatype)} methods.
	 * 
	 * <p> Use {@link #isReadable()} beforehand to check whether the property
	 * can be read.
	 * @return value
	 * @throws IllegalStateException if not readable (you should have called isReadable() first)
	 * @throws DBThrownByEndUserCodeException if any user code throws an exception
	 */
	public Object rawJavaValue() {
		return propertyDefinition.rawJavaValue(target);
	}
	
	/**
	 * Set the value of the declared property in the end-user's target object,
	 * without type conversion to/from the DBvolution-centric type.
	 * 
	 * <p> In most cases you will not need to call this method, as type
	 * conversion is done transparently via the {@link #getQueryableDatatype(Object)} and
	 * {@link #setQueryableDatatype(Object, QueryableDatatype)} methods.
	 * 
	 * <p> Use {@link #isWritable()} beforehand to check whether the property
	 * can be modified.
	 * @param value new value
	 * @throws IllegalStateException if not writable (you should have called isWritable() first)
	 * @throws DBThrownByEndUserCodeException if any user code throws an exception
	 */
	public void setRawJavaValue(Object value) {
		propertyDefinition.setRawJavaValue(target, value);
	}
	
	/**
	 * Gets the declared type of the property in the end-user's target object,
	 * prior to type conversion to the DBvolution-centric type.
	 * 
	 * <p> In most cases you will not need to call this method, as type
	 * conversion is done transparently via the {@link #getQueryableDatatype(Object)} and
	 * {@link #setQueryableDatatype(Object, QueryableDatatype)} methods.
	 * Use the {@link #type()} method to get the DBv-centric property type,
	 * after type conversion.
	 * @return
	 */
	public Class<?> getRawJavaType() {
		return propertyDefinition.getRawJavaType();
	}
}

package nz.co.gregs.dbvolution.internal;

import nz.co.gregs.dbvolution.DBRuntimeException;
import nz.co.gregs.dbvolution.DBTypeAdaptor;
import nz.co.gregs.dbvolution.QueryableDataType;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;

/**
 * Adapts a single field or bean property within a class.
 * Includes support for understanding all DBv annotations.
 * 
 * <p> Provides a virtual view over the property that is internally
 * managed as a property's declared type and value which is
 * then mapped by the property's type adaptor.
 * If no explicit type adaptor is provided, then a dummy adaptor is used
 * which returns the same type and value as declared.
 * 
 * <p> 
 * The virtual view of the property is the type and value
 * returned by the adaptor, given the declared type and value on the
 * property itself - which is populated from the database.
 * 
 * <p> It is assumed that type adaptors allow mapping from
 * a DBv property type to a non-DBv type.
 * 
 * <p> Properties have the following types and values:
 * <ul>
 * <li> rawType/rawValue - the type and value actually stored on the declared property
 * <li> dbvType/dbvValue - the type and value used within DBv (a QueryableDataType)
 * <li> databaseType/databaseValue - the type and value of the database itself (this class doesn't deal with these) 
 * </ul>
 * 
 * <p> The current implementations assume that only {@code DBColumn} properties are
 * actually wrapped as instances of {@code Property}.
 */
public interface Property {
	
	/**
	 * Gets the DBvolution-centric value of the property.
	 * The value returned may have undergone type conversion from the target object's
	 * actual property type, if a type adaptor is present.
	 * 
	 * <p> Use {@link #isReadable()} beforehand to check whether the property
	 * can be read.
	 * @return
	 * @throws IllegalStateException if not readable; this exception indicates a bug within DBvolution
	 */
	public Object value();
	
	/**
	 * Sets the DBvolution-centric value of the property.
	 * The value set may have undergone type conversion to the target object's
	 * actual property type, if a type adaptor is present.
	 * 
	 * <p> Use {@link #isWritable()} beforehand to check whether the property
	 * can be modified.
	 * @param value
	 * @throws IllegalStateException if not writable, this exception indicates a bug within DBvolution
	 */
	public void setValue(Object value);
	
	/**
	 * Gets the DBvolution-centric type of the property.
	 * If a type adaptor is present, then this is the type after conversion
	 * from the target object's actual property type.
	 * @return
	 */
	public Class<? extends QueryableDataType> type();
	
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
	public String columnName();
	
	/**
	 * Indicates whether this property is a foreign key.
	 * @return {@code true} if this property is a foreign key
	 */
	public boolean isForeignKey();

	/**
	 * Gets the class referenced by this property, if this property
	 * is a foreign key.
	 * @return the referenced class or null if not applicable
	 */
	public Class<?> foreignClass();
	
	/**
	 * Gets the column name in the foreign table referenced by this property,
	 * if this property is a foreign key.
	 * Referenced column names may not be specified, in which case the foreign key
	 * references the primary key in the foreign class/table.
	 * 
	 * <p> Use {@link #getDBForeignKeyAnnotation} for low level access.
	 * @return the referenced column, or null if not specified or not applicable
	 */
	public String foreignColumnName();
	
	/**
	 * Gets the {@link DBColumn} annotation on the property, if it exists.
	 * @return the annotation or null
	 */
	public DBColumn getDBColumnAnnotation();

	/**
	 * Gets the {@link DBForeignKey} annotation on the property, if it exists.
	 * @return the annotation or null
	 */
	public DBForeignKey getDBForeignKeyAnnotation();
	
	/**
	 * Gets the {@link DBTypeAdaptor} annotation on the property, if it exists.
	 * @return the annotation or null
	 */
	public DBTypeAdaptor getDBTypeAdaptorAnnotation();
	
	/**
	 * Indicates whether the value of the property can be retrieved.
	 * Bean properties which are missing a 'getter' can not be read,
	 * but may be able to be set.
	 * @return
	 */
	public boolean isReadable();

	/**
	 * Indicates whether the value of the property can be modified.
	 * Bean properties which are missing a 'setter' can not be written to,
	 * but may be able to be read.
	 * @return
	 */
	public boolean isWritable();

	/**
	 * Gets the value of the declared property in the end-user's target object,
	 * prior to type conversion to the DBvolution-centric type.
	 * 
	 * <p> Use {@link #isReadable()} beforehand to check whether the property
	 * can be read.
	 * @return value
	 * @throws IllegalStateException if not readable
	 */
	public Object getRawValue();
	
	/**
	 * Set the value of the declared property in the end-user's target object,
	 * without type conversion to/from the DBvolution-centric type.
	 * 
	 * <p> Use {@link #isWritable()} beforehand to check whether the property
	 * can be modified.
	 * @param value new value
	 * @throws IllegalStateException if not writable
	 */
	public void setRawValue(Object value);
	
	/**
	 * Gets the declared type of the property in the end-user's target object,
	 * prior to type conversion to the DBvolution-centric type.
	 * @return
	 */
	public Class<?> getRawType();
}

package nz.co.gregs.dbvolution.internal.properties;

import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.DBThrownByEndUserCodeException;

/**
 * Abstracts a java field or bean-property as a DBvolution-centric
 * property, which contains values from a specific column in a database table.
 * Transparently handles all annotations associated with the property,
 * including type adaption.
 * 
 * <p> Provides access to the meta-data defined on a single java property of a class,
 * and provides methods for reading and writing the value of the property
 * on target objects.
 * Instances of this class are not bound to specific target objects, nor are they
 * bound to specific database definitions.
 * 
 * <p> For binding to specific target objects and database definitions,
 * use the {@link DBProperty} class.
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
 * <p> Note: instances of this class are expensive to create and should be cached.
 * 
 * <p> This class is <i>thread-safe</i>.
 */
public class PropertyWrapperDefinition {
	private final DBRowClassWrapper classWrapper;
	private final JavaProperty adaptee;
	
	private final ColumnHandler columnHandler;
	private final PropertyTypeHandler typeHandler;
	private final ForeignKeyHandler foreignKeyHandler;
	
	PropertyWrapperDefinition(DBRowClassWrapper classWrapper, JavaProperty javaProperty, boolean processIdentityOnly) {
		this.classWrapper = classWrapper;
		this.adaptee = javaProperty;
		
		// handlers
		this.columnHandler = new ColumnHandler(javaProperty);
		this.typeHandler = new PropertyTypeHandler(javaProperty, processIdentityOnly);
		this.foreignKeyHandler = new ForeignKeyHandler(javaProperty, processIdentityOnly);
	}
	
	JavaProperty getRawJavaProperty() {
		return adaptee;
	}
	
	/**
	 * Gets a string representation of the wrapped property,
	 * suitable for debugging and logging.
	 */
	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();
		buf.append(adaptee);
		if (isTypeAdapted()) {
			buf.append(" (");
			buf.append(getRawJavaType().getName());
			buf.append(")");
		}
		return buf.toString();
	}

	/**
	 * Generates a hash-code of this property wrapper definition,
	 * based entirely on the java property it wraps.
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((adaptee == null) ? 0 : adaptee.hashCode());
		return result;
	}

	/**
	 * Equality of this property wrapper definition,
	 * based on the java property it wraps in a specific class.
	 * Two instances are identical if they wrap the same java property (field or bean-property)
	 * in the same class and the same class-loader.
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof PropertyWrapperDefinition)) {
			return false;
		}
		PropertyWrapperDefinition other = (PropertyWrapperDefinition) obj;
		if (adaptee == null) {
			if (other.adaptee != null) {
				return false;
			}
		} else if (!adaptee.equals(other.adaptee)) {
			return false;
		}
		return true;
	}

	/**
	 * Gets the name of the underlying java property.
	 * Mainly used within logging and error messages.
	 * 
	 * <p> Use {@link #columnName()} to determine column name.
	 * @return
	 */
	public String javaName() {
		return adaptee.name();
	}
	
	/**
	 * Gets the qualified name of the underlying java property.
	 * Mainly used within logging and error messages.
	 * @return
	 */
	public String qualifiedJavaName() {
		return adaptee.qualifiedName();
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
		return typeHandler.getType();
	}
	
	/**
	 * Convenience method for testing the type.
	 * Equivalent to {@code refType.isAssignableFrom(this.type())}.
	 * @param refType
	 * @return
	 */
	public boolean isInstanceOf(Class<? extends QueryableDatatype> refType) {
		return refType.isAssignableFrom(type());
	}
	
	/**
	 * Gets the annotated table name of the table this
	 * property belongs to.
	 * Equivalent to {@code getDBRowClassWrapper().tableName()}.
	 * @return
	 */
	public String tableName() {
		return classWrapper.tableName();
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
		return columnHandler.getColumnName();
	}

	/**
	 * Indicates whether this property is a column.
	 * @return {@code true} if this property is a column
	 */
	public boolean isColumn() {
		return columnHandler.isColumn();
	}
	
	/**
	 * Indicates whether this property is a primary key.
	 * @return {@code true} if this property is a primary key
	 */
	public boolean isPrimaryKey() {
		return columnHandler.isPrimaryKey();
	}
	
	/**
	 * Indicates whether this property is a foreign key.
	 * @return {@code true} if this property is a foreign key
	 */
	public boolean isForeignKey() {
		return foreignKeyHandler.isForeignKey();
	}

	/**
	 * Gets the class referenced by this property, if this property
	 * is a foreign key.
	 * @return the referenced class if this property is a foreign key;
	 * null if not a foreign key
	 */
	public Class<? extends DBRow> referencedClass() {
		return foreignKeyHandler.getReferencedClass();
	}
	
	/**
	 * Gets the table referenced by this property, if this property
	 * is a foreign key.
	 * @return the referenced table name if this property is a foreign key;
	 * null if not a foreign key
	 */
	public String referencedTableName() {
		return foreignKeyHandler.getReferencedTableName();
	}
	
	/**
	 * Gets the column name in the foreign table referenced by this property.
	 * The referenced column is either explicitly indicated by use of the
	 * {@link DBForeignKey#column()} attribute, or it is implicitly the
	 * single primary key of the referenced table if the {@link DBForeignKey#column()}
	 * attribute is unset.
	 * 
	 * @return the referenced column name if this property is a foreign key;
	 * null if not a foreign key
	 */
	public String referencedColumnName() {
		return foreignKeyHandler.getReferencedColumnName();
	}

	/**
	 * Gets identity information for the referenced property in the referenced table.
	 * The referenced property is either explicitly indicated by use of the
	 * {@link DBForeignKey#column()} attribute, or it is implicitly the
	 * single primary key of the referenced table.
	 * 
	 * <p> Note that the property definition returned provides identity of
	 * the property only. It provides access to the property's: java name, column name,
	 * type, and identity information about the table it belongs to (ie: table name).
	 * Attempts to get or set its value or get the type adaptor instance will
	 * result in an internal exception.
	 * 
	 * @return the referenced property if this property is a foreign key;
	 * null if not a foreign key
	 */
	public PropertyWrapperDefinition referencedPropertyDefinitionIdentity() {
		return foreignKeyHandler.getReferencedPropertyDefinitionIdentity();
	}

	/**
	 * Indicates whether the value of the property can be retrieved.
	 * Bean properties which are missing a 'getter' can not be read,
	 * but may be able to be set.
	 * @return
	 */
	public boolean isReadable() {
		return adaptee.isReadable();
	}

	/**
	 * Indicates whether the value of the property can be modified.
	 * Bean properties which are missing a 'setter' can not be written to,
	 * but may be able to be read.
	 * @return
	 */
	public boolean isWritable() {
		return adaptee.isWritable();
	}
	
    /**
     * Indicates whether the property's type is adapted by an explicit or
     * implicit type adaptor. (Note: at present there is no support for implicit
     * type adaptors)
     *
     * @return {@code true} if a type adaptor is being used
     */
    public boolean isTypeAdapted() {
        return typeHandler.isTypeAdapted();
    }

	/**
	 * Gets the DBvolution-centric value of the property.
	 * The value returned may have undergone type conversion from the target object's
	 * actual property type, if a type adaptor is present.
	 * 
	 * <p> Use {@link #isReadable()} beforehand to check whether the property
	 * can be read.
	 * @param target object instance containing this property
	 * @return
	 * @throws IllegalStateException if not readable (you should have called isReadable() first)
	 * @throws DBThrownByEndUserCodeException if any user code throws an exception
	 */
	public QueryableDatatype getQueryableDatatype(Object target) {
		return typeHandler.getJavaPropertyAsQueryableDatatype(target);
	}
	
	/**
	 * Sets the DBvolution-centric value of the property.
	 * The value set may have undergone type conversion to the target object's
	 * actual property type, if a type adaptor is present.
	 * 
	 * <p> Use {@link #isWritable()} beforehand to check whether the property
	 * can be modified.
	 * @param target object instance containing this property
	 * @param value
	 * @throws IllegalStateException if not writable (you should have called isWritable() first)
	 * @throws DBThrownByEndUserCodeException if any user code throws an exception
	 */
	public void setQueryableDatatype(Object target, QueryableDatatype value) {
		typeHandler.setJavaPropertyAsQueryableDatatype(target, value);
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
	 * @param target object instance containing this property
	 * @return value
	 * @throws IllegalStateException if not readable (you should have called isReadable() first)
	 * @throws DBThrownByEndUserCodeException if any user code throws an exception
	 */
	public Object rawJavaValue(Object target) {
		return adaptee.get(target);
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
	 * @param target object instance containing this property
	 * @param value new value
	 * @throws IllegalStateException if not writable (you should have called isWritable() first)
	 * @throws DBThrownByEndUserCodeException if any user code throws an exception
	 */
	public void setRawJavaValue(Object target, Object value) {
		adaptee.set(target, value);
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
		return adaptee.type();
	}
	
    /**
     * Gets the wrapper for the DBRow class containing
     * this property.
     * @return
     */
    public DBRowClassWrapper getDBRowClassWrapper() {
    	return classWrapper;
    }
}

package nz.co.gregs.dbvolution.internal.properties;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.datatypes.DBEnumValue;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.DBThrownByEndUserCodeException;
import nz.co.gregs.dbvolution.expressions.DBExpression;

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
 * <p> Note: instances of this class are cheap to create and do not need to be cached.
 * 
 * <p> This class is <i>thread-safe</i>.
 * 
 * <p> This class is not serializable. References to it within serializable classes
 * should be marked as {@code transient}.
 */
public class PropertyWrapper {
	private final DBRowInstanceWrapper dbRowInstanceWrapper;
	private final PropertyWrapperDefinition propertyDefinition;
	private final Object target;
	
	/**
	 * @param classProperty the class-level wrapper
	 * @param target the target object containing the given property
	 */
	public PropertyWrapper(DBRowInstanceWrapper instanceWrapper,
			PropertyWrapperDefinition classProperty, Object target) {
		this.dbRowInstanceWrapper = instanceWrapper;
		this.propertyDefinition = classProperty;
		this.target = target;
	}

	/**
	 * Gets a string representation of the wrapped property,
	 * suitable for debugging and logging.
	 * eg {@code "DBInteger nz.co.mycompany.myproject.Vehicle.fkSpecOptionColour<fk_17> = [15241672]"}
	 */
	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();
		buf.append(type().getSimpleName());
		buf.append(" ");
		buf.append(qualifiedJavaName());
		if (!javaName().equalsIgnoreCase(columnName())) {
			buf.append("<").append(columnName()).append(">");
		}
		if (isReadable()) {
			buf.append(" = [");
			try {
				buf.append(getQueryableDatatype());
			} catch (Exception e) {
				buf.append("<exception occurred>");
			}
			buf.append("]");
		}
		
		if (isTypeAdapted()) {
			buf.append(" (");
			buf.append(getRawJavaType().getSimpleName());
			if (isReadable()) {
				buf.append(" = [");
				try {
					buf.append(rawJavaValue());
				} catch (Exception e) {
					buf.append("<exception occurred>");
				}
				buf.append("]");
			}
			buf.append(")");
		}
		return buf.toString();
	}
	
	/**
	 * Generates a hash-code of this property wrapper definition,
	 * based on the java property it wraps and the referenced target object.
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((propertyDefinition == null) ? 0 : propertyDefinition.hashCode());
		result = prime * result + ((target == null) ? 0 : target.hashCode());
		return result;
	}

	/**
	 * Equality of this property wrapper definition,
	 * based on the java property it wraps in a specific class,
	 * plus the underlying object reference containing the wrapped property.
	 * Two instances are identical if they wrap the same java property (field or bean-property)
	 * in the same object instance (by object reference, rather than {@code .equals()} equality).
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof PropertyWrapper)) {
			return false;
		}
		PropertyWrapper other = (PropertyWrapper) obj;
		if (propertyDefinition == null) {
			if (other.propertyDefinition != null) {
				return false;
			}
		} else if (!propertyDefinition.equals(other.propertyDefinition)) {
			return false;
		}
		if (target != other.target) {
			return false;
		}
		return true;
	}

	/**
	 * Gets the name of the java property,
	 * without the containing class name.
	 * Mainly used within error messages.
	 * eg: {@code "uid"}
	 * 
	 * <p> Use {@link #columnName()} to determine column name.
	 * @return
	 */
	public String javaName() {
		return propertyDefinition.javaName();
	}

	/**
	 * Gets the partially qualified name of the underlying java property,
	 * using the short-name of the containing class.
	 * Mainly used within logging and error messages.
	 * eg: {@code "Customer.uid"}
	 * 
	 * <p> Use {@link #columnName()} to determine column name.
	 * @return
	 */
	public String shortQualifiedJavaName() {
		return propertyDefinition.shortQualifiedJavaName();
	}
	
	/**
	 * Gets the fully qualified name of the underlying java property,
	 * including the fully qualified name of the containing class.
	 * Mainly used within logging and error messages.
	 * eg: {@code "nz.co.mycompany.myproject.Customer.uid"}
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
	 * Convenience method for testing the type of the QueryableDatatype.
	 * Equivalent to {@code refType.isAssignableFrom(this.type())}.
	 * @param refType
	 * @return
	 */
	public boolean isInstanceOf(Class<? extends QueryableDatatype> refType) {
		return propertyDefinition.isInstanceOf(refType);
	}
	
	/**
	 * Gets the annotated table name of the table this
	 * property belongs to.
	 * Equivalent to calling {@code getDBRowInstanceWrapper().tableName()}.
	 * @return
	 */
	public String tableName() {
		return propertyDefinition.tableName();
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
	 * @return the referenced class if this property is a foreign key;
	 * null if not a foreign key
	 */
	public Class<? extends DBRow> referencedClass() {
		return propertyDefinition.referencedClass();
	}
	
	/**
	 * Gets the table referenced by this property, if this property
	 * is a foreign key.
	 * @return the referenced table name if this property is a foreign key;
	 * null if not a foreign key
	 */
	public String referencedTableName() {
		return propertyDefinition.referencedTableName();
	}
	
	/**
	 * Gets the column name in the foreign table referenced by this property.
	 * The referenced column is either explicitly indicated by use of the
	 * {@link DBForeignKey#column()} attribute, or it is implicitly the
	 * single primary key of the referenced table if the {@link DBForeignKey#column()}
	 * attribute is unset.
	 * 
	 * @return the non-null referenced column name if this property is a foreign key;
	 * null if not a foreign key
	 */
	public String referencedColumnName() {
		return propertyDefinition.referencedColumnName();
	}

	/**
	 * Gets information for the referenced property in the referenced table.
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
		return propertyDefinition.referencedPropertyDefinitionIdentity();
	}
	
	/**
	 * Gets the enum type, or null if not appropriate
	 * @return the enum type, which may also implement {@link DBEnumValue}
	 */
	public Class<? extends Enum<?>> getEnumType() {
		return propertyDefinition.getEnumType();
	}
	
	/**
	 * Gets the type of the code supplied by enum values.
	 * This is derived from the {@link DBEnumValue} implementation in the enum.
	 * @return null if not known or not appropriate
	 */
	public Class<?> getEnumCodeType() {
		return propertyDefinition.getEnumCodeType();
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
     * Indicates whether the property's type is adapted by an explicit or
     * implicit type adaptor. (Note: at present there is no support for implicit
     * type adaptors)
     *
     * @return {@code true} if a type adaptor is being used
     */
    public boolean isTypeAdapted() {
        return propertyDefinition.isTypeAdapted();
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

    /**
     * Gets the definition of the property, independent of any DBRow instance.
     * @return the propertyDefinition
     */
    public PropertyWrapperDefinition getDefinition() {
        return propertyDefinition;
    }
    
    /**
     * Gets the wrapper for the DBRow instance containing
     * this property.
     * @return
     */
    public DBRowInstanceWrapper getDBRowInstanceWrapper() {
    	return dbRowInstanceWrapper;
    }

    public boolean hasColumnExpression() {
        return getDefinition().hasColumnExpression();
//        QueryableDatatype qdt = getQueryableDatatype();
//        if(qdt.isNull()){
//            return false;
//        }else{
//            DBExpression value = qdt.getExpression();
//            if (value !=null){
//                return true;
//            }
//        }
//        return false;
    }

    public DBExpression getColumnExpression() throws ClassCastException{
        return getDefinition().getColumnExpression();
//        QueryableDatatype qdt = getQueryableDatatype();
//        DBExpression value = qdt.getExpression();
//        if (value !=null){
//            return value;
//        }
//        throw new ClassCastException("Attempt To Retreive Non-Existant Expression: Field "+javaName()+" for column "+columnName()+" on "+dbRowInstanceWrapper.javaName()+" does not contain an expression, do not call getExpression() on it.");
    }

    public String getSelectableName(DBDatabase db) {
        final DBRow actualRow = this.getDBRowInstanceWrapper().adapteeDBRow();
        return getDefinition().getSelectableName(db, actualRow);
//        DBDefinition defn = db.getDefinition();
//        if (hasColumnExpression()){
//            return getColumnExpression().toSQLString(db);
//        }else {
//            final DBRow actualRow = this.getDBRowInstanceWrapper().adapteeDBRow();
//            return defn.formatTableAliasAndColumnName(actualRow, columnName());
//        }
    }

    public String getColumnAlias(DBDatabase db) {            
        final DBRow actualRow = this.getDBRowInstanceWrapper().adapteeDBRow();
        return propertyDefinition.getColumnAlias(db, actualRow);
//        DBDefinition defn = db.getDefinition();
//        if (hasColumnExpression()){
//            return defn.formatForColumnAlias(String.valueOf(getColumnExpression().hashCode()));
//        }else {
//            final DBRow actualRow = this.getDBRowInstanceWrapper().adapteeDBRow();
//            return defn.formatColumnNameForDBQueryResultSet(actualRow, columnName());
//        }
    }
}

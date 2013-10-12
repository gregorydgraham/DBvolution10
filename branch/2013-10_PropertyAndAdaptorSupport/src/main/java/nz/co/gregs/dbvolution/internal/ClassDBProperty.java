package nz.co.gregs.dbvolution.internal;

import nz.co.gregs.dbvolution.DBThrownByEndUserCodeException;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 * Abstracts a java field or bean-property as a DBvolution-centric
 * property, which contains values from a specific column
 * in a databse table.
 * Transparently handles all annotations associated with the property.
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
 * <p> Note: instances of this class are expensive to create and should be cached.
 * 
 * <p> The current implementations assume that only {@code DBColumn} properties are
 * actually wrapped as instances of {@code DBProperty}.
 */
// TODO: improve description above
public class ClassDBProperty {
	private final JavaProperty adaptee;
	
	private final ColumnHandler columnHandler;
	private final PropertyTypeHandler typeHandler;
	private final ForeignKeyHandler foreignKeyHandler;
	
	public ClassDBProperty(JavaProperty javaProperty) {
		this.adaptee = javaProperty;
		
		// handlers
		this.columnHandler = new ColumnHandler(javaProperty);
		this.typeHandler = new PropertyTypeHandler(javaProperty);
		this.foreignKeyHandler = new ForeignKeyHandler(javaProperty);
	}
	
	/**
	 * Gets the name of the java property.
	 * Mainly used within error messages.
	 * 
	 * <p> Use {@link #columnName()} to determine column name.
	 * @return
	 */
	public String name() {
		return adaptee.name();
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
		return typeHandler.getType();
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
	 * @return the referenced class or null if not applicable
	 */
	public Class<?> referencedClass() {
		return foreignKeyHandler.getReferencedClass();
	}
	
	/**
	 * Gets the table referenced by this property, if this property
	 * is a foreign key.
	 * @return the referenced table name, or null if not applicable
	 */
	public String referencedTableName() {
		return foreignKeyHandler.getReferencedTableName();
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
		return foreignKeyHandler.getReferencedColumnName();
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
	public Object value(Object target) {
		return typeHandler.getDBvValue(target);
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
	public void setValue(Object target, QueryableDatatype value) {
		typeHandler.setObjectValue(target, value);
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
	 * @param target object instance containing this property
	 * @return value
	 * @throws IllegalStateException if not readable (you should have called isReadable() first)
	 * @throws DBThrownByEndUserCodeException if any user code throws an exception
	 */
	public Object getRawValue(Object target) {
		// hmm, does this class work on the object or the type?
		// Think: DBProperty - object
		//        ClassDBProperty - class
		// or should DBProperty be on the class and have no object version?
		// Then DBv internals supplies the object each time.....it's actually more efficient,
		// but it's not so nice within the rest of DBv code.
		//return adaptee.get(what object goes here?);
		return adaptee.get(target);
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
	 * @param target object instance containing this property
	 * @param value new value
	 * @throws IllegalStateException if not writable (you should have called isWritable() first)
	 * @throws DBThrownByEndUserCodeException if any user code throws an exception
	 */
	public void setRawValue(Object target, Object value) {
		adaptee.set(target, value);
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
		return adaptee.type();
	}
	
	// commented out because shouldn't be needed:
//		/**
//		 * Gets the {@link DBColumn} annotation on the property, if it exists.
//		 * @return the annotation or null
//		 */
//		public DBColumn getDBColumnAnnotation() {
//			return columnHandler.getDBColumnAnnotation();
//		}

	// commented out because shouldn't be needed:
//		/**
//		 * Gets the {@link DBForeignKey} annotation on the property, if it exists.
//		 * @return the annotation or null
//		 */
//		public DBForeignKey getDBForeignKeyAnnotation() {
//			return foreignKeyHandler.getDBForeignKeyAnnotation();
//		}
		
	// commented out because shouldn't be needed:
//		/**
//		 * Gets the {@link DBTypeAdaptor} annotation on the property, if it exists.
//		 * @return the annotation or null
//		 */
//		public DBAdaptType getDBTypeAdaptorAnnotation() {
//			return typeHandler.getDBTypeAdaptorAnnotation();
//		}
}

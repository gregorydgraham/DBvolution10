package nz.co.gregs.dbvolution.internal.properties;

import java.io.Serializable;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.AutoFillDuringQueryIfPossible;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBEnumValue;
import nz.co.gregs.dbvolution.datatypes.DBNumberStatistics;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.DBThrownByEndUserCodeException;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 * Abstracts a java field or bean-property on a target object as a
 * DBvolution-centric property, which contains values from a specific column in
 * a database table. Transparently handles all annotations associated with the
 * property, including type adaption.
 *
 * <p>
 * Provides access to the meta-data defined on a single java property of a
 * class, and provides methods for reading and writing the value of the property
 * on a single bound object, given a specified database definition.
 *
 * <p>
 * DB properties can be seen to have the types and values in the table that
 * follows. This class provides a virtual view over the property whereby the
 * DBv-centric type and value are easily accessible via the
 * {@link #getQueryableDatatype()} and
 * {@link #setQueryableDatatype(QueryableDatatype) } methods.
 * <ul>
 * <li> rawType/rawValue - the type and value actually stored on the declared
 * java property
 * <li> dbvType/dbvValue - the type and value used within DBv (a
 * QueryableDataType)
 * <li> databaseType/databaseValue - the type and value of the database column
 * itself (this class doesn't deal with these)
 * </ul>
 *
 * <p>
 * Note: instances of this class are cheap to create and do not need to be
 * cached.
 *
 * <p>
 * This class is <i>thread-safe</i>.
 *
 * <p>
 * This class is not serializable. References to it within serializable classes
 * should be marked as {@code transient}.
 */
public class PropertyWrapper implements Serializable{

	private static final long serialVersionUID = 1l;

	private final RowDefinitionInstanceWrapper dbRowInstanceWrapper;
	private final PropertyWrapperDefinition propertyDefinition;
	private final RowDefinition target;

	/**
	 * @param instanceWrapper instanceWrapper
	 * @param classProperty the class-level wrapper
	 * @param target the target object containing the given property
	 */
	public PropertyWrapper(RowDefinitionInstanceWrapper instanceWrapper,
			PropertyWrapperDefinition classProperty, RowDefinition target) {
		this.dbRowInstanceWrapper = instanceWrapper;
		this.propertyDefinition = classProperty;
		this.target = target;
	}

	/**
	 * Gets a string representation of the wrapped property, suitable for
	 * debugging and logging.
	 *
	 * <p>
	 * For example: <br>
	 * {@code "DBInteger nz.co.mycompany.myproject.Vehicle.fkSpecOptionColour<fk_17> = [15241672]"}
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a String representing this PropertyWrapper
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
	 * Generates a hash-code of this property wrapper definition, based on the
	 * java property it wraps and the referenced target object.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a hash code for this instance
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
	 * Equality of this property wrapper definition, based on the java property it
	 * wraps in a specific class, plus the underlying object reference containing
	 * the wrapped property.
	 *
	 * <p>
	 * Two instances are identical if they wrap the same java property (field or
	 * bean-property) in the same object instance (by object reference, rather
	 * than {@code .equals()} equality).
	 *
	 * @param obj obj
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return TRUE if this PropertyWrapper wraps the same property on the same
	 * RowDefinition as the object supplied, FALSE otherwise
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
		return target == other.target;
	}

	/**
	 * Gets the name of the java property, without the containing class name.
	 * Mainly used within error messages. eg: {@code "uid"}
	 *
	 * <p>
	 * Use {@link #columnName()} to determine column name.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a String of the declared field name of this property
	 */
	public String javaName() {
		return propertyDefinition.javaName();
	}

	/**
	 * Gets the partially qualified name of the underlying java property, using
	 * the short-name of the containing class. Mainly used within logging and
	 * error messages. eg: {@code "Customer.uid"}
	 *
	 * <p>
	 * Use {@link #columnName()} to determine column name.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a convenient String including the class name of the RowDefinition
	 * and the field name for this property
	 */
	public String shortQualifiedJavaName() {
		return propertyDefinition.shortQualifiedJavaName();
	}

	/**
	 * Gets the fully qualified name of the underlying java property, including
	 * the fully qualified name of the containing class. Mainly used within
	 * logging and error messages. eg:
	 * {@code "nz.co.mycompany.myproject.Customer.uid"}
	 *
	 * <p>
	 * Use {@link #columnName()} to determine column name.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the String of the full class name of the containing RowDefinition.
	 */
	public String qualifiedJavaName() {
		return propertyDefinition.qualifiedJavaName();
	}

	/**
	 * Gets the DBvolution-centric type of the property. If a type adaptor is
	 * present, then this is the type after conversion from the target object's
	 * actual property type.
	 *
	 * <p>
	 * Use {@link #getRawJavaType()} in the rare case that you need to know the
	 * underlying java property type.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the Class of the QDT used internally to handle database values.
	 */
	@SuppressWarnings("unchecked")
	public Class<? extends QueryableDatatype<?>> type() {
		return propertyDefinition.type();
	}

	/**
	 * Convenience method for testing the type of the QueryableDatatype.
	 * Equivalent to {@code refType.isAssignableFrom(this.type())}.
	 *
	 * @param refType refType
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return TRUE if this property's internal QueryableDatatype is the similar
	 * to that of the supplied instance.
	 */
	public boolean isInstanceOf(Class<? extends QueryableDatatype<?>> refType) {
		return propertyDefinition.isInstanceOf(refType);
	}

	/**
	 * Convenience method for testing the type of the QueryableDatatype.
	 * Equivalent to {@code refType.isAssignableFrom(this.type())}.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if this property's internal QueryableDatatype is the similar
	 * to that of the supplied instance.
	 */
	public boolean isInstanceOfLargeObject() {
		return propertyDefinition.isInstanceOfLargeObject();
	}

	/**
	 * Gets the annotated table name of the table this property belongs to.
	 * Equivalent to calling
	 * {@code getRowDefinitionInstanceWrapper().tableName()}.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a String of the table name for this property
	 */
	public String tableName() {
		return propertyDefinition.tableName();
	}

	/**
	 * Gets the annotated column name. Applies defaulting if the {@code DBColumn}
	 * annotation is present but does not explicitly specify the column name.
	 *
	 * <p>
	 * If the {@code DBColumn} annotation is missing, this method returns
	 * {@code null}.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the column name, if specified explicitly or implicitly
	 */
	public String columnName() {
		return propertyDefinition.getColumnName();
	}

	/**
	 * Indicates whether this property is a column.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return {@code true} if this property is a column
	 */
	public boolean isColumn() {
		return propertyDefinition.isColumn();
	}

	/**
	 * Indicates whether this property is a primary key.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return {@code true} if this property is a primary key
	 */
	public boolean isPrimaryKey() {
		return propertyDefinition.isPrimaryKey();
	}

	/**
	 * Indicates whether this property is a foreign key.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return {@code true} if this property is a foreign key
	 */
	public boolean isForeignKey() {
		return propertyDefinition.isForeignKey();
	}

	/**
	 * Gets the class referenced by this property, if this property is a foreign
	 * key.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the referenced class if this property is a foreign key; null if not
	 * a foreign key
	 */
	public Class<? extends DBRow> referencedClass() {
		return propertyDefinition.referencedClass();
	}

	/**
	 * Gets the table referenced by this property, if this property is a foreign
	 * key.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the referenced table name if this property is a foreign key; null
	 * if not a foreign key
	 */
	public String referencedTableName() {
		return propertyDefinition.referencedTableName();
	}

	/**
	 * Gets the column name in the foreign table referenced by this property. The
	 * referenced column is either explicitly indicated by use of the
	 * {@link DBForeignKey#column()} attribute, or it is implicitly the single
	 * primary key of the referenced table if the {@link DBForeignKey#column()}
	 * attribute is unset.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the non-null referenced column name if this property is a foreign
	 * key; null if not a foreign key
	 */
	public String referencedColumnName() {
		return propertyDefinition.referencedColumnName();
	}

	/**
	 * Gets information for the referenced property in the referenced table. The
	 * referenced property is either explicitly indicated by use of the
	 * {@link DBForeignKey#column()} attribute, or it is implicitly the single
	 * primary key of the referenced table.
	 *
	 * <p>
	 * Note that the property definition returned provides identity of the
	 * property only. It provides access to the property's: java name, column
	 * name, type, and identity information about the table it belongs to (ie:
	 * table name). Attempts to get or set its value or get the type adaptor
	 * instance will result in an internal exception.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the referenced property if this property is a foreign key; null if
	 * not a foreign key
	 */
	public PropertyWrapperDefinition referencedPropertyDefinitionIdentity() {
		return propertyDefinition.referencedPropertyDefinitionIdentity();
	}

	/**
	 * Gets the enum type, or null if not appropriate
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the enum type, which may also implement {@link DBEnumValue}
	 */
	public Class<? extends Enum<?>> getEnumType() {
		return propertyDefinition.getEnumType();
	}

	/**
	 * Gets the type of the code supplied by enum values. This is derived from the
	 * {@link DBEnumValue} implementation in the enum.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return null if not known or not appropriate
	 */
	public Class<?> getEnumCodeType() {
		return propertyDefinition.getEnumCodeType();
	}

	/**
	 * Indicates whether the value of the property can be retrieved. Bean
	 * properties which are missing a 'getter' can not be read, but may be able to
	 * be set.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if this property is readable, FALSE otherwise.
	 */
	public boolean isReadable() {
		return propertyDefinition.isReadable();
	}

	/**
	 * Indicates whether the value of the property can be modified. Bean
	 * properties which are missing a 'setter' can not be written to, but may be
	 * able to be read.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if the property can set, FALSE otherwise.
	 */
	public boolean isWritable() {
		return propertyDefinition.isWritable();
	}

	/**
	 * Indicates whether the property's type is adapted by an explicit or implicit
	 * type adaptor. (Note: at present there is no support for implicit type
	 * adaptors)
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return {@code true} if a type adaptor is being used
	 */
	public boolean isTypeAdapted() {
		return propertyDefinition.isTypeAdapted();
	}

	/**
	 * Gets the DBvolution-centric value of the property. The value returned may
	 * have undergone type conversion from the target object's actual property
	 * type, if a type adaptor is present.
	 *
	 * <p>
	 * Use {@link #isReadable()} beforehand to check whether the property can be
	 * read.
	 *
	 * @param <A> the QDT type
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return The queryableDatatype instance representing this property
	 * @throws IllegalStateException if not readable (you should have called
	 * isReadable() first)
	 * @throws DBThrownByEndUserCodeException if any user code throws an exception
	 */
	@SuppressWarnings("unchecked")
	public <A extends QueryableDatatype<?>> A getQueryableDatatype() {
		return (A) propertyDefinition.getQueryableDatatype(target);
	}

	/**
	 * Sets the DBvolution-centric value of the property. The value set may have
	 * undergone type conversion to the target object's actual property type, if a
	 * type adaptor is present.
	 *
	 * <p>
	 * Use {@link #isWritable()} beforehand to check whether the property can be
	 * modified.
	 *
	 * @param value value
	 * @throws IllegalStateException if not writable (you should have called
	 * isWritable() first)
	 * @throws DBThrownByEndUserCodeException if any user code throws an exception
	 */
	public void setQueryableDatatype(QueryableDatatype<?> value) {
		propertyDefinition.setQueryableDatatype(target, value);
	}

	/**
	 * Gets the value of the declared property in the end-user's target object,
	 * prior to type conversion to the DBvolution-centric type.
	 *
	 * <p>
	 * In most cases you will not need to call this method, as type conversion is
	 * done transparently via the {@link #getQueryableDatatype()} and
	 * {@link #setQueryableDatatype(QueryableDatatype)} methods.
	 *
	 * <p>
	 * Use {@link #isReadable()} beforehand to check whether the property can be
	 * read.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return value
	 * @throws IllegalStateException if not readable (you should have called
	 * isReadable() first)
	 * @throws DBThrownByEndUserCodeException if any user code throws an exception
	 */
	public Object rawJavaValue() {
		return propertyDefinition.rawJavaValue(target);
	}

	/**
	 * Set the value of the declared property in the end-user's target object,
	 * without type conversion to/from the DBvolution-centric type.
	 *
	 * <p>
	 * In most cases you will not need to call this method, as type conversion is
	 * done transparently via the {@link #getQueryableDatatype()} and
	 * {@link #setQueryableDatatype(QueryableDatatype)} methods.
	 *
	 * <p>
	 * Use {@link #isWritable()} beforehand to check whether the property can be
	 * modified.
	 *
	 * @param value new value
	 * @throws IllegalStateException if not writable (you should have called
	 * isWritable() first)
	 * @throws DBThrownByEndUserCodeException if any user code throws an exception
	 */
	public void setRawJavaValue(Object value) {
		propertyDefinition.setRawJavaValue(target, value);
	}

	/**
	 * Gets the declared type of the property in the end-user's target object,
	 * prior to type conversion to the DBvolution-centric type.
	 *
	 * <p>
	 * In most cases you will not need to call this method, as type conversion is
	 * done transparently via the {@link #getQueryableDatatype()} and
	 * {@link #setQueryableDatatype(QueryableDatatype)} methods. Use the
	 * {@link #type()} method to get the DBv-centric property type, after type
	 * conversion.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the declared Java class of the property.
	 */
	public Class<?> getRawJavaType() {
		return propertyDefinition.getRawJavaType();
	}

	/**
	 * Gets the definition of the property, independent of any DBRow instance.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the propertyDefinition
	 */
	public PropertyWrapperDefinition getPropertyWrapperDefinition() {
		return propertyDefinition;
	}

	/**
	 * Gets the wrapper for the DBRow instance containing this property.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the RowDefinitionInstanceWrapper for this property.
	 */
	public RowDefinitionInstanceWrapper getRowDefinitionInstanceWrapper() {
		return dbRowInstanceWrapper;
	}

	/**
	 * Indicates whether this property has a ColumnExpression.
	 *
	 * <p>
	 * Column expressions are derived values created with
	 * {@link DBExpression DBexpressions} like
	 * {@link StringExpression}, {@link NumberExpression}, and
	 * {@link BooleanExpression}. The often involve database columns using
	 * {@link DBRow#column(java.lang.Boolean) the DBRow column methods} or literal
	 * values using the, for instance, the StringExpression
	 * {@link StringExpression#value(java.lang.String) value method}.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if this property uses an expression to generate a value,
	 * otherwise FALSE.
	 */
	public boolean hasColumnExpression() {
		return this.getQueryableDatatype().hasColumnExpression();
	}

	/**
	 * Returns the Column Expression, if any, defined on this property.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the expression used by this property to generate values, or null.
	 *
	 */
	public DBExpression[] getColumnExpression() throws ClassCastException {
		return this.getQueryableDatatype().getColumnExpression();
	}

	/**
	 * The names and aliases of this property as it will appear in a SELECT and
	 * WHERE clauses.
	 *
	 * <p>
	 * Multiple names and aliases are supported for
	 * {@link QueryableDatatype QueryableDatatypes} like
	 * {@link DBNumberStatistics} which retrieve several expressions at once.</p>
	 *
	 * @param db db
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return A map of all the selectable name and column aliases for this
	 * property.
	 */
	public List<PropertyWrapperDefinition.ColumnAspects> getColumnAspects(DBDefinition db) {
		final RowDefinition adapteeRowProvider = this.getRowDefinitionInstanceWrapper().adapteeRowDefinition();
		return getPropertyWrapperDefinition().getColumnAspects(db, adapteeRowProvider);
	}

	/**
	 * The name of this property as it will appear in a SELECT and WHERE clauses.
	 *
	 * @param db db
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return A String of the property for use in SELECT and WHERE clauses.
	 */
//	private String[] getSelectableName(DBDatabase db) {
//		final RowDefinition adapteeRowProvider = this.getRowDefinitionInstanceWrapper().adapteeRowDefinition();
//		return getPropertyWrapperDefinition().getSelectableName(db, adapteeRowProvider);
//	}
	/**
	 * The alias to the column for use in the select clause and during value
	 * retrieval
	 *
	 * @param defn db
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the column alias for this property.
	 */
	public String[] getColumnAlias(DBDefinition defn) {
		final RowDefinition actualRow = this.getRowDefinitionInstanceWrapper().adapteeRowDefinition();
		return propertyDefinition.getColumnAlias(defn, actualRow);
	}

	/**
	 * Returns TRUE if the property wrapped is a foreign key reference to the
	 * table supplied
	 *
	 * @param table	table
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return TRUE if the property wrapped is a foreign key reference to the
	 * table supplied, otherwise FALSE.
	 */
	public boolean isForeignKeyTo(DBRow table) {
		return propertyDefinition.isForeignKeyTo(table);
	}

	/**
	 * Returns true if the property wrapped is an auto-incrementing column.
	 *
	 * <p>
	 * This generally means that the column is a primary key. and definitely means
	 * you do not need to set the value of the column.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if the value of this column is provided by the database's
	 * auto-increment functionality, otherwise FALSE.
	 */
	public boolean isAutoIncrement() {
		return propertyDefinition.isAutoIncrementColumn();
	}

	/**
	 * Returns true if the property wrapped is a Spatial2D column.
	 *
	 * <p>
	 * This generally means that the column is a polygon, line, point, or other 2D
	 * geometry.
	 *
	 * <p>
	 * Spatial columns are special in that some databases need indexes to support
	 * them properly.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if the value of this column is a 2D geometry type, otherwise
	 * FALSE.
	 */
	public boolean isSpatial2DType() {
		return propertyDefinition.isSpatial2DType();
	}

	/**
	 * Returns true if the property wrapped is an auto-filling field.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if the value of this column is to be automatically filled
	 * during queries if possible, otherwise FALSE.
	 */
	public boolean isAutoFilling() {
		return propertyDefinition.isAutoFilling();
	}

	/**
	 * Returns the class provided to the {@link AutoFillDuringQueryIfPossible}
	 * annotation.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the class that should be auto-filled if it is present in the query.
	 */
	public Class<?> getAutoFillingClass() {
		return propertyDefinition.getAutoFillingClass();
	}
}

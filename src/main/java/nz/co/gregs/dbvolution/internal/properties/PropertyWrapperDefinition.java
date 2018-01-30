package nz.co.gregs.dbvolution.internal.properties;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.AutoFillDuringQueryIfPossible;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBEnumValue;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.datatypes.InternalQueryableDatatypeProxy;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.DBThrownByEndUserCodeException;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.query.RowDefinition;
import nz.co.gregs.dbvolution.results.Spatial2DResult;

/**
 * Abstracts a java field or bean-property as a DBvolution-centric property,
 * which contains values from a specific column in a database table.
 * Transparently handles all annotations associated with the property, including
 * type adaption.
 *
 * <p>
 * Provides access to the meta-data defined on a single java property of a
 * class, and provides methods for reading and writing the value of the property
 * on target objects. Instances of this class are not bound to specific target
 * objects, nor are they bound to specific database definitions.
 *
 * <p>
 * For binding to specific target objects and database definitions, use the
 * {@link PropertyWrapper} class.
 *
 * <p>
 * DB properties can be seen to have the types and values in the table that
 * follows. This class provides a virtual view over the property whereby the
 * DBv-centric type and value are easily accessible via the
 * {@link #getQueryableDatatype(nz.co.gregs.dbvolution.query.RowDefinition)  value()}
 * and
 * {@link #setQueryableDatatype(nz.co.gregs.dbvolution.query.RowDefinition, nz.co.gregs.dbvolution.datatypes.QueryableDatatype)  setValue()}
 * methods.
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
 * Note: instances of this class are expensive to create and should be cached.
 *
 * <p>
 * This class is <i>thread-safe</i>.
 *
 * <p>
 * This class is not serializable. References to it within serializable classes
 * should be marked as {@code transient}.
 */
public class PropertyWrapperDefinition  implements Serializable{

	private static final long serialVersionUID = 1l;

	private final RowDefinitionClassWrapper classWrapper;
	private final JavaProperty javaProperty;

	private final ColumnHandler columnHandler;
	private final PropertyTypeHandler typeHandler;
	private final ForeignKeyHandler foreignKeyHandler;
	private final EnumTypeHandler enumTypeHandler;
	private boolean checkedForColumnExpression = false;
	private Integer columnIndex = null;
	private DBExpression[] columnExpression = new DBExpression[]{}; // empty if not present on propertyf
	public ArrayList<ColumnAspects> allColumnAspects = null;

	PropertyWrapperDefinition(RowDefinitionClassWrapper classWrapper, JavaProperty javaProperty, boolean processIdentityOnly) {
		this.classWrapper = classWrapper;
		this.javaProperty = javaProperty;

		// handlers
		this.columnHandler = new ColumnHandler(javaProperty);
		this.typeHandler = new PropertyTypeHandler(javaProperty, processIdentityOnly);
		this.foreignKeyHandler = new ForeignKeyHandler(javaProperty, processIdentityOnly);
		this.enumTypeHandler = new EnumTypeHandler(javaProperty, this.columnHandler);
	}

	JavaProperty getRawJavaProperty() {
		return javaProperty;
	}

	/**
	 * Gets a string representation of the wrapped property, suitable for
	 * debugging and logging.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a string representation of this object
	 */
	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();
		buf.append(type().getSimpleName());
		buf.append(" ");
		buf.append(qualifiedJavaName());
		if (!javaName().equalsIgnoreCase(getColumnName())) {
			buf.append("<").append(getColumnName()).append(">");
		}

		if (isTypeAdapted()) {
			buf.append(" (");
			buf.append(getRawJavaType().getSimpleName());
			buf.append(")");
		}
		return buf.toString();
	}

	/**
	 * Generates a hash-code of this property wrapper definition, based entirely
	 * on the java property it wraps.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a hash-code.
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((javaProperty == null) ? 0 : javaProperty.hashCode());
		return result;
	}

	/**
	 * Equality of this property wrapper definition, based on the java property it
	 * wraps in a specific class. Two instances are identical if they wrap the
	 * same java property (field or bean-property) in the same class and the same
	 * class-loader.
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
		if (!(obj instanceof PropertyWrapperDefinition)) {
			return false;
		}
		PropertyWrapperDefinition other = (PropertyWrapperDefinition) obj;
		if (javaProperty == null) {
			if (other.javaProperty != null) {
				return false;
			}
		} else if (!javaProperty.equals(other.javaProperty)) {
			return false;
		}
		return true;
	}

	/**
	 * Gets the name of the java property, without the containing class name.
	 * Mainly used within error messages. eg: {@code "uid"}
	 *
	 * <p>
	 * Use {@link #getColumnName()} to determine column name.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a String of the Java field name for this property
	 */
	public String javaName() {
		return javaProperty.name();
	}

	/**
	 * Gets the partially qualified name of the underlying java property, using
	 * the short-name of the containing class. Mainly used within logging and
	 * error messages. eg: {@code "Customer.uid"}
	 *
	 * <p>
	 * Use {@link #getColumnName()} to determine column name.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a String of the short name of the declared class of this property
	 */
	public String shortQualifiedJavaName() {
		return javaProperty.shortQualifiedName();
	}

	/**
	 * Gets the fully qualified name of the underlying java property, including
	 * the fully qualified name of the containing class. Mainly used within
	 * logging and error messages. eg:
	 * {@code "nz.co.mycompany.myproject.Customer.uid"}
	 *
	 * <p>
	 * Use {@link #getColumnName()} to determine column name.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a String of the full name of the class of this property
	 */
	public String qualifiedJavaName() {
		return javaProperty.qualifiedName();
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
	 * @return the Class of the internal QueryableDatatype used by this property
	 */
	@SuppressWarnings("unchecked")
	public Class<QueryableDatatype<?>> type() {
		return (Class<QueryableDatatype<?>>) typeHandler.getQueryableDatatypeClass();
	}

	/**
	 * Convenience method for testing the type. Equivalent to
	 * {@code refType.isAssignableFrom(this.type())}.
	 *
	 * @param refType	refType
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return TRUE if the supplied type is assignable from the internal
	 * QueryableDatatype, FALSE otherwise.
	 */
	public boolean isInstanceOf(Class<? extends QueryableDatatype<?>> refType) {
		return refType.isAssignableFrom(type());
	}

	/**
	 * Convenience method for testing the type. Equivalent to
	 * {@code this.type().isAssignableFrom(DBLargeObject.class)}.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if a DBLargeObject is assignable from the internal
	 * QueryableDatatype, FALSE otherwise.
	 */
	public boolean isInstanceOfLargeObject() {
		return DBLargeObject.class.isAssignableFrom(type());
	}

	/**
	 * Gets the annotated table name of the table this property belongs to.
	 * Equivalent to {@code getRowDefinitionClassWrapper().tableName()}.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a String of the table name containing this property.
	 */
	public String tableName() {
		return classWrapper.tableName();
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
	public String getColumnName() {
		return columnHandler.getColumnName();
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
		return columnHandler.isColumn();
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
		return columnHandler.isPrimaryKey();
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
		return foreignKeyHandler.isForeignKey();
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
		return foreignKeyHandler.getReferencedClass();
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
		return foreignKeyHandler.getReferencedTableName();
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
	 * @return the referenced column name if this property is a foreign key; null
	 * if not a foreign key
	 */
	public String referencedColumnName() {
		return foreignKeyHandler.getReferencedColumnName();
	}

	/**
	 * Gets identity information for the referenced property in the referenced
	 * table. The referenced property is either explicitly indicated by use of the
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
		return foreignKeyHandler.getReferencedPropertyDefinitionIdentity();
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
		return enumTypeHandler.getEnumType();
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
		return enumTypeHandler.getEnumLiteralValueType();
	}

	/**
	 * Indicates whether the value of the property can be retrieved. Bean
	 * properties which are missing a 'getter' can not be read, but may be able to
	 * be set.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if the property is readable, FALSE otherwise.
	 */
	public boolean isReadable() {
		return javaProperty.isReadable();
	}

	/**
	 * Indicates whether the value of the property can be modified. Bean
	 * properties which are missing a 'setter' can not be written to, but may be
	 * able to be read.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if the property can be set, FALSE otherwise
	 */
	public boolean isWritable() {
		return javaProperty.isWritable();
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
		return typeHandler.isTypeAdapted();
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
	 * @param target object instance containing this property
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the QueryableDatatype used internally.
	 * @throws IllegalStateException if not readable (you should have called
	 * isReadable() first)
	 * @throws DBThrownByEndUserCodeException if any user code throws an exception
	 */
	public QueryableDatatype<?> getQueryableDatatype(RowDefinition target) {
		QueryableDatatype<?> qdt = typeHandler.getJavaPropertyAsQueryableDatatype(target);
		new InternalQueryableDatatypeProxy(qdt).setPropertyWrapper(this);
		return qdt;
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
	 * @param target object instance containing this property
	 * @param value value value
	 *
	 * @throws IllegalStateException if not writable (you should have called
	 * isWritable() first)
	 * @throws DBThrownByEndUserCodeException if any user code throws an exception
	 */
	public void setQueryableDatatype(RowDefinition target, QueryableDatatype<?> value) {
		new InternalQueryableDatatypeProxy(value).setPropertyWrapper(this);
		typeHandler.setJavaPropertyAsQueryableDatatype(target, value);
	}

	/**
	 * Gets the value of the declared property in the end-user's target object,
	 * prior to type conversion to the DBvolution-centric type.
	 *
	 * <p>
	 * In most cases you will not need to call this method, as type conversion is
	 * done transparently via the {@link #getQueryableDatatype(nz.co.gregs.dbvolution.query.RowDefinition)
	 * } and
	 * {@link #setQueryableDatatype(nz.co.gregs.dbvolution.query.RowDefinition, nz.co.gregs.dbvolution.datatypes.QueryableDatatype) }
	 * methods.
	 *
	 * <p>
	 * Use {@link #isReadable()} beforehand to check whether the property can be
	 * read.
	 *
	 * @param target object instance containing this property
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return value
	 * @throws IllegalStateException if not readable (you should have called
	 * isReadable() first)
	 * @throws DBThrownByEndUserCodeException if any user code throws an exception
	 */
	public Object rawJavaValue(Object target) {
		return javaProperty.get(target);
	}

	/**
	 * Set the value of the declared property in the end-user's target object,
	 * without type conversion to/from the DBvolution-centric type.
	 *
	 * <p>
	 * In most cases you will not need to call this method, as type conversion is
	 * done transparently via the {@link #getQueryableDatatype(nz.co.gregs.dbvolution.query.RowDefinition)
	 * } and
	 * {@link #setQueryableDatatype(nz.co.gregs.dbvolution.query.RowDefinition, nz.co.gregs.dbvolution.datatypes.QueryableDatatype) }
	 * methods.
	 *
	 * <p>
	 * Use {@link #isWritable()} beforehand to check whether the property can be
	 * modified.
	 *
	 * @param target object instance containing this property
	 * @param value new value
	 * @throws IllegalStateException if not writable (you should have called
	 * isWritable() first)
	 * @throws DBThrownByEndUserCodeException if any user code throws an exception
	 */
	public void setRawJavaValue(Object target, Object value) {
		javaProperty.set(target, value);
	}

	/**
	 * Gets the declared type of the property in the end-user's target object,
	 * prior to type conversion to the DBvolution-centric type.
	 *
	 * <p>
	 * In most cases you will not need to call this method, as type conversion is
	 * done transparently via the {@link #getQueryableDatatype(nz.co.gregs.dbvolution.query.RowDefinition)
	 * } and
	 * {@link #setQueryableDatatype(nz.co.gregs.dbvolution.query.RowDefinition, nz.co.gregs.dbvolution.datatypes.QueryableDatatype) }
	 * methods. Use the {@link #type()} method to get the DBv-centric property
	 * type, after type conversion.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the declared class of the property
	 */
	public Class<?> getRawJavaType() {
		return javaProperty.type();
	}

	/**
	 * Gets the wrapper for the RowDefinition (DBRow or DBReport) subclass
	 * containing this property.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the RowDefinitionClassWrapper representing the enclosing object of
	 * this property
	 */
	public RowDefinitionClassWrapper getRowDefinitionClassWrapper() {
		return classWrapper;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the columnExpression
	 */
	public DBExpression[] getColumnExpression() {
		return Arrays.copyOf(columnExpression, columnExpression.length);
	}

	void setColumnExpression(DBExpression... expression) {
		columnExpression = Arrays.copyOf(expression, expression.length);
	}

	boolean hasColumnExpression() {
		return getColumnExpression().length > 0;
	}

	public synchronized List<ColumnAspects> getColumnAspects(DBDefinition defn, RowDefinition actualRow) {
		allColumnAspects = new ArrayList<ColumnAspects>();
		checkForColumnExpression(actualRow);
		if (hasColumnExpression()) {
			DBExpression[] columnExpression1 = getColumnExpression();
			for (DBExpression dBExpression : columnExpression1) {
				allColumnAspects.add(new ColumnAspects(
						defn.transformToStorableType(dBExpression).toSQLString(defn),
						defn.formatForColumnAlias(String.valueOf(dBExpression.hashCode())),
						dBExpression)
				);
			}
		} else {
			allColumnAspects.add(new ColumnAspects(
					defn.formatTableAliasAndColumnName(actualRow, getColumnName()),
					defn.formatColumnNameForDBQueryResultSet(actualRow, getColumnName())
			));
		}
		return allColumnAspects;
	}

	String[] getColumnAlias(DBDefinition defn, RowDefinition actualRow) {
		checkForColumnExpression(actualRow);
		if (hasColumnExpression()) {
			ArrayList<String> strList = new ArrayList<String>();
			DBExpression[] columnExpression1 = getColumnExpression();
			for (DBExpression dBExpression : columnExpression1) {
				final String formattedForColumnAlias = defn.formatForColumnAlias(String.valueOf(dBExpression.hashCode()));
//				dBExpression.setColumnAlias(formattedForColumnAlias);
				strList.add(formattedForColumnAlias);
			}
			return strList.toArray(new String[]{});
		} else {
			return new String[]{defn.formatColumnNameForDBQueryResultSet(actualRow, getColumnName())};
		}
	}

	void checkForColumnExpression(RowDefinition actualRow) {
		if (!checkedForColumnExpression && !hasColumnExpression()) {
			Object value = this.getRawJavaProperty().get(actualRow);
			if (value != null && QueryableDatatype.class.isAssignableFrom(value.getClass())) {
				QueryableDatatype<?> qdt = (QueryableDatatype) value;
				if (qdt.hasColumnExpression()) {
					this.setColumnExpression(qdt.getColumnExpression());
				}
			}
			checkedForColumnExpression = true;
		}
	}

	boolean isForeignKeyTo(DBRow table) {
		return foreignKeyHandler.isForeignKeyTo(table.getClass());
	}

	void setColumnIndex(int columnIndex) {
		this.columnIndex = columnIndex;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the columnIndex
	 */
	public Integer getColumnIndex() {
		return columnIndex;
	}

	boolean isAutoIncrementColumn() {
		return columnHandler.isAutoIncrement();
	}

	boolean isSpatial2DType() {
		Class<? extends QueryableDatatype<?>> qdt = type();
		return (Spatial2DResult.class.isAssignableFrom(qdt));
	}

	boolean isAutoFilling() {
		return this.javaProperty.isAnnotationPresent(AutoFillDuringQueryIfPossible.class);
	}

	Class<?> getAutoFillingClass() {
		return this.javaProperty.getAnnotation(AutoFillDuringQueryIfPossible.class).requiredClass();
	}

	public static class ColumnAspects implements Serializable{

	private static final long serialVersionUID = 1l;

		public final String selectableName;
		public final String columnAlias;
		public final DBExpression expression;

		public ColumnAspects(String selectableName, String columnAlias, DBExpression expression) {
			this.selectableName = selectableName;
			this.columnAlias = columnAlias;
			this.expression = expression;
		}

		public ColumnAspects(String selectableName, String columnAlias) {
			this.selectableName = selectableName;
			this.columnAlias = columnAlias;
			this.expression = null;
		}
	}
}

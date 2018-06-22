package nz.co.gregs.dbvolution.datatypes;

import java.lang.reflect.Array;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapperDefinition;
import nz.co.gregs.dbvolution.operators.DBPermittedValuesOperator;

/**
 * Base class for enumeration-aware queryable datatypes. Enumeration-aware
 * queryable datatypes map automatically from the database value to the
 * enumeration value via the {@link DBEnumValue} interface.
 *
 * <p>
 * Internally stores only the database-centric literal value in its type.
 * Conversion to the enumeration type is done lazily so that it's possible to
 * handle the case where a database has an invalid value or a new value that
 * isn't in the enumeration.</p>
 * 
 * <p>
 * The easiest way to use DBEnum is via {@link DBIntegerEnum} or {@link DBStringEnum}.
 * </p>
 * 
 * <p>
 * Example implementations are available in 
 * </p>
 *
 * @param <E> the enumeration type. Must implement {@link DBEnumValue}.
 * @param <T> the base type used on the database.
 */
public abstract class DBEnum<E extends Enum<E> & DBEnumValue<T>, T> extends QueryableDatatype<T> {

	private static final long serialVersionUID = 1L;
	private Class<E> enumType;

	/**
	 * The default constructor for DBEnums.
	 *
	 * <p>
	 * Creates an unset undefined DBEnum object.
	 *
	 * <p>
	 * Normal used in your DBRow sub-classes as:
	 * {@code			public DBEnum<MyDBEnumValue> field = new DBEnum<MyDBEnumValue>();}
	 * Where MyDBEnumValue is a sub-class of {@link DBEnumValue} and probably a
	 * {@link DBIntegerEnum} or {@link DBStringEnum}.
	 */
	public DBEnum() {
	}

	/**
	 * Creates a DBEnum with the value provided.
	 *
	 * <p>
	 * The resulting DBEnum will be set as having the value provided but will not
	 * be defined in the database.
	 *
	 * @param literalValue	literalValue
	 */
	protected DBEnum(T literalValue) {
		super(literalValue);
	}

	/**
	 * Create a DBEnum with a permanent column expression.
	 *
	 * <p>
	 * Use this method within a DBRow sub-class to create a column that uses an
	 * expression to create the value at query time.
	 *
	 * <p>
	 * This is particularly useful for trimming strings or converting between
	 * types but also allows for complex arithmetic and transformations.
	 *
	 * @param columnExpression	columnExpression
	 */
	protected DBEnum(DBExpression columnExpression) {
		super(columnExpression);
	}

	/**
	 * Creates a DBEnum with the value set to the provided value.
	 *
	 * <p>
	 * Creates an undefined DBEnum object.
	 *
	 * <p>
	 * Normally used in your DBRow sub-classes as:
	 * {@code			public DBEnum<MyDBEnumValue> field = new DBEnum<MyDBEnumValue>();}
	 * Where MyDBEnumValue is a sub-class of {@link DBEnumValue} and probably a
	 * {@link DBIntegerEnum} or {@link DBStringEnum}.
	 *
	 * @param value an enumeration value.
	 */
	@SuppressWarnings("unchecked")
	public DBEnum(E value) {
		this.enumType = (value == null) ? null : (Class<E>) value.getClass();
		setLiteralValue(convertToLiteral(value));
	}

	/**
	 * Sets the value based on the given enumeration.
	 *
	 * @param enumValue	enumValue
	 */
	@SuppressWarnings("unchecked")
	public void setValue(E enumValue) {
		this.enumType = (enumValue == null) ? null : (Class<E>) enumValue.getClass();
		super.setLiteralValue(convertToLiteral(enumValue));
	}

	/**
	 * Validates whether the given type is acceptable as a literal value. Enum
	 * values with null literal values are tolerated and should not be rejected by
	 * this method. See documentation for {@link DBEnumValue#getCode()}.
	 *
	 * <p>
	 * Throw an IncompatibleClassChangeError to indicate that the literal value
	 * failed validation</p>
	 *
	 * @param enumValue non-null enum value, for which the literal value may be
	 * null
	 * @throws IncompatibleClassChangeError on incompatible types
	 */
	protected abstract void validateLiteralValue(E enumValue) throws IncompatibleClassChangeError;

	/**
	 * Gets the enumeration value.
	 * <p>
	 * Converts in-line from the database's raw value to the enum type.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the Enumeration instance that is appropriate to this instance
	 * @throws IllegalArgumentException if the database's raw value does not have
	 * a corresponding value in the enum
	 */
	public E enumValue() {
		// get actual literal value: a String or a Long
		Object localValue = super.getValue();
		if (localValue == null) {
			return null;
		}

		// attempt conversion
		E[] enumValues = getEnumType().getEnumConstants();
		for (E enumValue : enumValues) {
			Object enumLiteralValue = enumValue.getCode();
			if (areLiteralValuesEqual(localValue, enumLiteralValue)) {
				return enumValue;
			}
		}
		throw new IncompatibleClassChangeError("Invalid literal value [" + localValue + "] encountered"
				+ " when converting to enum type " + enumType.getName());
	}

	/**
	 * Tests whether two objects represent the same value. Handles subtle
	 * differences in type.
	 *
	 *
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return {@code true} if both null or equivalent on value, {@code false} if
	 * not equal
	 * @throws IncompatibleClassChangeError if can't recognise the type
	 */
	private static boolean areLiteralValuesEqual(Object o1, Object o2) {
		if (o1 == null && o2 == null) {
			return true;
		} else if (o1 == null ^ o2 == null) {
			return false;
		}

		// handle same types and related types
		// (includes support for: String, BicDecimal, BigInteger, custom types)
		if (o1.getClass().isAssignableFrom(o2.getClass())) {
			// t2 extends t1: assume t2 knows how to compare them
			return o2.equals(o1);
		} else if (o2.getClass().isAssignableFrom(o1.getClass())) {
			// t1 extends t2: assume t1 knows how to compare them
			return o1.equals(o2);
		}

		// handle java.lang.Number variations
		// (Get the values at the greatest common precision,
		//  then compare them)
		if (o1 instanceof Number && o2 instanceof Number
				&& isRecognisedRealOrIntegerType((Number) o1)
				&& isRecognisedRealOrIntegerType((Number) o2)) {
			Number n1 = (Number) o1;
			Number n2 = (Number) o2;
			Object v1 = null; // value at greatest common precision
			Object v2 = null; // value at greatest common precision

			if (n1 instanceof Double || n2 instanceof Double) {
				v1 = n1.doubleValue();
				v2 = n2.doubleValue();
			} else if (n1 instanceof Float || n2 instanceof Float) {
				v1 = n1.floatValue();
				v2 = n2.floatValue();
			} else if (n1 instanceof Long || n2 instanceof Long) {
				v1 = n1.longValue();
				v2 = n2.longValue();
			} else if (n1 instanceof Integer || n2 instanceof Integer) {
				v1 = n1.intValue();
				v2 = n2.intValue();
			} else if (n1 instanceof Short || n2 instanceof Short) {
				v1 = n1.shortValue();
				v2 = n2.shortValue();
			} else if (n1 instanceof Float || n2 instanceof Float) {
				v1 = n1.floatValue();
				v2 = n2.floatValue();
			}

			if (v1 != null && v2 != null) {
				return v1.equals(v2);
			}
		}

		throw new IncompatibleClassChangeError("Unable to compare " + o1.getClass().getName() + " with " + o2.getClass().getName());
	}

	/**
	 * Checks whether its one of the recognised types that can be easily converted
	 * between each other in {@link #areLiteralValuesEqual()}.
	 */
	private static boolean isRecognisedRealOrIntegerType(Number n) {
		return (n instanceof Double)
				|| (n instanceof Float)
				|| (n instanceof Short)
				|| (n instanceof Long)
				|| (n instanceof Integer)
				|| (n instanceof Short);
	}

	/**
	 * Gets the declared type of enumeration that the literal value is to be
	 * mapped to. Dependent on the property wrapper being injected, or the
	 * enumType being set
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return non-null enum type
	 * @throws IllegalStateException if not configured correctly
	 */
	@SuppressWarnings("unchecked")
	private Class<E> getEnumType() {
		if (enumType == null) {
			PropertyWrapperDefinition propertyWrapper = getPropertyWrapperDefinition();
			if (propertyWrapper == null) {
				throw new IllegalStateException(
						"Unable to convert literal value to enum: enum type unable to be inferred at this point. "
						+ "Row needs to be queried from database, or value set with an actual enum.");
			}
			Class<?> type = propertyWrapper.getEnumType();
			if (type == null) {
				throw new IllegalStateException(
						"Unable to convert literal value to enum: enum type unable to be inferred at this point. "
						+ "Row needs to be queried from database, or value set with an actual enum, "
						+ "on " + propertyWrapper.qualifiedJavaName() + ".");
			}
			enumType = (Class<E>) type;
		}
		return enumType;
	}

	@Override
	protected String formatValueForSQLStatement(DBDefinition db) {
		final Object databaseValue = super.getValue();
		if (databaseValue == null) {
			return db.getNull();
		} else {
			QueryableDatatype<?> qdt = QueryableDatatype.getQueryableDatatypeForObject(databaseValue);
			return qdt.formatValueForSQLStatement(db);
		}
	}

	/**
	 * Provides the literal values for all the enumeration values provided.
	 *
	 * @param enumValues	enumValues
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of the literal database values for the enumeration values.
	 */
	@SuppressWarnings("unchecked")
	protected T[] convertToLiteral(E... enumValues) {
		// Use Array native method to create array
		// of a type only known at run time
		if (enumValues.length == 0) {
			return (T[]) new Object[]{};
		} else {
			int index = 0;
			E firstValue = null;
			while (firstValue == null && index < enumValues.length) {
				index++;
				firstValue = enumValues[index];
			}
			if (enumType == null && firstValue == null) {
				return (T[]) new Object[]{};
			} else {
				Class<?> baseType = convertToLiteral(firstValue).getClass();
				enumType = (Class<E>) baseType;
				final T[] result = (T[]) Array.newInstance(enumType, enumValues.length);
				for (int i = 0; i < enumValues.length; i++) {
					E enumValue = enumValues[i];
					result[i] = convertToLiteral(enumValue);
				}
				return result;
			}
		}
	}

	/**
	 * Provides the value for the enumeration value provided.
	 *
	 * @param enumValue	enumValue
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the literal database value for the enumeration value.
	 */
	protected final T convertToLiteral(E enumValue) {
		if (enumValue == null || enumValue.getCode() == null) {
			return null;
		} else {
			validateLiteralValue(enumValue);
			T newLiteralValue = enumValue.getCode();
			return newLiteralValue;
		}
	}

	@Override
	public boolean isAggregator() {
		return false;
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of objects
	 *
	 * @param permitted	permitted
	 */
	@SafeVarargs
	public final void permittedValues(T... permitted) {
		this.setOperator(new DBPermittedValuesOperator<T>(permitted));
	}

	/**
	 *
	 * reduces the rows to only the object, Set, List, Array, or vararg of objects
	 *
	 * @param permitted	permitted
	 */
	@SafeVarargs
	public final void permittedValues(E... permitted) {
		this.setOperator(new DBPermittedValuesOperator<T>(convertToLiteral(permitted)));
	}

	/**
	 * Reduces the rows returned from a query by excluding those matching the
	 * provided objects.
	 *
	 * <p>
	 * The case, upper or lower, will be ignored.
	 *
	 * <p>
	 * Defining case for Unicode characters is complicated and may not work as
	 * expected.
	 *
	 * @param excluded	excluded
	 */
	@SafeVarargs
	public final void excludedValues(T... excluded) {
		this.setOperator(new DBPermittedValuesOperator<T>(excluded));
		negateOperator();
	}

	/**
	 * Reduces the rows returned from a query by excluding those matching the
	 * provided objects.
	 *
	 * <p>
	 * For Strings, the case, upper or lower, will be ignored.</p>
	 *
	 * @param excluded	excluded
	 */
	@SafeVarargs
	public final void excludedValues(E... excluded) {
		this.setOperator(new DBPermittedValuesOperator<T>(convertToLiteral(excluded)));
		negateOperator();
	}
}

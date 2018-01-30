package nz.co.gregs.dbvolution.internal.properties;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;

import nz.co.gregs.dbvolution.datatypes.DBEnum;
import nz.co.gregs.dbvolution.datatypes.DBEnumValue;
import nz.co.gregs.dbvolution.exceptions.InvalidDeclaredTypeException;

/**
 * Handles reflection of generics on field/properties of type {@link DBEnum}.
 * Identifies the exact enum type from the field or property declaration.
 *
 * <p>
 * A generic parameter to the {@link DBEnum} field type is mandatory if the QDT
 * type is {@code DBEnum}.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Malcolm Lett
 */
class EnumTypeHandler implements Serializable{

	private static final long serialVersionUID = 1l;

	private final Class<? extends Enum<?>> enumType; // null if not present on property
	private final Class<?> enumLiteralValueType; // null if not present or not able to be inferred

	@SuppressWarnings("unchecked")
	EnumTypeHandler(JavaProperty javaProperty, ColumnHandler columnHandler) {
		Type type = javaProperty.genericType();
		Class<?> propertyClass = classOf(type);
		Class<? extends Enum<?>> identifiedEnumType = null;
		Class<?> identifiedEnumLiteralValueType = null;

		if (columnHandler.isColumn() && propertyClass != null && DBEnum.class.isAssignableFrom(propertyClass)) {
			if (type instanceof ParameterizedType) {
				Type[] parameterTypes = ((ParameterizedType) type).getActualTypeArguments();
				if (parameterTypes.length >= 1) {
					identifiedEnumType = (Class<? extends Enum<?>>) classOf(parameterTypes[0]);
				}
			} else if (type instanceof WildcardType) {
				throw new InvalidDeclaredTypeException(
						"Wildcard generics must not be used on " + propertyClass.getSimpleName()
						+ " declaration for " + javaProperty);
			}

			if (identifiedEnumType == null) {
				throw new InvalidDeclaredTypeException(
						"Use of " + propertyClass.getSimpleName() + " declaration requires concrete generic parameter"
						+ " on " + javaProperty);
			} else {
				identifiedEnumLiteralValueType = enumLiteralValueTypeOf(identifiedEnumType);
			}
		}

		this.enumType = identifiedEnumType;
		this.enumLiteralValueType = identifiedEnumLiteralValueType;
	}

	/**
	 * Gets the enum type, or null if not appropriate. Null if not a column.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the enum type, which will also implement {@link DBEnumValue}
	 */
	public Class<? extends Enum<?>> getEnumType() {
		return enumType;
	}

	/**
	 * Gets the type of the code supplied by enum values. This is derived from the
	 * {@link DBEnumValue} implementation in the enum. Null if not a column.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return null if not appropriate, or not able to be inferred
	 */
	public Class<?> getEnumLiteralValueType() {
		return enumLiteralValueType;
	}

	// reflection helper method
	private static Class<?> classOf(Type type) {
		if (type instanceof Class) {
			return (Class<?>) type;
		} else if (type instanceof ParameterizedType) {
			return classOf(((ParameterizedType) type).getRawType());
		} else {
			return null;
		}
	}

	/**
	 * Determines the type of literal code values for the given enum type.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return Ummm...
	 */
	private static Class<?> enumLiteralValueTypeOf(Class<? extends Enum<?>> enumType) {
		Enum<?>[] enumValues = enumType.getEnumConstants();
		if (enumValues != null) {
			for (Enum<?> enumValue : enumValues) {
				if (enumValue instanceof DBEnumValue) {
					Object code = ((DBEnumValue<?>) enumValue).getCode();
					if (code != null) {
						return code.getClass();
					}
				}
			}
		}
		return null;
	}
}

package nz.co.gregs.dbvolution.internal;

import nz.co.gregs.dbvolution.annotations.DBEnumType;
import nz.co.gregs.dbvolution.datatypes.DBEnum;
import nz.co.gregs.dbvolution.datatypes.DBEnumValue;
import nz.co.gregs.dbvolution.exceptions.DBPebkacException;

/**
 * Handles annotation processing, business logic, validation rules, defaulting,
 * and error handling associated with the {@code DBEnumType} annotation.
 * Ideally this would be merged into the {@code PropertyTypeHandler} class, but
 * that's already too complex.
 * 
 * <p> {@code DBEnumType} annotation is mandatory if the QDT type is {@code DBEnum} 
 * @author Malcolm Lett
 */
class EnumTypeHandler {
	private final DBEnumType enumTypeAnnotation; // null if not present on property
	
	EnumTypeHandler(JavaProperty javaProperty, PropertyTypeHandler propertyTypeHandler) {
		this.enumTypeAnnotation = javaProperty.getAnnotation(DBEnumType.class);
		
		// validate: @DBEnumType mandatory if field is DBEnum
		if (DBEnum.class.isAssignableFrom(propertyTypeHandler.getType())) {
			if (enumTypeAnnotation == null) {
                throw new DBPebkacException(javaProperty.type().getName() + " requires a "+
                		"@" + DBEnumType.class.getSimpleName() + " annotation");
			}
		}
	}
	
	/**
	 * Gets the enum type, or null if not appropriate
	 * @return the enum type, which may also implement {@link DBEnumValue}
	 */
	public Class<? extends Enum<?>> getEnumType() {
		return (enumTypeAnnotation == null) ? null : enumTypeAnnotation.value();
	}
	
	/**
	 * Gets the type of the code supplied by enum values.
	 * This is derived from the {@link DBEnumValue} implementation in the enum.
	 * @return null if not known or not appropriate
	 */
	public Class<?> getEnumCodeType() {
		Class<? extends Enum<?>> enumType = getEnumType();
		if (enumType != null && enumType.isEnum()) {
			Enum<?>[] enumValues = enumType.getEnumConstants();
			for (Enum<?> enumValue: enumValues) {
				if (enumValue instanceof DBEnumValue) {
					Object code = ((DBEnumValue) enumValue).getCode();
					if (code != null) {
						return code.getClass();
					}
				}
			}
		}
		return null;
	}
}

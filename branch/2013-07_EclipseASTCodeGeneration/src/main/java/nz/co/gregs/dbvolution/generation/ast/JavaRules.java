package nz.co.gregs.dbvolution.generation.ast;

public class JavaRules {
	/**
	 * Gets the bean property name from a getter or setter method.
	 * @param method a standard bean property getter or setter method
	 * @return the property name in camelCase.
	 * @throws IllegalArgumentException if not a standard getter or setter method
	 */
	public static String propertyNameOf(ParsedMethod method) {
		String propertyNamePart;
		if (method.getName().startsWith("get") && method.getName().length() > "get".length()) {
			propertyNamePart = method.getName().substring("get".length());
		}
		else if (method.getName().startsWith("set") && method.getName().length() > "set".length()) {
			propertyNamePart = method.getName().substring("set".length());
		}
		else if (method.getName().startsWith("is") && method.getName().length() > "is".length()) {
			propertyNamePart = method.getName().substring("is".length());
		}
		else {
			throw new IllegalArgumentException("method "+method.getName()+" is not a bean property getter or setter");
		}
		return propertyNamePart.substring(0,1).toLowerCase() + propertyNamePart.substring(1);
	}
	
	/**
	 * Gets the standard bean getter method name for the supplied field.
	 * @param field
	 * @return
	 */
	public static String getterMethodNameForField(ParsedField field) {
		Class<?> fieldType = null;
		if (field.getType().isJavaType(boolean.class)) {
			fieldType = boolean.class;
		}
		return getterMethodNameForField(field.getName(), fieldType);		
	}

	/**
	 * Gets the standard bean setter method name for the supplied field.
	 * @param field
	 * @return
	 */
	public static String setterMethodNameForField(ParsedField field) {
		return setterMethodNameForField(field.getName(), field.getType().getJavaTypeIfKnown());		
	}

	/**
	 * Gets the standard bean getter method name for the supplied field.
	 * @param fieldName
	 * @param fieldType optional
	 * @return
	 */
	public static String getterMethodNameForField(String fieldName, Class<?> fieldType) {
		String prefix;
		if (fieldType != null && fieldType.isPrimitive() && fieldType.equals(Boolean.class)) {
			prefix = "is";
		}
		else {
			prefix = "get";
		}
		
		return prefix + titleCaseOf(fieldName);
	}

	/**
	 * Gets the standard bean setter method name for the supplied field.
	 * @param fieldName
	 * @param fieldType optional
	 * @return
	 */
	public static String setterMethodNameForField(String fieldName, Class<?> fieldType) {
		return "set" + titleCaseOf(fieldName);
	}
	
	public static String titleCaseOf(String name) {
		return name.substring(0,1).toUpperCase() + name.substring(1);
	}
}

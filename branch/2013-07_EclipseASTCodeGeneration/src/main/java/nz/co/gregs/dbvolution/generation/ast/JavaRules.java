package nz.co.gregs.dbvolution.generation.ast;

public class JavaRules {
	/**
	 * Gets the bean property name from a getter or setter method.
	 * @param method a standard bean property getter or setter method
	 * @return the property name in camelCase.
	 * @throws IllegalArgumentException if not a standard getter or setter method
	 */
	public static String propertyNameOf(ParsedMethod method) {
		if (!method.getName().startsWith("get") && !method.getName().startsWith("set")) {
			throw new IllegalArgumentException("method "+method.getName()+" is not a getter or setter");
		}
		if (method.getName().length() <= 3) {
			throw new IllegalArgumentException("method "+method.getName()+" not a standard bean property");
		}
		String propertyName = method.getName().substring("get".length());
		return propertyName.substring(0,1).toLowerCase() + propertyName.substring(1);
	}

}

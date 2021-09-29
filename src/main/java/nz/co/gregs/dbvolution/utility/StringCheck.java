/*
 * Copyright 2019 Gregory Graham.
 *
 * Commercial licenses are available, please contact info@gregs.co.nz for details.
 * 
 * This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. 
 * To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/4.0/ 
 * or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.
 * 
 * You are free to:
 *     Share - copy and redistribute the material in any medium or format
 *     Adapt - remix, transform, and build upon the material
 * 
 *     The licensor cannot revoke these freedoms as long as you follow the license terms.               
 *     Under the following terms:
 *                 
 *         Attribution - 
 *             You must give appropriate credit, provide a link to the license, and indicate if changes were made. 
 *             You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.
 *         NonCommercial - 
 *             You may not use the material for commercial purposes.
 *         ShareAlike - 
 *             If you remix, transform, or build upon the material, 
 *             you must distribute your contributions under the same license as the original.
 *         No additional restrictions - 
 *             You may not apply legal terms or technological measures that legally restrict others from doing anything the 
 *             license permits.
 * 
 * Check the Creative Commons website for any details, legalese, and updates.
 */
package nz.co.gregs.dbvolution.utility;

/**
 * Returns the first non-null, non-empty string.
 *
 * @author gregorygraham
 */
public class StringCheck {

	/**
	 * Returns the supplied value or the default if the supplied value is null or
	 * empty.
	 *
	 *
	 * @param initialValue
	 * @param defaultValue
	 * @return the intialValue or the default if the initialValue is null or empty
	 */
	public static String check(String initialValue, String defaultValue) {
		return initialValue == null || initialValue.isEmpty() ? defaultValue : initialValue;
	}

	/**
	 * Returns the first value that is neither null nor empty.
	 *
	 * <p>
	 * Returns the empty string if no value is non-null and non-empty.<p>
	 *
	 * @param initialValue
	 * @param defaultValues
	 * @return the empty string or the first non-null non-empty value
	 */
	public static String check(String initialValue, String... defaultValues) {
		if (isNotEmptyNorNull(initialValue)) {
			return initialValue;
		}
		for (String value : defaultValues) {
			if (isNotEmptyNorNull(value)) {
				return value;
			}
		}
		return "";
	}

	/**
	 * Returns the supplied value or the default if the supplied value is null.
	 *
	 *
	 * @param initialValue
	 * @param nullValue
	 * @return the intialValue or the default if the initialValue is null
	 */
	public static String checkNotNull(String initialValue, String nullValue) {
		return initialValue == null ? nullValue : initialValue;
	}

	/**
	 * Returns the first value that is non-null.
	 *
	 * <p>
	 * Returns the empty string if no value is non-null.<p>
	 *
	 * @param initialValue
	 * @param defaultValues
	 * @return the empty string or the first non-null non-empty value
	 */
	public static String checkNotNull(String initialValue, String... defaultValues) {
		if (isNotNull(initialValue)) {
			return initialValue;
		}
		for (String value : defaultValues) {
			if (isNotNull(value)) {
				return value;
			}
		}
		return "";
	}

	/**
	 * Returns the supplied value or the null value if the supplied value is null
	 * and the empty value if the supplied value is the empty string.
	 *
	 *
	 * @param initialValue
	 * @param nullValue
	 * @param emptyValue
	 * @return the intialValue if it is non-null and non-empty, nullValue if it is
	 * null, or emptyValue if it is empty
	 */
	public static String checkNotNullOrEmpty(String initialValue, String nullValue, String emptyValue) {
		return initialValue == null ? nullValue : (initialValue.isEmpty() ? emptyValue : initialValue);
	}

	/**
	 * Synonym for intialValue == null
	 *
	 * @param initialValue
	 * @return
	 */
	public static boolean isNull(String initialValue) {
		return initialValue == null;
	}

	/**
	 * Synonym for intialValue != null
	 *
	 * @param initialValue
	 * @return
	 */
	public static boolean isNotNull(String initialValue) {
		return initialValue != null;
	}

	/**
	 * Synonym for value != null && !value.isEmpty() but a lot more concise.
	 *
	 * @param value
	 * @return
	 */
	public static boolean isNotEmptyNorNull(String value) {
		return value != null && !value.isEmpty();
	}

	/**
	 * Synonym for value == null || value.isEmpty() but a lot more concise.
	 *
	 * @param value
	 * @return
	 */
	public static boolean isEmptyOrNull(String value) {
		return value == null || value.isEmpty();
	}

	private StringCheck() {
	}
}

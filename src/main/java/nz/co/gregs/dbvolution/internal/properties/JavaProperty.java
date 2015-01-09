package nz.co.gregs.dbvolution.internal.properties;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import nz.co.gregs.dbvolution.exceptions.DBPebkacException;
import nz.co.gregs.dbvolution.exceptions.DBThrownByEndUserCodeException;

/**
 * Low-level internal abstraction layer over java fields and bean-properties.
 *
 * @author Malcolm Lett
 */
interface JavaProperty {

	/**
	 * Gets a string that clearly identifies the field or bean-property, suitable for inclusion in an exception message
	 * or logging.
	 *
	 * <p>
	 * The format is suitable such that you may generate strings via constructs such as the following, and achieve the
	 * indicated result:
	 * <ul>
	 * <li> {@code new Exception("Invalid valid for "+property)}
	 * <ul>
	 * <li> Generates a message like: {@code "Invalid valid for field com.mycompany.myproject.MyTable.fieldName"}
	 * </ul>
	 * </ul>
	 */
	@Override
	public String toString();

	/**
	 * Tests for equality, based entirely on whether the underlying java field or bean-property is the same.
	 */
	@Override
	public boolean equals(Object other);

	/**
	 * Hash-code based on the underlying java field or bean-property.
	 */
	@Override
	public int hashCode();

	/**
	 * Indicates whether this java property is a field.
	 *
	 * @return {@code true} if a field, {@code false} if a bean-property.
	 */
	public boolean isField();

	/**
	 * Gets the property name, without the declaring class. For fields this is the same as the field name. For
	 * bean-properties it's the inferred name that (usually) starts with a lower-case letter. eg: {@code "uid"}
	 *
	 * @return the java property name
	 */
	public String name();

	/**
	 * Gets the partially qualified name of the property in the class that declares it, including only the short name of
	 * the declaring class. Fields and bean-properties are both formatted using the name of the property without
	 * indication of field vs. method. eg: {@code "Customer.uid"}
	 *
	 * @return the qualified name of the java property
	 */
	public String shortQualifiedName();

	/**
	 * Gets the fully qualified name of the property in the class that declares it, including the fully qualified name
	 * of the declaring class. Fields and bean-properties are both formatted using the name of the property without
	 * indication of field vs. method. eg: {@code "nz.co.mycompany.myproject.Customer.uid"}
	 *
	 * @return the qualified name of the java property
	 */
	public String qualifiedName();

	/**
	 * Gets the property type.
	 *
	 * <p>
	 * On bean-properties, it is possible for the getter and setter to specify different types. When the getter's type
	 * is a sub-type of the setter's type, this can be safely resolved to the getter's type. All other cases are invalid
	 * are result in an exception, including when the setter's type is a sub-type of the getter's type. In those other
	 * cases there is no one reference type that can be used when calling both the getter and setter.
	 *
	 * @return the type if a single consistent type can be resolved
	 * @throws DBPebkacException if the types are different and unable to be resolved to a single type
	 */
	public Class<?> type();

	/**
	 * Gets a <tt>Type</tt> object that represents the formal type of the property, including generic parameters, if
	 * any.
	 *
	 * @return the generic type of the property.
	 */
	public Type genericType();

	/**
	 * Get the property's value on the given target object. Use {@link #isReadable()} to determine if the property is
	 * readable, prior to calling this method.
	 *
	 * @param target the object to get the property from
	 * @return the property's value
	 * @throws DBThrownByEndUserCodeException if the getter on the target object throws any runtime or checked
	 * exceptions
	 * @throws IllegalStateException if the property is not readable
	 */
	public Object get(Object target);

	/**
	 * Set the property's value on the given target object. Use {@link #isWritable()} to determine if the property is
	 * writable, prior to calling this method.
	 *
	 * @param target the object to affect
	 * @param value the value to set
	 * @throws DBThrownByEndUserCodeException if the getter on the target object throws any runtime or checked
	 * exceptions
	 * @throws IllegalStateException if the property is not writable
	 */
	public void set(Object target, Object value);

	/**
	 * Indicates whether the value of the property can be retrieved. Bean properties which are missing a 'getter' can
	 * not be read, but may be able to be set.
	 *
	 * @return {@code true} if readable
	 */
	public boolean isReadable();

	/**
	 * Indicates whether the value of the property can be modified. Bean properties which are missing a 'setter' can not
	 * be written to, but may be able to be read.
	 *
	 * @return {@code true} if writable
	 */
	public boolean isWritable();

	/**
	 * Indicates whether the specified annotation is declared on this java property.
	 *
	 * <p>
	 * This method handles inheritance of annotations as per the standard java specification. Note that, where the
	 * inherited method specifies the same annotation as the overriding method, it's likely that the java specification
	 * says the overriding method's version of the annotation is the only one seen.
	 *
	 * <p>
	 * On bean-properties, it is possible for the getter and setter to both specify the same annotation. This method
	 * makes no attempt to validate them if present more than once.
	 *
	 * @param annotationClass the annotation to check for
	 * @return {@code true} if the annotation is present
	 */
	public boolean isAnnotationPresent(Class<? extends Annotation> annotationClass);

	/**
	 * Gets the specified annotation, if it exists.
	 *
	 * <p>
	 * This method handles inheritance of annotations as per the standard java specification. Note that, where the
	 * inherited method specifies the same annotation as the overriding method, it's likely that the java specification
	 * says the overriding method's version of the annotation is the only one seen.
	 *
	 * <p>
	 * On bean-properties, it is possible for the getter and setter to both specify the same annotation. This that
	 * circumstance, this method asserts that the declared annotations must be identical. If not, an exception is
	 * thrown.
	 *
	 * @param A the annotation type
	 * @param annotationClass the annotation to check for
	 * @return the annotation, or null if not found
	 * @throws DBPebkacException if the annotation is duplicated and different
	 */
	public <A extends Annotation> A getAnnotation(Class<A> annotationClass);

}

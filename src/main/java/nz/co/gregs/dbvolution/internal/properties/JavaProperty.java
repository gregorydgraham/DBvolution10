package nz.co.gregs.dbvolution.internal.properties;

import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import nz.co.gregs.dbvolution.exceptions.DBPebkacException;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.exceptions.DBThrownByEndUserCodeException;
import nz.co.gregs.dbvolution.exceptions.FailedToSetPropertyValueOnRowDefinition;

/**
 * Low-level internal abstraction layer over java fields and bean-properties.
 * @author Malcolm Lett
 */
interface JavaProperty {
	
	/**
	 * Gets a string that clearly identifies the field or bean-property,
	 * suitable for inclusion in an exception message or logging.
	 * 
	 * <p> The format is suitable such that you may generate strings
	 * via constructs such as the following, and achieve the indicated
	 * result:
	 * <ul>
	 * <li> {@code new Exception("Invalid valid for "+property)}
	 *      <ul>
	 *      <li> Generates a message like: {@code "Invalid valid for field com.mycompany.myproject.MyTable.fieldName"}
	 *      </ul>
	 * </ul> 
	 */
	@Override
	public String toString();
	
	/**
	 * Tests for equality, based entirely on whether the underlying java field or bean-property
	 * is the same.
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
	 * @return {@code true} if a field, {@code false} if a bean-property.
	 */
	public boolean isField();
	
	/**
	 * Gets the property name, without the declaring class.
	 * For fields this is the same as the field name.
	 * For bean-properties it's the inferred name that (usually) starts with a lower-case letter.
	 * eg: {@code "uid"}
	 * @return the java property name
	 */
	public String name();
	
	/**
	 * Gets the partially qualified name of the property in the class that declares it,
	 * including only the short name of the declaring class.
	 * Fields and bean-properties are both formatted using the name of the property
	 * without indication of field vs. method.
	 * eg: {@code "Customer.uid"}
	 * @return the qualified name of the java property
	 */
	public String shortQualifiedName();
	
	/**
	 * Gets the fully qualified name of the property in the class that declares it,
	 * including the fully qualified name of the declaring class.
	 * Fields and bean-properties are both formatted using the name of the property
	 * without indication of field vs. method.
	 * eg: {@code "nz.co.mycompany.myproject.Customer.uid"}
	 * @return the qualified name of the java property
	 */
	public String qualifiedName();
	
	/**
	 * Gets the property type.
	 * 
	 * <p> On bean-properties, it is possible for the getter and setter to specify
	 * different types. When the getter's type is a sub-type of the setter's type,
	 * this can be safely resolved to the getter's type.
	 * All other cases are invalid are result in an exception,
	 * including when the setter's type is a sub-type of the getter's type.
	 * In those other cases there is no one reference type that can be
	 * used when calling both the getter and setter. 
	 * @return the type if a single consistent type can be resolved
	 * @throws DBPebkacException if the types are different and unable to
	 * be resolved to a single type
	 */
	public Class<?> type();
	
	/**
     * Gets a <tt>Type</tt> object that represents the formal type of the property,
     * including generic parameters, if any.
	 * @return
	 */
	public Type genericType();
	
	/**
	 * Get the property's value on the given target object.
	 * Use {@link #isReadable()} to determine if the property is readable,
	 * prior to calling this method.
	 * @param target
	 * @return the property's value
	 * @throws DBThrownByEndUserCodeException if the getter on the target object throws any runtime or checked exceptions
	 * @throws IllegalStateException if the property is not readable
	 */
	public Object get(Object target);

	/**
	 * Set the property's value on the given target object.
	 * Use {@link #isWritable()} to determine if the property is writable,
	 * prior to calling this method.
	 * @param target
	 * @param value
	 * @throws DBThrownByEndUserCodeException if the getter on the target object throws any runtime or checked exceptions
	 * @throws IllegalStateException if the property is not writable
	 */
	public void set(Object target, Object value);
	
	/**
	 * Indicates whether the value of the property can be retrieved.
	 * Bean properties which are missing a 'getter' can not be read,
	 * but may be able to be set.
	 * @return {@code true} if readable
	 */
	public boolean isReadable();
	
	/**
	 * Indicates whether the value of the property can be modified.
	 * Bean properties which are missing a 'setter' can not be written to,
	 * but may be able to be read.
	 * @return {@code true} if writable
	 */
	public boolean isWritable();
	
	/**
	 * Indicates whether the specified annotation is declared
	 * on this java property.
	 * 
	 * <p> This method handles inheritance of annotations as per the standard 
	 * java specification. Note that, where the inherited method specifies the same
	 * annotation as the overriding method, it's likely that the java specification says
	 * the overriding method's version of the annotation is the only one seen.
	 * 
	 * <p> On bean-properties, it is possible for the getter and setter to both specify
	 * the same annotation. This method makes no attempt to validate them if present
	 * more than once.
	 * @param annotationClass
	 * @return {@code true} if the annotation is present
	 */
	public boolean isAnnotationPresent(Class<? extends Annotation> annotationClass);
	
	/**
	 * Gets the specified annotation, if it exists.
	 * 
	 * <p> This method handles inheritance of annotations as per the standard 
	 * java specification. Note that, where the inherited method specifies the same
	 * annotation as the overriding method, it's likely that the java specification says
	 * the overriding method's version of the annotation is the only one seen.
	 * 
	 * <p> On bean-properties, it is possible for the getter and setter to both specify
	 * the same annotation. This that circumstance, this method asserts that the declared
	 * annotations must be identical. If not, an exception is thrown.
	 * @param annotationClass
	 * @return the annotation, or null if not found
	 * @throws DBPebkacException if the annotation is duplicated and different
	 */
	public <A extends Annotation> A getAnnotation(Class<A> annotationClass);

	/**
	 * Implementation over java fields.
	 */
	public class JavaField implements JavaProperty {
		private Field field;
		
		public JavaField(Field field) {
			this.field = field;
		}
		
		@Override
		public String toString() {
			return "field "+qualifiedName();
		}

		/**
		 * Hash-code based on the underlying java field or bean-property.
		 */
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((field == null) ? 0 : field.hashCode());
			return result;
		}

		/**
		 * Tests for equality, based entirely on whether the underlying java field or bean-property
		 * is the same.
		 */
		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (!(obj instanceof JavaField)) {
				return false;
			}
			JavaField other = (JavaField) obj;
			if (field == null) {
				if (other.field != null) {
					return false;
				}
			} else if (!field.equals(other.field)) {
				return false;
			}
			return true;
		}

		@Override
		public boolean isField() {
			return true;
		}
		
		@Override
		public String name() {
			return field.getName();
		}

		@Override
		public String shortQualifiedName() {
			return field.getDeclaringClass().getSimpleName()+"."+field.getName();
		}
		
		@Override
		public String qualifiedName() {
			return field.getDeclaringClass().getName()+"."+field.getName();
		}
		
		@Override
		public Class<?> type() {
			return field.getType();
		}

		@Override
		public Type genericType() {
			return field.getGenericType();
		}
		
		@Override
		public boolean isReadable() {
			return true;
		}

		@Override
		public boolean isWritable() {
			return true;
		}
		
		@Override
		public Object get(Object target) {
			try {
				return field.get(target);
			} catch (IllegalArgumentException e) {
				// usually thrown when 'target' isn't of the same type as 'field' is declared on,
				// so this is probably a bug
				String class1 = (target == null) ? "null" : target.getClass().getName();
				throw new FailedToSetPropertyValueOnRowDefinition(qualifiedName(), class1, e);
			} catch (IllegalAccessException e) {
				// caused by a Java security manager or an attempt to access a non-visible field
				// without first making it visible
				throw new DBRuntimeException("Java security error reading field "+qualifiedName()+": "+
						e.getLocalizedMessage(), e);
			}
		}

		@Override
		public void set(Object target, Object value) {
			try {
				field.set(target, value);
			} catch (IllegalArgumentException e) {
				// usually thrown when 'target' isn't of the same type as 'field' is declared on,
				// so this is probably a bug
				String class1 = (target == null) ? "null" : target.getClass().getName();
				throw new IllegalArgumentException("Internal error writing field "+
						qualifiedName()+" on object of type "+class1+" (this is probably a DBvolution bug): "+
						e.getLocalizedMessage(), e);
			} catch (IllegalAccessException e) {
				// caused by a Java security manager or an attempt to access a non-visible field
				// without first making it visible
				throw new DBRuntimeException("Java security error writing field "+qualifiedName()+": "+
						e.getLocalizedMessage(), e);
			}
		}

		@Override
		public boolean isAnnotationPresent(Class<? extends Annotation> annotationClass) {
			return field.isAnnotationPresent(annotationClass);
		}

		@Override
		public <A extends Annotation> A getAnnotation(Class<A> annotationClass) {
			return field.getAnnotation(annotationClass);
		}
	}
	
	/**
	 * Implementation over bean properties.
	 */
	public class JavaBeanProperty implements JavaProperty {
		private String name;
		private Class<?> type;
		private Type genericType;
		private Method getter;
		private Method setter;
		
		public JavaBeanProperty(PropertyDescriptor descriptor) {
			this.name = descriptor.getName();
			this.type = descriptor.getPropertyType();
			this.getter = descriptor.getReadMethod();
			this.setter = descriptor.getWriteMethod();
			
			if (this.getter != null) {
				this.genericType = this.getter.getGenericReturnType();
			}
			else if (this.setter != null) {
				Type[] types = this.setter.getGenericParameterTypes();
				if (types.length == 1) {
					this.genericType = types[0];
				}
			}
		}

		/**
		 * String representation suitable for debugging and logging
		 */
		@Override
		public String toString() {
			return "property "+type().getSimpleName()+" "+qualifiedName();
		}
		
		/**
		 * Hash-code based on the underlying java getter and
		 * setter methods.
		 */
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((getter == null) ? 0 : getter.hashCode());
			result = prime * result
					+ ((setter == null) ? 0 : setter.hashCode());
			return result;
		}

		/**
		 * Tests for equality, based on the underlying java
		 * getter and setter methods.
		 */
		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (!(obj instanceof JavaBeanProperty)) {
				return false;
			}
			JavaBeanProperty other = (JavaBeanProperty) obj;
			if (getter == null) {
				if (other.getter != null) {
					return false;
				}
			} else if (!getter.equals(other.getter)) {
				return false;
			}
			if (setter == null) {
				if (other.setter != null) {
					return false;
				}
			} else if (!setter.equals(other.setter)) {
				return false;
			}
			return true;
		}

		@Override
		public boolean isField() {
			return false;
		}
		
		@Override
		public String name() {
			return name;
		}
		
		@Override
		public String shortQualifiedName() {
			if (getter != null) {
				return getter.getDeclaringClass().getSimpleName()+"."+name;
			}
			else if (setter != null) {
				return setter.getDeclaringClass().getSimpleName()+"."+name;
			}
			else {
				return name;
			}
		}

		@Override
		public String qualifiedName() {
			if (getter != null) {
				return getter.getDeclaringClass().getName()+"."+name;
			}
			else if (setter != null) {
				return setter.getDeclaringClass().getName()+"."+name;
			}
			else {
				return name;
			}
		}
		
		@Override
		public Class<?> type() {
			return type;
		}
		
		@Override
		public Type genericType() {
			return genericType;
		}
		
		@Override
		public boolean isReadable() {
			return getter != null;
		}

		@Override
		public boolean isWritable() {
			return setter != null;
		}
		
		@Override
		public Object get(Object target) {
			if (getter == null) {
				// caller should have checked the isReadable() method first
				throw new IllegalStateException("Internal error attempting to read non-readable property "+
						qualifiedName()+" (this is probably a DBvolution bug)");
			}
			
			try {
				return getter.invoke(target);
			} catch (IllegalArgumentException e) {
				// usually thrown when 'target' isn't of the same type as 'field' is declared on,
				// so this is probably a bug
				String class1 = (target == null) ? "null" : target.getClass().getName();
				throw new DBRuntimeException("Internal error reading property "+
						qualifiedName()+" on object of type "+class1+" (this is probably a DBvolution bug): "+
						e.getLocalizedMessage(), e);
			} catch (IllegalAccessException e) {
				// caused by a Java security manager or an attempt to access a non-visible field
				// without first making it visible
				throw new DBRuntimeException("Java security error reading property "+qualifiedName()+": "+
						e.getLocalizedMessage(), e);
			} catch (InvocationTargetException e) {
				// any checked or runtime exception thrown by the setter method itself
				Throwable cause = (e.getCause() == null) ? e : e.getCause();
				String msg = (cause.getLocalizedMessage() == null) ? "" : ": "+cause.getLocalizedMessage();
				throw new DBThrownByEndUserCodeException("Accessor method threw "+cause.getClass().getSimpleName()+" reading property "+
						qualifiedName()+msg, cause);
			}
		}

		@Override
		public void set(Object target, Object value) {
			if (setter == null) {
				// caller should have checked the isWritable method first
				throw new IllegalStateException("Internal error attempting to write to non-writable property "+
						qualifiedName()+" (this is probably a DBvolution bug)");
			}
			
			try {
				setter.invoke(target, value);
			} catch (IllegalArgumentException e) {
				// usually thrown when 'target' isn't of the same type as 'field' is declared on,
				// so this is probably a bug
				String class1 = (target == null) ? "null" : target.getClass().getName();
				throw new IllegalArgumentException("internal error writing to property "+
						qualifiedName()+" on object of type "+class1+" (this is probably a DBvolution bug): "+
						e.getLocalizedMessage(), e);
			} catch (IllegalAccessException e) {
				// caused by a Java security manager or an attempt to access a non-visible field
				// without first making it visible
				throw new DBRuntimeException("Java security error writing to property "+qualifiedName()+": "+
						e.getLocalizedMessage(), e);
			} catch (InvocationTargetException e) {
				// any checked or runtime exception thrown by the setter method itself
				Throwable cause = (e.getCause() == null) ? e : e.getCause();
				String msg = (cause.getLocalizedMessage() == null) ? "" : ": "+cause.getLocalizedMessage();
				throw new DBThrownByEndUserCodeException("Accessor method threw "+cause.getClass().getSimpleName()+" writing to property "+
						qualifiedName()+msg, cause);
			}
			
		}

		@Override
		public boolean isAnnotationPresent(Class<? extends Annotation> annotationClass) {
			return (getter != null && getter.isAnnotationPresent(annotationClass)) ||
					(setter != null && setter.isAnnotationPresent(annotationClass));
		}
		
		@Override
		public <A extends Annotation> A getAnnotation(Class<A> annotationClass) {
			A getterAnnotation = null;
			A setterAnnotation = null;
			if (getter != null) {
				getterAnnotation = getter.getAnnotation(annotationClass);
			}
			if (setter != null) {
				setterAnnotation = setter.getAnnotation(annotationClass);
			}
			
			if (getterAnnotation != null && setterAnnotation != null) {
				if (!annotationsEqual(getterAnnotation, setterAnnotation)) {
					String name = annotationClass.getSimpleName();
					throw new DBPebkacException("@"+name+" different on "+qualifiedName()+" getter and setter ");
				}
			}
			
			if (getterAnnotation != null) {
				return getterAnnotation;
			}
			else if (setterAnnotation != null) {
				return setterAnnotation;
			}
			return null;
		}
		
		/**
		 * Tests whether two annotations are semantically identical. 
		 * @param ann1
		 * @param ann2
		 * @return
		 */
		protected static <A extends Annotation> boolean annotationsEqual(A ann1, A ann2) {
			List<Object> values1 = getAnnotationValues(ann1);
			List<Object> values2 = getAnnotationValues(ann2);
			return values1.equals(values2);
		}
		
		/**
		 * Gets the attribute values of the annotation.
		 * @param annotation
		 * @return
		 */
		protected static <A extends Annotation> List<Object> getAnnotationValues(A annotation) {
			List<Object> values = new ArrayList<Object>();
			
			Method[] methods = annotation.annotationType().getMethods();
			for (Method method: methods) {
				if (!method.getDeclaringClass().isAssignableFrom(Annotation.class)) {
					try {
						Object value = method.invoke(annotation);
						values.add(value);
					} catch (IllegalArgumentException e) {
						// usually thrown when 'target' isn't of the same type as 'field' is declared on,
						// so this is probably a bug
						String name = "@"+annotation.annotationType().getSimpleName()+"."+method.getName();
						throw new DBRuntimeException("Internal error reading annotation value "+name+
								" (this is probably a DBvolution bug): "+
								e.getLocalizedMessage(), e);
					} catch (IllegalAccessException e) {
						// caused by a Java security manager or an attempt to access a non-visible field
						// without first making it visible
						String name = "@"+annotation.annotationType().getSimpleName()+"."+method.getName();
						throw new DBRuntimeException("Java security error reading annotation value "+name+": "+
								e.getLocalizedMessage(), e);
					} catch (InvocationTargetException e) {
						Throwable cause = (e.getCause() == null) ? e : e.getCause();
						String msg = (cause.getLocalizedMessage() == null) ? "" : ": "+cause.getLocalizedMessage();
						String name = "@"+annotation.annotationType().getSimpleName()+"."+method.getName();
						throw new DBRuntimeException("Internal error reading annotation value "+name+
								" (this is probably a DBvolution bug)"+msg, cause);
					}
				}
			}
			
			return values;
		}
	}
}

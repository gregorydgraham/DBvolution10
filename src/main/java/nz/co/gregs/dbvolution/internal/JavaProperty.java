package nz.co.gregs.dbvolution.internal;

import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.exceptions.DBThrownByEndUserCodeException;

/**
 * Low-level internal abstraction layer over java fields and bean-properties.
 * @author Malcolm Lett
 */
// TODO: need to test support for private fields
public interface JavaProperty {
	
	/**
	 * Gets a string representation suitable for debugging and logging.
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
	 * If it is not a field, then it is a bean-property.
	 * @return {@code true} if a field, {@code false} if a bean-property.
	 */
	public boolean isField();
	
	/**
	 * Gets the property name.
	 * For fields this is the same as the field name.
	 * For bean-properties it's the inferred name that (usually) starts with a lower-case letter.
	 * @return the java property name
	 */
	public String name();
	
	/**
	 * Gets the fully qualified name of the property in the class that declares it.
	 * Fields and bean-properties are both formatted using the name of the property
	 * without indication of field vs. method.
	 * @return the qualified name of the java property
	 */
	public String qualifiedName();
	
	/**
	 * Gets the property type.
	 * @return the type
	 */
	public Class<?> type();
	
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
	 * Gets the specified annotation, if it exists.
	 * 
	 * <p> This method handles inheritance of annotations as per the standard 
	 * java specification. Note that, where the inherited method specifies the same
	 * annotation as the overriding method, it's likely that the java specification says
	 * the overriding method's version of the annotation is the only one seen.
	 * 
	 * <p> This method returns the first encountered annotation where multiple exist
	 * for the given type. Multiple annotations of the same type is generally
	 * not possible in java because it doesn't allow the same annotation type on a given
	 * class member. However bean properties are made up of a 'getter' and a 'setter' method,
	 * which may each include the same annotation.
	 * {@link #getAnnotations(Class)} should be used to retrieve all versions of the annotation
	 * and validation performed against them.
	 * @param annotationClass
	 * @return the annotation, or null if not found
	 */
	// TODO: change this to automatically check the annotations are identical
	// TODO: throw a useful (checked?) exception if it does occur
	public <A extends Annotation> A getAnnotation(Class<A> annotationClass);

	/**
	 * Gets all applicable instances of the specified annotation on the property.
	 * This method should be used when first initialising in order to perform validation
	 * on user specified annotations.
	 * @param annotationClass
	 * @return list of annotations of the given type, empty list if none
	 */
	// TODO: remove this in favour of smarter logic in the getAnnotation() method
	public <A extends Annotation> List<A> getAnnotations(Class<A> annotationClass);
	
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
			return "field "+name();
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
		public Class<?> type() {
			return field.getType();
		}

		@Override
		public String qualifiedName() {
			return field.getDeclaringClass().getName()+"."+field.getName();
		}
		
		@Override
		public Object get(Object target) {
			try {
				return field.get(target);
			} catch (IllegalArgumentException e) {
				// usually thrown when 'target' isn't of the same type as 'field' is declared on,
				// so this is probably a bug
				String class1 = (target == null) ? "null" : target.getClass().getName();
				throw new IllegalArgumentException("Internal error reading field "+
						qualifiedName()+" on object of type "+class1+" (this is probably a DBvolution bug): "+
						e.getLocalizedMessage(), e);
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
		public boolean isReadable() {
			return true;
		}

		@Override
		public boolean isWritable() {
			return true;
		}

		@Override
		public <A extends Annotation> A getAnnotation(Class<A> annotationClass) {
			return field.getAnnotation(annotationClass);
		}

		// fields can only ever have one instance of the annotation
		@Override
		public <A extends Annotation> List<A> getAnnotations(Class<A> annotationClass) {
			A annotation = getAnnotation(annotationClass);
			if (annotation == null) {
				return Collections.emptyList();
			}
			else {
				return Collections.singletonList(annotation);
			}
		}
	}
	
	/**
	 * Implementation over bean properties.
	 */
	public class JavaBeanProperty implements JavaProperty {
		private String name;
		private Class<?> type;
		private Method getter;
		private Method setter;
		
		public JavaBeanProperty(PropertyDescriptor descriptor) {
			this.name = descriptor.getName();
			this.type = descriptor.getPropertyType();
			this.getter = descriptor.getReadMethod();
			this.setter = descriptor.getWriteMethod();
		}

		/**
		 * String representation suitable for debugging and logging
		 */
		@Override
		public String toString() {
			return "property "+name();
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
		public Class<?> type() {
			return type;
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
		public Object get(Object target) {
			if (getter == null) {
				// caller should have checked the isReadable() method first
				throw new IllegalStateException("Internal error attempting to read non-readable property "+
						qualifiedName()+" (this is probably a DBvolution bug)");
			}
			
			try {
				return setter.invoke(target);
			} catch (IllegalArgumentException e) {
				// usually thrown when 'target' isn't of the same type as 'field' is declared on,
				// so this is probably a bug
				String class1 = (target == null) ? "null" : target.getClass().getName();
				throw new IllegalArgumentException("Internal error reading property "+
						qualifiedName()+" on object of type "+class1+" (this is probably a DBvolution bug): "+
						e.getLocalizedMessage(), e);
			} catch (IllegalAccessException e) {
				// caused by a Java security manager or an attempt to access a non-visible field
				// without first making it visible
				throw new DBRuntimeException("Java security error reading property "+qualifiedName()+": "+
						e.getLocalizedMessage(), e);
			} catch (InvocationTargetException e) {
				// any checked or runtime exception thrown by the setter method itself
				// TODO: check that this exception wraps runtime exceptions as well
				Throwable cause = e.getCause();
				throw new DBThrownByEndUserCodeException("Accessor method threw "+cause.getClass().getSimpleName()+" reading property "+
						qualifiedName()+": "+cause.getLocalizedMessage(), cause);
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
				// TODO: check that this exception wraps runtime exceptions as well
				Throwable cause = e.getCause();
				throw new DBThrownByEndUserCodeException("Accessor method threw "+cause.getClass().getSimpleName()+" writing to property "+
						qualifiedName()+": "+cause.getLocalizedMessage(), cause);
			}
			
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
		public <A extends Annotation> A getAnnotation(Class<A> annotationClass) {
			if (getter != null) {
				A annotation = getter.getAnnotation(annotationClass);
				if (annotation != null) {
					return annotation;
				}
			}
			if (setter != null) {
				A annotation = setter.getAnnotation(annotationClass);
				if (annotation != null) {
					return annotation;
				}
			}
			return null;
		}

		@Override
		public <A extends Annotation> List<A> getAnnotations(Class<A> annotationClass) {
			List<A> annotations = new ArrayList<A>();
			if (getter != null) {
				A annotation = getter.getAnnotation(annotationClass);
				if (annotation != null) {
					annotations.add(annotation);
				}
			}
			if (setter != null) {
				A annotation = setter.getAnnotation(annotationClass);
				if (annotation != null) {
					annotations.add(annotation);
				}
			}
			return annotations;
		}
		
	}
}

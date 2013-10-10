package nz.co.gregs.dbvolution.internal;

import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import nz.co.gregs.dbvolution.DBRuntimeException;

/**
 * Low-level internal abstraction layer over java fields and bean-properties.
 * @author Malcolm Lett
 */
// TODO: need to test support for private fields
interface JavaProperty {
	
	/**
	 * Gets the property name.
	 * For fields this is the same as the field name.
	 * For bean-properties it's the inferred name that (usually) starts with a lower-case letter.
	 * @return
	 */
	public String name();
	
	/**
	 * Gets the property type.
	 * @return
	 */
	public Class<?> type();
	
	/**
	 * Get the property's value on the given target object.
	 * Use {@link #isReadable()} to determine if the property is readable,
	 * prior to calling this method.
	 * @param target
	 * @return
	 * @throws DBRuntimeException if the getter on the target object throws any runtime or checked exceptions
	 * @throws IllegalStateException if the property is not readable
	 */
	public Object get(Object target);

	/**
	 * Set the property's value on the given target object.
	 * Use {@link #isWritable()} to determine if the property is writable,
	 * prior to calling this method.
	 * @param target
	 * @param value
	 * @throws DBRuntimeException if the getter on the target object throws any runtime or checked exceptions
	 */
	public void set(Object target, Object value);
	
	/**
	 * Indicates whether the value of the property can be retrieved.
	 * Bean properties which are missing a 'getter' can not be read,
	 * but may be able to be set.
	 * @return
	 */
	public boolean isReadable();
	
	/**
	 * Indicates whether the value of the property can be modified.
	 * Bean properties which are missing a 'setter' can not be written to,
	 * but may be able to be read.
	 * @return
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
	 * @return
	 */
	public <A extends Annotation> A getAnnotation(Class<A> annotationClass);

	/**
	 * Gets all applicable instances of the specified annotation on the property.
	 * This method should be used when first initialising in order to perform validation
	 * on user specified annotations.
	 * @param annotationClass
	 * @return list of annotations of the given type, empty list if none
	 */
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
		public String name() {
			return field.getName();
		}
		
		@Override
		public Class<?> type() {
			return field.getType();
		}

		private String qualifiedName() {
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

		@Override
		public String name() {
			return name;
		}
		
		@Override
		public Class<?> type() {
			return type;
		}
		
		private String qualifiedName() {
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
				throw new IllegalStateException("Internal error attempting to read non-readable property "+
						qualifiedName()+" (this is probably a DBvolution bug)");
			}
			
			try {
				return setter.invoke(target);
			} catch (IllegalArgumentException e) {
				// usually thrown when 'target' isn't of the same type as 'field' is declared on,
				// so this is probably a bug
				String class1 = (target == null) ? "null" : target.getClass().getName();
				throw new IllegalArgumentException("internal error reading property "+
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
				throw new DBRuntimeException("Accessor method threw "+cause.getClass().getSimpleName()+" reading property "+
						qualifiedName()+": "+cause.getLocalizedMessage(), cause);
			}
		}

		@Override
		public void set(Object target, Object value) {
			if (setter == null) {
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
				throw new DBRuntimeException("Accessor method threw "+cause.getClass().getSimpleName()+" writing to property "+
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

/*
 * Copyright 2014 Gregory Graham.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nz.co.gregs.dbvolution.internal.properties;

import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.exceptions.DBThrownByEndUserCodeException;
import nz.co.gregs.dbvolution.exceptions.ReferenceToUndefinedPrimaryKeyException;

/**
 * Implementation over bean properties.
 */
public class JavaBeanProperty implements JavaProperty, Serializable {

	private static final long serialVersionUID = 1l;

	private final String name;
	private final Class<?> type;
	private Type genericType;
	private transient final Method getter;
	private transient final Method setter;

	/**
	 * Create a new JavaBeanProperty from the supplied descriptor.
	 *
	 * @param descriptor	descriptor
	 */
	public JavaBeanProperty(PropertyDescriptor descriptor) {
		this.name = descriptor.getName();
		this.type = descriptor.getPropertyType();
		this.getter = descriptor.getReadMethod();
		this.setter = descriptor.getWriteMethod();
		if (this.getter != null) {
			this.genericType = this.getter.getGenericReturnType();
		} else if (this.setter != null) {
			Type[] types = this.setter.getGenericParameterTypes();
			if (types.length == 1) {
				this.genericType = types[0];
			}
		}
	}

	/**
	 * String representation suitable for debugging and logging
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a very brief summary of the property.
	 */
	@Override
	public String toString() {
		return "property " + type().getSimpleName() + " " + qualifiedName();
	}

	/**
	 * Hash-code based on the underlying java getter and setter methods.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the hash-code of this property.
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((getter == null) ? 0 : getter.hashCode());
		result = prime * result + ((setter == null) ? 0 : setter.hashCode());
		return result;
	}

	/**
	 * Tests for equality, based on the underlying java getter and setter methods.
	 *
	 * @param obj the other object to compare to.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return TRUE if the two objects are the same, otherwise FALSE
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
			return getter.getDeclaringClass().getSimpleName() + "." + name;
		} else if (setter != null) {
			return setter.getDeclaringClass().getSimpleName() + "." + name;
		} else {
			return name;
		}
	}

	@Override
	public String qualifiedName() {
		if (getter != null) {
			return getter.getDeclaringClass().getName() + "." + name;
		} else if (setter != null) {
			return setter.getDeclaringClass().getName() + "." + name;
		} else {
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
			throw new IllegalStateException("Internal error attempting to read non-readable property " + qualifiedName() + " (this is probably a DBvolution bug)");
		}
		try {
			return getter.invoke(target);
		} catch (IllegalArgumentException e) {
			// usually thrown when 'target' isn't of the same type as 'field' is declared on,
			// so this is probably a bug
			String class1 = (target == null) ? "null" : target.getClass().getName();
			throw new DBRuntimeException("Internal error reading property " + qualifiedName() + " on object of type " + class1 + " (this is probably a DBvolution bug): " + e.getLocalizedMessage(), e);
		} catch (IllegalAccessException e) {
			// caused by a Java security manager or an attempt to access a non-visible field
			// without first making it visible
			throw new DBRuntimeException("Java security error reading property " + qualifiedName() + ": " + e.getLocalizedMessage(), e);
		} catch (InvocationTargetException e) {
			// any checked or runtime exception thrown by the setter method itself
			Throwable cause = (e.getCause() == null) ? e : e.getCause();
			String msg = (cause.getLocalizedMessage() == null) ? "" : ": " + cause.getLocalizedMessage();
			throw new DBThrownByEndUserCodeException("Accessor method threw " + cause.getClass().getSimpleName() + " reading property " + qualifiedName() + msg, cause);
		}
	}

	@Override
	public void set(Object target, Object value) {
		if (setter == null) {
			// caller should have checked the isWritable method first
			throw new IllegalStateException("Internal error attempting to write to non-writable property " + qualifiedName() + " (this is probably a DBvolution bug)");
		}
		try {
			setter.invoke(target, value);
		} catch (IllegalArgumentException e) {
			// usually thrown when 'target' isn't of the same type as 'field' is declared on,
			// so this is probably a bug
			String class1 = (target == null) ? "null" : target.getClass().getName();
			throw new IllegalArgumentException("internal error writing to property " + qualifiedName() + " on object of type " + class1 + " (this is probably a DBvolution bug): " + e.getLocalizedMessage(), e);
		} catch (IllegalAccessException e) {
			// caused by a Java security manager or an attempt to access a non-visible field
			// without first making it visible
			throw new DBRuntimeException("Java security error writing to property " + qualifiedName() + ": " + e.getLocalizedMessage(), e);
		} catch (InvocationTargetException e) {
			// any checked or runtime exception thrown by the setter method itself
			Throwable cause = (e.getCause() == null) ? e : e.getCause();
			String msg = (cause.getLocalizedMessage() == null) ? "" : ": " + cause.getLocalizedMessage();
			throw new DBThrownByEndUserCodeException("Accessor method threw " + cause.getClass().getSimpleName() + " writing to property " + qualifiedName() + msg, cause);
		}
	}

	@Override
	public boolean isAnnotationPresent(Class<? extends Annotation> annotationClass) {
		return (getter != null && getter.isAnnotationPresent(annotationClass)) || (setter != null && setter.isAnnotationPresent(annotationClass));
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
				throw new ReferenceToUndefinedPrimaryKeyException("@" + name + " different on " + qualifiedName() + " getter and setter ");
			}
		}
		if (getterAnnotation != null) {
			return getterAnnotation;
		} else if (setterAnnotation != null) {
			return setterAnnotation;
		}
		return null;
	}

	/**
	 * Tests whether two annotations are semantically identical.
	 *
	 * @param <A> the annotation type
	 * @param ann1 ann1
	 * @param ann2 ann2
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return TRUE if the annotations are semantically identical, otherwise
	 * FALSE.
	 */
	protected static <A extends Annotation> boolean annotationsEqual(A ann1, A ann2) {
		List<Object> values1 = getAnnotationValues(ann1);
		List<Object> values2 = getAnnotationValues(ann2);
		return values1.equals(values2);
	}

	/**
	 * Gets the attribute values of the annotation.
	 *
	 * @param <A> the annotation type
	 * @param annotation annotation
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of the values associated with the annotation.
	 */
	protected static <A extends Annotation> List<Object> getAnnotationValues(A annotation) {
		List<Object> values = new ArrayList<Object>();
		Method[] methods = annotation.annotationType().getMethods();
		for (Method method : methods) {
			if (!method.getDeclaringClass().isAssignableFrom(Annotation.class)) {
				try {
					Object value = method.invoke(annotation);
					values.add(value);
				} catch (IllegalArgumentException e) {
					// usually thrown when 'target' isn't of the same type as 'field' is declared on,
					// so this is probably a bug
					String name = "@" + annotation.annotationType().getSimpleName() + "." + method.getName();
					throw new DBRuntimeException("Internal error reading annotation value " + name + " (this is probably a DBvolution bug): " + e.getLocalizedMessage(), e);
				} catch (IllegalAccessException e) {
					// caused by a Java security manager or an attempt to access a non-visible field
					// without first making it visible
					String name = "@" + annotation.annotationType().getSimpleName() + "." + method.getName();
					throw new DBRuntimeException("Java security error reading annotation value " + name + ": " + e.getLocalizedMessage(), e);
				} catch (InvocationTargetException e) {
					Throwable cause = (e.getCause() == null) ? e : e.getCause();
					String msg = (cause.getLocalizedMessage() == null) ? "" : ": " + cause.getLocalizedMessage();
					String name = "@" + annotation.annotationType().getSimpleName() + "." + method.getName();
					throw new DBRuntimeException("Internal error reading annotation value " + name + " (this is probably a DBvolution bug)" + msg, cause);
				}
			}
		}
		return values;
	}

}

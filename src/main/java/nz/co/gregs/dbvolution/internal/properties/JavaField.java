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

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.exceptions.FailedToSetPropertyValueOnRowDefinition;

/**
 * Implementation over java fields.
 */
public class JavaField implements JavaProperty, Serializable {

	private static final long serialVersionUID = 1l;

	private transient final Field field;

	/**
	 * Create a JavaField for the supplied field.
	 *
	 * @param field	field
	 */
	public JavaField(Field field) {
		this.field = field;
		field.setAccessible(true);
	}

	@Override
	public String toString() {
		return "field " + qualifiedName();
	}

	/**
	 * Hash-code based on the underlying java field or bean-property.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the hash code
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((field == null) ? 0 : field.hashCode());
		return result;
	}

	/**
	 * Tests for equality, based entirely on whether the underlying java field or
	 * bean-property is the same.
	 *
	 * @param second	second
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return TRUE if second is equal to this instance, otherwise FALSE
	 */
	@Override
	public boolean equals(Object second) {
		if (this == second) {
			return true;
		}
		if (second == null) {
			return false;
		}
		if (!(second instanceof JavaField)) {
			return false;
		}
		JavaField other = (JavaField) second;
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
		return field.getDeclaringClass().getSimpleName() + "." + field.getName();
	}

	@Override
	public String qualifiedName() {
		return field.getDeclaringClass().getName() + "." + field.getName();
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
			throw new DBRuntimeException("Java security error reading field " + qualifiedName() + ": " + e.getLocalizedMessage(), e);
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
			throw new IllegalArgumentException("Internal error writing field " + qualifiedName() + " on object of type " + class1 + " (this is probably a DBvolution bug): " + e.getLocalizedMessage(), e);
		} catch (IllegalAccessException e) {
			// caused by a Java security manager or an attempt to access a non-visible field
			// without first making it visible
			throw new DBRuntimeException("Java security error writing field " + qualifiedName() + ": " + e.getLocalizedMessage(), e);
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

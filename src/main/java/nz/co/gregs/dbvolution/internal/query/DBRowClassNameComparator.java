/*
 * Copyright 2014 gregory.graham.
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
package nz.co.gregs.dbvolution.internal.query;

import java.io.Serializable;
import java.util.Comparator;

/**
 *
 * @author gregory.graham
 */
public class DBRowClassNameComparator implements Comparator<Class<?>>, Serializable {

	static final long serialVersionUID = 1L;

	/**
	 * Compares DBRow Classes using their canonical names.
	 *
	 */
	public DBRowClassNameComparator() {
	}

	@Override
	public int compare(Class<?> first, Class<?> second) {
		String firstCanonicalName = first.getCanonicalName();
		String secondCanonicalName = second.getCanonicalName();
		if (firstCanonicalName != null && secondCanonicalName != null) {
			return firstCanonicalName.compareTo(secondCanonicalName);
		} else {
			return first.getSimpleName().compareTo(second.getSimpleName());
		}
	}

}

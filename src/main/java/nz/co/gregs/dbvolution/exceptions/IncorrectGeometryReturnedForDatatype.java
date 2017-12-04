/*
 * Copyright 2015 gregorygraham.
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
package nz.co.gregs.dbvolution.exceptions;

import com.vividsolutions.jts.geom.Geometry;

/**
 * Thrown when the database has returned a geometry that cannot be interpreted
 * as the geometry expected.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class IncorrectGeometryReturnedForDatatype extends DBRuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * Thrown when the database has returned a geometry that cannot be interpreted
	 * as the geometry expected.
	 *
	 * @param databaseReturned
	 * @param wasExpecting
	 */
	public IncorrectGeometryReturnedForDatatype(Geometry databaseReturned, Geometry wasExpecting) {
		super("Geometry Type Returned By Database Clashes With Declared Geometry: Was expecting " + (wasExpecting == null ? "NULL" : wasExpecting.getGeometryType()) + " but the database has provided a " + (databaseReturned == null ? "NULL" : databaseReturned.getGeometryType()) + ".  Please check that the field is declared correctly and that the expression returns the correct type.");
	}

}

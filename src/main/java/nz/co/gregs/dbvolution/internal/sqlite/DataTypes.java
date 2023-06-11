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
package nz.co.gregs.dbvolution.internal.sqlite;

import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBLine2D;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBMultiPoint2D;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBPoint2D;

/**
 *
 *
 * @author gregorygraham
 */
public enum DataTypes {

	/**
	 *
	 */
	LARGETEXT("NTEXT", "NTEXT", DBLargeText.class),	
	/**
	 *
	 */
	BIGINT("BIGINT", "BIGINT", DBInteger.class),
	/**
	 *
	 */
	JAVAOBJECT("JAVAOBJECT", "BLOB", DBJavaObject.class),
	/**
	 *
	 */
	LARGEBINARY("BLOB", "BLOB", DBLargeObject.class),
	/**
	 *
	 */
	BOOLEANARRAY("BOOLEANARRAY", "VARCHAR(64)", DBBooleanArray.class),
	/**
	 *
	 */
	DATETIME("DATETIME", "DATETIME", DBDate.class),
	/**
	 *
	 */
	POINT2D("POINT", "VARCHAR(2000)", DBPoint2D.class),
	/**
	 *
	 */
	LINE2D("LINESTRING", "VARCHAR(2001)", DBLine2D.class),
	/**
	 *
	 */
	LINESEGMENT2D("LINESTRING", "VARCHAR(2001)", DBLine2D.class),
	/**
	 *
	 */
	MULTIPOINT2D("MULTIPOINT", "VARCHAR(2002)", DBMultiPoint2D.class);
	
	private final String databaseType;
	private final String conceptualType;
	private final Class<? extends QueryableDatatype> qdtClass;

	DataTypes(String conceptualType, String databaseType, Class<? extends QueryableDatatype> clazz) {
		this.databaseType = databaseType;
		this.conceptualType = conceptualType;
		this.qdtClass = clazz;
	}

	public Class<? extends QueryableDatatype> getQdtClass() {
		return qdtClass;
	}

	@Override
	public String toString() {
		return conceptualType+"/"+databaseType;
	}

	/**
	 * @return the databaseType
	 */
	public String getDatabaseType() {
		return databaseType;
	}

	/**
	 * @return the conceptualType
	 */
	public String getConceptualType() {
		return conceptualType;
	}

}

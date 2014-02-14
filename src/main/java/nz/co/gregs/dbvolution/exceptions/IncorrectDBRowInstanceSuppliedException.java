/*
 * Copyright 2013 Gregory Graham.
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

import nz.co.gregs.dbvolution.DBRow;

/**
 *
 * @author Gregory Graham
 */
public class IncorrectDBRowInstanceSuppliedException extends RuntimeException{
    
    public static final long serialVersionUID = 1L;

    public IncorrectDBRowInstanceSuppliedException() {
        super("The Field Supplied Is Not A Field Of This Instance: use only fields from this instance.");
    }

    public IncorrectDBRowInstanceSuppliedException(DBRow row, Object qdt) {
        super(constructMessage(row, qdt));
    }

    public static IncorrectDBRowInstanceSuppliedException newMultiRowInstance(Object qdt) {
    	StringBuilder buf = new StringBuilder();
    	buf.append("The ");
    	if (qdt == null) {
    		buf.append("null");
    	}
    	else {
    		buf.append(qdt.getClass().getSimpleName());
    	}
    	buf.append(" Field Supplied Is Not A Field Of Any Of The DBRow ");
    	buf.append(" Instances: use only fields from these instances.");
        return new IncorrectDBRowInstanceSuppliedException(buf.toString());
    }
    
    public IncorrectDBRowInstanceSuppliedException(String message) {
    	super(message);
    }
    
    private static String constructMessage(DBRow row, Object qdt) {
    	StringBuilder buf = new StringBuilder();
    	buf.append("The ");
    	if (qdt == null) {
    		buf.append("null");
    	}
    	else {
    		buf.append(qdt.getClass().getSimpleName());
    	}
    	buf.append(" Field Supplied Is Not A Field Of The ");
    	if (row == null) {
    		buf.append("null");
    	}
    	else {
    		buf.append(row.getClass().getSimpleName());
    	}
    	buf.append(" Instance: use only fields from this instance.");
        return buf.toString();
    }
}

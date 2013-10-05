/*
 * Copyright 2013 gregorygraham.
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
package nz.co.gregs.dbvolution.actions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author gregorygraham
 */
public class DBActionList extends ArrayList<DBAction>{
    private static final long serialVersionUID = 1L;
    
    public List<String> getSQL(){
        List<String> sqlList = new ArrayList<String>();
        for (Iterator<DBAction> it = this.iterator(); it.hasNext();) {
            DBAction act = it.next();
            sqlList.add(act.getSQLRepresentation());
        }
        return sqlList;
    }
        
}

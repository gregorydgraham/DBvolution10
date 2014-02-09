/*
 * Copyright 2014 greg.
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

/**
 *
 * @author greg
 */
public class QueryOptions {
    
    private boolean matchAll = true;

    /**
     * 
     * 
     * @return the matchAll
     */
    public boolean isMatchAll() {
        return matchAll;
    }

    public boolean isMatchAny() {
        return !matchAll;
    }

    /**
     * Changes the DBQuery to using all ANDs to connect the criteria
     * 
     */
    public void setMatchAll() {
        this.matchAll = true;
    }
    
    /**
     * Changes the DBQuery to using all ORs to connect the criteria
     *
     */
    public void setMatchAny() {
        this.matchAll = false;
    }
    
    
    
}

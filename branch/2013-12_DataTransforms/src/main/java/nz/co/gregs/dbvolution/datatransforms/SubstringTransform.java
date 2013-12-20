/*
 * Copyright 2013 gregory.graham.
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

package nz.co.gregs.dbvolution.datatransforms;


public class SubstringTransform extends BaseTransform {
    public static final long serialVersionUID = 1L;
    
    private Integer startingPosition;
    private Integer length;

    public SubstringTransform(Integer startingIndex0Based, Integer endIndex0Based, DataTransform innerTransform) {
        super(innerTransform);
        this.startingPosition = startingIndex0Based;
        this.length = endIndex0Based;
    }

    /**
     * 
     * Uses the same indexes as String.substring(start, end)
     *
     * @param startingIndex0Based
     * @param endIndex0Based
     */
    public SubstringTransform(Integer startingIndex0Based, Integer endIndex0Based) {
        this(startingIndex0Based, endIndex0Based, new NullTransform());
    }

    public SubstringTransform() {
        super();
    }

    @Override
    protected String insertAfterValue() {
        return " FROM "+(startingPosition+1)+(length!=null?" for "+(length-startingPosition):"")+") ";
    }

    @Override
    protected String insertBeforeValue() {
        return " SUBSTRING(";
    }

    @Override
    public DataTransform copy() throws InstantiationException, IllegalAccessException {
        SubstringTransform copy = (SubstringTransform)super.copy();
        copy.startingPosition = this.startingPosition;
        copy.length = this.length;
        return copy;
    }
    
    
    
}

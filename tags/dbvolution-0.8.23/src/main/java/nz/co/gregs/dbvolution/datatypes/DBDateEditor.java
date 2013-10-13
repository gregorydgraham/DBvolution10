/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.datatypes;

import nz.co.gregs.dbvolution.datatypes.DBDate;
import java.beans.PropertyEditorSupport;

/**
 *
 * @author gregory.graham
 */
public class DBDateEditor  extends PropertyEditorSupport {

    private String format;

    /**
     *
     * @param format
     */
    public void setFormat(String format) {
        this.format = format;
    }
    
    /**
     *
     * @param text
     */
    @Override
    public void setAsText(String text) {
        DBDate type = new DBDate(text);
        setValue(type);
    }
}
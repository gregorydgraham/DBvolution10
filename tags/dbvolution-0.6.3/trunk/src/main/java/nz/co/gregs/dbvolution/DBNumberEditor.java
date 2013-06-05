/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

import java.beans.PropertyEditorSupport;

/**
 *
 * @author gregory.graham
 */
public class DBNumberEditor extends PropertyEditorSupport {

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
        DBNumber type = new DBNumber(text);
        setValue(type);
    }
}
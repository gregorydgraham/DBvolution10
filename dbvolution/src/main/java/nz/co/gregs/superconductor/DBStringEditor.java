/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.superconductor;

import java.beans.PropertyEditorSupport;

/**
 *
 * @author greg
 */
public class DBStringEditor extends PropertyEditorSupport {

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
        DBString type = new DBString(text);
        setValue(type);
    }
    
}

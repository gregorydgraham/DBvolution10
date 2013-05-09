/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.superconductor;

import java.beans.PropertyEditorSupport;

/**
 *
 * @author gregory.graham
 */
public class QueryableDatatypeEditor extends PropertyEditorSupport {

    private String format;

    public void setFormat(String format) {
        this.format = format;
    }
    
    @Override
    public void setAsText(String text) {
        if (format != null && format.equals("upperCase")) {
            text = text.toUpperCase();
        }
        QueryableDatatype type = new QueryableDatatype(text);
        setValue(type);
    }
}
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

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
        Object value = getValue();
        if (value instanceof DBString) {
            DBString qdt = (DBString) value;
            qdt.isLiterally(text);
        } else {
            DBString type = new DBString();
            type.isLiterally(text);
            setValue(type);
        }
    }
}

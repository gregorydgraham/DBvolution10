/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.datatypes;

import nz.co.gregs.dbvolution.datatypes.DBString;
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
        DBString type;
        Object value = getValue();
        if (value instanceof DBString) {
            type = (DBString) value;
        } else {
            type = new DBString();
        }
        if (text != null && !text.isEmpty()) {
            type.useEqualsOperator(text);
        }
        setValue(type);
    }
}

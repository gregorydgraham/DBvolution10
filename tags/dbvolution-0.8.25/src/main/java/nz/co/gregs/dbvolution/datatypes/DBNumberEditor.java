/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.datatypes;

import nz.co.gregs.dbvolution.datatypes.DBNumber;
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
        DBNumber type;
        if (text == null || text.isEmpty()) {
            type = new DBNumber();
        } else {
            type = new DBNumber();
            type.useEqualsOperator(text);
        }
        setValue(type);
    }
}
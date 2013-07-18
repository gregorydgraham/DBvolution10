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
public class DBIntegerEditor extends PropertyEditorSupport {

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
        DBInteger type;
        if (text == null || text.isEmpty()) {
            type = new DBInteger();
        } else {
            type = new DBInteger();
            type.isLiterally(text);
        }
        setValue(type);
    }
}
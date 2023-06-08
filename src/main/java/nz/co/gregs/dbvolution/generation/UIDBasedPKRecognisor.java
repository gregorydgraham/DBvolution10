/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.generation;

import nz.co.gregs.dbvolution.databases.metadata.PrimaryKeyRecognisor;

/**
 *
 * @author gregorygraham
 */
public class UIDBasedPKRecognisor extends PrimaryKeyRecognisor {

	public UIDBasedPKRecognisor() {
	}

	/**
	 * Returns TRUE if the column starts with "uid_".
	 *
	 * @param tableName tableName
	 * @param columnName columnName
	 * @return TRUE if the column looks like a primary key.
	 */
	@Override
	public boolean isPrimaryKeyColumn(String tableName, String columnName) {
		if (columnName != null && tableName != null) {
			return columnName.toLowerCase().equals("uid_" + tableName.toLowerCase());
		}
		return false;
	}

}

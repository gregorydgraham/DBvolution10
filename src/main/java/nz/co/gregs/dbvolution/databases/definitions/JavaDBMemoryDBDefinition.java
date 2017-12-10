/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.databases.definitions;

/**
 *
 * @author gregorygraham
 */
public class JavaDBMemoryDBDefinition extends JavaDBDefinition {

	public JavaDBMemoryDBDefinition() {
		super();
	}

	@Override
	public boolean persistentConnectionRequired() {
		return true;
	}

	@Override
	public boolean supportsFullOuterJoinNatively() {
		return false;
	}
}

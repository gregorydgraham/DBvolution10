/*
 * Copyright 2019 Gregory Graham.
 *
 * Commercial licenses are available, please contact info@gregs.co.nz for details.
 * 
 * This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. 
 * To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/4.0/ 
 * or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.
 * 
 * You are free to:
 *     Share - copy and redistribute the material in any medium or format
 *     Adapt - remix, transform, and build upon the material
 * 
 *     The licensor cannot revoke these freedoms as long as you follow the license terms.               
 *     Under the following terms:
 *                 
 *         Attribution - 
 *             You must give appropriate credit, provide a link to the license, and indicate if changes were made. 
 *             You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.
 *         NonCommercial - 
 *             You may not use the material for commercial purposes.
 *         ShareAlike - 
 *             If you remix, transform, or build upon the material, 
 *             you must distribute your contributions under the same license as the original.
 *         No additional restrictions - 
 *             You may not apply legal terms or technological measures that legally restrict others from doing anything the 
 *             license permits.
 * 
 * Check the Creative Commons website for any details, legalese, and updates.
 */
package nz.co.gregs.dbvolution.utility;

/**
 *
 * @author gregorygraham
 */
public class StringLeftPadded {

	private final String string;
	private String padding;
	private Integer length;

	private StringLeftPadded(String string) {
		this.string = string;
		this.length = 1;
		this.padding = " ";
	}
	
	public static StringLeftPadded pad(String str){
		StringLeftPadded lps = new StringLeftPadded(str);
		return lps;
	}
	
	public StringLeftPadded with(String padding){
		this.padding = padding;
		return this;
	}
	
	public StringLeftPadded toLength(Integer length){
		this.length = length;
		return this;
	}
	
	@Override
	public String toString(){
		return String.format("%1$" + length + "s", string).replace(" ", padding);
	}

}

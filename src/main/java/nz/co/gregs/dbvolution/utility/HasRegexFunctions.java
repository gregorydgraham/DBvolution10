/*
 * Copyright 2020 Gregory Graham.
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
public interface HasRegexFunctions {

	HasRegexFunctions add(Regex second);

	HasRegexFunctions addGroup(Regex second);

	HasRegexFunctions anyBetween(Character lowest, Character highest);

	HasRegexFunctions anyCharacter();

	HasRegexFunctions anyOf(String literals);

	HasRegexFunctions asterisk();

	HasRegexFunctions atLeastOnce();

	HasRegexFunctions atLeastThisManyTimes(int x);

	HasRegexFunctions atLeastXAndNoMoreThanYTimes(int x, int y);

	HasRegexFunctions backslash();

	HasRegexFunctions bell();

	HasRegexFunctions bracket();

	HasRegexFunctions capture(Regex regexp);

	HasRegexFunctions carat();

	HasRegexFunctions carriageReturn();

	HasRegexFunctions controlCharacter(String x);

	HasRegexFunctions digit();

	HasRegexFunctions digits();

	HasRegexFunctions dollarSign();

	HasRegexFunctions dot();

	HasRegexFunctions endOfTheString();

	HasRegexFunctions escapeCharacter();

	HasRegexFunctions extend(Regex second);

	HasRegexFunctions formfeed();

	HasRegexFunctions gapBetweenWords();

	HasRegexFunctions groupEverythingBeforeThis();

	HasRegexFunctions integer();

	HasRegexFunctions literal(String literals);

	HasRegexFunctions literal(Character character);

	HasRegexFunctions negativeInteger();

	HasRegexFunctions newline();

	HasRegexFunctions nonWhitespace();

	HasRegexFunctions nonWordBoundary();

	HasRegexFunctions nonWordCharacter();

	HasRegexFunctions nondigit();

	HasRegexFunctions nondigits();

	HasRegexFunctions noneOf(String literals);

	HasRegexFunctions notFollowedBy(String literalValue);

	HasRegexFunctions notFollowedBy(Regex literalValue);

	HasRegexFunctions notPrecededBy(String literalValue);

	HasRegexFunctions notPrecededBy(Regex literalValue);

	HasRegexFunctions nothingBetween(Character lowest, Character highest);

	HasRegexFunctions number();

	HasRegexFunctions numberLike();

	HasRegexFunctions once();

	HasRegexFunctions onceOrNotAtAll();

	HasRegexFunctions oneOrMore();

	HasRegexFunctions optionalMany();

	HasRegexFunctions pipe();

	HasRegexFunctions plus();

	HasRegexFunctions positiveInteger();

	HasRegexFunctions questionMark();

	HasRegexFunctions space();

	HasRegexFunctions squareBracket();

	HasRegexFunctions star();

	HasRegexFunctions tab();

	HasRegexFunctions theBeginningOfTheInput();

	HasRegexFunctions theEndOfTheInput();

	HasRegexFunctions theEndOfTheInputButForTheFinalTerminator();

	HasRegexFunctions theEndOfThePreviousMatch();

	HasRegexFunctions thisManyTimes(int x);

	HasRegexFunctions whitespace();

	HasRegexFunctions word();

	HasRegexFunctions wordBoundary();

	HasRegexFunctions wordCharacter();

	HasRegexFunctions zeroOrMore();
	
}

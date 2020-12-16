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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * @author gregorygraham
 */
public abstract class Regex implements HasRegexFunctions<Regex> {

	private Pattern compiledVersion;

	private Regex() {
	}

	public abstract String getRegex();

	/**
	 * Create a new empty regular expression.
	 *
	 * @return a new empty regular expression
	 */
	public static Regex startingAnywhere() {
		return new UnescapedSequence("");
	}

	/**
	 * Create a new regular expression that includes a test for the start of the
	 * string.
	 *
	 * @return a new regular expression
	 */
	public static Regex startingFromTheBeginning() {
		return new UnescapedSequence("^");
	}

	/**
	 * Adds the regular expression to the end of current expression as a new
	 * group.
	 *
	 * <p>
	 * For example Regex.startingAnywhere().add(allowedValue).add(separator) will
	 * add the "separator" regular expression to the "allowedValue" expression
	 * (the rest of the instruction adds nothing). Assuming that allowedValue is
	 * "[0-9]" and separator is ":", the full regexp will be "([0-9])(:)".
	 *
	 * @param second
	 * @return a new regular expression consisting of the current expression and
	 * the supplied expression added together
	 */
	@Override
	public Regex add(HasRegexFunctions<?> second) {
		return new RegexpCombination(this, second.groupEverythingBeforeThis());
	}

	/**
	 * Adds the regular expression to the end of current expression without
	 * grouping it.
	 *
	 * <p>
	 * Not grouping the added regular expression can produce counter-intuitive
	 * results and breaks encapsulation so use it carefully. In Particular
	 * extend(myRegex).onceOrNotAtAll() will only apply the "onceOrNotAtAll" to
	 * last element of myRegex and not the entire expression. Using
	 * digit.extend(Regex.startAnywhere().dot().digits()).onceOrNotAtAll() will
	 * match "0." and "0.5" but not "0". If you want grouping use add() instead.
	 *
	 * <p>
	 * For example Regex.startingAnywhere().extend(allowedValue).extend(separator)
	 * will add the "separator" regular expression to the "allowedValue"
	 * expression (the rest of the instruction adds nothing). Assuming that
	 * allowedValue is "[0-9]" and separator is ":", the full regexp will be
	 * "[0-9]:".
	 *
	 * @param second
	 * @return a new regular expression consisting of the current expression and
	 * the supplied expression added together
	 */
	@Override
	public Regex extend(HasRegexFunctions<?> second) {
		return new RegexpCombination(this, second);
	}

	@Override
	public Regex literal(Character character) {
		if (character.equals('/')) {
			return extend(backslash());
		} else {
			return extend(new SingleCharacter(character));
		}
	}

	/**
	 * Adds a literal string to the regexp without grouping it.
	 *
	 *
	 * @param literals
	 * @return a new regexp
	 */
	@Override
	public Regex literal(String literals) {
		return extend(new LiteralSequence(literals));
	}

	/**
	 * Adds a unescaped sequence to the regexp without grouping it.
	 *
	 *
	 * @param literals
	 * @return a new regexp
	 */
	@Override
	public Regex unescaped(String literals) {
		return extend(new UnescapedSequence(literals));
	}

	/**
	 * Adds a literal backslash(\) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex backslash() {
		return extend(new UnescapedSequence("\\\\"));
	}

	/**
	 * Adds a literal carat (^) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex carat() {
		return extend(new UnescapedSequence("\\^"));
	}

	/**
	 * Adds a literal dollar sign($) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex dollarSign() {
		return extend(new UnescapedSequence("\\$"));
	}

	/**
	 * Adds a literal dot(.) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex dot() {
		return extend(new UnescapedSequence("\\."));
	}

	/**
	 * Adds a literal question mark(?) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex questionMark() {
		return extend(new UnescapedSequence("\\?"));
	}

	/**
	 * Adds a literal plus sign(+) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex plus() {
		return extend(new UnescapedSequence("\\+"));
	}

	/**
	 * Adds a literal star(*) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex star() {
		return extend(new UnescapedSequence("\\*"));
	}

	/**
	 * Adds a literal asterisk(*) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex asterisk() {
		return extend(new UnescapedSequence("\\*"));
	}

	/**
	 * Adds a literal pipe(|) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex pipe() {
		return extend(new UnescapedSequence("\\|"));
	}

	/**
	 * Adds a literal square bracket([) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex squareBracket() {
		return extend(new UnescapedSequence("\\["));
	}

	/**
	 * Adds a literal bracket, i.e. "(", to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex bracket() {
		return extend(new UnescapedSequence("\\("));
	}

	/**
	 * Adds a tab character(\t) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex tab() {
		return extend(new UnescapedSequence("\\t"));
	}

	/**
	 * Adds a newline character(\n) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex newline() {
		return extend(new UnescapedSequence("\\n"));
	}

	/**
	 * Adds a carriage return character(\r) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex carriageReturn() {
		return extend(new UnescapedSequence("\\r"));
	}

	/**
	 * Adds a formfeed character(\f) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex formfeed() {
		return extend(new UnescapedSequence("\\f"));
	}

	/**
	 * Adds a bell character(\a) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex bell() {
		return extend(new UnescapedSequence("\\a"));
	}

	/**
	 * Adds a escape character(\e) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex escapeCharacter() {
		return extend(new UnescapedSequence("\\e"));
	}

	/**
	 * Adds a control character(\cX) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex controlCharacter(String x) {
		return extend(new UnescapedSequence("\\c" + x));
	}

	/**
	 * Adds a match for any single character to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex anyCharacter() {
		return extend(new UnescapedSequence("."));
	}

	/**
	 * Alters the previous element in the regexp so that it only matches if the
	 * element appears in that position exactly once.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex once() {
		return extend(new UnescapedSequence("{1}"));
	}

	/**
	 * Alters the previous element in the regexp so that it only matches if the
	 * element appears in that position exactly X number of times.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex thisManyTimes(int x) {
		return extend(new UnescapedSequence("{" + x + "}"));
	}

	/**
	 * Alters the previous element in the regexp so that it only matches if the
	 * element appears in that position X times or more.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex atLeastThisManyTimes(int x) {
		return extend(new UnescapedSequence("{" + x + ",}"));
	}

	/**
	 * Alters the previous element in the regexp so that it only matches if the
	 * element appears in that position X or more times but no more than Y times.
	 *
	 * <p>
	 * literal('a').atLeastXAndNoMoreThanYTimes(2,3) will match "aa" and "aaa" but
	 * not "aa" nor "aaaa".
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex atLeastXAndNoMoreThanYTimes(int x, int y) {
		return extend(new UnescapedSequence("{" + x + "," + y + "}"));
	}

	/**
	 * Alters the previous element in the regexp so that it only matches if the
	 * element appears in that position exactly once or not at all.
	 *
	 * <p>
	 * literal('a').literal('b)'.onceOrNotAtAll() will match "a" or "ab", but not
	 * "abb"
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex onceOrNotAtAll() {
		return extend(new UnescapedSequence("?"));
	}

	/**
	 * Alters the previous element in the regexp so that it only matches if the
	 * element appears in that position exactly once or not at all.
	 *
	 * <p>
	 * literal('a').literal('b)'.atLeastOnce() will match "ab" or "abb", but not
	 * "a"
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex atLeastOnce() {
		return extend(new UnescapedSequence("+"));
	}

	/**
	 * Alters the previous element in the regexp so that it only matches if the
	 * element appears in that position exactly once or not at all.
	 *
	 * <p>
	 * literal('a').literal('b)'.atLeastOnce() will match "ab" or "abb", but not
	 * "a"
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex oneOrMore() {
		return atLeastOnce();
	}

	/**
	 * Alters the previous element in the regexp so that it matches if the element
	 * appears in that position or not.
	 *
	 * <p>
	 * literal('a').literal('b)'.zeroOrMore().literal('c') will match "ac" or
	 * "abc".
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex zeroOrMore() {
		return extend(new UnescapedSequence("*"));
	}

	/**
	 * Alters the previous element in the regexp so that it matches if the element
	 * appears in that position or not.
	 *
	 * <p>
	 * literal('a').literal('b)'.zeroOrMore().literal('c') will match "ac" or
	 * "abc".
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex optionalMany() {
		return zeroOrMore();
	}

	/**
	 * Adds a check for the end of the string to the regular expression without
	 * grouping.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex endOfTheString() {
		return extend(new UnescapedSequence("$"));
	}

	/**
	 * Adds a check for a digit(0123456789) to the regular expression without
	 * grouping.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex digit() {
		return extend(new UnescapedSequence("\\d"));
	}

	/**
	 * Adds a check for one or more digits to the regular expression without
	 * grouping.
	 *
	 * <p>
	 * Please note that digits is not the same as a valid integer or number, use {@link #positiveInteger() }, {@link #negativeInteger() }, {@link #integer()
	 * }, or {@link #number()} instead.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex digits() {
		return digit().oneOrMore();
	}

	/**
	 * Adds a check for anything other than a digit(0123456789) to the regular
	 * expression without grouping.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex nondigit() {
		return extend(new UnescapedSequence("\\D"));
	}

	/**
	 * Adds a check for one or more of anything other than a digit(0123456789) to
	 * the regular expression without grouping.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex nondigits() {
		return nondigit().oneOrMore();
	}

	/**
	 * Adds a check for a word character(\w) to the regular expression without
	 * grouping.
	 *
	 * <p>
	 * A word character is any letter A-Z, upper or lowercase, any digit, or the
	 * underscore character.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex wordCharacter() {
		return extend(new UnescapedSequence("\\w"));
	}

	/**
	 * Adds a check for one or more word characters(\w+) to the regular expression
	 * without grouping.
	 *
	 * <p>
	 * A word character is any letter A-Z, upper or lowercase, any digit, or the
	 * underscore character.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex word() {
		return wordCharacter().oneOrMore();
	}

	/**
	 * Adds a check for one or more non-word characters(\w) to the regular
	 * expression without grouping.
	 *
	 * <p>
	 * A word character is any letter A-Z, upper or lowercase, any digit, or the
	 * underscore character. A non-word character is any other character.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex gapBetweenWords() {
		return nonWordCharacter().oneOrMore();
	}

	/**
	 * Adds a check for a non-word character(\w) to the regular expression without
	 * grouping.
	 *
	 * <p>
	 * A word character is any letter A-Z, upper or lowercase, any digit, or the
	 * underscore character. A non-word character is any other character.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex nonWordCharacter() {
		return extend(new UnescapedSequence("\\W"));
	}

	/**
	 * Adds a check for a whitespace character(\w) to the regular expression
	 * without grouping.
	 *
	 * <p>
	 * A whitespace character is [ \t\n\x0B\f\r], that is a space, tab, newline,
	 * char(11), formfeed, or carriage return.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex whitespace() {
		return extend(new UnescapedSequence("\\s"));
	}

	/**
	 * Adds a check for a non-whitespace character(\w) to the regular expression
	 * without grouping.
	 *
	 * <p>
	 * A whitespace character is [ \t\n\x0B\f\r], that is a space, tab, newline,
	 * char(11), form-feed, or carriage return. A non-whitespace character is
	 * anything else.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex nonWhitespace() {
		return extend(new UnescapedSequence("\\S"));
	}

	@Override
	public Regex wordBoundary() {
		return extend(new UnescapedSequence("\\b"));
	}

	@Override
	public Regex nonWordBoundary() {
		return extend(new UnescapedSequence("\\B"));
	}

	@Override
	public Regex theBeginningOfTheInput() {
		return extend(new UnescapedSequence("\\A"));
	}

	@Override
	public Regex theEndOfThePreviousMatch() {
		return extend(new UnescapedSequence("\\G"));
	}

	@Override
	public Regex theEndOfTheInput() {
		return extend(new UnescapedSequence("\\z"));
	}

	@Override
	public Regex theEndOfTheInputButForTheFinalTerminator() {
		return extend(new UnescapedSequence("\\Z"));
	}

	/**
	 * Adds a check for a space character( ) to the regular expression without
	 * grouping.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex space() {
		return extend(new UnescapedSequence(" "));
	}

	/**
	 * Places the regular expression in a capturing group.
	 *
	 * <p>
	 * capturing and grouping are the same, there are methods of both names to
	 * capture the intent.
	 *
	 * @param regexp
	 * @return a new regexp
	 */
	@Override
	public Regex capture(Regex regexp) {
		return new UnescapedSequence("(" + regexp.getRegex() + ")");
	}

	/**
	 * Adds a check for a negative integer to the regular expression without
	 * grouping.
	 *
	 * <p>
	 * Will capture the minus so watch out for that in your calculator
	 * application.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex negativeInteger() {
		return extend(literal('-').anyBetween('1', '9').once().digit().zeroOrMore());
	}

	/**
	 * Adds a check for a positive integer to the regular expression without
	 * grouping.
	 *
	 * <p>
	 * Will capture the plus so watch out for that in your calculator application.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex positiveInteger() {
		return extend(
				startingAnywhere()
						.notPrecededBy("-")
						.plus().onceOrNotAtAll()
						.anyBetween('1', '9').once()
						.digit().zeroOrMore()
		);
	}

	/**
	 * Adds a check for a simple range to the regular expression without grouping.
	 *
	 * <p>
	 * To add more complex ranges use .add(new Regex.Range(lowest, highest)).
	 *
	 * @param lowest the (inclusive) start of the character range
	 * @param highest the (inclusive) end of the character range
	 * @return a new regexp
	 */
	@Override
	public Regex anyBetween(Character lowest, Character highest) {
		return extend(startingAnywhere().openRange(lowest, highest).closeRange());
	}

	/**
	 * Adds a check for a simple range to the regular expression without grouping.
	 *
	 * <p>
	 * To add more complex ranges use .add(new Regex.Range(rangeItems)).
	 *
	 * @param literals all the characters to be included in the range, for example
	 * "abcdeABCDE"
	 * @return a new regexp
	 */
	@Override
	public Regex anyOf(String literals) {
		return extend(startingAnywhere().openRange(literals).closeRange());
	}

	/**
	 * Adds a check to exclude a simple range from the regular expression without
	 * grouping.
	 *
	 * <p>
	 * To add more complex ranges use .add(new Regex.Range(lowest, highest)).
	 *
	 * @param lowest the (inclusive) start of the character range
	 * @param highest the (inclusive) end of the character range
	 * @return a new regexp
	 */
	@Override
	public Regex nothingBetween(Character lowest, Character highest) {
		return extend(startingAnywhere().openRange(lowest, highest).negated().closeRange());
	}

	/**
	 * Adds a check to exclude a simple range from the regular expression without
	 * grouping.
	 *
	 * <p>
	 * To add more complex ranges use .add(new Regex.Range(rangeItems)).
	 *
	 * @param literals all the characters to be included in the range, for example
	 * "abcdeABCDE"
	 * @return a new regexp
	 */
	@Override
	public Regex noneOf(String literals) {
		return extend(startingAnywhere().openRange(literals).negated().closeRange());
	}

	/**
	 * Places the regular expression in a group and adds it as one element for the
	 * next instruction.
	 *
	 * <p>
	 * capturing and grouping are the same, there are methods of both names to
	 * capture the intent.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex groupEverythingBeforeThis() {
		return new Group(this);
	}

	/**
	 * Places the regular expression in a group and add it as one element for the
	 * next instruction.
	 *
	 * <p>
	 * capturing and grouping are the same, there are methods of both names to
	 * capture the intent.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex addGroup(HasRegexFunctions<?> regex) {
		return this.extend(regex.groupEverythingBeforeThis());
	}

	protected final Pattern getCompiledVersion() {
		return getPattern();
	}

	protected final Pattern getPattern() {
		if (compiledVersion == null) {
			final String regexp = this.getRegex();
			compiledVersion = Pattern.compile(regexp);
		}
		return compiledVersion;
	}

	public boolean matchesEntireString(String string) {
		return getMatcher(string).matches();
	}

	public boolean matchesWithinString(String string) {
		return getMatcher(string).find();
	}

	@Override
	public RegexGroup.NamedCapture<Regex> namedCapture(String name) {
		return new RegexGroup.NamedCapture<>(this, name);
	}

	public Stream<MatchResult> getMatchResultsStream(String string) {
		return getMatcher(string).results();
	}

	protected Matcher getMatcher(String string) {
		return getCompiledVersion().matcher(string);
	}

	/**
	 *
	 * Convenient access to Matcher.group().
	 *
	 * Returns the input subsequence matched by the previous match.
	 *
	 * <p>
	 * For a matcher <i>m</i> with input sequence <i>s</i>, the expressions
	 * <i>m.</i>{@code group()} and
	 * <i>s.</i>{@code substring(}<i>m.</i>{@code start(),}&nbsp;<i>m.</i>{@code end())}
	 * are equivalent.  </p>
	 *
	 * <p>
	 * Note that some patterns, for example {@code a*}, match the empty string.
	 * This method will return the empty string when the pattern successfully
	 * matches the empty string in the input.  </p>
	 *
	 * @return The (possibly empty) subsequence matched by the previous match, in
	 * string form
	 *
	 * @throws IllegalStateException If no match has yet been attempted, or if the
	 * previous match operation failed
	 */
	public String getFirstMatch(String string) {
		return getMatcher(string).group();
	}

	/**
	 * Convenient access to Matcher.toMatchResult.
	 * <p>
	 * Returns the match state of this matcher as a {@link MatchResult}. The
	 * result is unaffected by subsequent operations performed upon this matcher.
	 *
	 * @return a {@code MatchResult} with the state of this matcher
	 * @since 1.5
	 */
	public MatchResult getMatchResult(String string) {
		return getMatcher(string).toMatchResult();
	}

	public Optional<String> getNamedMatch(String string, String name) {
		final String found = getMatcher(string).group(name);
		if (found == null) {
			return Optional.empty();
		} else {
			return Optional.of(found);
		}
	}

	public HashMap<String, String> getAllNamedGroups(String string) {
		HashMap<String, String> resultMap = new HashMap<String, String>(0);
		try {
			Matcher matcher = getMatcher(string);
			if (matcher.find()) {
				Class<? extends Pattern> patternClass = getPattern().getClass();
				Method method = patternClass.getDeclaredMethod("namedGroups");
				method.setAccessible(true);
				Object invoke = method.invoke(getPattern());
				@SuppressWarnings("unchecked")
				Map<String, Integer> map = (Map<String, Integer>) invoke;
				if (map.size() > 0) {
					for (String name : map.keySet()) {
						final String group = matcher.group(name);
						if (group != null) {
							resultMap.put(name, group);
						}
					}
				}
			}
		} catch (NoSuchMethodException ex) {
			Logger.getLogger(Regex.class.getName()).log(Level.SEVERE, null, ex);
		} catch (SecurityException ex) {
			Logger.getLogger(Regex.class.getName()).log(Level.SEVERE, null, ex);
		} catch (IllegalAccessException ex) {
			Logger.getLogger(Regex.class.getName()).log(Level.SEVERE, null, ex);
		} catch (IllegalArgumentException ex) {
			Logger.getLogger(Regex.class.getName()).log(Level.SEVERE, null, ex);
		} catch (InvocationTargetException ex) {
			Logger.getLogger(Regex.class.getName()).log(Level.SEVERE, null, ex);
		}
		return resultMap;
	}

	/**
	 * Adds a check for a that the next element does not have the literal value
	 * before it.
	 *
	 * <p>
	 * For instance a positive integer is an integer that may have a plus in front
	 * of it but definitely isn't preceded by a minus. So it uses a notPrecededBy:
	 * startingAnywhere().notPrecededBy("-").plus().onceOrNotAtAll()...
	 *
	 * @param literalValue
	 * @return a new regexp
	 */
	@Override
	public Regex notPrecededBy(String literalValue) {
		return this.notPrecededBy(new LiteralSequence(literalValue));
	}

	/**
	 * Adds a check for a that the next element does not have the literal value
	 * before it.
	 *
	 * <p>
	 * For instance a positive integer is an integer that may have a plus in front
	 * of it but definitely isn't preceded by a minus. So it uses a notPrecededBy:
	 * startingAnywhere().notPrecededBy("-").plus().onceOrNotAtAll()...
	 *
	 * @param literalValue
	 * @return a new regexp
	 */
	@Override
	public Regex notPrecededBy(Regex literalValue) {
		return this
				.extend(new UnescapedSequence("(?<!"))
				.extend(literalValue)
				.extend(new UnescapedSequence(")"));
	}

	/**
	 * Adds a check for a that the next element does not have the literal value
	 * immediately after it.
	 *
	 * <p>
	 * For instance to match words but not e-mail addresses you might use
	 * Regex.startingAnywhere().word().notFollowedBy("@").
	 *
	 * @param literalValue
	 * @return a new regexp
	 */
	@Override
	public Regex notFollowedBy(String literalValue) {
		return this.notFollowedBy(new LiteralSequence(literalValue));
	}

	/**
	 * Adds a check for a that the next element does not have the literal value
	 * immediately after it.
	 *
	 * <p>
	 * For instance to match words but not e-mail addresses you might use
	 * Regex.startingAnywhere().word().notFollowedBy("@").
	 *
	 * @param literalValue
	 * @return a new regexp
	 */
	@Override
	public Regex notFollowedBy(Regex literalValue) {
		return this
				.extend(new UnescapedSequence("(?!"))
				.extend(literalValue)
				.extend(new UnescapedSequence(")"));
	}

	/**
	 * Starts making a character range, use {@link RangeBuilder#closeRange() } to
	 * return to the regex.
	 *
	 * <p>
	 * This provides more options than the {@link #anyBetween(java.lang.Character, java.lang.Character)
	 * } and {@link #anyOf(java.lang.String) } methods for creating ranges.
	 *
	 * @param lowest
	 * @param highest
	 * @return the start of a range.
	 */
	@Override
	public RangeBuilder<Regex> openRange(char lowest, char highest) {
		return new RangeBuilder<>(this, lowest, highest);
	}

	@Override
	public RangeBuilder<Regex> openRange(String literals) {
		return new RangeBuilder<>(this, literals);
	}

	/**
	 * Create a regular expression that includes all the regexps supplied within
	 * an OR grouping.
	 *
	 * <p>
	 * for instance, use this to generate "(FRED|EMILY|GRETA|DONALD)".
	 *
	 * <p>
	 * {@code Regex regex =  Regex.startGroup().literal("A").or().literal("B").closeGroup();
	 * } produces "(A|B)".
	 *
	 * @return a new regular expression
	 */
	public static RegexGroup.Or<Regex> startGroup() {
		return new RegexGroup.Or<>(startingAnywhere());
	}

	public List<String> getAllMatches(String string) {
		Matcher matcher = getMatcher(string);
		List<String> matches = matcher.results().map(m -> m.group()).collect(Collectors.toList());
		return matches;
	}

	public List<String> getAllGroups(String string) {
		Matcher matcher = getMatcher(string);
		List<String> groups = new ArrayList<>(0);
		while (matcher.find()) {
			int count = matcher.groupCount();
			for (int i = 0; i < count; i++) {
				final String foundGroup = matcher.group(i);
				if (foundGroup != null) {
					groups.add(foundGroup);
				}
			}
		}
		return groups;
	}

	@Override
	public Regex literalCaseInsensitive(String literal) {
		return this
				.addGroup(Regex.startingAnywhere()
						.caseInsensitiveGroup()
						.literal(literal)
						.caseInsensitiveEnd()
				);
	}

	public RegexGroup.CaseInsensitive<Regex> caseInsensitiveGroup() {
		return new RegexGroup.CaseInsensitive<>(this);
	}

	public static class SingleCharacter extends Regex {

		private final Character literal;

		protected SingleCharacter(Character character) {
			this.literal = character;
		}

		@Override
		public String getRegex() {
			return "" + literal;
		}
	}

	public static class LiteralSequence extends Regex {

		private final String literal;

		public LiteralSequence(String literals) {
			if (literals == null) {
				this.literal = "";
			} else {
				this.literal = literals
						.replaceAll("\\\\", "\\")
						.replaceAll("\\.", "\\.")
						.replaceAll("\\?", "\\?")
						.replaceAll("\\+", "\\\\+")
						.replaceAll("\\*", "\\*")
						.replaceAll("\\^", "\\^")
						.replaceAll("\\$", "\\$")
						.replaceAll("\\|", "\\|")
						.replaceAll("\\[", "\\[");
			}
		}

		@Override
		public String getRegex() {
			return "" + literal;
		}
	}

	protected static class UnescapedSequence extends Regex {

		private final String literal;

		protected UnescapedSequence(String literals) {
			if (literals == null) {
				this.literal = "";
			} else {
				this.literal = literals;
			}
		}

		@Override
		public String getRegex() {
			return "" + literal;
		}
	}

	protected static class RegexpCombination extends Regex {

		private final HasRegexFunctions<?> first;
		private final HasRegexFunctions<?> second;

		protected RegexpCombination(HasRegexFunctions<?> first, HasRegexFunctions<?> second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public String getRegex() {
			return first.getRegex() + second.getRegex();
		}
	}

	@Deprecated
	public static class Group extends Regex {

		private final Regex regexp;

		public Group(Regex regexp) {
			this.regexp = regexp;
		}

		@Override
		public String getRegex() {
			return "(" + regexp.getRegex() + ")";
		}
	}

	public static class RangeBuilder<REGEX extends HasRegexFunctions<REGEX>> {

		private final REGEX origin;
		private String literals;

		private boolean negated = false;
		private boolean includeMinus = false;
		private boolean includeOpenBracket = false;
		private boolean includeCloseBracket = false;

		public RangeBuilder(REGEX original) {
			this.origin = original;
		}

		public RangeBuilder(REGEX original, Character lowest, Character highest) {
			this(original);
			addRange(lowest, highest);
		}

		public RangeBuilder(REGEX original, String literals) {
			this(original);
			addLiterals(literals);
		}

		protected final RangeBuilder<REGEX> addRange(Character lowest, Character highest) {
			this.literals = lowest + "-" + highest;
			return this;
		}

		protected final RangeBuilder<REGEX> addLiterals(String literals1) {
			this.literals = literals1.replaceAll("-", "").replaceAll("]", "");
			this.includeMinus = literals1.contains("-");
			this.includeOpenBracket = literals1.contains("[");
			this.includeCloseBracket = literals1.contains("]");
			return this;
		}

		public RangeBuilder<REGEX> not() {
			this.negated = true;
			return this;
		}

		public RangeBuilder<REGEX> negated() {
			return not();
		}

		public RangeBuilder<REGEX> includeMinus() {
			includeMinus = true;
			return this;
		}

		public RangeBuilder<REGEX> and(Character lowest, Character highest) {
			return addRange(lowest, highest);
		}

		public RangeBuilder<REGEX> and(String literals) {
			return addLiterals(literals);
		}

		public RangeBuilder<REGEX> excluding(Character lowest, Character highest) {
			excluding(new RangeBuilder<>(Regex.startingAnywhere(), lowest, highest));
			return this;
		}

		public RangeBuilder<REGEX> excluding(String literals) {
			excluding(new RangeBuilder<>(Regex.startingAnywhere(), literals));
			return this;
		}

		public RangeBuilder<REGEX> excluding(RangeBuilder<?> newRange) {
			this.literals = this.literals + "-" + newRange.encloseInBrackets();
			return this;
		}

		public String encloseInBrackets() {
			return "["
					+ (negated ? "^" : "")
					+ (includeMinus ? "-" : "")
					+ (includeOpenBracket ? "\\[" : "")
					+ (includeCloseBracket ? "\\]" : "")
					+ literals
					+ "]";
		}

		public REGEX closeRange() {
			return origin.extend(new UnescapedSequence(encloseInBrackets()));
		}
	}

}

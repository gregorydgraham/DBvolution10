package nz.co.gregs.dbvolution.internal;

import java.util.Collection;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

class PropertyMatchers {
	
	public static <E> E itemOf(Collection<E> c, Matcher<? super E> matcher) {
		for (E item: c) {
			if (matcher.matches(item)) {
				return item;
			}
		}
		return null; // not found
	}
	
	/**
	 * Decorates another Matcher, retaining the behaviour but allowing tests
	 * to be slightly more expressive.
	 * <p>
	 * For example:  itemOf(collection, hasName(smelly))
	 *          vs.  itemOf(collection, that(hasName(smelly)))
	 */
	public static <T> Matcher<T> that(final Matcher<T> matcher) {
		return new BaseMatcher<T>() {

			@Override
			public boolean matches(Object item) {
				return matcher.matches(item);
			}

			@Override
			public void describeTo(Description description) {
				description.appendText("that ").appendDescriptionOf(matcher);
			}
			
		    @Override
		    public void describeMismatch(Object item, Description mismatchDescription) {
		        matcher.describeMismatch(item, mismatchDescription);
		    }
		};
	}
	
	public static Matcher<JavaProperty> hasJavaPropertyName(final String name) {
		return new TypeSafeDiagnosingMatcher<JavaProperty>() {
			@Override
			public void describeTo(Description description) {
				description.appendText("has name ").appendValue(name);
			}

			@Override
			protected boolean matchesSafely(JavaProperty item, Description mismatchDescription) {
				if (!name.equals(item.name())) {
					mismatchDescription.appendText("has name ").appendValue(item.name());
					return false;
				}
				return true;
			}
		};
	}

	public static Matcher<JavaProperty> isJavaPropertyField() {
		return new TypeSafeDiagnosingMatcher<JavaProperty>() {
			@Override
			public void describeTo(Description description) {
				description.appendText("is field ");
			}

			@Override
			protected boolean matchesSafely(JavaProperty item, Description mismatchDescription) {
				if (item.isField()) {
					mismatchDescription.appendText("is bean-property");
					return false;
				}
				return true;
			}
		};
	}
}

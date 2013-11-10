package nz.co.gregs.dbvolution.internal;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import nz.co.gregs.dbvolution.datatypes.DBTypeAdaptor;
import nz.co.gregs.dbvolution.internal.SafeOneWayTypeAdaptor.Direction;

import org.junit.Test;

public class SafeOneWayTypeAdaptorTest {
	@Test
	public void sourceTypeCorrectGivenIntegerToStringAdaptorToExternal() {
		SafeOneWayTypeAdaptor oneWay = new SafeOneWayTypeAdaptor("myField",
				new IntegerToStringAdaptor(), Direction.TO_EXTERNAL, null, null);
		assertThat(oneWay.getSourceType(), is((Object)String.class));
	}

	@Test
	public void targetTypeCorrectGivenIntegerToStringAdaptorToExternal() {
		SafeOneWayTypeAdaptor oneWay = new SafeOneWayTypeAdaptor("myField",
				new IntegerToStringAdaptor(), Direction.TO_EXTERNAL, null, null);
		assertThat(oneWay.getTargetType(), is((Object)Integer.class));
	}

	@Test
	public void sourceTypeCorrectGivenIntegerToStringAdaptorToInternal() {
		SafeOneWayTypeAdaptor oneWay = new SafeOneWayTypeAdaptor("myField",
				new IntegerToStringAdaptor(), Direction.TO_INTERNAL, null, null);
		assertThat(oneWay.getSourceType(), is((Object)Integer.class));
	}

	@Test
	public void targetTypeCorrectGivenIntegerToStringAdaptorToInternal() {
		SafeOneWayTypeAdaptor oneWay = new SafeOneWayTypeAdaptor("myField",
				new IntegerToStringAdaptor(), Direction.TO_INTERNAL, null, null);
		assertThat(oneWay.getTargetType(), is((Object)String.class));
	}

	@Test
	public void targetTypeCorrectGivenIntegerToStringAdaptorToExternalAndExplicitLongTarget() {
		SafeOneWayTypeAdaptor oneWay = new SafeOneWayTypeAdaptor("myField",
				new IntegerToStringAdaptor(), Direction.TO_EXTERNAL, null, Long.class);
		assertThat(oneWay.getTargetType(), is((Object)Long.class));
	}

	@Test
	public void targetTypeCorrectGivenIntegerToStringAdaptorToExternalAndExplicitShortTarget() {
		SafeOneWayTypeAdaptor oneWay = new SafeOneWayTypeAdaptor("myField",
				new IntegerToStringAdaptor(), Direction.TO_EXTERNAL, null, Short.class);
		assertThat(oneWay.getTargetType(), is((Object)Short.class));
	}
	
	@Test
	public void sourceTypeCorrectGivenIntegerToStringAdaptorToInternalAndExplicitLongSource() {
		SafeOneWayTypeAdaptor oneWay = new SafeOneWayTypeAdaptor("myField",
				new IntegerToStringAdaptor(), Direction.TO_INTERNAL, Long.class, null);
		assertThat(oneWay.getSourceType(), is((Object)Long.class));
	}

	@Test
	public void sourceTypeCorrectGivenIntegerToStringAdaptorToInternalAndExplicitShortSource() {
		SafeOneWayTypeAdaptor oneWay = new SafeOneWayTypeAdaptor("myField",
				new IntegerToStringAdaptor(), Direction.TO_INTERNAL, Short.class, null);
		assertThat(oneWay.getSourceType(), is((Object)Short.class));
	}
	
	@Test
	public void convertsGivenIntegerTypeAdaptorToExternal() {
		SafeOneWayTypeAdaptor oneWay = new SafeOneWayTypeAdaptor("myField",
				new IntegerToStringAdaptor(), Direction.TO_EXTERNAL, null, null);
		Object result = oneWay.convert("23");
		assertThat(result, is(instanceOf(Integer.class)));
		assertThat((Integer)result, is(23));
	}

	@Test
	public void convertsGivenIntegerTypeAdaptorToInternal() {
		SafeOneWayTypeAdaptor oneWay = new SafeOneWayTypeAdaptor("myField",
				new IntegerToStringAdaptor(), Direction.TO_INTERNAL, null, null);
		Integer value = 23;
		Object result = oneWay.convert(value);
		assertThat(result, is(instanceOf(String.class)));
		assertThat((String)result, is("23"));
	}
	
	@Test
	public void castsIntegerToLongGivenIntegerTypeAdaptorToExternalAndExplicitLongTarget() {
		SafeOneWayTypeAdaptor oneWay = new SafeOneWayTypeAdaptor("myField",
				new IntegerToStringAdaptor(), Direction.TO_EXTERNAL, null, Long.class);
		Object result = oneWay.convert("23");
		assertThat(result, is(instanceOf(Long.class)));
		assertThat((Long)result, is(23L));
	}

	@Test
	public void castsLongToIntegerGivenIntegerTypeAdaptorToInternalAndExplicitLongSource() {
		SafeOneWayTypeAdaptor oneWay = new SafeOneWayTypeAdaptor("myField",
				new IntegerToStringAdaptor(), Direction.TO_INTERNAL, Long.class, null);
		Long value = 23L;
		Object result = oneWay.convert(value);
		assertThat(result, is(instanceOf(String.class)));
		assertThat((String)result, is("23"));
	}
	
	static class IntegerToStringAdaptor implements DBTypeAdaptor<Integer, String> {
		@Override
		public Integer fromDatabaseValue(String dbvValue) {
			return (dbvValue == null) ? null : Integer.parseInt(dbvValue);
		}

		@Override
		public String toDatabaseValue(Integer objectValue) {
			return (objectValue == null) ? null : objectValue.toString();
		}
	}
}

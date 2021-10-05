package nl.fews.archivedatabase.mongodb;

import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.*;
import nl.wldelft.fews.system.data.timeseries.FewsTimeSeriesHeader;
import nl.wldelft.util.LogUtils;
import nl.wldelft.util.TimeSpan;
import nl.wldelft.util.TimeUnit;
import nl.wldelft.util.timeseries.*;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.LongStream;

public class TestUtil {

	static {
		LogUtils.initConsole();
	}

	@SuppressWarnings({"unchecked"})
	public static TimeSeriesArrays<TimeSeriesHeader> getDefaultTimeSeriesArrays() {
		List<TimeSeriesArray<TimeSeriesHeader>> arrays = new ArrayList<>();
		for (int y = 2012; y < 2014; y++) {
			for (int h = 0; h < 10; h++) {
				DefaultTimeSeriesHeader timeSeriesHeader = new DefaultTimeSeriesHeader();
				timeSeriesHeader.setApprovedTime(Instant.parse(y + "-01-01T00:00:00Z").toEpochMilli());
				timeSeriesHeader.setCreationTime(Instant.parse(y + "-01-01T00:00:00Z").toEpochMilli());
				timeSeriesHeader.setEnsembleId("ensembleId" + h);
				timeSeriesHeader.setEnsembleMemberId("ensembleMemberId" + h);
				timeSeriesHeader.setEnsembleMemberIndex(1);
				timeSeriesHeader.setForecastTime(Instant.parse(y + "-01-01T00:00:00Z").toEpochMilli());
				timeSeriesHeader.setLocationId("locationId" + h);
				timeSeriesHeader.setLocationName("locationName" + h);
				timeSeriesHeader.setModuleInstanceId("moduleInstanceId" + h);
				timeSeriesHeader.setParameterId("parameterId" + h);
				timeSeriesHeader.setParameterName("parameterName" + h);
				timeSeriesHeader.setParameterType(ParameterType.INSTANTANEOUS);
				timeSeriesHeader.setQualifierIds("qualifierId" + h, "qualifierId" + h);
				timeSeriesHeader.setTimeStep(TimeStepUtils.createEquidistant(new TimeSpan(TimeUnit.HOUR, 6), TimeZone.getTimeZone("UTC"), "timeStepLabel"));
				timeSeriesHeader.setUnit("unit");

				TimeSeriesArray<TimeSeriesHeader> timeSeriesArray = new TimeSeriesArray<>(timeSeriesHeader);
				timeSeriesArray.setForecastTime(timeSeriesHeader.getForecastTime());
				String year = y + "-01-01T00:00:00Z";
				timeSeriesArray.ensureTimes(LongStream.range(0, 10).map(x -> Instant.parse(year).plus(x*6, ChronoUnit.HOURS).toEpochMilli()).toArray());
				for (int i = 0; i < 10; i++) {
					timeSeriesArray.setValue(i,  i);
					timeSeriesArray.setFlag(i, TimeSeriesArray.FLAG_SOURCE_NONE);
					timeSeriesArray.setComment(i, "comment" + i);
					timeSeriesArray.setUser(i, "user" + i);
				}
				arrays.add(timeSeriesArray);
			}
		}
		return new TimeSeriesArrays<>(arrays.toArray(TimeSeriesArray[]::new));
	}

	/**
	 * ArchiveDatabaseUnitConverterTestImplementation
	 */
	public static class ArchiveDatabaseUnitConverterTestImplementation implements ArchiveDatabaseUnitConverter {

		@Override
		public float[] convert(String parameterId, float[] floats) { return floats; }

		@Override
		public String getOutputUnitType(String parameterId) { return "unit"; }
	}

	/**
	 * ArchiveDatabaseTimeConverterTestImplementation
	 */
	public static class ArchiveDatabaseTimeConverterTestImplementation implements ArchiveDatabaseTimeConverter {

		@Override
		public long[] convert(long[] longs) { return longs; }

		@Override
		public TimeZone getLocalTimeZone() { return TimeZone.getTimeZone("UTC"); }
	}

	/**
	 * ArchiveDatabaseRegionConfigInfoProviderTestImplementation
	 */
	public static class ArchiveDatabaseRegionConfigInfoProviderTestImplementation implements ArchiveDatabaseRegionConfigInfoProvider {

		@Override
		public LocationInfo getLocationInfo(String locationId) {
			return new LocationInfo(locationId, locationId, locationId);
		}

		@Override
		public ParameterInfo getParameterInfo(String parameterId) {
			return new ParameterInfo(parameterId, parameterId, parameterId, "unit");
		}
	}

	/**
	 * HeaderProviderTestImplementation
	 */
	public static class HeaderProviderTestImplementation implements FewsTimeSeriesHeaderProvider{

		@Override
		public TimeSeriesHeader getHeader(HeaderRequest headerRequest) {
			DefaultTimeSeriesHeader defaultTimeSeriesHeader = new DefaultTimeSeriesHeader();
			defaultTimeSeriesHeader.setLocationId(headerRequest.getLocationId());
			defaultTimeSeriesHeader.setParameterId(headerRequest.getParameterId());
			defaultTimeSeriesHeader.setModuleInstanceId(headerRequest.getModuleInstanceId());
			defaultTimeSeriesHeader.setTimeStep(headerRequest.getTimeStep());
			defaultTimeSeriesHeader.setQualifierIds(headerRequest.getQualifiersIds());
			return defaultTimeSeriesHeader;
		}
	}
}

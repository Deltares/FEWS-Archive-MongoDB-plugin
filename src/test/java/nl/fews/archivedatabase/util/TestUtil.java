package nl.fews.archivedatabase.util;

import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseTimeConverter;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseUnitConverter;
import nl.wldelft.util.LogUtils;
import nl.wldelft.util.TimeSpan;
import nl.wldelft.util.TimeUnit;
import nl.wldelft.util.timeseries.*;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.LongStream;

public class TestUtil {

    static {
        LogUtils.initConsole();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static TimeSeriesArrays getDefaultTimeSeriesArrays() {
        List<TimeSeriesArray<TimeSeriesHeader>> arrays = new ArrayList<>();
        for (int y = 2012; y < 2014; y++) {
            for (int h = 0; h < 10; h++) {
                DefaultTimeSeriesHeader defaultTimeSeriesHeader = new DefaultTimeSeriesHeader();
                defaultTimeSeriesHeader.setApprovedTime(Instant.parse(y + "-01-01T00:00:00Z").toEpochMilli());
                defaultTimeSeriesHeader.setCreationTime(Instant.parse(y + "-01-01T00:00:00Z").toEpochMilli());
                defaultTimeSeriesHeader.setEnsembleId("ensembleId" + h);
                defaultTimeSeriesHeader.setEnsembleMemberId("ensembleMemberId" + h);
                defaultTimeSeriesHeader.setEnsembleMemberIndex(1);
                defaultTimeSeriesHeader.setForecastTime(Instant.parse(y + "-01-01T00:00:00Z").toEpochMilli());
                defaultTimeSeriesHeader.setLocationId("locationId" + h);
                defaultTimeSeriesHeader.setLocationName("locationName" + h);
                defaultTimeSeriesHeader.setModuleInstanceId("moduleInstanceId" + h);
                defaultTimeSeriesHeader.setParameterId("parameterId" + h);
                defaultTimeSeriesHeader.setParameterName("parameterName" + h);
                defaultTimeSeriesHeader.setParameterType(ParameterType.INSTANTANEOUS);
                defaultTimeSeriesHeader.setQualifierIds("qualifierId" + h, "qualifierId" + h);
                defaultTimeSeriesHeader.setTimeStep(TimeStepUtils.createEquidistant(new TimeSpan(TimeUnit.HOUR, 6), TimeZone.getTimeZone("UTC"), "timeStepLabel"));
                defaultTimeSeriesHeader.setUnit("unit");
                TimeSeriesArray<TimeSeriesHeader> timeSeriesArray = new TimeSeriesArray<>(defaultTimeSeriesHeader);
                timeSeriesArray.setForecastTime(defaultTimeSeriesHeader.getForecastTime());
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
}

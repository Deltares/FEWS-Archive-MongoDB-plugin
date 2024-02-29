package nl.fews.archivedatabase.mongodb.query;

import nl.fews.archivedatabase.mongodb.MongoDbArchiveDatabase;
import nl.fews.archivedatabase.mongodb.TestUtil;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseReadResult;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseResultSearchParameters;
import nl.wldelft.fews.system.data.timeseries.TimeSeriesType;
import nl.wldelft.util.LogUtils;
import nl.wldelft.util.Period;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MongoDbArchiveDatabaseTimeSeriesReaderTest2 {

    private static JSONObject testSettings = null;

    static{
        LogUtils.initConsole();
    }

    @BeforeAll
    static void setUpClass() throws IOException {
        Path path = Paths.get("src", "test", "resources", "TestSettings.json").toAbsolutePath();
        if (path.toFile().exists()) {
            testSettings = new JSONObject(Files.readString(path));
        }
    }

    @Test
    void testImportSingleDataImportRequest() throws Exception {
//		MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
//		mongoDbArchiveDatabase.setArchiveDatabaseUrl("mongodb://chadwmongodb1.main.tva.gov/FEWS_ARCHIVE?authSource=admin&tls=true");
//
//		MongoDbArchiveDatabaseTimeSeriesReader mongoDbArchiveDatabaseTimeSeriesReader = (MongoDbArchiveDatabaseTimeSeriesReader)mongoDbArchiveDatabase.getArchiveDataBaseTimeSeriesReader();
//		mongoDbArchiveDatabaseTimeSeriesReader.setHeaderProvider(new TestUtil.HeaderProviderTestImplementation());
//
//		DefaultTimeSeriesHeader timeSeriesHeader = new DefaultTimeSeriesHeader();
//		timeSeriesHeader.setModuleInstanceId("Preprocess_HP");
//		timeSeriesHeader.setLocationId("HADT1");
//		timeSeriesHeader.setParameterId("HP");
//		timeSeriesHeader.setTimeStep(TimeStepUtils.decode("SETS60"));
//		TimeSeriesArray<TimeSeriesHeader> timeSeriesArray = new TimeSeriesArray<>(timeSeriesHeader);
//		TimeSeriesArrays<TimeSeriesHeader> timeSeriesArrays = new TimeSeriesArrays<>(timeSeriesArray);
//		Period period = new Period(new SimpleDateFormat("yyyy-MM-dd").parse("1994-07-17").getTime(), new SimpleDateFormat("yyyy-MM-dd").parse("2019-07-16").getTime());
//
//		List<SingleExternalDataImportRequest> singleExternalDataImportRequests = mongoDbArchiveDatabaseTimeSeriesReader.getObservedDataImportRequest(period, timeSeriesArrays);
//		for (int i = 0; i < 100; i++) {
//			TimeSeriesArrays<TimeSeriesHeader> results = mongoDbArchiveDatabaseTimeSeriesReader.importSingleDataImportRequest(singleExternalDataImportRequests.get(0));
//			assertEquals(1, results.size());
//		}
    }

    @Test
    void test() throws Exception {
//        String hostName = InetAddress.getLocalHost().getHostName();
//        if(testSettings != null && hostName.equalsIgnoreCase(testSettings.getString("hostName"))) {
//            MongoDbArchiveDatabase mongoDbArchiveDatabase = MongoDbArchiveDatabase.create();
//            mongoDbArchiveDatabase.setArchiveDatabaseUrl(testSettings.getString("archiveDatabaseUrl"));
//            mongoDbArchiveDatabase.setUserNamePassword(testSettings.isNull("userName") ? "" : testSettings.getString("userName"), testSettings.isNull("password") ? "" : testSettings.getString("password"));
//
//            MongoDbArchiveDatabaseTimeSeriesReader mongoDbArchiveDatabaseTimeSeriesReader = (MongoDbArchiveDatabaseTimeSeriesReader) mongoDbArchiveDatabase.getArchiveDataBaseTimeSeriesReader();
//            mongoDbArchiveDatabaseTimeSeriesReader.setHeaderProvider(new TestUtil.HeaderProviderTestImplementation());
//
//            ArchiveDatabaseResultSearchParameters archiveDatabaseResultSearchParameters = new ArchiveDatabaseResultSearchParameters(
//                    "scalar", TimeSeriesType.EXTERNAL_HISTORICAL, new Period(1561978800000L, 1562842800000L), Set.of("scalar_externalhistorical_QS"), Set.of(), Set.of(), Set.of());
//
//            ArchiveDatabaseReadResult archiveDatabaseReadResult = mongoDbArchiveDatabaseTimeSeriesReader.read(archiveDatabaseResultSearchParameters);
//            int count = 0;
//            while (archiveDatabaseReadResult.hasNext()) {
//                archiveDatabaseReadResult.next();
//                count++;
//            }
//            assertEquals(148, count);
//        }
    }
}

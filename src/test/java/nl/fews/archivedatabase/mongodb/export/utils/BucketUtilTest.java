package nl.fews.archivedatabase.mongodb.export.utils;

import junit.framework.TestCase;
import nl.fews.archivedatabase.mongodb.export.enums.BucketSize;
import org.bson.Document;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BucketUtilTest extends TestCase {

	public void testGetBucketSize() {
		List<Document> timeseries = IntStream.range(0, 999).boxed().map(x -> new Document()).collect(Collectors.toList());
		Document document = new Document("metaData", new Document("timeStepMinutes", Integer.MIN_VALUE)).append("startTime", Date.from(Instant.parse("2000-01-01T00:00:00Z"))).append("endTime", Date.from(Instant.parse("2000-01-02T00:00:00Z"))).append("timeseries", timeseries);
		assertEquals(BucketSize.MONTHLY, BucketUtil.getBucketSize(document));
		assertEquals(BucketSize.MONTHLY, BucketUtil.getBucketSize(document.append("metaData", new Document("timeStepMinutes", 0))));
		assertEquals(BucketSize.MONTHLY, BucketUtil.getBucketSize(document.append("metaData", new Document("timeStepMinutes", 14))));
		assertEquals(BucketSize.MONTHLY, BucketUtil.getBucketSize(document.append("metaData", new Document("timeStepMinutes", 15))));
		assertEquals(BucketSize.YEARLY, BucketUtil.getBucketSize(document.append("metaData", new Document("timeStepMinutes", 16))));
		assertEquals(BucketSize.YEARLY, BucketUtil.getBucketSize(document.append("metaData", new Document("timeStepMinutes", Integer.MAX_VALUE))));
		assertEquals(BucketSize.YEARLY, BucketUtil.getBucketSize(document.append("metaData", new Document("timeStepMinutes", Integer.MIN_VALUE)).append("timeseries", timeseries.subList(0, 4))));
		assertEquals(BucketSize.YEARLY, BucketUtil.getBucketSize(document.append("metaData", new Document("timeStepMinutes", 0)).append("timeseries", timeseries.subList(0, 4))));
	}

	public void testGetBucket() {
		assertEquals(202001, BucketUtil.getBucket(Date.from(Instant.parse("2020-01-01T00:00:00Z")), BucketSize.MONTHLY));
		assertEquals(2020, BucketUtil.getBucket(Date.from(Instant.parse("2020-01-01T00:00:00Z")), BucketSize.YEARLY));
	}
}
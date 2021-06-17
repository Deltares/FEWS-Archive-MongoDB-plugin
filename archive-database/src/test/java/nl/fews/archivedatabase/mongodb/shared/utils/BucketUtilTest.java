package nl.fews.archivedatabase.mongodb.shared.utils;

import nl.fews.archivedatabase.mongodb.shared.enums.BucketSize;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Date;

class BucketUtilTest {

	@Test
	void getBucket() {
		Assertions.assertEquals(BucketSize.YEARLY, BucketSize.valueOf(BucketUtil.getBucket(new Document()).getString("bucketSize")));
		Assertions.assertEquals(false, BucketUtil.getBucket(new Document()).getBoolean("collapse"));
		Assertions.assertEquals(true, BucketUtil.getBucket(new Document("moduleInstanceId", "SpillGates").append("locationId", "NJH-01").append("parameterId", "NS").append("encodedTimeStepId", "NETS")).getBoolean("collapse"));
	}

	@Test
	void getBucketValue() {
		Date date = new Date();
		LocalDateTime l = DateUtil.getLocalDateTime(date);
		Assertions.assertEquals(l.getYear() / 10 * 10, BucketUtil.getBucketValue(date, BucketSize.DECADE));
		Assertions.assertEquals(l.getYear(), BucketUtil.getBucketValue(date, BucketSize.YEARLY));
		Assertions.assertEquals(l.getYear() * 100 + l.getMonthValue(), BucketUtil.getBucketValue(date, BucketSize.MONTHLY));
		Assertions.assertEquals(l.getYear() * 10000 + l.getMonthValue() * 100 + l.getDayOfMonth(), BucketUtil.getBucketValue(date, BucketSize.DAILY));
	}
}
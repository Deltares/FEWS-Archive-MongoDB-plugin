package nl.fews.archivedatabase.mongodb.shared.utils;

import nl.fews.archivedatabase.mongodb.shared.enums.BucketSize;
import org.junit.jupiter.api.Test;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BucketUtilTest {

	@Test
	void getBucketSize() {
		assertEquals(BucketSize.YEAR, BucketUtil.getBucketSize(60));
		assertEquals(BucketSize.YEAR, BucketUtil.getBucketSize(61));
		assertEquals(BucketSize.MONTH, BucketUtil.getBucketSize(59));
	}

	@Test
	void getBucketValue() {
		Date date = new Date();
		LocalDateTime l = DateUtil.getLocalDateTime(date);
		assertEquals(l.getYear() / 10 * 10, BucketUtil.getBucketValue(date, BucketSize.DECADE));
		assertEquals(l.getYear(), BucketUtil.getBucketValue(date, BucketSize.YEAR));
		assertEquals(l.getYear() * 100L + l.getMonthValue(), BucketUtil.getBucketValue(date, BucketSize.MONTH));
		assertEquals(l.getYear() * 10000L + l.getMonthValue() * 100 + l.getDayOfMonth(), BucketUtil.getBucketValue(date, BucketSize.DAY));
	}

	@Test
	void getEstimatedBucketSize() throws Exception {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		assertEquals(BucketSize.YEAR, BucketUtil.getEstimatedBucketSize(simpleDateFormat.parse("2020-01-01"), simpleDateFormat.parse("2020-01-02"), 23));
		assertEquals(BucketSize.YEAR, BucketUtil.getEstimatedBucketSize(simpleDateFormat.parse("2020-01-01"), simpleDateFormat.parse("2020-01-02"), 24));
		assertEquals(BucketSize.MONTH, BucketUtil.getEstimatedBucketSize(simpleDateFormat.parse("2020-01-01"), simpleDateFormat.parse("2020-01-02"), 25));
	}

	@Test
	void getInferredBucketSize() throws Exception {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		assertEquals(BucketSize.DAY, BucketUtil.getInferredBucketSize(simpleDateFormat.parse("2020-01-01T00:00:00"), simpleDateFormat.parse("2020-01-01T23:59:59")));
		assertEquals(BucketSize.MONTH, BucketUtil.getInferredBucketSize(simpleDateFormat.parse("2020-01-01T00:00:00"), simpleDateFormat.parse("2020-01-31T23:59:59")));
		assertEquals(BucketSize.YEAR, BucketUtil.getInferredBucketSize(simpleDateFormat.parse("2020-01-01T00:00:00"), simpleDateFormat.parse("2020-12-31T23:59:59")));
	}
}
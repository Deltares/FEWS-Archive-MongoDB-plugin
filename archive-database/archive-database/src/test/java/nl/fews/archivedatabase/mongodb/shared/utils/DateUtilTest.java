package nl.fews.archivedatabase.mongodb.shared.utils;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DateUtilTest {

	@Test
	void getDates() {
		Date now = new Date();
		Date[] dates = DateUtil.getDates(new long[]{0, now.getTime()});
		assertEquals(new Date(0), dates[0]);
		assertEquals(now, dates[1]);
	}

	@Test
	void getLocalDateTime() {
		Date now = new Date();
		LocalDateTime localDateTime = DateUtil.getLocalDateTime(now);
		assertEquals(now.getTime(), localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli());
	}
}
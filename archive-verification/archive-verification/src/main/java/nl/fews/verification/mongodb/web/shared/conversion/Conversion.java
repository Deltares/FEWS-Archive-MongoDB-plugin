package nl.fews.verification.mongodb.web.shared.conversion;

import graphql.schema.DataFetchingEnvironment;
import graphql.schema.SelectedField;
import org.bson.Document;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.stream.Collectors;

public final class Conversion {
	private Conversion(){}

	public static Document getProjection(DataFetchingEnvironment e){
		return new Document(e.getSelectionSet().getFields().stream().collect(Collectors.toMap(SelectedField::getName, s -> 1)));
	}

	public static OffsetDateTime dateToOffsetDateTime(Date date) {
		return date.toInstant().atZone(ZoneId.of("UTC")).toOffsetDateTime();
	}

	public static Date offsetDateTimeToDate(OffsetDateTime offsetDateTime) {
		return Date.from(offsetDateTime.toInstant());
	}
}

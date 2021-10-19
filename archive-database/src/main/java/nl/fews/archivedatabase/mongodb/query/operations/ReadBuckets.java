package nl.fews.archivedatabase.mongodb.query.operations;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoCursor;
import nl.fews.archivedatabase.mongodb.query.interfaces.Read;
import nl.fews.archivedatabase.mongodb.shared.database.Database;
import org.bson.Document;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Provides streaming capability for bucketed timeseries
 */
public final class ReadBuckets implements Read {

	/**
	 *
	 * @param collection the collection in the database from which the result was derived
	 * @param query the query filter having 'and' field keys with 1..n values to 'or' match
	 * @param startDate event startDate, inclusive
	 * @param endDate event endDate, inclusive
	 * @return MongoCursor<Document>
	 */
	public MongoCursor<Document> read(String collection, Map<String, List<Object>> query, Date startDate, Date endDate) {
		Document document = new Document();

		if(startDate != null && endDate != null) {
			document.append("endTime", new Document("$gte", startDate));
			document.append("startTime", new Document("$lte", endDate));
		}
		else if(startDate != null)
			document.append("endTime", new Document("$gte", startDate));
		else if(endDate != null)
			document.append("startTime", new Document("$lte", endDate));

		query.forEach((k, v) -> {
			if(!v.isEmpty())
				document.append(k, v.size() == 1 ? v.get(0) : new Document("$in", v));
		});

		Document sort = new Document();
		Database.getCollectionKeys(collection).forEach(field -> sort.append(field, 1));
		return new MongoBucketCursor(Database.find(collection, document).sort(sort).allowDiskUse(true).iterator(), collection, startDate, endDate);
	}

	/**
	 * an iterator whose next method returns a merged and ranged timeseries composed it its constituent buckets
	 */
	public static final class MongoBucketCursor extends MongoPeekingCursor implements MongoCursor<Document>{
		/**
		 * lost of collection fields that compose a minimally unique key for documents
		 */
		private final List<String> collectionKeys;

		/**
		 * event start date, inclusive
		 */
		private final Date startDate;

		/**
		 * event end date, inclusive
		 */
		private final Date endDate;

		/**
		 * the keys to remove after merging buckets
		 */
		private final List<String> bucketKeys = List.of("bucketSize", "bucket");

		/**
		 * an iterator whose next method returns a merged and ranged timeseries composed it its constituent buckets
		 * @param result result iterator from a mongo query
		 * @param collection the collection in the database from which the result was derived
		 * @param startDate event startDate, inclusive
		 * @param endDate event endDate, inclusive
		 */
		public MongoBucketCursor(MongoCursor<Document> result, String collection, Date startDate, Date endDate) {
			super(result);
			this.collectionKeys = Database.getCollectionKeys(collection).stream().filter(s -> !bucketKeys.contains(s)).collect(Collectors.toList());
			this.startDate = startDate;
			this.endDate = endDate;
		}

		/**
		 * returns a merged and ranged timeseries composed it its constituent buckets
		 * @return Document
		 */
		@Override
		public Document next(){
			Document next = super.next();
			if(next != null){
				List<Document> events = next.getList("timeseries", Document.class).stream().filter(s -> !s.getDate("t").before(startDate) && !s.getDate("t").after(endDate)).collect(Collectors.toList());
				String currentKey = collectionKeys.stream().map(s -> next.get(s).toString()).collect(Collectors.joining("_"));
				while (hasNext()){
					String nextKey = collectionKeys.stream().map(s -> peek().get(s).toString()).collect(Collectors.joining("_"));
					if(!nextKey.equals(currentKey))
						break;
					events.addAll(super.next().getList("timeseries", Document.class).stream().filter(s -> !s.getDate("t").before(startDate) && !s.getDate("t").after(endDate)).collect(Collectors.toList()));
				}
				bucketKeys.forEach(next::remove);
				if(events.isEmpty()) {
					next.append("startTime", new Date(Long.MIN_VALUE)).append("endTime", new Date(Long.MIN_VALUE));
					if (next.containsKey("localStartTime")) next.append("localStartTime", new Date(Long.MIN_VALUE));
					if (next.containsKey("localEndTime")) next.append("localEndTime", new Date(Long.MIN_VALUE));
				}
				else{
					next.append("startTime", events.get(0).getDate("t")).append("endTime", events.get(events.size() - 1).getDate("t"));
					if (next.containsKey("localStartTime") && events.get(0).containsKey("lt")) next.append("localStartTime", events.get(0).getDate("lt"));
					if (next.containsKey("localEndTime") && events.get(events.size() - 1).containsKey("lt")) next.append("localEndTime", events.get(events.size() - 1).getDate("lt"));
				}
				next.append("timeseries", events);
			}
			return next;
		}
	}

	/**
	 * Adding peek functionality to iterator
	 */
	public static class MongoPeekingCursor implements MongoCursor<Document>{
		/**
		 * result iterator from a mongo query
		 */
		private final MongoCursor<Document> result;

		/**
		 * the peek cache 'next' document
		 */
		private Document next = null;

		/**
		 * Adding peek functionality to iterator
		 * @param result MongoCursor<Document>
		 */
		public MongoPeekingCursor(MongoCursor<Document> result) {
			this.result = result;
			if(result.hasNext())
				next = result.next();
		}

		/**
		 * return the 'next' cached document without advancing the iterator. returns null when empty
		 * @return Document
		 */
		public Document peek(){
			return next;
		}

		/**
		 * ibid
		 */
		@Override
		public void close() {
			result.close();
		}

		/**
		 * true of the next document from peek is not null
		 * @return boolean
		 */
		@Override
		public boolean hasNext() {
			return next != null;
		}

		/**
		 * return the 'next' cached document, advancing the iterator. returns null when empty
		 * @return Document
		 */
		@Override
		public Document next() {
			Document r = next;
			next = result.hasNext() ? result.next() : null;
			return r;
		}

		/**
		 * return the 'next' cached document, advancing the iterator. returns null when empty
		 * @return Document
		 */
		@Override
		public Document tryNext() {
			return next();
		}

		/**
		 * ibid
		 * @return ServerCursor
		 */
		@Override
		public ServerCursor getServerCursor() {
			return result.getServerCursor();
		}

		/**
		 * ibid
		 * @return ServerAddress
		 */
		@Override
		public ServerAddress getServerAddress() {
			return result.getServerAddress();
		}
	}
}

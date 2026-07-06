package nl.fews.archivedatabase.common.query.utils;

import nl.fews.archivedatabase.common.shared.database.Index;
import nl.fews.archivedatabase.common.shared.interfaces.ClosableIterator;
import nl.fews.archivedatabase.common.shared.streaming.PeekingIterator;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * an iterator whose next method returns a merged and ranged timeseries composed it its constituent buckets
 */
@SuppressWarnings("unchecked")
public final class BucketIterator extends PeekingIterator<Map<String, Object>> implements ClosableIterator<Map<String, Object>> {
	/**
	 * list of collection fields that compose a minimally unique key for documents
	 */
	private final List<String> keys;

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
	 * @param iterator iterator from aquery
	 * @param collection the collection in the database from which the result was derived
	 * @param startDate event startDate, inclusive
	 * @param endDate event endDate, inclusive
	 */
	public BucketIterator(ClosableIterator<Map<String, Object>> iterator, String collection, Date startDate, Date endDate) {
		super(iterator);
		this.keys = Index.getKeys(collection).stream().filter(s -> !bucketKeys.contains(s)).toList();
		this.startDate = startDate;
		this.endDate = endDate;
	}

	/**
	 * returns a merged and ranged timeseries composed it its constituent buckets
	 * @return Map<String, Object>
	 */
	@Override
	public Map<String, Object> next(){
		var next = super.next();
		if(next != null){
			var events = ((List<Map<String, Object>>)next.get("timeseries")).stream().filter(s -> !((Date)s.get("t")).before(startDate) && !((Date)s.get("t")).after(endDate)).collect(Collectors.toList());
			var currentKey = keys.stream().map(next::get).toList();
			while (hasNext()){
				var nextKey = keys.stream().map(s -> peek().get(s)).toList();
				if(!nextKey.equals(currentKey))
					break;
				events.addAll(((List<Map<String, Object>>)super.next().get("timeseries")).stream().filter(s -> !((Date)s.get("t")).before(startDate) && !((Date)s.get("t")).after(endDate)).toList());
			}
			bucketKeys.forEach(next::remove);
			if(events.isEmpty()) {
				next.put("startTime", new Date(Long.MIN_VALUE));
				next.put("endTime", new Date(Long.MIN_VALUE));
				next.put("localStartTime", new Date(Long.MIN_VALUE));
				next.put("localEndTime", new Date(Long.MIN_VALUE));
			}
			else{
				next.put("startTime", events.get(0).get("t"));
				next.put("endTime", events.get(events.size() - 1).get("t"));
				if (events.get(0).containsKey("lt")) next.put("localStartTime", events.get(0).get("lt"));
				if (events.get(events.size() - 1).containsKey("lt")) next.put("localEndTime", events.get(events.size() - 1).get("lt"));
			}
			next.put("timeseries", events);
		}
		return next;
	}
}

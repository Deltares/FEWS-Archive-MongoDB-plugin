package nl.fews.archivedatabase.postgres.database;

import org.jooq.*;
import org.jooq.conf.ParamType;
import org.jooq.conf.RenderQuotedNames;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;

import java.util.*;

import static org.jooq.impl.DSL.*;

@SuppressWarnings("unchecked")
public final class Sql {
	private static final DSLContext ctx = DSL.using(SQLDialect.POSTGRES, new Settings().withRenderQuotedNames(RenderQuotedNames.ALWAYS));

	private Sql() {}

	public record Rendered(String sql, List<Object> params) {}

	public static Rendered render(Query query) {
		return new Rendered(query.getSQL(ParamType.INDEXED), query.getBindValues());
	}

	public static DSLContext ctx() {
		return ctx;
	}

	public static String columnName(String name) {
		return name.replace(".", "_");
	}

	public static Table<?> table(String sql, Collection<? extends QueryPart> parts) {
		return DSL.table(sql, parts.toArray(QueryPart[]::new));
	}

	public static Table<?> table(String table) {
		return DSL.table(DSL.name(columnName(table)));
	}

	public static Field<Object> excluded(String name) {
		return DSL.excluded(field(name));
	}

	public static Field<Object> val(Object value) {
		return DSL.val(value);
	}

	public static WindowOverStep<Integer> rowNumber() {
		return DSL.rowNumber();
	}

	public static Asterisk asterisk() {
		return DSL.asterisk();
	}

	public static List<SelectFieldOrAsterisk> selectFieldList() {
		return new ArrayList<>();
	}

	public static Field<Integer> count(String name) {
		return DSL.count().as(name);
	}

	public static Field<Object> field(String name) {
		return DSL.field(DSL.name(columnName(name)));
	}

	public static Field<Object> field(String alias, String name) {
		return DSL.field(DSL.name(alias, columnName(name)));
	}

	public static Condition condition(Map<String, Object> whereKeys) {
		return condition(whereKeys, null);
	}

	public static Condition condition(Map<String, Object> whereKeys, String alias) {
		if (whereKeys.isEmpty())
			return noCondition();

		Condition condition = noCondition();
		for (var e : whereKeys.entrySet()) {
			condition = condition.and(switch (e.getKey()) {
				case "and" -> andCondition(e.getValue(), alias);
				case "or" -> orCondition(e.getValue(), alias);
				default -> conditionForField(alias == null ? field(e.getKey()) : field(alias, e.getKey()), e.getValue());
			});
		}
		return condition;
	}

	private static Condition andCondition(Object value, String alias) {
		Condition condition = noCondition();
		for (var item : (List<?>)value)
			condition = condition.and(condition((Map<String, Object>) item, alias));
		return condition;
	}

	private static Condition orCondition(Object value, String alias) {
		Condition condition = falseCondition();
		for (var item : (List<?>)value)
			condition = condition.or(condition((Map<String, Object>) item, alias));
		return condition;
	}

	private static Condition conditionForField(Field<Object> field, Object value) {
		if (value == null)
			return field.isNull();

		if (value instanceof List<?> values)
			return values.isEmpty() ? falseCondition() : field.in(values);

		if (value instanceof Map<?, ?> ops) {
			Condition condition = noCondition();

			for (var e : ops.entrySet()) {
				var op = e.getKey().toString();
				var v = e.getValue();

				condition = condition.and(switch (op) {
					case "gte" -> field.ge(v);
					case "lte" -> field.le(v);
					case "gt" -> field.gt(v);
					case "lt" -> field.lt(v);
					case "eq" -> v == null ? field.isNull() : field.eq(v);
					case "in" -> ((List<?>)v).isEmpty() ? falseCondition() : field.in((List<?>)v);
					default -> throw new IllegalArgumentException(op);
				});
			}
			return condition;
		}
		return field.eq(value);
	}

	public static List<SortField<Object>> sortFields(Map<String, Object> sort) {
		return sortFields(sort, null);
	}

	public static List<SortField<Object>> sortFields(Map<String, Object> sort, String alias) {
		return sort.keySet().stream().map(k -> Map.entry(k, alias == null ? field(k) : field(alias, k))).map(e ->
			getSortDirection(e.getValue(), ((Number)sort.get(e.getKey())).intValue())).toList();
	}

	public static SortField<Object> getSortDirection(Field<Object> field, int direction){
		return switch(direction){
			case 1 -> field.asc();
			case -1 -> field.desc();
			default -> throw new IllegalArgumentException(String.format("%s", direction));
		};
	}

	public static List<SelectFieldOrAsterisk> selectFields(List<String> columns, Map<String, Object> projection) {
		return projection.values().stream().anyMatch(v -> v.equals(1))
			? projection.keySet().stream().filter(k -> Objects.equals(projection.get(k), 1)).map(s -> (SelectFieldOrAsterisk)field(s)).toList()
			: columns.stream().filter(c -> !projection.containsKey(c)).map(s -> (SelectFieldOrAsterisk)field(s)).toList();
	}

	public static List<SelectFieldOrAsterisk> aliasedFields(Map<String, Object> fields, String alias) {
		return fields.entrySet().stream().map(e -> (SelectFieldOrAsterisk)field(e.getValue().toString()).as(alias + e.getKey())).toList();
	}

	public static List<SelectFieldOrAsterisk> aliasedFields(Map<String, Object> fields) {
		return fields.entrySet().stream().map(e -> (SelectFieldOrAsterisk)field(e.getValue().toString()).as(e.getKey())).toList();
	}

	public static List<Field<Object>> groupFields(Map<String, Object> groupId) {
		return groupId.values().stream().map(Object::toString).map(Sql::field).toList();
	}

	public static List<Field<Object>> groupFields(Map<String, Object> groupId, String tableAlias) {
		return groupId.values().stream().map(Object::toString).map(v -> field(tableAlias, v)).toList();
	}
}
package nl.fews.archivedatabase.common.shared.database;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class IndexTest {

    @Test
    void getKeyDocument(){
        var result = Index.getKeyDocument(List.of("1", "2", "3"), Map.of("1", "one", "2", "two", "3", "three", "4", "four"));
        assertInstanceOf(LinkedHashMap.class, result);
        assertEquals(new LinkedHashMap<>(Map.of("1", "one", "2", "two", "3", "three")), result);
    }

    @Test
    void getKey(){
        var result = Index.getKey(new LinkedHashMap<>(Map.of("1", "one", "2", "two", "3", "three")));
        assertEquals("{\"1\":\"one\",\"2\":\"two\",\"3\":\"three\"}", result);
    }
}

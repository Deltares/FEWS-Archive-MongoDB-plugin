package nl.fews.verification.mongodb.shared.io;

import nl.fews.verification.mongodb.shared.io.IO;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class IOTest {

	@Test
	void writeString() throws Exception{
        IO.writeString("writeString.txt", "test");
        assertEquals("test", Files.readString(Path.of("writeString.txt")));
        Files.delete(Path.of("writeString.txt"));
	}

	@Test
	void deleteFiles() throws Exception{
		Path tempDir = Files.createTempDirectory("deleteFiles");
        Files.createFile(tempDir.resolve("file1.txt"));
        Files.createFile(tempDir.resolve("file2.txt"));

        IO.deleteFiles(tempDir);

        assertEquals(0, Files.list(tempDir).count());

        Files.delete(tempDir);
	}

	@Test
	void execute() {
		String command = "echo execute";
        Object[] result = IO.execute(command);

        assertEquals(0, result[0]);
        assertEquals("execute", ((String)result[1]).trim());
	}

	@Test
	void readString() throws Exception {
		IO.writeString("writeString.txt", "test");
        assertEquals("test", IO.readString("writeString.txt"));
        Files.delete(Path.of("writeString.txt"));
	}
}
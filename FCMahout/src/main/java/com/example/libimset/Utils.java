package com.example.libimset;

import org.apache.mahout.cf.taste.impl.common.FastIDSet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class Utils {

    static FastIDSet[] parseMenWomen(String filePath) throws IOException {
        FastIDSet men = new FastIDSet(50000);
        FastIDSet women = new FastIDSet(50000);

        readAndConsume(filePath, consumeGenres(men, women));
        men.rehash();
        women.rehash();
        return new FastIDSet[] { men, women };
    }

    static Consumer<String> consumeGenres(FastIDSet men, FastIDSet women){
        return line -> {
            String[] l = line.split(",");
            if (l[1].equals("M")) {
                men.add(Long.parseLong(l[0]));
            } else if (l[1].equals("F")) {
                women.add(Long.parseLong(l[0]));
            }
        };
    }

    static void readAndConsume(String filePath, Consumer<String> consumer) throws IOException {
        Stream<String> lines = null;
        lines = Files.lines(Paths.get(filePath));
        lines.forEach(consumer);
    }
}

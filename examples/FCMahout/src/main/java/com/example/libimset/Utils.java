package com.example.libimset;

import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.common.iterator.FileLineIterable;

import java.io.File;
import java.io.IOException;
/*import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Consumer;
import java.util.stream.Stream;*/

public class Utils {

    /**
     * @param filePath File for the genders.
     * @return A FastIdSet array containing two elements, the first one only with men and the second one containing
     * women.
     * @throws IOException If the file does not exist.
     */
    static FastIDSet[] parseMenWomen(String filePath) throws IOException {
        FastIDSet men = new FastIDSet(50000);
        FastIDSet women = new FastIDSet(50000);

        for (String line : new FileLineIterable(new File(filePath))) {
            int comma = line.indexOf(',');
            char gender = line.charAt(comma + 1);
            long profileID = Long.parseLong(line.substring(0, comma));
            if (gender == 'M') {
                men.add(profileID);
            } else {
                women.add(profileID);
            }

        }

//                readAndConsume(filePath, consumeGenres(men, women));
        men.rehash();
        women.rehash();
        return new FastIDSet[]{men, women};
    }

    /**
     * Consume the genres and add each on a FastIdSet.
     *
     * @param men   The FastIdSet for the men.
     * @param women The FastIdSet for the women.
     * @return a Consumer for the the gender file.
     */
/*    private static Consumer<String> consumeGenres(FastIDSet men, FastIDSet women) {
        return line -> {
            String[] l = line.split(",");
            if (l[1].equals("M")) {
                men.add(Long.parseLong(l[0]));
            } else if (l[1].equals("F")) {
                women.add(Long.parseLong(l[0]));
            }
        };
    }

    /**
     * Read a file and consume each line using a consumer.
     *
     * @param filePath Path for the file to be consumed.
     * @param consumer A consumer to consume each line from the file.
     * @throws IOException If the file does not exist.
     */
   /* static void readAndConsume(String filePath, Consumer<String> consumer) throws IOException {
        Stream<String> lines = Files.lines(Paths.get(filePath));
        lines.forEach(consumer);
    }*/
}

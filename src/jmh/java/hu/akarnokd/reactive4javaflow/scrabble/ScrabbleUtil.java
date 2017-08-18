/*
 * Copyright 2017 David Karnok
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Original copyright: José Paumard, 2015, GPL 2+
 */
package hu.akarnokd.reactive4javaflow.scrabble;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

public final class ScrabbleUtil {

    private ScrabbleUtil() { }

    public static Set<String> readScrabbleWords() {
        Set<String> set = new HashSet<>() ;

        try (BufferedReader bin = new BufferedReader(new InputStreamReader(
                ScrabbleUtil.class.getResourceAsStream("ospd.txt")))) {
            String line;

            while ((line = bin.readLine()) != null) {
                set.add(line.toLowerCase());
            }
        } catch (Throwable ex) {
            // ignored
            ex.printStackTrace();
        }
        if (set.isEmpty()) {
            throw new RuntimeException("No data!");
        }
        return set ;
    }

    public static Set<String> readShakespeareWords() {
        Set<String> set = new HashSet<>() ;

        try (BufferedReader bin = new BufferedReader(new InputStreamReader(ScrabbleUtil.class.getResourceAsStream("words.shakespeare.txt")))) {
            String line;

            while ((line = bin.readLine()) != null) {
                set.add(line.toLowerCase());
            }
        } catch (Throwable ex) {
            ex.printStackTrace();
            // ignored
        }
        if (set.isEmpty()) {
            throw new RuntimeException("No data!");
        }
        return set ;
    }
}
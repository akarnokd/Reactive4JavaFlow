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

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.functionals.CheckedFunction;
import org.openjdk.jmh.annotations.*;

/**
 * Shakespeare plays Scrabble with Reactive4JavaFlow Folyam optimized.
 * @author José
 * @author akarnokd
 */
public class ShakespearePlaysScrabbleWithFolyamOpt extends ShakespearePlaysScrabble {
    static Folyam<Integer> chars(String word) {
//        return Flowable.range(0, word.length()).map(i -> (int)word.charAt(i));
        return Folyam.characters(word);
    }

    @SuppressWarnings("unused")
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(
        iterations = 5
    )
    @Measurement(
        iterations = 5, time = 1
    )
    @Fork(value = 1, jvmArgs = {
            "-XX:MaxInlineLevel=20"
//            , "-XX:+UnlockDiagnosticVMOptions",
//            , "-XX:+PrintAssembly",
//            , "-XX:+TraceClassLoading",
//            , "-XX:+LogCompilation"
    })
    public List<Entry<Integer, List<String>>> measureThroughput() throws Throwable {

        //  to compute the score of a given word
        CheckedFunction<Integer, Integer> scoreOfALetter = letter -> letterScores[letter - 'a'];

        // score of the same letters in a word
        CheckedFunction<Entry<Integer, MutableLong>, Integer> letterScore =
                entry ->
                        letterScores[entry.getKey() - 'a'] *
                        Integer.min(
                                (int)entry.getValue().get(),
                                scrabbleAvailableLetters[entry.getKey() - 'a']
                            )
                    ;


        CheckedFunction<String, Folyam<Integer>> toIntegerFolyam =
                string -> chars(string);

        // Histogram of the letters in a given word
        CheckedFunction<String, Esetleg<HashMap<Integer, MutableLong>>> histoOfLetters =
                word -> toIntegerFolyam.apply(word)
                            .collect(
                                () -> new HashMap<>(),
                                (HashMap<Integer, MutableLong> map, Integer value) ->
                                    {
                                        MutableLong newValue = map.get(value) ;
                                        if (newValue == null) {
                                            newValue = new MutableLong();
                                            map.put(value, newValue);
                                        }
                                        newValue.incAndSet();
                                    }

                            ) ;

        // number of blanks for a given letter
        CheckedFunction<Entry<Integer, MutableLong>, Long> blank =
                entry ->
                        Long.max(
                            0L,
                            entry.getValue().get() -
                            scrabbleAvailableLetters[entry.getKey() - 'a']
                        )
                    ;

        // number of blanks for a given word
        CheckedFunction<String, Esetleg<Long>> nBlanks =
                word ->
                            histoOfLetters.apply(word).flatMapIterable(
                                    map -> map.entrySet()
                            )
                            .map(blank)
                            .sumLong(v -> v)

                    ;


        // can a word be written with 2 blanks?
        CheckedFunction<String, Esetleg<Boolean>> checkBlanks =
                word -> nBlanks.apply(word)
                            .map(l -> l <= 2L) ;

        // score taking blanks into account letterScore1
        CheckedFunction<String, Esetleg<Integer>> score2 =
                word ->
                            histoOfLetters.apply(word).flatMapIterable(
                                map -> map.entrySet()
                            )
                            .map(letterScore)
                            .sumInt(v -> v)
                            ;

        // Placing the word on the board
        // Building the streams of first and last letters
        CheckedFunction<String, Folyam<Integer>> first3 =
                word -> chars(word).take(3) ;
        CheckedFunction<String, Folyam<Integer>> last3 =
                word -> chars(word).skip(3) ;


        // Stream to be maxed
        CheckedFunction<String, Folyam<Integer>> toBeMaxed =
            word -> Folyam.concatArray(first3.apply(word), last3.apply(word))
            ;

        // Bonus for double letter
        CheckedFunction<String, Esetleg<Integer>> bonusForDoubleLetter =
            word -> toBeMaxed.apply(word)
                        .map(scoreOfALetter)
                        .max(Comparator.naturalOrder())
                        ;

        // score of the word put on the board
        CheckedFunction<String, Esetleg<Integer>> score3 =
            word ->
                Folyam.concatArray(
                    score2.apply(word),
                    bonusForDoubleLetter.apply(word)
                )
                .sumInt(v -> v)
                .map(v -> v * 2 + (word.length() == 7 ? 50 : 0))

                ;

        CheckedFunction<CheckedFunction<String, Esetleg<Integer>>, Esetleg<TreeMap<Integer, List<String>>>> buildHistoOnScore =
                score -> Folyam.fromIterable(shakespeareWords)
                                .filter(scrabbleWords::contains)
                                .filter(word -> checkBlanks.apply(word).blockingGet(false))
                                .collect(
                                    () -> new TreeMap<Integer, List<String>>(Comparator.reverseOrder()),
                                    (TreeMap<Integer, List<String>> map, String word) -> {
                                        Integer key = score.apply(word).blockingGet(0) ;
                                        List<String> list = map.get(key) ;
                                        if (list == null) {
                                            list = new ArrayList<>() ;
                                            map.put(key, list) ;
                                        }
                                        list.add(word) ;
                                    }
                                ) ;

        // best key / value pairs
        List<Entry<Integer, List<String>>> finalList2 =
                    buildHistoOnScore.apply(score3).flatMapIterable(
                            map -> map.entrySet()
                    )
                    .take(3)
                    .collect(
                        () -> new ArrayList<Entry<Integer, List<String>>>(),
                        (list, entry) -> {
                            list.add(entry) ;
                        }
                    )
                    .blockingGet(null) ;


//        System.out.println(finalList2);

        return finalList2 ;
    }

    public static void main(String[] args) throws Throwable {
        ShakespearePlaysScrabbleWithFolyamOpt s = new ShakespearePlaysScrabbleWithFolyamOpt();
        s.init();
        FolyamSynchronousProfiler p = new FolyamSynchronousProfiler();
        p.start();
        for (int i = 0; i < 100; i++) {
            System.out.println(s.measureThroughput());
        }
        p.clear();

        System.out.println(s.measureThroughput());

        p.stop();

        System.out.println();

        p.print();
    }
}
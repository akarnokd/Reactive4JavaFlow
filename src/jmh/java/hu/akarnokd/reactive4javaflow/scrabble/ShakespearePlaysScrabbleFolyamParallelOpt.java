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
import java.util.concurrent.*;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.functionals.CheckedFunction;
import org.openjdk.jmh.annotations.*;

/**
 * Shakespeare plays Scrabble with RxJava 2 parallel.
 * @author José
 * @author akarnokd
 */
public class ShakespearePlaysScrabbleFolyamParallelOpt extends ShakespearePlaysScrabble {

    static Folyam<Integer> chars(String word) {
//        return Folyam.range(0, word.length()).map(i -> (int)word.charAt(i));
        return Folyam.characters(word);
    }

    final SchedulerService scheduler = SchedulerServices.computation(); // = new WeakParallelScheduler();

    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(
        iterations = 5
    )
    @Measurement(
        iterations = 5, time = 1
    )
    @Fork(1)
    @SuppressWarnings("unused")
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
                ShakespearePlaysScrabbleFolyamParallelOpt::chars;

        // Histogram of the letters in a given word
        CheckedFunction<String, Esetleg<HashMap<Integer, MutableLong>>> histoOfLetters =
                word -> toIntegerFolyam.apply(word)
                            .collect(
                                    HashMap::new,
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
                word -> histoOfLetters.apply(word)
                            .toFolyam()
                            .flatMapIterable(HashMap::entrySet)
                            .map(blank)
                            .sumLong(v -> v)
                            ;


        // can a word be written with 2 blanks?
        CheckedFunction<String, Esetleg<Boolean>> checkBlanks =
                word -> nBlanks.apply(word)
                            .map(l -> l <= 2L) ;

        // score taking blanks into account letterScore1
        CheckedFunction<String, Esetleg<Integer>> score2 =
                word -> histoOfLetters.apply(word)
                            .toFolyam()
                            .flatMapIterable(HashMap::entrySet)
                            .map(letterScore)
                            .sumInt(v -> v);

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
                ).sumInt(v -> v).map(v -> 2 * v + (word.length() == 7 ? 50 : 0));

        CheckedFunction<CheckedFunction<String, Esetleg<Integer>>, Esetleg<TreeMap<Integer, List<String>>>> buildHistoOnScore =
                score ->
                Folyam.fromIterable(shakespeareWords)
                .parallel()
                .runOn(scheduler)
                .filter(scrabbleWords::contains)
                .filter(word -> checkBlanks.apply(word).blockingGet(false))
                .collect(
                    () -> new TreeMap<>(Comparator.reverseOrder()),
                    (TreeMap<Integer, List<String>> map, String word) -> {
                        Integer key = score.apply(word).blockingGet(0);
                        List<String> list = map.get(key) ;
                        if (list == null) {
                            list = new ArrayList<>() ;
                            map.put(key, list) ;
                        }
                        list.add(word) ;
                    }
                )
                .reduce((m1, m2) -> {
                    for (Map.Entry<Integer, List<String>> e : m2.entrySet()) {
                        List<String> list = m1.get(e.getKey());
                        if (list == null) {
                            m1.put(e.getKey(), e.getValue());
                        } else {
                            list.addAll(e.getValue());
                        }
                    }
                    return m1;
                });

        // best key / value pairs
        List<Entry<Integer, List<String>>> finalList2 =
                buildHistoOnScore.apply(score3)
                    .flatMapIterable(TreeMap::entrySet)
                    .take(3)
                    .collect(
                            (Callable<ArrayList<Entry<Integer, List<String>>>>) ArrayList::new,
                            ArrayList::add
                    )
                    .blockingGet(null) ;


//        System.out.println(finalList2);

        return finalList2 ;
    }

    public static void main(String[] args) throws Throwable {
        ShakespearePlaysScrabbleFolyamParallelOpt s = new ShakespearePlaysScrabbleFolyamParallelOpt();
        s.init();
        System.out.println(s.measureThroughput());
    }
}
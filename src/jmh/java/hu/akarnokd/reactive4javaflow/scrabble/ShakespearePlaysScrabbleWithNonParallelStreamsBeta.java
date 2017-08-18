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

import java.util.stream.Stream;

/**
 * Shakespeare plays Scrabble with Java Streams (slightly modified).
 * @author José
 */
public class ShakespearePlaysScrabbleWithNonParallelStreamsBeta extends ShakespearePlaysScrabbleWithStreamsBeta {

    @Override
    Stream<String> buildShakerspeareWordsStream() {
        return shakespeareWords.stream() ;
    }

    public static void main(String[] args) throws Exception {
        ShakespearePlaysScrabbleWithNonParallelStreamsBeta s = new ShakespearePlaysScrabbleWithNonParallelStreamsBeta();
        s.init();
        System.out.println(s.measureThroughput());
    }
}
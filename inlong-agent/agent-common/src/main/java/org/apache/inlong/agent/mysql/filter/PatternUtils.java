/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.agent.mysql.filter;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternCompiler;
import org.apache.oro.text.regex.Perl5Compiler;

public class PatternUtils {

    private static LoadingCache<String, Pattern> patternss = CacheBuilder
            .newBuilder()
            .softValues()
            .build(new CacheLoader<String, Pattern>() {
                @Override
                public Pattern load(String pattern) throws Exception {
                    try {
                        PatternCompiler pc = new Perl5Compiler();
                        return pc.compile(pattern,
                                Perl5Compiler.CASE_INSENSITIVE_MASK
                                        | Perl5Compiler.READ_ONLY_MASK
                                        | Perl5Compiler.SINGLELINE_MASK);
                    } catch (MalformedPatternException e) {
                        throw new RuntimeException(e);
                    }
                }
            });

    public static Pattern getPattern(String pattern) {
        return patternss.getUnchecked(pattern);
    }

    public static void clear() {
        patternss.invalidateAll();
    }
}

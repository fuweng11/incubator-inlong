package org.apache.inlong.agent.mysql.filter;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternCompiler;
import org.apache.oro.text.regex.Perl5Compiler;

/**
 * 提供{@linkplain Pattern}的lazy get处理
 * 
 * @author jianghang 2013-1-22 下午09:36:44
 * @version 1.0.0
 */
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
//        return patterns.get(pattern);
        return patternss.getUnchecked(pattern);
    }

    public static void clear() {
//        patterns.clear();
        patternss.invalidateAll();
    }
}

package com.hiko.audit;

import org.apache.log4j.helpers.OptionConverter;
import org.apache.log4j.pattern.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class LogFilePatternLayoutBuilder
{
    public static String getLogFormatFromPatternLayout(String patternLayout) {
        String input = OptionConverter.convertSpecialChars(patternLayout);
        List converters = new ArrayList();
        List fields = new ArrayList();
        Map converterRegistry = null;

        PatternParser.parse(input, converters, fields, converterRegistry, PatternParser.getPatternLayoutRules());
        return getFormatFromConverters(converters);
    }

    private static String getFormatFromConverters(List converters) {
        StringBuffer buffer = new StringBuffer();
        for (Iterator iter = converters.iterator();iter.hasNext();) {
            LoggingEventPatternConverter converter = (LoggingEventPatternConverter)iter.next();
            if (converter instanceof DatePatternConverter) {
                buffer.append("TIMESTAMP");
            } else if (converter instanceof MessagePatternConverter) {
                buffer.append("MESSAGE");
            } else if (converter instanceof LoggerPatternConverter) {
                buffer.append("LOGGER");
            } else if (converter instanceof ClassNamePatternConverter) {
                buffer.append("CLASS");
            } else if (converter instanceof RelativeTimePatternConverter) {
                buffer.append("PROP(RELATIVETIME)");
            } else if (converter instanceof ThreadPatternConverter) {
                buffer.append("THREAD");
            } else if (converter instanceof NDCPatternConverter) {
                buffer.append("NDC");
            } else if (converter instanceof LiteralPatternConverter) {
                LiteralPatternConverter literal = (LiteralPatternConverter)converter;
                //format shouldn't normally take a null, but we're getting a literal, so passing in the buffer will work
                literal.format(null, buffer);
            } else if (converter instanceof SequenceNumberPatternConverter) {
                buffer.append("PROP(log4jid)");
            } else if (converter instanceof LevelPatternConverter) {
                buffer.append("LEVEL");
            } else if (converter instanceof MethodLocationPatternConverter) {
                buffer.append("METHOD");
            } else if (converter instanceof FullLocationPatternConverter) {
                buffer.append("PROP(locationInfo)");
            } else if (converter instanceof LineLocationPatternConverter) {
                buffer.append("LINE");
            } else if (converter instanceof FileLocationPatternConverter) {
                buffer.append("FILE");
            } else if (converter instanceof PropertiesPatternConverter) {
//                PropertiesPatternConverter propertiesConverter = (PropertiesPatternConverter) converter;
//                String option = propertiesConverter.getOption();
//                if (option != null && option.length() > 0) {
//                    buffer.append("PROP(" + option + ")");
//                } else {
                buffer.append("PROP(PROPERTIES)");
//                }
            } else if (converter instanceof LineSeparatorPatternConverter) {
                //done
            }
        }
        return buffer.toString();
    }



}

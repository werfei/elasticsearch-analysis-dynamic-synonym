package com.bellszhu.elasticsearch.plugin.synonym.analysis;

import com.bellszhu.elasticsearch.plugin.utils.JDBCUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.elasticsearch.env.Environment;

import java.io.Reader;
import java.io.StringReader;
import java.util.List;

public class DBRemoteSynonymFile implements SynonymFile {
    private static final Logger logger = LogManager.getLogger("dynamic-synonym");

    private final String format;

    private final boolean expand;
    private final boolean lenient;

    private final Analyzer analyzer;

    // 数据库配置
    private final String location;
    private long lastModified = 0L;


    DBRemoteSynonymFile(Environment env, Analyzer analyzer,
                        boolean expand, boolean lenient, String format, String location) {
        this.analyzer = analyzer;
        this.expand = expand;
        this.lenient = lenient;
        this.format = format;
        this.location = location;
    }

    /**
     * 加载同义词词典至SynonymMap中
     *
     * @return SynonymMap
     */
    @Override
    public SynonymMap reloadSynonymMap() {
        try {
            logger.info("start reload local synonym from {}.", location);
            Reader rulesReader = getReader();
            SynonymMap.Builder parser = RemoteSynonymFile.getSynonymParser(rulesReader, format, expand, lenient, analyzer);
            return parser.build();
        } catch (Exception e) {
            logger.error("reload local synonym {} error!", location, e);
            throw new IllegalArgumentException(
                    "could not reload local synonyms file to build synonyms", e);
        }
    }

    /**
     * 判断是否需要进行重新加载
     *
     * @return true or false
     */
    @Override
    public boolean isNeedReloadSynonymMap() {
        try {
            long lastModify = JDBCUtils.queryMaxSynonymRuleVersion(location);
            if (lastModified < lastModify) {
                lastModified = lastModify;
                return true;
            }
        } catch (Exception e) {
            logger.error(e);
        }

        return false;
    }

    /**
     * 同义词库的加载
     *
     * @return Reader
     */
    @Override
    public Reader getReader() {

        StringBuilder sb = new StringBuilder();
        try {
            List<String> dbData = JDBCUtils.querySynonymRules(location);
            for (String dbDatum : dbData) {
                logger.info("load the synonym from db," + dbDatum);
                sb.append(dbDatum)
                        .append(System.getProperty("line.separator"));
            }
        } catch (Exception e) {
            logger.error("reload synonym from db failed");
        }
        return new StringReader(sb.toString());
    }
}

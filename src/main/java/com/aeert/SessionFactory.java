package com.aeert;


import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.model.ConfigChange;
import io.vavr.Function2;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.CaseUtils;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.internal.utils.KieHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * @author l'amour solitaire
 * @description drools基于apollo动态更新规则
 * @date 2020/3/27 上午11:22
 */
@Component
public class SessionFactory implements CommandLineRunner {

    private Logger log = LoggerFactory.getLogger(SessionFactory.class);

    /**
     * 规则nameSpace
     */
    @Value("${rule.nameSpace:rules}")
    private String nameSpace;

    /**
     * 规则命名开头匹配
     */
    @Value("${rule.start:rules}")
    private String start;

    private Map<String, KieSession> sessionMap = new HashMap<String, KieSession>();

    private static final String EMPTY_CONTENT = "规则 %s 为空,请检查!";
    private static final String LOAD_SUCCESS = "规则 %s 初始化成功.";
    private static final String PROPERTY_CHANGE_LOG = "规则 %s 发生变更: %s -> %s .";

    @Override
    public void run(String... args) {
        // initialize
        Config configTemp = ConfigService.getConfig(nameSpace);
        configTemp.getPropertyNames().stream().filter(m -> findRules.apply(m, start)).forEach(m -> {
            String content = Optional.ofNullable(configTemp.getProperty(m, null)).orElseThrow(() -> new IllegalArgumentException(String.format(EMPTY_CONTENT, m)));
            String ruleName = CaseUtils.toCamelCase(m, false, new char[]{'.'});
            sessionMap.put(ruleName, new KieHelper().addContent(content, ResourceType.DRL).build().newKieSession());
            log.info(String.format(LOAD_SUCCESS, ruleName));
        });

        // monitoring
        configTemp.addChangeListener(configChangeEvent -> {
            Set<String> keys = configChangeEvent.changedKeys();
            keys.stream().filter(m -> findRules.apply(m, start)).forEach(m -> {
                ConfigChange configChange = configChangeEvent.getChange(m);
                log.info(String.format(PROPERTY_CHANGE_LOG, configChange.getPropertyName(), configChange.getOldValue(), configChange.getNewValue()));
                sessionMap.put(CaseUtils.toCamelCase(m, false, new char[]{'.'}), new KieHelper().addContent(configChange.getNewValue(), ResourceType.DRL).build().newKieSession());
            });
        });
    }

    /**
     * 判断规则是否符合以start开头，多个命名以","分隔
     */
    Function2<String, String, Boolean> findRules = (key, start) -> {
        if (StringUtils.isBlank(start)) {
            return Boolean.TRUE;
        }
        List<String> startWords = Arrays.asList(start.split(","));
        return startWords.parallelStream().anyMatch(m -> key.split("\\.")[0].startsWith(m));
    };

    public KieSession getSession(String key) {
        return sessionMap.get(key);
    }

}

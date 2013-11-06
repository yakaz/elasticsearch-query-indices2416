package org.elasticsearch.plugin.query.indices2416;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;

import java.util.Collection;

import static org.elasticsearch.common.collect.Lists.newArrayList;

public class Indices2416Plugin extends AbstractPlugin {

    @Override
    public String name() {
        return "query-indices2416";
    }

    @Override
    public String description() {
        return "Indices query/filter that skip parsing for irrelevant indices (see ticket #2416 and pull request #4111)";
    }

    @Override
    public Collection<Class<? extends Module>> indexModules() {
        Collection<Class<? extends Module>> modules = newArrayList();
        modules.add(Indices2416ParserModule.class);
        return modules;
    }
}

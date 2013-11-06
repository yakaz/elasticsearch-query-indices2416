package org.elasticsearch.plugin.query.indices2416;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.index.query.RegisterIndices2416QueryParsers;

public class Indices2416ParserModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(RegisterIndices2416QueryParsers.class).asEagerSingleton();
    }
}

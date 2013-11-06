package org.elasticsearch.index.query;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;

public class RegisterIndices2416QueryParsers extends AbstractIndexComponent {

    @Inject
    public RegisterIndices2416QueryParsers(Index index, @IndexSettings Settings indexSettings, IndicesQueriesRegistry indicesQueriesRegistry, Injector injector) {
        super(index, indexSettings);

        ClusterService clusterService = injector.getInstance(ClusterService.class);

        indicesQueriesRegistry.addQueryParser(new Indices2416QueryParser(clusterService));
        indicesQueriesRegistry.addFilterParser(new Indices2416FilterParser(clusterService));
    }
}

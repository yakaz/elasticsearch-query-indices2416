Indices2416 Query Plugin
========================

This plugin provides the changes proposed in the [pull request #4111][pull4111] for the [ticket #2416][ticket2416].
It provides with a new query and filter type `indices2416`, that maps exactly how the builtin `indices` equivalent works.

Installation
------------

Simply run at the root of your ElasticSearch installation:

	bin/plugin --url https://github.com/yakaz/elasticsearch-query-indices2416/releases/download/v1.0.0/elasticsearch-query-indices2416-1.0.0.zip --install elasticsearch-query-indices2416

This will download the plugin from the Central Maven Repository.

Version matrix:

	┌────────┬───────────────┐
	│ Plugin │ ElasticSearch │
	├────────┼───────────────┤
	│ 1.0.x  │ 0.90.4+       │
	└────────┴───────────────┘

Description
-----------

If you experience `QueryParsingException`, for instance if you get `No mapping for type [X]`, for a query run against multiple indices, there are chances you need to specialize parts of your query to only run against specific indices.
Moreover queries like `has_child` or `has_parent` won't even parse against the wrong indices. And constraining using a `type` filter won't help.

The [ticket #2416][ticket2416] and [pull request #4111][pull4111] solve this by skipping parsing altogether, should the `indices` be provided before the queries/filters.

The `indices2416` query and filter work exactly like their builtin `indices` equivalents.
When using them, make sure you provide the `indices` field before the `query` and `no_match_query` fields (resp. `filter` and `no_match_filter`).
If you use `none` or `all` as value for `no_match_query` (resp. `no_match_filter`), the order does not matter.

See also
--------

https://github.com/elasticsearch/elasticsearch/issues/2416 − Indices filter parsed for indices to which it should not apply

https://github.com/elasticsearch/elasticsearch/pull/4111 − Indices query/filter skip parsing altogether for irrelevant indices

[ticket2416]: https://github.com/elasticsearch/elasticsearch/issues/2416
[pull4111]: https://github.com/elasticsearch/elasticsearch/pull/4111

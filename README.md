Census Reporter API
===================

The home for the API that powers the [Census Reporter](http://censusreporter.org/) project.

It queries a [census-postgres](https://github.com/censusreporter/census-postgres) database and
generates JSON output that can be read by other clients. One such client is
[censusreporter.org](https://github.com/censusreporter/censusreporter).

## Endpoints

Think of the American Community Survey as a spreadshet with thousands of columns and hundreds of thousands of rows (geographies). The goal of this API is to make it easy to access any chunk of that spreadsheet with simple HTTP calls and get the result as an easy-to-parse JSON object.

To continue with the spreadsheet metaphor, the endpoints for this API can be roughly broken into 3 pieces:

1. information about columns
2. information about geographies (rows)
3. data at the intersection of one or more of the above

### Column, Table, and Tabulations

#### `GET /1.0/table/suggest`

 Query Argument | Type   | Required? | Description
:---------------|:-------|:----------|:-----------
 `q`            | string | Yes       | The string to use for suggestions.

Retrieves a list of table and column suggestions given a search term using `q`. This is meant to support autocomplete text boxes.

#### `GET /1.0/table/elasticsearch`

 Query Argument | Type   | Required? | Description
:---------------|:-------|:----------|:-----------
 `q`            | string | Yes       | The string to use for suggestions.
 `start`        | int    | No        | Where in the results list to start.
 `size`         | int    | No        | The number of results to return.
 `topics`       | string | No        | A column-separated list of topics to limit the search by.
 `acs`          | string | No        | The ACS release code to limit the search by.

Returns table and column information relevant to the search term given in `q`. This endpoint supports paging using the `start` and `size` parameters. You can narrow your search by specifying a `topics` parameter. The `acs` parameter limits your search to a specific ACS release. By default it will use the most recent release.

#### `GET /1.0/tabulation/<tabulation_id>`

 URL Argument    | Type   | Required? | Description
:----------------|:-------|:----------|:-----------
 `tabulation_id` | string | Yes       | The tabulation ID to retrieve.

Returns information about the specified tabulation. A tabulation is a grouping of tables that share the numeric part of the table ID. Each tabulation can have multiple tables spread across the three yearly American Community Survey releases (1-, 3-, and 5-year).

#### `GET /1.0/table/<table_id>`

 URL Argument    | Type   | Required? | Description
:----------------|:-------|:----------|:-----------
 `table_id`      | string | Yes       | The table ID to retrieve.


 Query Argument | Type   | Required? | Description
:---------------|:-------|:----------|:-----------
 `acs`          | string | No        | The ACS release to use. Defaults to the most recent version.

Returns information about the specified table in the specified release. Information returned includes the table's title, subject area, universe, a list of topics covered by the table, and a list of columns contained in the table.

### Geography

#### `GET /1.0/geo/suggest`

 Query Argument | Type   | Required? | Description
:---------------|:-------|:----------|:-----------
 `q`            | string | Yes       | The string to use for suggestions.

Retrieves a list of geography suggestions given a search term using `q`. This is meant to support autocomplete text boxes.

#### `GET /1.0/geo/elasticsearch`

 Query Argument | Type   | Required? | Description
:---------------|:-------|:----------|:-----------
 `q`            | string | Yes       | The string to use for suggestions.
 `start`        | int    | No        | Where in the results list to start.
 `size`         | int    | No        | The number of results to return.
 `sumlevs`      | string | No        | A column-separated list of 3-digit summary level codes to limit the search by.

Returns geography information relevant to the search term given in `q`. This endpoint supports paging using the `start` and `size` parameters. You can narrow your search by specifying a comma-separated list of 3-digit summary levels with the `sumlevs` parameter.

#### `GET /1.0/geo/tiger2012/tiles/<sumlevel>/<zoom>/<x>/<y>.geojson`

 URL Argument    | Type   | Required? | Description
:----------------|:-------|:----------|:-----------
 `sumlevel`      | string | Yes       | The summary to use in the tile.
 `zoom`          | int    | Yes       | The zoom level for the tile.
 `x`             | int    | Yes       | The x value for the tile.
 `y`             | int    | Yes       | The y value for the tile.

Returns a [GeoJSON](http://geojson.org/) representation of all geographies at summary level `sumlevel` and contained within a [map tile](http://www.maptiler.org/google-maps-coordinates-tile-bounds-projection/) specified by the `zoom`, `x`, and `y` parameters. You can use this to create a map of Census geographies on top of an existing map. The returned GeoJSON data includes attributes for the name and geoid of the geography.

#### `GET /1.0/geo/tiger2012/<geoid>`

 URL Argument    | Type   | Required? | Description
:----------------|:-------|:----------|:-----------
 `geoid`         | string | Yes       | The geography identifier to retrieve data for.

 Query Argument | Type   | Required? | Description
:---------------|:-------|:----------|:-----------
 `geom`         | bool   | No        | Whether or not to include the geography portion of the GeoJSON.

Returns a [GeoJSON](http://geojson.org/) representation of the specified Census geography specified by the `geoid` parameter. By default, the returned GeoJSON only contains the attributes for the geography (including the land and water area, name, and geography ID). You can include the geography by setting the `geom` query argument to `true`. Note that this will usually make the response significantly larger, but will allow you to draw it on a map.

#### `GET /1.0/geo/tiger2012/<geoid>/parents`

 URL Argument    | Type   | Required? | Description
:----------------|:-------|:----------|:-----------
 `geoid`         | string | Yes       | The geography identifier to retrieve parent geographies for.

Returns a list of geographies that might be considered the parent of the specified geography. The information returned includes the name, geoid, and summary level code for each geography.

In some cases, the requested geography sits in multiple parents of the same summary level. In these cases, the `coverage` attribute returned for each parent of the same summary level will specify how much of this geography sits inside that parent geography (in percent).

This endpoint will also return the specified geography with a `relation` of `this`.

#### `GET /1.0/geo/show/tiger2012`

 Query Argument | Type   | Required? | Description
:---------------|:-------|:----------|:-----------
 `geo_ids`      | string | Yes       | A comma-separated list of geographies to request information about.

Returns a [GeoJSON](http://geojson.org/) representation of the specified comma-separated list of Census geographies. Each item in the comma-separated list can either be a single geoid or a "geoid grouping" specified by `<child summary level>|<parent geoid>`. A grouping is a shortcut so you don't have to specify individual geoids for contiguous groups of geographies. For example, to get states (summary level `040`) in the United States (geoid `01000US`), you'd use `040|01000US` as an element in your `geo_ids` list.

The data included will always have the geography data included. Unlike the single-geography request above you cannot disable geography representation in the response.

The attributes in the response will only include the geography name and the geoid.

### Data Retrieval

#### `GET /1.0/data/show/<acs>`

 URL Argument    | Type   | Required? | Description
:----------------|:-------|:----------|:-----------
 `acs`           | string | Yes       | The release to use for this data.

 Query Argument | Type   | Required? | Description
:---------------|:-------|:----------|:-----------
 `table_ids`    | string | Yes       | A comma-separated list of table IDs to request data for.
 `geo_ids`      | string | Yes       | A comma-separated list of geographies to request information about.

Returns the data for the given comma-separated list of table IDs in the given geo IDs. The data includes basic information about the specified tables and geographies along with the estimate and error data.

The `acs` parameter specifies which release to use. If you aren't sure, use the word `latest` and we will pick the most recent release that contains data for all the tables across lal the geographies you asked for.

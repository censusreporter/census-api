CREATE TABLE profile_search_metadata AS (
    SELECT display_name, sumlevel, full_geoid,
        setweight(to_tsvector(coalesce(display_name, ' ')), 'A') AS document
    FROM (
        SELECT DISTINCT geoid, sumlevel, population, display_name, full_geoid, priority
        FROM tiger2014.census_name_lookup
    ) profile_search
    WHERE full_geoid = profile_search.full_geoid
    --GROUP BY display_name, sumlevel, full_geoid
    ORDER BY priority, population DESC NULLS LAST
);
ALTER TABLE profile_search_metadata OWNER TO census;
CREATE INDEX ON profile_search_metadata USING GIN(document);

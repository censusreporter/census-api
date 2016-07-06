CREATE TABLE profile_search_metadata AS (
    SELECT geoid, sumlevel, population, display_name, full_geoid, priority, 
        setweight(to_tsvector(coalesce(display_name, ' ')), 'A') AS document
    FROM (
        SELECT DISTINCT geoid, sumlevel, population, display_name, full_geoid, priority
        FROM tiger2014.census_name_lookup
    ) profile_search
    WHERE full_geoid = profile_search.full_geoid
    ORDER BY priority, population DESC NULLS LAST
);
ALTER TABLE profile_search_metadata OWNER TO census;
CREATE INDEX ON profile_search_metadata USING GIN(document);

-- Warning: This query will take a while to execute. There are a lot
-- of places in the US.
-- 
-- Profiles metadata table is created out of relevant information
-- that was used in the search before. Only display_name is tranformed
-- into a tsvector, because there isn't much more information about places
-- that we have access to.
--
-- This script has been integrated into metadata_script.sql.
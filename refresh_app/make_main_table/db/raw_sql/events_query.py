# query to get data from mat. view trade_news_events_raw and added status column
SQL_TRADEEVENTS = """
SELECT 
    tne2.id,
    tne2.classes,
    tne2.itc_codes,
    tne2.locations,
    tne2.product,
    tne2.title,
    tne2.url,
    tne2.dates,
    tne2.article_ids,
    ids_status.status
FROM trade_news_events_raw tne2
JOIN ( SELECT distinct article_ids.id,
               COALESCE(united_ids.status, 'not_seen'::text) AS status
        FROM ( SELECT tne.id,
                unnest(tne.article_ids)::integer AS article_id
               FROM trade_news_events_raw tne) article_ids
        LEFT JOIN ( SELECT ne.excluded_id AS id, 'excluded_id'::text AS status
                    FROM newsfeedner_excludedids ne
                        UNION
                    SELECT nc.checked_id AS id, 'checked_id'::text AS status
                    FROM newsfeedner_checkedids nc
                    	UNION
                    SELECT nd.duplicated_id AS id, 'duplicated_id'::text AS status
                    FROM newsfeedner_duplicatedids nd) united_ids                     
        ON article_ids.article_id = united_ids.id) ids_status 
ON tne2.id = ids_status.id
ORDER BY tne2.dates DESC ;
"""

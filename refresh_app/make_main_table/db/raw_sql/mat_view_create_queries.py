SQL_drop_mat_views = """
DROP MATERIALIZED VIEW IF EXISTS public.trade_news_events_raw; 
-- DROP MATERIALIZED VIEW IF EXISTS public.trade_news_events; 
DROP MATERIALIZED VIEW IF EXISTS public.trade_news_events_main;
DROP MATERIALIZED VIEW IF EXISTS public.trade_news_events_minpromtorg;
DROP MATERIALIZED VIEW IF EXISTS public.trade_news_view;
DROP MATERIALIZED VIEW IF EXISTS public.trade_news_view_all;
"""

#  creates new trade_news_view on start
SQL_trade_news_view = """
CREATE MATERIALIZED VIEW public.trade_news_view
TABLESPACE pg_default
AS SELECT na.id,
    na.feed_id,
    na.id_in_feed,
    na.title,
    na.published_parsed,
    na.url,
    na.text
   FROM newsfeedner_article na
   JOIN trade_news_classification tnc ON na.id = tnc.article_id
   JOIN newsfeedner_feed nf ON na.feed_id = nf.id
   LEFT JOIN (SELECT ne.excluded_id AS id
              FROM newsfeedner_excludedids ne
                 UNION
              SELECT nc.checked_id AS id
              FROM newsfeedner_checkedids nc
                UNION
              SELECT nd.duplicated_id AS id
              FROM newsfeedner_duplicatedids nd
                UNION
              SELECT napp.approved_id AS id
              FROM newsfeedner_approvedids napp) ids
   ON na.id=ids.id
   WHERE ids.id IS NULL
      AND nf.used = True
      AND (tnc.investment >= 0.6::double precision 
         OR tnc.projects >= 0.6::double precision 
         OR tnc.trade >= 0.6::double precision 
         OR tnc.sanction >= 0.6::double precision
         OR tnc.foreign_relations >= 0.6::double precision
         OR tnc.other_relations >=0.6::double precision)       
UNION
 SELECT na.id,
    na.feed_id,
    na.id_in_feed,
    na.title,
    na.published_parsed,
    na.url,
    na.text
   FROM newsfeedner_article na
     JOIN trade_news_classification tnc ON na.id = tnc.article_id
     JOIN newsfeedner_feed nf ON na.feed_id = nf.id
     LEFT JOIN (SELECT ne.excluded_id AS id
              FROM newsfeedner_excludedids ne
                 UNION
              SELECT nc.checked_id AS id
              FROM newsfeedner_checkedids nc
                UNION
              SELECT nd.duplicated_id AS id
              FROM newsfeedner_duplicatedids nd
                UNION
              SELECT napp.approved_id AS id
              FROM newsfeedner_approvedids napp) ids
   ON na.id=ids.id
   WHERE ids.id IS NULL
      AND (nf.name::text ~~* '%minpromtorg%'::text 
         OR nf.name::text ~~* '%torg_pred%'::text 
         OR nf.name::text ~~* '%exportcenter%'::text)
      AND (tnc.investment >= 0.2::double precision 
         OR tnc.projects >= 0.2::double precision 
         OR tnc.trade >= 0.2::double precision 
         OR tnc.sanction >= 0.2::double precision
         OR tnc.foreign_relations >= 0.2::double precision
         OR tnc.other_relations >=0.2::double precision)  
WITH DATA;

-- Permissions

ALTER TABLE public.trade_news_view OWNER TO postgres;
GRANT ALL ON TABLE public.trade_news_view TO postgres;
-- GRANT ALL ON TABLE public.trade_news_view TO readuser;
-- GRANT ALL ON TABLE public.trade_news_view TO newsfeeduser;
-- GRANT ALL ON TABLE public.trade_news_view TO trade_news_user;
-- GRANT SELECT ON TABLE public.trade_news_view TO read_user;
"""

SQL_trade_news_view_all = r"""
CREATE MATERIALIZED VIEW public.trade_news_view_all
TABLESPACE pg_default
AS SELECT na.id,
    na.feed_id,
    na.id_in_feed,
    na.title,
    na.published_parsed,
    na.url,
    na.text
   FROM newsfeedner_article na
   JOIN trade_news_classification tnc ON na.id = tnc.article_id
   JOIN newsfeedner_feed nf ON na.feed_id = nf.id
   WHERE  nf.used = True
      AND (tnc.investment >= 0.6::double precision 
         OR tnc.projects >= 0.6::double precision 
         OR tnc.trade >= 0.6::double precision 
         OR tnc.sanction >= 0.6::double precision
         OR tnc.foreign_relations >= 0.6::double precision
         OR tnc.other_relations >=0.6::double precision)       
UNION
 SELECT na.id,
    na.feed_id,
    na.id_in_feed,
    na.title,
    na.published_parsed,
    na.url,
    na.text
   FROM newsfeedner_article na
     JOIN trade_news_classification tnc ON na.id = tnc.article_id
     JOIN newsfeedner_feed nf ON na.feed_id = nf.id
   WHERE (nf.name::text ~~* '%minpromtorg%'::text 
         OR nf.name::text ~~* '%torg_pred%'::text 
         OR nf.name::text ~~* '%exportcenter%'::text)
      AND (tnc.investment >= 0.2::double precision 
         OR tnc.projects >= 0.2::double precision 
         OR tnc.trade >= 0.2::double precision 
         OR tnc.sanction >= 0.2::double precision
         OR tnc.foreign_relations >= 0.2::double precision
         OR tnc.other_relations >= 0.2::double precision)  
WITH DATA;

-- Permissions

ALTER TABLE public.trade_news_view_all OWNER TO postgres;
GRANT ALL ON TABLE public.trade_news_view_all TO postgres;
-- GRANT ALL ON TABLE public.trade_news_view TO readuser;
-- GRANT ALL ON TABLE public.trade_news_view TO newsfeeduser;
-- GRANT ALL ON TABLE public.trade_news_view TO trade_news_user;
-- GRANT SELECT ON TABLE public.trade_news_view TO read_user;
"""

SQL_events_main = r"""
CREATE MATERIALIZED VIEW public.trade_news_events_main
TABLESPACE pg_default
AS SELECT row_number() OVER () AS id,
    array_to_string(array_agg(DISTINCT grouped.classes), '; '::text) AS classes,
    array_to_string(array_agg(DISTINCT grouped.itc_codes), ';; '::text) AS itc_codes,
    array_to_string(array_agg(DISTINCT grouped.locations), ', '::text) AS locations,
    array_to_string(array_agg(DISTINCT grouped.product), ', '::text) AS product,
    (max(grouped.title) || '\n\n'::text) || max(grouped.text) AS title,
    max(grouped.url) AS url,
    array_agg(DISTINCT grouped.dates) AS dates,
    array_agg(DISTINCT grouped.article_ids) AS article_ids
   FROM ( SELECT array_to_string(array_agg(DISTINCT tnf2.smtk_code), ';; '::text) AS itc_codes,
            array_to_string(array_agg(DISTINCT tnf1.name), ', '::text) AS locations,
            array_to_string(array_agg(DISTINCT tnf2.name), ', '::text) AS product,
            max(tnv.title) AS title,
            max(tnv.text) AS text,
            max(tnv.url::text) AS url,
            array_to_string(array_agg(DISTINCT tnv.published_parsed), ', '::text) AS dates,
            array_to_string(array_agg(DISTINCT date_trunc('week'::text, tnv.published_parsed)), ', '::text) AS weekly,
            array_to_string(((((array_agg(DISTINCT investment.class))
                            || array_agg(DISTINCT projects.class))
                            || array_agg(DISTINCT trade.class))
                            || array_agg(DISTINCT sanction.class))
                            || array_agg(DISTINCT other_relations.class), '; '::text) AS classes,
            array_to_string(array_agg(DISTINCT tnv.id), ', '::text) AS article_ids
           FROM trade_news_view tnv
             JOIN ( SELECT DISTINCT tne.name,
                                    tnf.article_id
                    FROM trade_news_feedentities tnf
                    JOIN trade_news_entity tne 
                    ON tne.id = tnf.ent_id
                    WHERE tne.name::text !~~* '%росс%'::text 
                      AND tne.name::text !~~* '%рф%'::text 
                      AND tne.name::text !~~* '%москва%'::text
                      AND tne.name::text !~~* '%вашингтон%'::text
                      AND tne.name::text !~~* '%анкара%'::text
                      AND tne.name::text !~~* '%кабул%'::text 
                      AND tne.name::text !~~* '%пекин%'::text 
                      AND (tne.entity_class::text = 'COUNTRY'::text OR tne.entity_class::text = 'GPE'::text)) tnf1 
             ON tnf1.article_id = tnv.id
             JOIN ( SELECT DISTINCT tne.name,
                                    tne.smtk_code,
                                    tnf.article_id
                    FROM trade_news_feedentities tnf
                    JOIN trade_news_entity tne 
                    ON tne.id = tnf.ent_id
                    WHERE tne.entity_class::text = 'PRODUCT'::text 
                      AND tne.smtk_code IS NOT NULL 
                      AND tne.smtk_code::text <> '100 - not product'::text) tnf2 
                    ON tnf2.article_id = tnv.id
             LEFT JOIN ( SELECT 'Внешняя торговля'::text AS class,
                                tnc.article_id
                         FROM trade_news_classification tnc
                         WHERE (tnc.foreign_relations >= 0.6::double precision or tnc.trade >= 0.6::double precision) 
                         AND (tnc.foreign_relations > tnc.irrelevant OR tnc.trade > tnc.irrelevant)) trade 
             ON trade.article_id = tnv.id
             LEFT JOIN ( SELECT 'Инвестиции'::text AS class,
                                tnc.article_id
                         FROM trade_news_classification tnc
                         WHERE tnc.investment >= 0.6::double precision 
                         AND tnc.investment > tnc.irrelevant) investment 
             ON investment.article_id = tnv.id
             LEFT JOIN ( SELECT 'Совместные проекты и программы'::text AS class,
                              tnc.article_id
                         FROM trade_news_classification tnc
                         WHERE tnc.projects >= 0.6::double precision 
                         AND tnc.projects > tnc.irrelevant) projects 
             ON projects.article_id = tnv.id
             LEFT JOIN ( SELECT 'Санкции'::text AS class,
                          tnc.article_id
                         FROM trade_news_classification tnc
                         WHERE tnc.sanction >= 0.6::double precision 
                         AND tnc.sanction > tnc.irrelevant) sanction 
             ON sanction.article_id = tnv.id             
             LEFT JOIN ( SELECT 'Специальные отношения, не классифицированные по типу'::text AS class,
                          tnc.article_id
                         FROM trade_news_classification tnc
                         WHERE tnc.other_relations >= 0.6::double precision 
                         AND tnc.other_relations > tnc.irrelevant) other_relations 
             ON other_relations.article_id = tnv.id
          GROUP BY tnv.id
          ORDER BY tnv.id DESC) grouped
  GROUP BY grouped.weekly, grouped.product
  ORDER BY grouped.weekly DESC
WITH DATA;
"""
# creates new mat_view trade_news_events_main on start
SQL_events_minprom = r"""
CREATE MATERIALIZED VIEW public.trade_news_events_minpromtorg
TABLESPACE pg_default
AS SELECT row_number() OVER () AS id,
    array_to_string(array_agg(DISTINCT grouped.classes), '; '::text) AS classes,
    array_to_string(array_agg(DISTINCT grouped.itc_codes), ';; '::text) AS itc_codes,
    array_to_string(array_agg(DISTINCT grouped.locations), ', '::text) AS locations,
    array_to_string(array_agg(DISTINCT grouped.product), ', '::text) AS product,
    (max(grouped.title) || '\n\n'::text) || max(grouped.text) AS title,
    max(grouped.url) AS url,
    array_agg(DISTINCT grouped.dates) AS dates,
    array_agg(DISTINCT grouped.article_ids) AS article_ids
   FROM ( SELECT array_to_string(array_agg(DISTINCT tnf2.smtk_code), ';; '::text) AS itc_codes,
            array_to_string(array_agg(DISTINCT tnf1.name), ', '::text) AS locations,
            array_to_string(array_agg(DISTINCT tnf2.name), ', '::text) AS product,
            max(tnv.title) AS title,
            max(tnv.text) AS text,
            max(tnv.url::text) AS url,
            array_to_string(array_agg(DISTINCT tnv.published_parsed), ','::text) AS dates,
            array_to_string(array_agg(DISTINCT date_trunc('week'::text, tnv.published_parsed)), ','::text) AS weekly,
            array_to_string(((((array_agg(DISTINCT investment.class))
                            || array_agg(DISTINCT projects.class))
                            || array_agg(DISTINCT trade.class))
                            || array_agg(DISTINCT sanction.class))
                            || array_agg(DISTINCT other_relations.class), '; '::text) AS classes,
            array_to_string(array_agg(DISTINCT tnv.id), ', '::text) AS article_ids
           FROM trade_news_view tnv
           JOIN ( SELECT DISTINCT tne.name,
                    tnf.article_id
                  FROM trade_news_feedentities tnf 
                  JOIN trade_news_entity tne 
                  ON tne.id = tnf.ent_id
                  WHERE tne.name::text !~~* '%росс%'::text 
                    AND tne.name::text !~~* '%рф%'::text 
                    AND tne.name::text !~~* '%москва%'::text 
                    AND tne.name::text !~~* '%вашингтон%'::text 
                    AND tne.name::text !~~* '%анкара%'::text 
                    AND tne.name::text !~~* '%кабул%'::text 
                    AND tne.name::text !~~* '%пекин%'::text 
                    AND (tne.entity_class::text = 'COUNTRY'::text 
                      OR tne.entity_class::text = 'GPE'::text)) tnf1 ON tnf1.article_id = tnv.id
           JOIN ( SELECT DISTINCT tne.name,
                    tne.smtk_code,
                    tnf.article_id
                  FROM trade_news_feedentities tnf
                  JOIN trade_news_entity tne 
                  ON tne.id = tnf.ent_id
                  WHERE tne.entity_class::text = 'PRODUCT'::text 
                    AND tne.smtk_code IS NOT NULL 
                    AND tne.smtk_code::text <> '100 - not product'::text) tnf2 
           ON tnf2.article_id = tnv.id
           LEFT JOIN ( SELECT 'Внешняя торговля'::text AS class,
                                tnc.article_id
                         FROM trade_news_classification tnc
                         WHERE tnc.foreign_relations >= 0.3::double precision or tnc.trade >= 0.3::double precision) trade 
             ON trade.article_id = tnv.id
           LEFT JOIN ( SELECT 'Инвестиции'::text AS class,
                         tnc.article_id
                       FROM trade_news_classification tnc
                       WHERE tnc.investment >= 0.3::double precision) investment
           ON investment.article_id = tnv.id
           LEFT JOIN ( SELECT 'Совместные проекты и программы'::text AS class,
                            tnc.article_id
                       FROM trade_news_classification tnc
                       WHERE tnc.projects >= 0.3::double precision) projects 
           ON projects.article_id = tnv.id
           LEFT JOIN ( SELECT 'Санкции'::text AS class,
                         tnc.article_id
                       FROM trade_news_classification tnc
                       WHERE tnc.sanction >= 0.3::double precision) sanction 
           ON sanction.article_id = tnv.id
           LEFT JOIN ( SELECT 'Специальные отношения, не классифицированные по типу'::text AS class,
                       tnc.article_id
                       FROM trade_news_classification tnc
                       WHERE tnc.other_relations >= 0.6::double precision 
                         AND tnc.other_relations > tnc.irrelevant) other_relations 
           ON other_relations.article_id = tnv.id
           WHERE tnv.id_in_feed::text ~~* '%minpromtorg%'::text 
              OR tnv.id_in_feed::text ~~* '%exportcenter%'::text
          GROUP BY tnv.id
          ORDER BY tnv.id DESC) grouped
  GROUP BY grouped.weekly, grouped.product
  ORDER BY grouped.weekly DESC
WITH DATA;
"""

SQL_events_union_raw = """
-- public.trade_news_events_raw source

CREATE MATERIALIZED VIEW public.trade_news_events_raw
TABLESPACE pg_default
AS SELECT uuid_generate_v4() AS id,
    merged.classes,
    merged.itc_codes,
    merged.locations,
    merged.product,
    merged.title,
    merged.url,
    merged.dates,
    merged.article_ids
   FROM ( SELECT tnem.id,
            tnem.classes,
            tnem.itc_codes,
            tnem.locations,
            tnem.product,
            tnem.title,
            tnem.url,
            tnem.dates,
            tnem.article_ids
           FROM trade_news_events_main tnem
        UNION
         SELECT tnem2.id,
            tnem2.classes,
            tnem2.itc_codes,
            tnem2.locations,
            tnem2.product,
            tnem2.title,
            tnem2.url,
            tnem2.dates,
            tnem2.article_ids
           FROM trade_news_events_minpromtorg tnem2) merged
  ORDER BY merged.dates DESC
WITH DATA;

-- Permissions

ALTER TABLE public.trade_news_events_raw OWNER TO postgres;
GRANT ALL ON TABLE public.trade_news_events_raw TO postgres;
-- GRANT ALL ON TABLE public.trade_news_view TO readuser;
-- GRANT ALL ON TABLE public.trade_news_view TO newsfeeduser;
-- GRANT ALL ON TABLE public.trade_news_view TO trade_news_user;
-- GRANT SELECT ON TABLE public.trade_news_view TO read_user;
"""
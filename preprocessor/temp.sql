CREATE TABLE before AS 
SELECT MIN(chn.name) AS uncredited_voiced_character,
       MIN(t.title) AS russian_movie
FROM char_name AS chn,
     cast_info AS ci,
     company_name AS cn,
     company_type AS ct,
     movie_companies AS mc,
     role_type AS rt,
     title AS t
WHERE ci.note LIKE '%(voice)%'
  AND ci.note LIKE '%(uncredited)%'
  AND cn.country_code = '[ru]'
  AND rt.role = 'actor'
  AND t.production_year > 2005
  AND t.id = mc.movie_id
  AND t.id = ci.movie_id
  AND ci.movie_id = mc.movie_id
  AND chn.id = ci.person_role_id
  AND rt.id = ci.role_id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id;

DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/10a/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/10a/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS rt;
CREATE TABLE rt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/10a/rt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/10a/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(chn.name) AS uncredited_voiced_character, MIN(t.title) AS russian_movie
 FROM rt, ci, company_type AS ct, t, movie_companies AS mc, char_name AS chn, cn, 
WHERE t.id = mc.movie_id
AND t.id = ci.movie_id
AND ci.movie_id = mc.movie_id
AND chn.id = ci.person_role_id
AND rt.id = ci.role_id
AND cn.id = mc.company_id
AND ct.id = mc.company_type_id
;
.print 'Testing 10a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(chn.name) AS character,
       MIN(t.title) AS russian_mov_with_actor_producer
FROM char_name AS chn,
     cast_info AS ci,
     company_name AS cn,
     company_type AS ct,
     movie_companies AS mc,
     role_type AS rt,
     title AS t
WHERE ci.note LIKE '%(producer)%'
  AND cn.country_code = '[ru]'
  AND rt.role = 'actor'
  AND t.production_year > 2010
  AND t.id = mc.movie_id
  AND t.id = ci.movie_id
  AND ci.movie_id = mc.movie_id
  AND chn.id = ci.person_role_id
  AND rt.id = ci.role_id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id;

DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/10b/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/10b/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS rt;
CREATE TABLE rt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/10b/rt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/10b/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(chn.name) AS character, MIN(t.title) AS russian_mov_with_actor_producer
 FROM rt, char_name AS chn, ci, cn, t, movie_companies AS mc, company_type AS ct, 
WHERE t.id = mc.movie_id
AND t.id = ci.movie_id
AND ci.movie_id = mc.movie_id
AND chn.id = ci.person_role_id
AND rt.id = ci.role_id
AND cn.id = mc.company_id
AND ct.id = mc.company_type_id
;
.print 'Testing 10b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(chn.name) AS character,
       MIN(t.title) AS movie_with_american_producer
FROM char_name AS chn,
     cast_info AS ci,
     company_name AS cn,
     company_type AS ct,
     movie_companies AS mc,
     role_type AS rt,
     title AS t
WHERE ci.note LIKE '%(producer)%'
  AND cn.country_code = '[us]'
  AND t.production_year > 1990
  AND t.id = mc.movie_id
  AND t.id = ci.movie_id
  AND ci.movie_id = mc.movie_id
  AND chn.id = ci.person_role_id
  AND rt.id = ci.role_id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id;
DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/10c/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/10c/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/10c/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(chn.name) AS character, MIN(t.title) AS movie_with_american_producer
 FROM char_name AS chn, ci, role_type AS rt, movie_companies AS mc, cn, company_type AS ct, t, 
WHERE t.id = mc.movie_id
AND t.id = ci.movie_id
AND ci.movie_id = mc.movie_id
AND chn.id = ci.person_role_id
AND rt.id = ci.role_id
AND cn.id = mc.company_id
AND ct.id = mc.company_type_id
;
.print 'Testing 10c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(cn.name) AS from_company,
       MIN(lt.link) AS movie_link_type,
       MIN(t.title) AS non_polish_sequel_movie
FROM company_name AS cn,
     company_type AS ct,
     keyword AS k,
     link_type AS lt,
     movie_companies AS mc,
     movie_keyword AS mk,
     movie_link AS ml,
     title AS t
WHERE cn.country_code !='[pl]'
  AND (cn.name LIKE '%Film%'
       OR cn.name LIKE '%Warner%')
  AND ct.kind ='production companies'
  AND k.keyword ='sequel'
  AND lt.link LIKE '%follow%'
  AND mc.note IS NULL
  AND t.production_year BETWEEN 1950 AND 2000
  AND lt.id = ml.link_type_id
  AND ml.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND t.id = mc.movie_id
  AND mc.company_type_id = ct.id
  AND mc.company_id = cn.id
  AND ml.movie_id = mk.movie_id
  AND ml.movie_id = mc.movie_id
  AND mk.movie_id = mc.movie_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/11a/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS ct;
CREATE TABLE ct AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/11a/ct.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/11a/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS lt;
CREATE TABLE lt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/11a/lt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/11a/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/11a/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(cn.name) AS from_company, MIN(lt.link) AS movie_link_type, MIN(t.title) AS non_polish_sequel_movie
 FROM lt, movie_link AS ml, mc, ct, t, movie_keyword AS mk, cn, k, 
WHERE lt.id = ml.link_type_id
AND ml.movie_id = t.id
AND t.id = mk.movie_id
AND mk.keyword_id = k.id
AND t.id = mc.movie_id
AND mc.company_type_id = ct.id
AND mc.company_id = cn.id
AND ml.movie_id = mk.movie_id
AND ml.movie_id = mc.movie_id
AND mk.movie_id = mc.movie_id
;
.print 'Testing 11a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(cn.name) AS from_company,
       MIN(lt.link) AS movie_link_type,
       MIN(t.title) AS sequel_movie
FROM company_name AS cn,
     company_type AS ct,
     keyword AS k,
     link_type AS lt,
     movie_companies AS mc,
     movie_keyword AS mk,
     movie_link AS ml,
     title AS t
WHERE cn.country_code !='[pl]'
  AND (cn.name LIKE '%Film%'
       OR cn.name LIKE '%Warner%')
  AND ct.kind ='production companies'
  AND k.keyword ='sequel'
  AND lt.link LIKE '%follows%'
  AND mc.note IS NULL
  AND t.production_year = 1998
  AND t.title LIKE '%Money%'
  AND lt.id = ml.link_type_id
  AND ml.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND t.id = mc.movie_id
  AND mc.company_type_id = ct.id
  AND mc.company_id = cn.id
  AND ml.movie_id = mk.movie_id
  AND ml.movie_id = mc.movie_id
  AND mk.movie_id = mc.movie_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/11b/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS ct;
CREATE TABLE ct AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/11b/ct.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/11b/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS lt;
CREATE TABLE lt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/11b/lt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/11b/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/11b/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(cn.name) AS from_company, MIN(lt.link) AS movie_link_type, MIN(t.title) AS sequel_movie
 FROM ct, movie_link AS ml, k, mc, lt, movie_keyword AS mk, t, cn, 
WHERE lt.id = ml.link_type_id
AND ml.movie_id = t.id
AND t.id = mk.movie_id
AND mk.keyword_id = k.id
AND t.id = mc.movie_id
AND mc.company_type_id = ct.id
AND mc.company_id = cn.id
AND ml.movie_id = mk.movie_id
AND ml.movie_id = mc.movie_id
AND mk.movie_id = mc.movie_id
;
.print 'Testing 11b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(cn.name) AS from_company,
       MIN(mc.note) AS production_note,
       MIN(t.title) AS movie_based_on_book
FROM company_name AS cn,
     company_type AS ct,
     keyword AS k,
     link_type AS lt,
     movie_companies AS mc,
     movie_keyword AS mk,
     movie_link AS ml,
     title AS t
WHERE cn.country_code !='[pl]'
  AND (cn.name LIKE '20th Century Fox%'
       OR cn.name LIKE 'Twentieth Century Fox%')
  AND ct.kind != 'production companies'
  AND ct.kind IS NOT NULL
  AND k.keyword IN ('sequel',
                    'revenge',
                    'based-on-novel')
  AND mc.note IS NOT NULL
  AND t.production_year > 1950
  AND lt.id = ml.link_type_id
  AND ml.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND t.id = mc.movie_id
  AND mc.company_type_id = ct.id
  AND mc.company_id = cn.id
  AND ml.movie_id = mk.movie_id
  AND ml.movie_id = mc.movie_id
  AND mk.movie_id = mc.movie_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/11c/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS ct;
CREATE TABLE ct AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/11c/ct.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/11c/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/11c/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/11c/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(cn.name) AS from_company, MIN(mc.note) AS production_note, MIN(t.title) AS movie_based_on_book
 FROM ct, t, k, mc, movie_link AS ml, link_type AS lt, movie_keyword AS mk, cn, 
WHERE lt.id = ml.link_type_id
AND ml.movie_id = t.id
AND t.id = mk.movie_id
AND mk.keyword_id = k.id
AND t.id = mc.movie_id
AND mc.company_type_id = ct.id
AND mc.company_id = cn.id
AND ml.movie_id = mk.movie_id
AND ml.movie_id = mc.movie_id
AND mk.movie_id = mc.movie_id
;
.print 'Testing 11c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(cn.name) AS from_company,
       MIN(mc.note) AS production_note,
       MIN(t.title) AS movie_based_on_book
FROM company_name AS cn,
     company_type AS ct,
     keyword AS k,
     link_type AS lt,
     movie_companies AS mc,
     movie_keyword AS mk,
     movie_link AS ml,
     title AS t
WHERE cn.country_code !='[pl]'
  AND ct.kind != 'production companies'
  AND ct.kind IS NOT NULL
  AND k.keyword IN ('sequel',
                    'revenge',
                    'based-on-novel')
  AND mc.note IS NOT NULL
  AND t.production_year > 1950
  AND lt.id = ml.link_type_id
  AND ml.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND t.id = mc.movie_id
  AND mc.company_type_id = ct.id
  AND mc.company_id = cn.id
  AND ml.movie_id = mk.movie_id
  AND ml.movie_id = mc.movie_id
  AND mk.movie_id = mc.movie_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/11d/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS ct;
CREATE TABLE ct AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/11d/ct.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/11d/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/11d/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/11d/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(cn.name) AS from_company, MIN(mc.note) AS production_note, MIN(t.title) AS movie_based_on_book
 FROM cn, t, movie_link AS ml, link_type AS lt, k, mc, ct, movie_keyword AS mk, 
WHERE lt.id = ml.link_type_id
AND ml.movie_id = t.id
AND t.id = mk.movie_id
AND mk.keyword_id = k.id
AND t.id = mc.movie_id
AND mc.company_type_id = ct.id
AND mc.company_id = cn.id
AND ml.movie_id = mk.movie_id
AND ml.movie_id = mc.movie_id
AND mk.movie_id = mc.movie_id
;
.print 'Testing 11d.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(cn.name) AS movie_company,
       MIN(mi_idx.info) AS rating,
       MIN(t.title) AS drama_horror_movie
FROM company_name AS cn,
     company_type AS ct,
     info_type AS it1,
     info_type AS it2,
     movie_companies AS mc,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     title AS t
WHERE cn.country_code = '[us]'
  AND ct.kind = 'production companies'
  AND it1.info = 'genres'
  AND it2.info = 'rating'
  AND mi.info IN ('Drama',
                  'Horror')
  AND mi_idx.info > '8.0'
  AND t.production_year BETWEEN 2005 AND 2008
  AND t.id = mi.movie_id
  AND t.id = mi_idx.movie_id
  AND mi.info_type_id = it1.id
  AND mi_idx.info_type_id = it2.id
  AND t.id = mc.movie_id
  AND ct.id = mc.company_type_id
  AND cn.id = mc.company_id
  AND mc.movie_id = mi.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND mi.movie_id = mi_idx.movie_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/12a/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS ct;
CREATE TABLE ct AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/12a/ct.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/12a/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/12a/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/12a/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi_idx;
.mode csv
CREATE TABLE mi_idx (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, info_type_id integer NOT NULL, info character varying NOT NULL, note character varying(1));
.import --csv --skip 1 '../../queries/join-order-benchmark-redux/data/12a/mi_idx.csv' mi_idx
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/12a/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(cn.name) AS movie_company, MIN(mi_idx.info) AS rating, MIN(t.title) AS drama_horror_movie
 FROM mi_idx, it2, cn, ct, it1, movie_companies AS mc, t, mi, 
WHERE t.id = mi.movie_id
AND t.id = mi_idx.movie_id
AND mi.info_type_id = it1.id
AND mi_idx.info_type_id = it2.id
AND t.id = mc.movie_id
AND ct.id = mc.company_type_id
AND cn.id = mc.company_id
AND mc.movie_id = mi.movie_id
AND mc.movie_id = mi_idx.movie_id
AND mi.movie_id = mi_idx.movie_id
;
.print 'Testing 12a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(mi.info) AS budget,
       MIN(t.title) AS unsuccsessful_movie
FROM company_name AS cn,
     company_type AS ct,
     info_type AS it1,
     info_type AS it2,
     movie_companies AS mc,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     title AS t
WHERE cn.country_code ='[us]'
  AND ct.kind IS NOT NULL
  AND (ct.kind ='production companies'
       OR ct.kind = 'distributors')
  AND it1.info ='budget'
  AND it2.info ='bottom 10 rank'
  AND t.production_year >2000
  AND (t.title LIKE 'Birdemic%'
       OR t.title LIKE '%Movie%')
  AND t.id = mi.movie_id
  AND t.id = mi_idx.movie_id
  AND mi.info_type_id = it1.id
  AND mi_idx.info_type_id = it2.id
  AND t.id = mc.movie_id
  AND ct.id = mc.company_type_id
  AND cn.id = mc.company_id
  AND mc.movie_id = mi.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND mi.movie_id = mi_idx.movie_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/12b/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS ct;
CREATE TABLE ct AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/12b/ct.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/12b/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/12b/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/12b/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(mi.info) AS budget, MIN(t.title) AS unsuccsessful_movie
 FROM cn, it1, movie_companies AS mc, ct, movie_info_idx AS mi_idx, it2, movie_info AS mi, t, 
WHERE t.id = mi.movie_id
AND t.id = mi_idx.movie_id
AND mi.info_type_id = it1.id
AND mi_idx.info_type_id = it2.id
AND t.id = mc.movie_id
AND ct.id = mc.company_type_id
AND cn.id = mc.company_id
AND mc.movie_id = mi.movie_id
AND mc.movie_id = mi_idx.movie_id
AND mi.movie_id = mi_idx.movie_id
;
.print 'Testing 12b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(cn.name) AS movie_company,
       MIN(mi_idx.info) AS rating,
       MIN(t.title) AS mainstream_movie
FROM company_name AS cn,
     company_type AS ct,
     info_type AS it1,
     info_type AS it2,
     movie_companies AS mc,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     title AS t
WHERE cn.country_code = '[us]'
  AND ct.kind = 'production companies'
  AND it1.info = 'genres'
  AND it2.info = 'rating'
  AND mi.info IN ('Drama',
                  'Horror',
                  'Western',
                  'Family')
  AND mi_idx.info > '7.0'
  AND t.production_year BETWEEN 2000 AND 2010
  AND t.id = mi.movie_id
  AND t.id = mi_idx.movie_id
  AND mi.info_type_id = it1.id
  AND mi_idx.info_type_id = it2.id
  AND t.id = mc.movie_id
  AND ct.id = mc.company_type_id
  AND cn.id = mc.company_id
  AND mc.movie_id = mi.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND mi.movie_id = mi_idx.movie_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/12c/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS ct;
CREATE TABLE ct AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/12c/ct.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/12c/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/12c/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/12c/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi_idx;
.mode csv
CREATE TABLE mi_idx (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, info_type_id integer NOT NULL, info character varying NOT NULL, note character varying(1));
.import --csv --skip 1 '../../queries/join-order-benchmark-redux/data/12c/mi_idx.csv' mi_idx
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/12c/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(cn.name) AS movie_company, MIN(mi_idx.info) AS rating, MIN(t.title) AS mainstream_movie
 FROM t, it2, mi_idx, cn, ct, it1, movie_companies AS mc, mi, 
WHERE t.id = mi.movie_id
AND t.id = mi_idx.movie_id
AND mi.info_type_id = it1.id
AND mi_idx.info_type_id = it2.id
AND t.id = mc.movie_id
AND ct.id = mc.company_type_id
AND cn.id = mc.company_id
AND mc.movie_id = mi.movie_id
AND mc.movie_id = mi_idx.movie_id
AND mi.movie_id = mi_idx.movie_id
;
.print 'Testing 12c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(mi.info) AS release_date,
       MIN(miidx.info) AS rating,
       MIN(t.title) AS german_movie
FROM company_name AS cn,
     company_type AS ct,
     info_type AS it,
     info_type AS it2,
     kind_type AS kt,
     movie_companies AS mc,
     movie_info AS mi,
     movie_info_idx AS miidx,
     title AS t
WHERE cn.country_code ='[de]'
  AND ct.kind ='production companies'
  AND it.info ='rating'
  AND it2.info ='release dates'
  AND kt.kind ='movie'
  AND mi.movie_id = t.id
  AND it2.id = mi.info_type_id
  AND kt.id = t.kind_id
  AND mc.movie_id = t.id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id
  AND miidx.movie_id = t.id
  AND it.id = miidx.info_type_id
  AND mi.movie_id = miidx.movie_id
  AND mi.movie_id = mc.movie_id
  AND miidx.movie_id = mc.movie_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/13a/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS ct;
CREATE TABLE ct AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/13a/ct.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/13a/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it;
CREATE TABLE it AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/13a/it.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS kt;
CREATE TABLE kt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/13a/kt.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(mi.info) AS release_date, MIN(miidx.info) AS rating, MIN(t.title) AS german_movie
 FROM title AS t, ct, it2, movie_info AS mi, kt, cn, it, movie_info_idx AS miidx, movie_companies AS mc, 
WHERE mi.movie_id = t.id
AND it2.id = mi.info_type_id
AND kt.id = t.kind_id
AND mc.movie_id = t.id
AND cn.id = mc.company_id
AND ct.id = mc.company_type_id
AND miidx.movie_id = t.id
AND it.id = miidx.info_type_id
AND mi.movie_id = miidx.movie_id
AND mi.movie_id = mc.movie_id
AND miidx.movie_id = mc.movie_id
;
.print 'Testing 13a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(cn.name) AS producing_company,
       MIN(miidx.info) AS rating,
       MIN(t.title) AS movie_about_winning
FROM company_name AS cn,
     company_type AS ct,
     info_type AS it,
     info_type AS it2,
     kind_type AS kt,
     movie_companies AS mc,
     movie_info AS mi,
     movie_info_idx AS miidx,
     title AS t
WHERE cn.country_code ='[us]'
  AND ct.kind ='production companies'
  AND it.info ='rating'
  AND it2.info ='release dates'
  AND kt.kind ='movie'
  AND t.title != ''
  AND (t.title LIKE '%Champion%'
       OR t.title LIKE '%Loser%')
  AND mi.movie_id = t.id
  AND it2.id = mi.info_type_id
  AND kt.id = t.kind_id
  AND mc.movie_id = t.id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id
  AND miidx.movie_id = t.id
  AND it.id = miidx.info_type_id
  AND mi.movie_id = miidx.movie_id
  AND mi.movie_id = mc.movie_id
  AND miidx.movie_id = mc.movie_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/13b/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS ct;
CREATE TABLE ct AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/13b/ct.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/13b/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it;
CREATE TABLE it AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/13b/it.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS kt;
CREATE TABLE kt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/13b/kt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/13b/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(cn.name) AS producing_company, MIN(miidx.info) AS rating, MIN(t.title) AS movie_about_winning
 FROM movie_companies AS mc, movie_info_idx AS miidx, ct, it2, cn, kt, movie_info AS mi, it, t, 
WHERE mi.movie_id = t.id
AND it2.id = mi.info_type_id
AND kt.id = t.kind_id
AND mc.movie_id = t.id
AND cn.id = mc.company_id
AND ct.id = mc.company_type_id
AND miidx.movie_id = t.id
AND it.id = miidx.info_type_id
AND mi.movie_id = miidx.movie_id
AND mi.movie_id = mc.movie_id
AND miidx.movie_id = mc.movie_id
;
.print 'Testing 13b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(cn.name) AS producing_company,
       MIN(miidx.info) AS rating,
       MIN(t.title) AS movie_about_winning
FROM company_name AS cn,
     company_type AS ct,
     info_type AS it,
     info_type AS it2,
     kind_type AS kt,
     movie_companies AS mc,
     movie_info AS mi,
     movie_info_idx AS miidx,
     title AS t
WHERE cn.country_code ='[us]'
  AND ct.kind ='production companies'
  AND it.info ='rating'
  AND it2.info ='release dates'
  AND kt.kind ='movie'
  AND t.title != ''
  AND (t.title LIKE 'Champion%'
       OR t.title LIKE 'Loser%')
  AND mi.movie_id = t.id
  AND it2.id = mi.info_type_id
  AND kt.id = t.kind_id
  AND mc.movie_id = t.id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id
  AND miidx.movie_id = t.id
  AND it.id = miidx.info_type_id
  AND mi.movie_id = miidx.movie_id
  AND mi.movie_id = mc.movie_id
  AND miidx.movie_id = mc.movie_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/13c/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS ct;
CREATE TABLE ct AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/13c/ct.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/13c/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it;
CREATE TABLE it AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/13c/it.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS kt;
CREATE TABLE kt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/13c/kt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/13c/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(cn.name) AS producing_company, MIN(miidx.info) AS rating, MIN(t.title) AS movie_about_winning
 FROM kt, it, cn, movie_info_idx AS miidx, t, ct, movie_info AS mi, it2, movie_companies AS mc, 
WHERE mi.movie_id = t.id
AND it2.id = mi.info_type_id
AND kt.id = t.kind_id
AND mc.movie_id = t.id
AND cn.id = mc.company_id
AND ct.id = mc.company_type_id
AND miidx.movie_id = t.id
AND it.id = miidx.info_type_id
AND mi.movie_id = miidx.movie_id
AND mi.movie_id = mc.movie_id
AND miidx.movie_id = mc.movie_id
;
.print 'Testing 13c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(cn.name) AS producing_company,
       MIN(miidx.info) AS rating,
       MIN(t.title) AS movie
FROM company_name AS cn,
     company_type AS ct,
     info_type AS it,
     info_type AS it2,
     kind_type AS kt,
     movie_companies AS mc,
     movie_info AS mi,
     movie_info_idx AS miidx,
     title AS t
WHERE cn.country_code ='[us]'
  AND ct.kind ='production companies'
  AND it.info ='rating'
  AND it2.info ='release dates'
  AND kt.kind ='movie'
  AND mi.movie_id = t.id
  AND it2.id = mi.info_type_id
  AND kt.id = t.kind_id
  AND mc.movie_id = t.id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id
  AND miidx.movie_id = t.id
  AND it.id = miidx.info_type_id
  AND mi.movie_id = miidx.movie_id
  AND mi.movie_id = mc.movie_id
  AND miidx.movie_id = mc.movie_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/13d/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS ct;
CREATE TABLE ct AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/13d/ct.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/13d/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it;
CREATE TABLE it AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/13d/it.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS kt;
CREATE TABLE kt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/13d/kt.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(cn.name) AS producing_company, MIN(miidx.info) AS rating, MIN(t.title) AS movie
 FROM it, kt, movie_info_idx AS miidx, title AS t, movie_companies AS mc, ct, it2, movie_info AS mi, cn, 
WHERE mi.movie_id = t.id
AND it2.id = mi.info_type_id
AND kt.id = t.kind_id
AND mc.movie_id = t.id
AND cn.id = mc.company_id
AND ct.id = mc.company_type_id
AND miidx.movie_id = t.id
AND it.id = miidx.info_type_id
AND mi.movie_id = miidx.movie_id
AND mi.movie_id = mc.movie_id
AND miidx.movie_id = mc.movie_id
;
.print 'Testing 13d.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(mi_idx.info) AS rating,
       MIN(t.title) AS northern_dark_movie
FROM info_type AS it1,
     info_type AS it2,
     keyword AS k,
     kind_type AS kt,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     movie_keyword AS mk,
     title AS t
WHERE it1.info = 'countries'
  AND it2.info = 'rating'
  AND k.keyword IN ('murder',
                    'murder-in-title',
                    'blood',
                    'violence')
  AND kt.kind = 'movie'
  AND mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Denish',
                  'Norwegian',
                  'German',
                  'USA',
                  'American')
  AND mi_idx.info < '8.5'
  AND t.production_year > 2010
  AND kt.id = t.kind_id
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND t.id = mi_idx.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND k.id = mk.keyword_id
  AND it1.id = mi.info_type_id
  AND it2.id = mi_idx.info_type_id;

DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/14a/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/14a/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/14a/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS kt;
CREATE TABLE kt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/14a/kt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/14a/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi_idx;
.mode csv
CREATE TABLE mi_idx (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, info_type_id integer NOT NULL, info character varying NOT NULL, note character varying(1));
.import --csv --skip 1 '../../queries/join-order-benchmark-redux/data/14a/mi_idx.csv' mi_idx
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/14a/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(mi_idx.info) AS rating, MIN(t.title) AS northern_dark_movie
 FROM mi_idx, kt, t, it1, k, movie_keyword AS mk, it2, mi, 
WHERE kt.id = t.kind_id
AND t.id = mi.movie_id
AND t.id = mk.movie_id
AND t.id = mi_idx.movie_id
AND mk.movie_id = mi.movie_id
AND mk.movie_id = mi_idx.movie_id
AND mi.movie_id = mi_idx.movie_id
AND k.id = mk.keyword_id
AND it1.id = mi.info_type_id
AND it2.id = mi_idx.info_type_id
;
.print 'Testing 14a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(mi_idx.info) AS rating,
       MIN(t.title) AS western_dark_production
FROM info_type AS it1,
     info_type AS it2,
     keyword AS k,
     kind_type AS kt,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     movie_keyword AS mk,
     title AS t
WHERE it1.info = 'countries'
  AND it2.info = 'rating'
  AND k.keyword IN ('murder',
                    'murder-in-title')
  AND kt.kind = 'movie'
  AND mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Denish',
                  'Norwegian',
                  'German',
                  'USA',
                  'American')
  AND mi_idx.info > '6.0'
  AND t.production_year > 2010
  AND (t.title LIKE '%murder%'
       OR t.title LIKE '%Murder%'
       OR t.title LIKE '%Mord%')
  AND kt.id = t.kind_id
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND t.id = mi_idx.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND k.id = mk.keyword_id
  AND it1.id = mi.info_type_id
  AND it2.id = mi_idx.info_type_id;

DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/14b/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/14b/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/14b/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS kt;
CREATE TABLE kt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/14b/kt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/14b/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi_idx;
.mode csv
CREATE TABLE mi_idx (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, info_type_id integer NOT NULL, info character varying NOT NULL, note character varying(1));
.import --csv --skip 1 '../../queries/join-order-benchmark-redux/data/14b/mi_idx.csv' mi_idx
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/14b/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(mi_idx.info) AS rating, MIN(t.title) AS western_dark_production
 FROM it1, t, mi_idx, kt, it2, mi, k, movie_keyword AS mk, 
WHERE kt.id = t.kind_id
AND t.id = mi.movie_id
AND t.id = mk.movie_id
AND t.id = mi_idx.movie_id
AND mk.movie_id = mi.movie_id
AND mk.movie_id = mi_idx.movie_id
AND mi.movie_id = mi_idx.movie_id
AND k.id = mk.keyword_id
AND it1.id = mi.info_type_id
AND it2.id = mi_idx.info_type_id
;
.print 'Testing 14b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(mi_idx.info) AS rating,
       MIN(t.title) AS north_european_dark_production
FROM info_type AS it1,
     info_type AS it2,
     keyword AS k,
     kind_type AS kt,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     movie_keyword AS mk,
     title AS t
WHERE it1.info = 'countries'
  AND it2.info = 'rating'
  AND k.keyword IS NOT NULL
  AND k.keyword IN ('murder',
                    'murder-in-title',
                    'blood',
                    'violence')
  AND kt.kind IN ('movie',
                  'episode')
  AND mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Danish',
                  'Norwegian',
                  'German',
                  'USA',
                  'American')
  AND mi_idx.info < '8.5'
  AND t.production_year > 2005
  AND kt.id = t.kind_id
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND t.id = mi_idx.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND k.id = mk.keyword_id
  AND it1.id = mi.info_type_id
  AND it2.id = mi_idx.info_type_id;

DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/14c/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/14c/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/14c/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS kt;
CREATE TABLE kt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/14c/kt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/14c/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi_idx;
.mode csv
CREATE TABLE mi_idx (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, info_type_id integer NOT NULL, info character varying NOT NULL, note character varying(1));
.import --csv --skip 1 '../../queries/join-order-benchmark-redux/data/14c/mi_idx.csv' mi_idx
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/14c/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(mi_idx.info) AS rating, MIN(t.title) AS north_european_dark_production
 FROM kt, k, t, it1, it2, mi_idx, mi, movie_keyword AS mk, 
WHERE kt.id = t.kind_id
AND t.id = mi.movie_id
AND t.id = mk.movie_id
AND t.id = mi_idx.movie_id
AND mk.movie_id = mi.movie_id
AND mk.movie_id = mi_idx.movie_id
AND mi.movie_id = mi_idx.movie_id
AND k.id = mk.keyword_id
AND it1.id = mi.info_type_id
AND it2.id = mi_idx.info_type_id
;
.print 'Testing 14c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(mi.info) AS release_date,
       MIN(t.title) AS internet_movie
FROM aka_title AS at,
     company_name AS cn,
     company_type AS ct,
     info_type AS it1,
     keyword AS k,
     movie_companies AS mc,
     movie_info AS mi,
     movie_keyword AS mk,
     title AS t
WHERE cn.country_code = '[us]'
  AND it1.info = 'release dates'
  AND mc.note LIKE '%(200%)%'
  AND mc.note LIKE '%(worldwide)%'
  AND mi.note LIKE '%internet%'
  AND mi.info LIKE 'USA:% 200%'
  AND t.production_year > 2000
  AND t.id = at.movie_id
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND t.id = mc.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mc.movie_id
  AND mk.movie_id = at.movie_id
  AND mi.movie_id = mc.movie_id
  AND mi.movie_id = at.movie_id
  AND mc.movie_id = at.movie_id
  AND k.id = mk.keyword_id
  AND it1.id = mi.info_type_id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/15a/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/15a/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/15a/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/15a/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/15a/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(mi.info) AS release_date, MIN(t.title) AS internet_movie
 FROM keyword AS k, aka_title AS at, cn, it1, t, company_type AS ct, movie_keyword AS mk, mi, mc, 
WHERE t.id = at.movie_id
AND t.id = mi.movie_id
AND t.id = mk.movie_id
AND t.id = mc.movie_id
AND mk.movie_id = mi.movie_id
AND mk.movie_id = mc.movie_id
AND mk.movie_id = at.movie_id
AND mi.movie_id = mc.movie_id
AND mi.movie_id = at.movie_id
AND mc.movie_id = at.movie_id
AND k.id = mk.keyword_id
AND it1.id = mi.info_type_id
AND cn.id = mc.company_id
AND ct.id = mc.company_type_id
;
.print 'Testing 15a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(mi.info) AS release_date,
       MIN(t.title) AS youtube_movie
FROM aka_title AS at,
     company_name AS cn,
     company_type AS ct,
     info_type AS it1,
     keyword AS k,
     movie_companies AS mc,
     movie_info AS mi,
     movie_keyword AS mk,
     title AS t
WHERE cn.country_code = '[us]'
  AND cn.name = 'YouTube'
  AND it1.info = 'release dates'
  AND mc.note LIKE '%(200%)%'
  AND mc.note LIKE '%(worldwide)%'
  AND mi.note LIKE '%internet%'
  AND mi.info LIKE 'USA:% 200%'
  AND t.production_year BETWEEN 2005 AND 2010
  AND t.id = at.movie_id
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND t.id = mc.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mc.movie_id
  AND mk.movie_id = at.movie_id
  AND mi.movie_id = mc.movie_id
  AND mi.movie_id = at.movie_id
  AND mc.movie_id = at.movie_id
  AND k.id = mk.keyword_id
  AND it1.id = mi.info_type_id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/15b/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/15b/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/15b/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/15b/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/15b/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(mi.info) AS release_date, MIN(t.title) AS youtube_movie
 FROM keyword AS k, mc, mi, aka_title AS at, t, it1, company_type AS ct, movie_keyword AS mk, cn, 
WHERE t.id = at.movie_id
AND t.id = mi.movie_id
AND t.id = mk.movie_id
AND t.id = mc.movie_id
AND mk.movie_id = mi.movie_id
AND mk.movie_id = mc.movie_id
AND mk.movie_id = at.movie_id
AND mi.movie_id = mc.movie_id
AND mi.movie_id = at.movie_id
AND mc.movie_id = at.movie_id
AND k.id = mk.keyword_id
AND it1.id = mi.info_type_id
AND cn.id = mc.company_id
AND ct.id = mc.company_type_id
;
.print 'Testing 15b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(mi.info) AS release_date,
       MIN(t.title) AS modern_american_internet_movie
FROM aka_title AS at,
     company_name AS cn,
     company_type AS ct,
     info_type AS it1,
     keyword AS k,
     movie_companies AS mc,
     movie_info AS mi,
     movie_keyword AS mk,
     title AS t
WHERE cn.country_code = '[us]'
  AND it1.info = 'release dates'
  AND mi.note LIKE '%internet%'
  AND mi.info IS NOT NULL
  AND (mi.info LIKE 'USA:% 199%'
       OR mi.info LIKE 'USA:% 200%')
  AND t.production_year > 1990
  AND t.id = at.movie_id
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND t.id = mc.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mc.movie_id
  AND mk.movie_id = at.movie_id
  AND mi.movie_id = mc.movie_id
  AND mi.movie_id = at.movie_id
  AND mc.movie_id = at.movie_id
  AND k.id = mk.keyword_id
  AND it1.id = mi.info_type_id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/15c/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/15c/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/15c/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/15c/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(mi.info) AS release_date, MIN(t.title) AS modern_american_internet_movie
 FROM cn, aka_title AS at, company_type AS ct, movie_companies AS mc, t, keyword AS k, movie_keyword AS mk, it1, mi, 
WHERE t.id = at.movie_id
AND t.id = mi.movie_id
AND t.id = mk.movie_id
AND t.id = mc.movie_id
AND mk.movie_id = mi.movie_id
AND mk.movie_id = mc.movie_id
AND mk.movie_id = at.movie_id
AND mi.movie_id = mc.movie_id
AND mi.movie_id = at.movie_id
AND mc.movie_id = at.movie_id
AND k.id = mk.keyword_id
AND it1.id = mi.info_type_id
AND cn.id = mc.company_id
AND ct.id = mc.company_type_id
;
.print 'Testing 15c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(at.title) AS aka_title,
       MIN(t.title) AS internet_movie_title
FROM aka_title AS at,
     company_name AS cn,
     company_type AS ct,
     info_type AS it1,
     keyword AS k,
     movie_companies AS mc,
     movie_info AS mi,
     movie_keyword AS mk,
     title AS t
WHERE cn.country_code = '[us]'
  AND it1.info = 'release dates'
  AND mi.note LIKE '%internet%'
  AND t.production_year > 1990
  AND t.id = at.movie_id
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND t.id = mc.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mc.movie_id
  AND mk.movie_id = at.movie_id
  AND mi.movie_id = mc.movie_id
  AND mi.movie_id = at.movie_id
  AND mc.movie_id = at.movie_id
  AND k.id = mk.keyword_id
  AND it1.id = mi.info_type_id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/15d/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/15d/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/15d/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/15d/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(at.title) AS aka_title, MIN(t.title) AS internet_movie_title
 FROM aka_title AS at, cn, keyword AS k, t, company_type AS ct, movie_companies AS mc, mi, movie_keyword AS mk, it1, 
WHERE t.id = at.movie_id
AND t.id = mi.movie_id
AND t.id = mk.movie_id
AND t.id = mc.movie_id
AND mk.movie_id = mi.movie_id
AND mk.movie_id = mc.movie_id
AND mk.movie_id = at.movie_id
AND mi.movie_id = mc.movie_id
AND mi.movie_id = at.movie_id
AND mc.movie_id = at.movie_id
AND k.id = mk.keyword_id
AND it1.id = mi.info_type_id
AND cn.id = mc.company_id
AND ct.id = mc.company_type_id
;
.print 'Testing 15d.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(an.name) AS cool_actor_pseudonym,
       MIN(t.title) AS series_named_after_char
FROM aka_name AS an,
     cast_info AS ci,
     company_name AS cn,
     keyword AS k,
     movie_companies AS mc,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE cn.country_code ='[us]'
  AND k.keyword ='character-name-in-title'
  AND t.episode_nr >= 50
  AND t.episode_nr < 100
  AND an.person_id = n.id
  AND n.id = ci.person_id
  AND ci.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND t.id = mc.movie_id
  AND mc.company_id = cn.id
  AND an.person_id = ci.person_id
  AND ci.movie_id = mc.movie_id
  AND ci.movie_id = mk.movie_id
  AND mc.movie_id = mk.movie_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/16a/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/16a/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/16a/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(an.name) AS cool_actor_pseudonym, MIN(t.title) AS series_named_after_char
 FROM cast_info AS ci, cn, t, aka_name AS an, k, movie_companies AS mc, movie_keyword AS mk, name AS n, 
WHERE an.person_id = n.id
AND n.id = ci.person_id
AND ci.movie_id = t.id
AND t.id = mk.movie_id
AND mk.keyword_id = k.id
AND t.id = mc.movie_id
AND mc.company_id = cn.id
AND an.person_id = ci.person_id
AND ci.movie_id = mc.movie_id
AND ci.movie_id = mk.movie_id
AND mc.movie_id = mk.movie_id
;
.print 'Testing 16a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(an.name) AS cool_actor_pseudonym,
       MIN(t.title) AS series_named_after_char
FROM aka_name AS an,
     cast_info AS ci,
     company_name AS cn,
     keyword AS k,
     movie_companies AS mc,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE cn.country_code ='[us]'
  AND k.keyword ='character-name-in-title'
  AND an.person_id = n.id
  AND n.id = ci.person_id
  AND ci.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND t.id = mc.movie_id
  AND mc.company_id = cn.id
  AND an.person_id = ci.person_id
  AND ci.movie_id = mc.movie_id
  AND ci.movie_id = mk.movie_id
  AND mc.movie_id = mk.movie_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/16b/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/16b/k.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(an.name) AS cool_actor_pseudonym, MIN(t.title) AS series_named_after_char
 FROM k, aka_name AS an, movie_keyword AS mk, title AS t, movie_companies AS mc, cast_info AS ci, name AS n, cn, 
WHERE an.person_id = n.id
AND n.id = ci.person_id
AND ci.movie_id = t.id
AND t.id = mk.movie_id
AND mk.keyword_id = k.id
AND t.id = mc.movie_id
AND mc.company_id = cn.id
AND an.person_id = ci.person_id
AND ci.movie_id = mc.movie_id
AND ci.movie_id = mk.movie_id
AND mc.movie_id = mk.movie_id
;
.print 'Testing 16b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(an.name) AS cool_actor_pseudonym,
       MIN(t.title) AS series_named_after_char
FROM aka_name AS an,
     cast_info AS ci,
     company_name AS cn,
     keyword AS k,
     movie_companies AS mc,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE cn.country_code ='[us]'
  AND k.keyword ='character-name-in-title'
  AND t.episode_nr < 100
  AND an.person_id = n.id
  AND n.id = ci.person_id
  AND ci.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND t.id = mc.movie_id
  AND mc.company_id = cn.id
  AND an.person_id = ci.person_id
  AND ci.movie_id = mc.movie_id
  AND ci.movie_id = mk.movie_id
  AND mc.movie_id = mk.movie_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/16c/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/16c/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/16c/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(an.name) AS cool_actor_pseudonym, MIN(t.title) AS series_named_after_char
 FROM movie_keyword AS mk, k, aka_name AS an, cast_info AS ci, movie_companies AS mc, t, name AS n, cn, 
WHERE an.person_id = n.id
AND n.id = ci.person_id
AND ci.movie_id = t.id
AND t.id = mk.movie_id
AND mk.keyword_id = k.id
AND t.id = mc.movie_id
AND mc.company_id = cn.id
AND an.person_id = ci.person_id
AND ci.movie_id = mc.movie_id
AND ci.movie_id = mk.movie_id
AND mc.movie_id = mk.movie_id
;
.print 'Testing 16c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(an.name) AS cool_actor_pseudonym,
       MIN(t.title) AS series_named_after_char
FROM aka_name AS an,
     cast_info AS ci,
     company_name AS cn,
     keyword AS k,
     movie_companies AS mc,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE cn.country_code ='[us]'
  AND k.keyword ='character-name-in-title'
  AND t.episode_nr >= 5
  AND t.episode_nr < 100
  AND an.person_id = n.id
  AND n.id = ci.person_id
  AND ci.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND t.id = mc.movie_id
  AND mc.company_id = cn.id
  AND an.person_id = ci.person_id
  AND ci.movie_id = mc.movie_id
  AND ci.movie_id = mk.movie_id
  AND mc.movie_id = mk.movie_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/16d/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/16d/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/16d/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(an.name) AS cool_actor_pseudonym, MIN(t.title) AS series_named_after_char
 FROM aka_name AS an, cast_info AS ci, movie_companies AS mc, movie_keyword AS mk, cn, t, k, name AS n, 
WHERE an.person_id = n.id
AND n.id = ci.person_id
AND ci.movie_id = t.id
AND t.id = mk.movie_id
AND mk.keyword_id = k.id
AND t.id = mc.movie_id
AND mc.company_id = cn.id
AND an.person_id = ci.person_id
AND ci.movie_id = mc.movie_id
AND ci.movie_id = mk.movie_id
AND mc.movie_id = mk.movie_id
;
.print 'Testing 16d.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(n.name) AS member_in_charnamed_american_movie,
       MIN(n.name) AS a1
FROM cast_info AS ci,
     company_name AS cn,
     keyword AS k,
     movie_companies AS mc,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE cn.country_code ='[us]'
  AND k.keyword ='character-name-in-title'
  AND n.name LIKE 'B%'
  AND n.id = ci.person_id
  AND ci.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND t.id = mc.movie_id
  AND mc.company_id = cn.id
  AND ci.movie_id = mc.movie_id
  AND ci.movie_id = mk.movie_id
  AND mc.movie_id = mk.movie_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/17a/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/17a/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/17a/n.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(n.name) AS member_in_charnamed_american_movie, MIN(n.name) AS a1
 FROM k, movie_keyword AS mk, cast_info AS ci, movie_companies AS mc, n, title AS t, cn, 
WHERE n.id = ci.person_id
AND ci.movie_id = t.id
AND t.id = mk.movie_id
AND mk.keyword_id = k.id
AND t.id = mc.movie_id
AND mc.company_id = cn.id
AND ci.movie_id = mc.movie_id
AND ci.movie_id = mk.movie_id
AND mc.movie_id = mk.movie_id
;
.print 'Testing 17a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(n.name) AS member_in_charnamed_movie,
       MIN(n.name) AS a1
FROM cast_info AS ci,
     company_name AS cn,
     keyword AS k,
     movie_companies AS mc,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE k.keyword ='character-name-in-title'
  AND n.name LIKE 'Z%'
  AND n.id = ci.person_id
  AND ci.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND t.id = mc.movie_id
  AND mc.company_id = cn.id
  AND ci.movie_id = mc.movie_id
  AND ci.movie_id = mk.movie_id
  AND mc.movie_id = mk.movie_id;

DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/17b/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/17b/n.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(n.name) AS member_in_charnamed_movie, MIN(n.name) AS a1
 FROM movie_keyword AS mk, title AS t, n, k, company_name AS cn, cast_info AS ci, movie_companies AS mc, 
WHERE n.id = ci.person_id
AND ci.movie_id = t.id
AND t.id = mk.movie_id
AND mk.keyword_id = k.id
AND t.id = mc.movie_id
AND mc.company_id = cn.id
AND ci.movie_id = mc.movie_id
AND ci.movie_id = mk.movie_id
AND mc.movie_id = mk.movie_id
;
.print 'Testing 17b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(n.name) AS member_in_charnamed_movie,
       MIN(n.name) AS a1
FROM cast_info AS ci,
     company_name AS cn,
     keyword AS k,
     movie_companies AS mc,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE k.keyword ='character-name-in-title'
  AND n.name LIKE 'X%'
  AND n.id = ci.person_id
  AND ci.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND t.id = mc.movie_id
  AND mc.company_id = cn.id
  AND ci.movie_id = mc.movie_id
  AND ci.movie_id = mk.movie_id
  AND mc.movie_id = mk.movie_id;

DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/17c/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/17c/n.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(n.name) AS member_in_charnamed_movie, MIN(n.name) AS a1
 FROM cast_info AS ci, company_name AS cn, movie_keyword AS mk, movie_companies AS mc, k, n, title AS t, 
WHERE n.id = ci.person_id
AND ci.movie_id = t.id
AND t.id = mk.movie_id
AND mk.keyword_id = k.id
AND t.id = mc.movie_id
AND mc.company_id = cn.id
AND ci.movie_id = mc.movie_id
AND ci.movie_id = mk.movie_id
AND mc.movie_id = mk.movie_id
;
.print 'Testing 17c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(n.name) AS member_in_charnamed_movie
FROM cast_info AS ci,
     company_name AS cn,
     keyword AS k,
     movie_companies AS mc,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE k.keyword ='character-name-in-title'
  AND n.name LIKE '%Bert%'
  AND n.id = ci.person_id
  AND ci.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND t.id = mc.movie_id
  AND mc.company_id = cn.id
  AND ci.movie_id = mc.movie_id
  AND ci.movie_id = mk.movie_id
  AND mc.movie_id = mk.movie_id;

DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/17d/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/17d/n.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(n.name) AS member_in_charnamed_movie
 FROM title AS t, cast_info AS ci, company_name AS cn, movie_keyword AS mk, k, n, movie_companies AS mc, 
WHERE n.id = ci.person_id
AND ci.movie_id = t.id
AND t.id = mk.movie_id
AND mk.keyword_id = k.id
AND t.id = mc.movie_id
AND mc.company_id = cn.id
AND ci.movie_id = mc.movie_id
AND ci.movie_id = mk.movie_id
AND mc.movie_id = mk.movie_id
;
.print 'Testing 17d.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(n.name) AS member_in_charnamed_movie
FROM cast_info AS ci,
     company_name AS cn,
     keyword AS k,
     movie_companies AS mc,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE cn.country_code ='[us]'
  AND k.keyword ='character-name-in-title'
  AND n.id = ci.person_id
  AND ci.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND t.id = mc.movie_id
  AND mc.company_id = cn.id
  AND ci.movie_id = mc.movie_id
  AND ci.movie_id = mk.movie_id
  AND mc.movie_id = mk.movie_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/17e/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/17e/k.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(n.name) AS member_in_charnamed_movie
 FROM movie_keyword AS mk, name AS n, title AS t, cn, k, cast_info AS ci, movie_companies AS mc, 
WHERE n.id = ci.person_id
AND ci.movie_id = t.id
AND t.id = mk.movie_id
AND mk.keyword_id = k.id
AND t.id = mc.movie_id
AND mc.company_id = cn.id
AND ci.movie_id = mc.movie_id
AND ci.movie_id = mk.movie_id
AND mc.movie_id = mk.movie_id
;
.print 'Testing 17e.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(n.name) AS member_in_charnamed_movie
FROM cast_info AS ci,
     company_name AS cn,
     keyword AS k,
     movie_companies AS mc,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE k.keyword ='character-name-in-title'
  AND n.name LIKE '%B%'
  AND n.id = ci.person_id
  AND ci.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND t.id = mc.movie_id
  AND mc.company_id = cn.id
  AND ci.movie_id = mc.movie_id
  AND ci.movie_id = mk.movie_id
  AND mc.movie_id = mk.movie_id;

DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/17f/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/17f/n.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(n.name) AS member_in_charnamed_movie
 FROM title AS t, cast_info AS ci, movie_companies AS mc, movie_keyword AS mk, company_name AS cn, k, n, 
WHERE n.id = ci.person_id
AND ci.movie_id = t.id
AND t.id = mk.movie_id
AND mk.keyword_id = k.id
AND t.id = mc.movie_id
AND mc.company_id = cn.id
AND ci.movie_id = mc.movie_id
AND ci.movie_id = mk.movie_id
AND mc.movie_id = mk.movie_id
;
.print 'Testing 17f.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(mi.info) AS movie_budget,
       MIN(mi_idx.info) AS movie_votes,
       MIN(t.title) AS movie_title
FROM cast_info AS ci,
     info_type AS it1,
     info_type AS it2,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     name AS n,
     title AS t
WHERE ci.note IN ('(producer)',
                  '(executive producer)')
  AND it1.info = 'budget'
  AND it2.info = 'votes'
  AND n.gender = 'm'
  AND n.name LIKE '%Tim%'
  AND t.id = mi.movie_id
  AND t.id = mi_idx.movie_id
  AND t.id = ci.movie_id
  AND ci.movie_id = mi.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND n.id = ci.person_id
  AND it1.id = mi.info_type_id
  AND it2.id = mi_idx.info_type_id;

DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/18a/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/18a/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/18a/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/18a/n.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(mi.info) AS movie_budget, MIN(mi_idx.info) AS movie_votes, MIN(t.title) AS movie_title
 FROM title AS t, ci, it1, it2, movie_info_idx AS mi_idx, movie_info AS mi, n, 
WHERE t.id = mi.movie_id
AND t.id = mi_idx.movie_id
AND t.id = ci.movie_id
AND ci.movie_id = mi.movie_id
AND ci.movie_id = mi_idx.movie_id
AND mi.movie_id = mi_idx.movie_id
AND n.id = ci.person_id
AND it1.id = mi.info_type_id
AND it2.id = mi_idx.info_type_id
;
.print 'Testing 18a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(mi.info) AS movie_budget,
       MIN(mi_idx.info) AS movie_votes,
       MIN(t.title) AS movie_title
FROM cast_info AS ci,
     info_type AS it1,
     info_type AS it2,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     name AS n,
     title AS t
WHERE ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)')
  AND it1.info = 'genres'
  AND it2.info = 'rating'
  AND mi.info IN ('Horror',
                  'Thriller')
  AND mi.note IS NULL
  AND mi_idx.info > '8.0'
  AND n.gender IS NOT NULL
  AND n.gender = 'f'
  AND t.production_year BETWEEN 2008 AND 2014
  AND t.id = mi.movie_id
  AND t.id = mi_idx.movie_id
  AND t.id = ci.movie_id
  AND ci.movie_id = mi.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND n.id = ci.person_id
  AND it1.id = mi.info_type_id
  AND it2.id = mi_idx.info_type_id;

DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/18b/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/18b/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/18b/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/18b/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi_idx;
.mode csv
CREATE TABLE mi_idx (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, info_type_id integer NOT NULL, info character varying NOT NULL, note character varying(1));
.import --csv --skip 1 '../../queries/join-order-benchmark-redux/data/18b/mi_idx.csv' mi_idx
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/18b/n.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/18b/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(mi.info) AS movie_budget, MIN(mi_idx.info) AS movie_votes, MIN(t.title) AS movie_title
 FROM it1, ci, mi, mi_idx, n, t, it2, 
WHERE t.id = mi.movie_id
AND t.id = mi_idx.movie_id
AND t.id = ci.movie_id
AND ci.movie_id = mi.movie_id
AND ci.movie_id = mi_idx.movie_id
AND mi.movie_id = mi_idx.movie_id
AND n.id = ci.person_id
AND it1.id = mi.info_type_id
AND it2.id = mi_idx.info_type_id
;
.print 'Testing 18b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(mi.info) AS movie_budget,
       MIN(mi_idx.info) AS movie_votes,
       MIN(t.title) AS movie_title
FROM cast_info AS ci,
     info_type AS it1,
     info_type AS it2,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     name AS n,
     title AS t
WHERE ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)')
  AND it1.info = 'genres'
  AND it2.info = 'votes'
  AND mi.info IN ('Horror',
                  'Action',
                  'Sci-Fi',
                  'Thriller',
                  'Crime',
                  'War')
  AND n.gender = 'm'
  AND t.id = mi.movie_id
  AND t.id = mi_idx.movie_id
  AND t.id = ci.movie_id
  AND ci.movie_id = mi.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND n.id = ci.person_id
  AND it1.id = mi.info_type_id
  AND it2.id = mi_idx.info_type_id;

DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/18c/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/18c/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/18c/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/18c/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/18c/n.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(mi.info) AS movie_budget, MIN(mi_idx.info) AS movie_votes, MIN(t.title) AS movie_title
 FROM ci, it2, mi, n, title AS t, movie_info_idx AS mi_idx, it1, 
WHERE t.id = mi.movie_id
AND t.id = mi_idx.movie_id
AND t.id = ci.movie_id
AND ci.movie_id = mi.movie_id
AND ci.movie_id = mi_idx.movie_id
AND mi.movie_id = mi_idx.movie_id
AND n.id = ci.person_id
AND it1.id = mi.info_type_id
AND it2.id = mi_idx.info_type_id
;
.print 'Testing 18c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(n.name) AS voicing_actress,
       MIN(t.title) AS voiced_movie
FROM aka_name AS an,
     char_name AS chn,
     cast_info AS ci,
     company_name AS cn,
     info_type AS it,
     movie_companies AS mc,
     movie_info AS mi,
     name AS n,
     role_type AS rt,
     title AS t
WHERE ci.note IN ('(voice)',
                  '(voice: Japanese version)',
                  '(voice) (uncredited)',
                  '(voice: English version)')
  AND cn.country_code ='[us]'
  AND it.info = 'release dates'
  AND mc.note IS NOT NULL
  AND (mc.note LIKE '%(USA)%'
       OR mc.note LIKE '%(worldwide)%')
  AND mi.info IS NOT NULL
  AND (mi.info LIKE 'Japan:%200%'
       OR mi.info LIKE 'USA:%200%')
  AND n.gender ='f'
  AND n.name LIKE '%Ang%'
  AND rt.role ='actress'
  AND t.production_year BETWEEN 2005 AND 2009
  AND t.id = mi.movie_id
  AND t.id = mc.movie_id
  AND t.id = ci.movie_id
  AND mc.movie_id = ci.movie_id
  AND mc.movie_id = mi.movie_id
  AND mi.movie_id = ci.movie_id
  AND cn.id = mc.company_id
  AND it.id = mi.info_type_id
  AND n.id = ci.person_id
  AND rt.id = ci.role_id
  AND n.id = an.person_id
  AND ci.person_id = an.person_id
  AND chn.id = ci.person_role_id;

DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/19a/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/19a/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it;
CREATE TABLE it AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/19a/it.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/19a/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/19a/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/19a/n.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS rt;
CREATE TABLE rt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/19a/rt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/19a/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(n.name) AS voicing_actress, MIN(t.title) AS voiced_movie
 FROM mi, cn, char_name AS chn, aka_name AS an, it, rt, ci, mc, n, t, 
WHERE t.id = mi.movie_id
AND t.id = mc.movie_id
AND t.id = ci.movie_id
AND mc.movie_id = ci.movie_id
AND mc.movie_id = mi.movie_id
AND mi.movie_id = ci.movie_id
AND cn.id = mc.company_id
AND it.id = mi.info_type_id
AND n.id = ci.person_id
AND rt.id = ci.role_id
AND n.id = an.person_id
AND ci.person_id = an.person_id
AND chn.id = ci.person_role_id
;
.print 'Testing 19a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(n.name) AS voicing_actress,
       MIN(t.title) AS kung_fu_panda
FROM aka_name AS an,
     char_name AS chn,
     cast_info AS ci,
     company_name AS cn,
     info_type AS it,
     movie_companies AS mc,
     movie_info AS mi,
     name AS n,
     role_type AS rt,
     title AS t
WHERE ci.note = '(voice)'
  AND cn.country_code ='[us]'
  AND it.info = 'release dates'
  AND mc.note LIKE '%(200%)%'
  AND (mc.note LIKE '%(USA)%'
       OR mc.note LIKE '%(worldwide)%')
  AND mi.info IS NOT NULL
  AND (mi.info LIKE 'Japan:%2007%'
       OR mi.info LIKE 'USA:%2008%')
  AND n.gender ='f'
  AND n.name LIKE '%Angel%'
  AND rt.role ='actress'
  AND t.production_year BETWEEN 2007 AND 2008
  AND t.title LIKE '%Kung%Fu%Panda%'
  AND t.id = mi.movie_id
  AND t.id = mc.movie_id
  AND t.id = ci.movie_id
  AND mc.movie_id = ci.movie_id
  AND mc.movie_id = mi.movie_id
  AND mi.movie_id = ci.movie_id
  AND cn.id = mc.company_id
  AND it.id = mi.info_type_id
  AND n.id = ci.person_id
  AND rt.id = ci.role_id
  AND n.id = an.person_id
  AND ci.person_id = an.person_id
  AND chn.id = ci.person_role_id;

DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/19b/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/19b/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it;
CREATE TABLE it AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/19b/it.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/19b/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/19b/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/19b/n.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS rt;
CREATE TABLE rt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/19b/rt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/19b/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(n.name) AS voicing_actress, MIN(t.title) AS kung_fu_panda
 FROM ci, t, aka_name AS an, rt, mc, n, cn, it, mi, char_name AS chn, 
WHERE t.id = mi.movie_id
AND t.id = mc.movie_id
AND t.id = ci.movie_id
AND mc.movie_id = ci.movie_id
AND mc.movie_id = mi.movie_id
AND mi.movie_id = ci.movie_id
AND cn.id = mc.company_id
AND it.id = mi.info_type_id
AND n.id = ci.person_id
AND rt.id = ci.role_id
AND n.id = an.person_id
AND ci.person_id = an.person_id
AND chn.id = ci.person_role_id
;
.print 'Testing 19b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(n.name) AS voicing_actress,
       MIN(t.title) AS jap_engl_voiced_movie
FROM aka_name AS an,
     char_name AS chn,
     cast_info AS ci,
     company_name AS cn,
     info_type AS it,
     movie_companies AS mc,
     movie_info AS mi,
     name AS n,
     role_type AS rt,
     title AS t
WHERE ci.note IN ('(voice)',
                  '(voice: Japanese version)',
                  '(voice) (uncredited)',
                  '(voice: English version)')
  AND cn.country_code ='[us]'
  AND it.info = 'release dates'
  AND mi.info IS NOT NULL
  AND (mi.info LIKE 'Japan:%200%'
       OR mi.info LIKE 'USA:%200%')
  AND n.gender ='f'
  AND n.name LIKE '%An%'
  AND rt.role ='actress'
  AND t.production_year > 2000
  AND t.id = mi.movie_id
  AND t.id = mc.movie_id
  AND t.id = ci.movie_id
  AND mc.movie_id = ci.movie_id
  AND mc.movie_id = mi.movie_id
  AND mi.movie_id = ci.movie_id
  AND cn.id = mc.company_id
  AND it.id = mi.info_type_id
  AND n.id = ci.person_id
  AND rt.id = ci.role_id
  AND n.id = an.person_id
  AND ci.person_id = an.person_id
  AND chn.id = ci.person_role_id;

DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/19c/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/19c/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it;
CREATE TABLE it AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/19c/it.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/19c/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/19c/n.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS rt;
CREATE TABLE rt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/19c/rt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/19c/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(n.name) AS voicing_actress, MIN(t.title) AS jap_engl_voiced_movie
 FROM mi, aka_name AS an, rt, char_name AS chn, t, cn, it, ci, n, movie_companies AS mc, 
WHERE t.id = mi.movie_id
AND t.id = mc.movie_id
AND t.id = ci.movie_id
AND mc.movie_id = ci.movie_id
AND mc.movie_id = mi.movie_id
AND mi.movie_id = ci.movie_id
AND cn.id = mc.company_id
AND it.id = mi.info_type_id
AND n.id = ci.person_id
AND rt.id = ci.role_id
AND n.id = an.person_id
AND ci.person_id = an.person_id
AND chn.id = ci.person_role_id
;
.print 'Testing 19c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(n.name) AS voicing_actress,
       MIN(t.title) AS jap_engl_voiced_movie
FROM aka_name AS an,
     char_name AS chn,
     cast_info AS ci,
     company_name AS cn,
     info_type AS it,
     movie_companies AS mc,
     movie_info AS mi,
     name AS n,
     role_type AS rt,
     title AS t
WHERE ci.note IN ('(voice)',
                  '(voice: Japanese version)',
                  '(voice) (uncredited)',
                  '(voice: English version)')
  AND cn.country_code ='[us]'
  AND it.info = 'release dates'
  AND n.gender ='f'
  AND rt.role ='actress'
  AND t.production_year > 2000
  AND t.id = mi.movie_id
  AND t.id = mc.movie_id
  AND t.id = ci.movie_id
  AND mc.movie_id = ci.movie_id
  AND mc.movie_id = mi.movie_id
  AND mi.movie_id = ci.movie_id
  AND cn.id = mc.company_id
  AND it.id = mi.info_type_id
  AND n.id = ci.person_id
  AND rt.id = ci.role_id
  AND n.id = an.person_id
  AND ci.person_id = an.person_id
  AND chn.id = ci.person_role_id;

DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/19d/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/19d/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it;
CREATE TABLE it AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/19d/it.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/19d/n.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS rt;
CREATE TABLE rt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/19d/rt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/19d/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(n.name) AS voicing_actress, MIN(t.title) AS jap_engl_voiced_movie
 FROM t, n, movie_companies AS mc, movie_info AS mi, ci, char_name AS chn, rt, it, cn, aka_name AS an, 
WHERE t.id = mi.movie_id
AND t.id = mc.movie_id
AND t.id = ci.movie_id
AND mc.movie_id = ci.movie_id
AND mc.movie_id = mi.movie_id
AND mi.movie_id = ci.movie_id
AND cn.id = mc.company_id
AND it.id = mi.info_type_id
AND n.id = ci.person_id
AND rt.id = ci.role_id
AND n.id = an.person_id
AND ci.person_id = an.person_id
AND chn.id = ci.person_role_id
;
.print 'Testing 19d.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(mc.note) AS production_note,
       MIN(t.title) AS movie_title,
       MIN(t.production_year) AS movie_year
FROM company_type AS ct,
     info_type AS it,
     movie_companies AS mc,
     movie_info_idx AS mi_idx,
     title AS t
WHERE ct.kind = 'production companies'
  AND it.info = 'top 250 rank'
  AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
  AND (mc.note LIKE '%(co-production)%'
       OR mc.note LIKE '%(presents)%')
  AND ct.id = mc.company_type_id
  AND t.id = mc.movie_id
  AND t.id = mi_idx.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND it.id = mi_idx.info_type_id;

DROP TABLE IF EXISTS ct;
CREATE TABLE ct AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/1a/ct.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it;
CREATE TABLE it AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/1a/it.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/1a/mc.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(mc.note) AS production_note, MIN(t.title) AS movie_title, MIN(t.production_year) AS movie_year
 FROM mc, movie_info_idx AS mi_idx, it, title AS t, ct, 
WHERE ct.id = mc.company_type_id
AND t.id = mc.movie_id
AND t.id = mi_idx.movie_id
AND mc.movie_id = mi_idx.movie_id
AND it.id = mi_idx.info_type_id
;
.print 'Testing 1a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(mc.note) AS production_note,
       MIN(t.title) AS movie_title,
       MIN(t.production_year) AS movie_year
FROM company_type AS ct,
     info_type AS it,
     movie_companies AS mc,
     movie_info_idx AS mi_idx,
     title AS t
WHERE ct.kind = 'production companies'
  AND it.info = 'bottom 10 rank'
  AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
  AND t.production_year BETWEEN 2005 AND 2010
  AND ct.id = mc.company_type_id
  AND t.id = mc.movie_id
  AND t.id = mi_idx.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND it.id = mi_idx.info_type_id;

DROP TABLE IF EXISTS ct;
CREATE TABLE ct AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/1b/ct.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it;
CREATE TABLE it AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/1b/it.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/1b/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/1b/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(mc.note) AS production_note, MIN(t.title) AS movie_title, MIN(t.production_year) AS movie_year
 FROM ct, movie_info_idx AS mi_idx, t, it, mc, 
WHERE ct.id = mc.company_type_id
AND t.id = mc.movie_id
AND t.id = mi_idx.movie_id
AND mc.movie_id = mi_idx.movie_id
AND it.id = mi_idx.info_type_id
;
.print 'Testing 1b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(mc.note) AS production_note,
       MIN(t.title) AS movie_title,
       MIN(t.production_year) AS movie_year
FROM company_type AS ct,
     info_type AS it,
     movie_companies AS mc,
     movie_info_idx AS mi_idx,
     title AS t
WHERE ct.kind = 'production companies'
  AND it.info = 'top 250 rank'
  AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
  AND (mc.note LIKE '%(co-production)%')
  AND t.production_year >2010
  AND ct.id = mc.company_type_id
  AND t.id = mc.movie_id
  AND t.id = mi_idx.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND it.id = mi_idx.info_type_id;

DROP TABLE IF EXISTS ct;
CREATE TABLE ct AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/1c/ct.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it;
CREATE TABLE it AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/1c/it.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/1c/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/1c/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(mc.note) AS production_note, MIN(t.title) AS movie_title, MIN(t.production_year) AS movie_year
 FROM ct, t, mc, it, movie_info_idx AS mi_idx, 
WHERE ct.id = mc.company_type_id
AND t.id = mc.movie_id
AND t.id = mi_idx.movie_id
AND mc.movie_id = mi_idx.movie_id
AND it.id = mi_idx.info_type_id
;
.print 'Testing 1c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(mc.note) AS production_note,
       MIN(t.title) AS movie_title,
       MIN(t.production_year) AS movie_year
FROM company_type AS ct,
     info_type AS it,
     movie_companies AS mc,
     movie_info_idx AS mi_idx,
     title AS t
WHERE ct.kind = 'production companies'
  AND it.info = 'bottom 10 rank'
  AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
  AND t.production_year >2000
  AND ct.id = mc.company_type_id
  AND t.id = mc.movie_id
  AND t.id = mi_idx.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND it.id = mi_idx.info_type_id;

DROP TABLE IF EXISTS ct;
CREATE TABLE ct AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/1d/ct.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it;
CREATE TABLE it AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/1d/it.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/1d/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/1d/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(mc.note) AS production_note, MIN(t.title) AS movie_title, MIN(t.production_year) AS movie_year
 FROM t, it, ct, mc, movie_info_idx AS mi_idx, 
WHERE ct.id = mc.company_type_id
AND t.id = mc.movie_id
AND t.id = mi_idx.movie_id
AND mc.movie_id = mi_idx.movie_id
AND it.id = mi_idx.info_type_id
;
.print 'Testing 1d.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(t.title) AS complete_downey_ironman_movie
FROM complete_cast AS cc,
     comp_cast_type AS cct1,
     comp_cast_type AS cct2,
     char_name AS chn,
     cast_info AS ci,
     keyword AS k,
     kind_type AS kt,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE cct1.kind = 'cast'
  AND cct2.kind LIKE '%complete%'
  AND chn.name NOT LIKE '%Sherlock%'
  AND (chn.name LIKE '%Tony%Stark%'
       OR chn.name LIKE '%Iron%Man%')
  AND k.keyword IN ('superhero',
                    'sequel',
                    'second-part',
                    'marvel-comics',
                    'based-on-comic',
                    'tv-special',
                    'fight',
                    'violence')
  AND kt.kind = 'movie'
  AND t.production_year > 1950
  AND kt.id = t.kind_id
  AND t.id = mk.movie_id
  AND t.id = ci.movie_id
  AND t.id = cc.movie_id
  AND mk.movie_id = ci.movie_id
  AND mk.movie_id = cc.movie_id
  AND ci.movie_id = cc.movie_id
  AND chn.id = ci.person_role_id
  AND n.id = ci.person_id
  AND k.id = mk.keyword_id
  AND cct1.id = cc.subject_id
  AND cct2.id = cc.status_id;

DROP TABLE IF EXISTS cct1;
CREATE TABLE cct1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/20a/cct1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cct2;
CREATE TABLE cct2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/20a/cct2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS chn;
CREATE TABLE chn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/20a/chn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/20a/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS kt;
CREATE TABLE kt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/20a/kt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/20a/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(t.title) AS complete_downey_ironman_movie
 FROM cct2, complete_cast AS cc, k, cct1, chn, kt, name AS n, t, movie_keyword AS mk, cast_info AS ci, 
WHERE kt.id = t.kind_id
AND t.id = mk.movie_id
AND t.id = ci.movie_id
AND t.id = cc.movie_id
AND mk.movie_id = ci.movie_id
AND mk.movie_id = cc.movie_id
AND ci.movie_id = cc.movie_id
AND chn.id = ci.person_role_id
AND n.id = ci.person_id
AND k.id = mk.keyword_id
AND cct1.id = cc.subject_id
AND cct2.id = cc.status_id
;
.print 'Testing 20a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(t.title) AS complete_downey_ironman_movie
FROM complete_cast AS cc,
     comp_cast_type AS cct1,
     comp_cast_type AS cct2,
     char_name AS chn,
     cast_info AS ci,
     keyword AS k,
     kind_type AS kt,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE cct1.kind = 'cast'
  AND cct2.kind LIKE '%complete%'
  AND chn.name NOT LIKE '%Sherlock%'
  AND (chn.name LIKE '%Tony%Stark%'
       OR chn.name LIKE '%Iron%Man%')
  AND k.keyword IN ('superhero',
                    'sequel',
                    'second-part',
                    'marvel-comics',
                    'based-on-comic',
                    'tv-special',
                    'fight',
                    'violence')
  AND kt.kind = 'movie'
  AND n.name LIKE '%Downey%Robert%'
  AND t.production_year > 2000
  AND kt.id = t.kind_id
  AND t.id = mk.movie_id
  AND t.id = ci.movie_id
  AND t.id = cc.movie_id
  AND mk.movie_id = ci.movie_id
  AND mk.movie_id = cc.movie_id
  AND ci.movie_id = cc.movie_id
  AND chn.id = ci.person_role_id
  AND n.id = ci.person_id
  AND k.id = mk.keyword_id
  AND cct1.id = cc.subject_id
  AND cct2.id = cc.status_id;

DROP TABLE IF EXISTS cct1;
CREATE TABLE cct1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/20b/cct1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cct2;
CREATE TABLE cct2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/20b/cct2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS chn;
CREATE TABLE chn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/20b/chn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/20b/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS kt;
CREATE TABLE kt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/20b/kt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/20b/n.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/20b/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(t.title) AS complete_downey_ironman_movie
 FROM cast_info AS ci, movie_keyword AS mk, n, complete_cast AS cc, k, cct2, chn, cct1, kt, t, 
WHERE kt.id = t.kind_id
AND t.id = mk.movie_id
AND t.id = ci.movie_id
AND t.id = cc.movie_id
AND mk.movie_id = ci.movie_id
AND mk.movie_id = cc.movie_id
AND ci.movie_id = cc.movie_id
AND chn.id = ci.person_role_id
AND n.id = ci.person_id
AND k.id = mk.keyword_id
AND cct1.id = cc.subject_id
AND cct2.id = cc.status_id
;
.print 'Testing 20b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(n.name) AS cast_member,
       MIN(t.title) AS complete_dynamic_hero_movie
FROM complete_cast AS cc,
     comp_cast_type AS cct1,
     comp_cast_type AS cct2,
     char_name AS chn,
     cast_info AS ci,
     keyword AS k,
     kind_type AS kt,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE cct1.kind = 'cast'
  AND cct2.kind LIKE '%complete%'
  AND chn.name IS NOT NULL
  AND (chn.name LIKE '%man%'
       OR chn.name LIKE '%Man%')
  AND k.keyword IN ('superhero',
                    'marvel-comics',
                    'based-on-comic',
                    'tv-special',
                    'fight',
                    'violence',
                    'magnet',
                    'web',
                    'claw',
                    'laser')
  AND kt.kind = 'movie'
  AND t.production_year > 2000
  AND kt.id = t.kind_id
  AND t.id = mk.movie_id
  AND t.id = ci.movie_id
  AND t.id = cc.movie_id
  AND mk.movie_id = ci.movie_id
  AND mk.movie_id = cc.movie_id
  AND ci.movie_id = cc.movie_id
  AND chn.id = ci.person_role_id
  AND n.id = ci.person_id
  AND k.id = mk.keyword_id
  AND cct1.id = cc.subject_id
  AND cct2.id = cc.status_id;

DROP TABLE IF EXISTS cct1;
CREATE TABLE cct1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/20c/cct1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cct2;
CREATE TABLE cct2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/20c/cct2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS chn;
CREATE TABLE chn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/20c/chn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/20c/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS kt;
CREATE TABLE kt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/20c/kt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/20c/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(n.name) AS cast_member, MIN(t.title) AS complete_dynamic_hero_movie
 FROM cast_info AS ci, name AS n, movie_keyword AS mk, kt, t, cct1, complete_cast AS cc, cct2, k, chn, 
WHERE kt.id = t.kind_id
AND t.id = mk.movie_id
AND t.id = ci.movie_id
AND t.id = cc.movie_id
AND mk.movie_id = ci.movie_id
AND mk.movie_id = cc.movie_id
AND ci.movie_id = cc.movie_id
AND chn.id = ci.person_role_id
AND n.id = ci.person_id
AND k.id = mk.keyword_id
AND cct1.id = cc.subject_id
AND cct2.id = cc.status_id
;
.print 'Testing 20c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(cn.name) AS company_name,
       MIN(lt.link) AS link_type,
       MIN(t.title) AS western_follow_up
FROM company_name AS cn,
     company_type AS ct,
     keyword AS k,
     link_type AS lt,
     movie_companies AS mc,
     movie_info AS mi,
     movie_keyword AS mk,
     movie_link AS ml,
     title AS t
WHERE cn.country_code !='[pl]'
  AND (cn.name LIKE '%Film%'
       OR cn.name LIKE '%Warner%')
  AND ct.kind ='production companies'
  AND k.keyword ='sequel'
  AND lt.link LIKE '%follow%'
  AND mc.note IS NULL
  AND mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Denish',
                  'Norwegian',
                  'German')
  AND t.production_year BETWEEN 1950 AND 2000
  AND lt.id = ml.link_type_id
  AND ml.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND t.id = mc.movie_id
  AND mc.company_type_id = ct.id
  AND mc.company_id = cn.id
  AND mi.movie_id = t.id
  AND ml.movie_id = mk.movie_id
  AND ml.movie_id = mc.movie_id
  AND mk.movie_id = mc.movie_id
  AND ml.movie_id = mi.movie_id
  AND mk.movie_id = mi.movie_id
  AND mc.movie_id = mi.movie_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/21a/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS ct;
CREATE TABLE ct AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/21a/ct.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/21a/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS lt;
CREATE TABLE lt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/21a/lt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/21a/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/21a/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/21a/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(cn.name) AS company_name, MIN(lt.link) AS link_type, MIN(t.title) AS western_follow_up
 FROM mi, movie_keyword AS mk, ct, k, mc, t, lt, movie_link AS ml, cn, 
WHERE lt.id = ml.link_type_id
AND ml.movie_id = t.id
AND t.id = mk.movie_id
AND mk.keyword_id = k.id
AND t.id = mc.movie_id
AND mc.company_type_id = ct.id
AND mc.company_id = cn.id
AND mi.movie_id = t.id
AND ml.movie_id = mk.movie_id
AND ml.movie_id = mc.movie_id
AND mk.movie_id = mc.movie_id
AND ml.movie_id = mi.movie_id
AND mk.movie_id = mi.movie_id
AND mc.movie_id = mi.movie_id
;
.print 'Testing 21a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(cn.name) AS company_name,
       MIN(lt.link) AS link_type,
       MIN(t.title) AS german_follow_up
FROM company_name AS cn,
     company_type AS ct,
     keyword AS k,
     link_type AS lt,
     movie_companies AS mc,
     movie_info AS mi,
     movie_keyword AS mk,
     movie_link AS ml,
     title AS t
WHERE cn.country_code !='[pl]'
  AND (cn.name LIKE '%Film%'
       OR cn.name LIKE '%Warner%')
  AND ct.kind ='production companies'
  AND k.keyword ='sequel'
  AND lt.link LIKE '%follow%'
  AND mc.note IS NULL
  AND mi.info IN ('Germany',
                  'German')
  AND t.production_year BETWEEN 2000 AND 2010
  AND lt.id = ml.link_type_id
  AND ml.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND t.id = mc.movie_id
  AND mc.company_type_id = ct.id
  AND mc.company_id = cn.id
  AND mi.movie_id = t.id
  AND ml.movie_id = mk.movie_id
  AND ml.movie_id = mc.movie_id
  AND mk.movie_id = mc.movie_id
  AND ml.movie_id = mi.movie_id
  AND mk.movie_id = mi.movie_id
  AND mc.movie_id = mi.movie_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/21b/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS ct;
CREATE TABLE ct AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/21b/ct.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/21b/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS lt;
CREATE TABLE lt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/21b/lt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/21b/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/21b/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/21b/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(cn.name) AS company_name, MIN(lt.link) AS link_type, MIN(t.title) AS german_follow_up
 FROM movie_link AS ml, movie_keyword AS mk, cn, lt, mc, ct, t, mi, k, 
WHERE lt.id = ml.link_type_id
AND ml.movie_id = t.id
AND t.id = mk.movie_id
AND mk.keyword_id = k.id
AND t.id = mc.movie_id
AND mc.company_type_id = ct.id
AND mc.company_id = cn.id
AND mi.movie_id = t.id
AND ml.movie_id = mk.movie_id
AND ml.movie_id = mc.movie_id
AND mk.movie_id = mc.movie_id
AND ml.movie_id = mi.movie_id
AND mk.movie_id = mi.movie_id
AND mc.movie_id = mi.movie_id
;
.print 'Testing 21b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(cn.name) AS company_name,
       MIN(lt.link) AS link_type,
       MIN(t.title) AS western_follow_up
FROM company_name AS cn,
     company_type AS ct,
     keyword AS k,
     link_type AS lt,
     movie_companies AS mc,
     movie_info AS mi,
     movie_keyword AS mk,
     movie_link AS ml,
     title AS t
WHERE cn.country_code !='[pl]'
  AND (cn.name LIKE '%Film%'
       OR cn.name LIKE '%Warner%')
  AND ct.kind ='production companies'
  AND k.keyword ='sequel'
  AND lt.link LIKE '%follow%'
  AND mc.note IS NULL
  AND mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Denish',
                  'Norwegian',
                  'German',
                  'English')
  AND t.production_year BETWEEN 1950 AND 2010
  AND lt.id = ml.link_type_id
  AND ml.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND t.id = mc.movie_id
  AND mc.company_type_id = ct.id
  AND mc.company_id = cn.id
  AND mi.movie_id = t.id
  AND ml.movie_id = mk.movie_id
  AND ml.movie_id = mc.movie_id
  AND mk.movie_id = mc.movie_id
  AND ml.movie_id = mi.movie_id
  AND mk.movie_id = mi.movie_id
  AND mc.movie_id = mi.movie_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/21c/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS ct;
CREATE TABLE ct AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/21c/ct.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/21c/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS lt;
CREATE TABLE lt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/21c/lt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/21c/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/21c/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/21c/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(cn.name) AS company_name, MIN(lt.link) AS link_type, MIN(t.title) AS western_follow_up
 FROM k, cn, movie_link AS ml, t, ct, movie_keyword AS mk, mc, mi, lt, 
WHERE lt.id = ml.link_type_id
AND ml.movie_id = t.id
AND t.id = mk.movie_id
AND mk.keyword_id = k.id
AND t.id = mc.movie_id
AND mc.company_type_id = ct.id
AND mc.company_id = cn.id
AND mi.movie_id = t.id
AND ml.movie_id = mk.movie_id
AND ml.movie_id = mc.movie_id
AND mk.movie_id = mc.movie_id
AND ml.movie_id = mi.movie_id
AND mk.movie_id = mi.movie_id
AND mc.movie_id = mi.movie_id
;
.print 'Testing 21c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(cn.name) AS movie_company,
       MIN(mi_idx.info) AS rating,
       MIN(t.title) AS western_violent_movie
FROM company_name AS cn,
     company_type AS ct,
     info_type AS it1,
     info_type AS it2,
     keyword AS k,
     kind_type AS kt,
     movie_companies AS mc,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     movie_keyword AS mk,
     title AS t
WHERE cn.country_code != '[us]'
  AND it1.info = 'countries'
  AND it2.info = 'rating'
  AND k.keyword IN ('murder',
                    'murder-in-title',
                    'blood',
                    'violence')
  AND kt.kind IN ('movie',
                  'episode')
  AND mc.note NOT LIKE '%(USA)%'
  AND mc.note LIKE '%(200%)%'
  AND mi.info IN ('Germany',
                  'German',
                  'USA',
                  'American')
  AND mi_idx.info < '7.0'
  AND t.production_year > 2008
  AND kt.id = t.kind_id
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND t.id = mi_idx.movie_id
  AND t.id = mc.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND mk.movie_id = mc.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mc.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND k.id = mk.keyword_id
  AND it1.id = mi.info_type_id
  AND it2.id = mi_idx.info_type_id
  AND ct.id = mc.company_type_id
  AND cn.id = mc.company_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22a/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22a/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22a/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22a/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS kt;
CREATE TABLE kt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22a/kt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22a/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22a/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi_idx;
.mode csv
CREATE TABLE mi_idx (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, info_type_id integer NOT NULL, info character varying NOT NULL, note character varying(1));
.import --csv --skip 1 '../../queries/join-order-benchmark-redux/data/22a/mi_idx.csv' mi_idx
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22a/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(cn.name) AS movie_company, MIN(mi_idx.info) AS rating, MIN(t.title) AS western_violent_movie
 FROM it1, movie_keyword AS mk, t, cn, k, mc, company_type AS ct, mi, mi_idx, kt, it2, 
WHERE kt.id = t.kind_id
AND t.id = mi.movie_id
AND t.id = mk.movie_id
AND t.id = mi_idx.movie_id
AND t.id = mc.movie_id
AND mk.movie_id = mi.movie_id
AND mk.movie_id = mi_idx.movie_id
AND mk.movie_id = mc.movie_id
AND mi.movie_id = mi_idx.movie_id
AND mi.movie_id = mc.movie_id
AND mc.movie_id = mi_idx.movie_id
AND k.id = mk.keyword_id
AND it1.id = mi.info_type_id
AND it2.id = mi_idx.info_type_id
AND ct.id = mc.company_type_id
AND cn.id = mc.company_id
;
.print 'Testing 22a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(cn.name) AS movie_company,
       MIN(mi_idx.info) AS rating,
       MIN(t.title) AS western_violent_movie
FROM company_name AS cn,
     company_type AS ct,
     info_type AS it1,
     info_type AS it2,
     keyword AS k,
     kind_type AS kt,
     movie_companies AS mc,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     movie_keyword AS mk,
     title AS t
WHERE cn.country_code != '[us]'
  AND it1.info = 'countries'
  AND it2.info = 'rating'
  AND k.keyword IN ('murder',
                    'murder-in-title',
                    'blood',
                    'violence')
  AND kt.kind IN ('movie',
                  'episode')
  AND mc.note NOT LIKE '%(USA)%'
  AND mc.note LIKE '%(200%)%'
  AND mi.info IN ('Germany',
                  'German',
                  'USA',
                  'American')
  AND mi_idx.info < '7.0'
  AND t.production_year > 2009
  AND kt.id = t.kind_id
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND t.id = mi_idx.movie_id
  AND t.id = mc.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND mk.movie_id = mc.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mc.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND k.id = mk.keyword_id
  AND it1.id = mi.info_type_id
  AND it2.id = mi_idx.info_type_id
  AND ct.id = mc.company_type_id
  AND cn.id = mc.company_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22b/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22b/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22b/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22b/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS kt;
CREATE TABLE kt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22b/kt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22b/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22b/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi_idx;
.mode csv
CREATE TABLE mi_idx (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, info_type_id integer NOT NULL, info character varying NOT NULL, note character varying(1));
.import --csv --skip 1 '../../queries/join-order-benchmark-redux/data/22b/mi_idx.csv' mi_idx
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22b/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(cn.name) AS movie_company, MIN(mi_idx.info) AS rating, MIN(t.title) AS western_violent_movie
 FROM k, it1, mc, t, mi, kt, cn, movie_keyword AS mk, company_type AS ct, it2, mi_idx, 
WHERE kt.id = t.kind_id
AND t.id = mi.movie_id
AND t.id = mk.movie_id
AND t.id = mi_idx.movie_id
AND t.id = mc.movie_id
AND mk.movie_id = mi.movie_id
AND mk.movie_id = mi_idx.movie_id
AND mk.movie_id = mc.movie_id
AND mi.movie_id = mi_idx.movie_id
AND mi.movie_id = mc.movie_id
AND mc.movie_id = mi_idx.movie_id
AND k.id = mk.keyword_id
AND it1.id = mi.info_type_id
AND it2.id = mi_idx.info_type_id
AND ct.id = mc.company_type_id
AND cn.id = mc.company_id
;
.print 'Testing 22b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(cn.name) AS movie_company,
       MIN(mi_idx.info) AS rating,
       MIN(t.title) AS western_violent_movie
FROM company_name AS cn,
     company_type AS ct,
     info_type AS it1,
     info_type AS it2,
     keyword AS k,
     kind_type AS kt,
     movie_companies AS mc,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     movie_keyword AS mk,
     title AS t
WHERE cn.country_code != '[us]'
  AND it1.info = 'countries'
  AND it2.info = 'rating'
  AND k.keyword IN ('murder',
                    'murder-in-title',
                    'blood',
                    'violence')
  AND kt.kind IN ('movie',
                  'episode')
  AND mc.note NOT LIKE '%(USA)%'
  AND mc.note LIKE '%(200%)%'
  AND mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Danish',
                  'Norwegian',
                  'German',
                  'USA',
                  'American')
  AND mi_idx.info < '8.5'
  AND t.production_year > 2005
  AND kt.id = t.kind_id
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND t.id = mi_idx.movie_id
  AND t.id = mc.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND mk.movie_id = mc.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mc.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND k.id = mk.keyword_id
  AND it1.id = mi.info_type_id
  AND it2.id = mi_idx.info_type_id
  AND ct.id = mc.company_type_id
  AND cn.id = mc.company_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22c/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22c/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22c/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22c/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS kt;
CREATE TABLE kt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22c/kt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22c/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22c/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi_idx;
.mode csv
CREATE TABLE mi_idx (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, info_type_id integer NOT NULL, info character varying NOT NULL, note character varying(1));
.import --csv --skip 1 '../../queries/join-order-benchmark-redux/data/22c/mi_idx.csv' mi_idx
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22c/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(cn.name) AS movie_company, MIN(mi_idx.info) AS rating, MIN(t.title) AS western_violent_movie
 FROM cn, mi_idx, k, mi, mc, movie_keyword AS mk, it2, t, it1, company_type AS ct, kt, 
WHERE kt.id = t.kind_id
AND t.id = mi.movie_id
AND t.id = mk.movie_id
AND t.id = mi_idx.movie_id
AND t.id = mc.movie_id
AND mk.movie_id = mi.movie_id
AND mk.movie_id = mi_idx.movie_id
AND mk.movie_id = mc.movie_id
AND mi.movie_id = mi_idx.movie_id
AND mi.movie_id = mc.movie_id
AND mc.movie_id = mi_idx.movie_id
AND k.id = mk.keyword_id
AND it1.id = mi.info_type_id
AND it2.id = mi_idx.info_type_id
AND ct.id = mc.company_type_id
AND cn.id = mc.company_id
;
.print 'Testing 22c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(cn.name) AS movie_company,
       MIN(mi_idx.info) AS rating,
       MIN(t.title) AS western_violent_movie
FROM company_name AS cn,
     company_type AS ct,
     info_type AS it1,
     info_type AS it2,
     keyword AS k,
     kind_type AS kt,
     movie_companies AS mc,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     movie_keyword AS mk,
     title AS t
WHERE cn.country_code != '[us]'
  AND it1.info = 'countries'
  AND it2.info = 'rating'
  AND k.keyword IN ('murder',
                    'murder-in-title',
                    'blood',
                    'violence')
  AND kt.kind IN ('movie',
                  'episode')
  AND mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Danish',
                  'Norwegian',
                  'German',
                  'USA',
                  'American')
  AND mi_idx.info < '8.5'
  AND t.production_year > 2005
  AND kt.id = t.kind_id
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND t.id = mi_idx.movie_id
  AND t.id = mc.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND mk.movie_id = mc.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mc.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND k.id = mk.keyword_id
  AND it1.id = mi.info_type_id
  AND it2.id = mi_idx.info_type_id
  AND ct.id = mc.company_type_id
  AND cn.id = mc.company_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22d/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22d/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22d/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22d/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS kt;
CREATE TABLE kt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22d/kt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22d/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi_idx;
.mode csv
CREATE TABLE mi_idx (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, info_type_id integer NOT NULL, info character varying NOT NULL, note character varying(1));
.import --csv --skip 1 '../../queries/join-order-benchmark-redux/data/22d/mi_idx.csv' mi_idx
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/22d/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(cn.name) AS movie_company, MIN(mi_idx.info) AS rating, MIN(t.title) AS western_violent_movie
 FROM company_type AS ct, movie_companies AS mc, mi_idx, k, cn, mi, it1, it2, t, kt, movie_keyword AS mk, 
WHERE kt.id = t.kind_id
AND t.id = mi.movie_id
AND t.id = mk.movie_id
AND t.id = mi_idx.movie_id
AND t.id = mc.movie_id
AND mk.movie_id = mi.movie_id
AND mk.movie_id = mi_idx.movie_id
AND mk.movie_id = mc.movie_id
AND mi.movie_id = mi_idx.movie_id
AND mi.movie_id = mc.movie_id
AND mc.movie_id = mi_idx.movie_id
AND k.id = mk.keyword_id
AND it1.id = mi.info_type_id
AND it2.id = mi_idx.info_type_id
AND ct.id = mc.company_type_id
AND cn.id = mc.company_id
;
.print 'Testing 22d.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(kt.kind) AS movie_kind,
       MIN(t.title) AS complete_us_internet_movie
FROM complete_cast AS cc,
     comp_cast_type AS cct1,
     company_name AS cn,
     company_type AS ct,
     info_type AS it1,
     keyword AS k,
     kind_type AS kt,
     movie_companies AS mc,
     movie_info AS mi,
     movie_keyword AS mk,
     title AS t
WHERE cct1.kind = 'complete+verified'
  AND cn.country_code = '[us]'
  AND it1.info = 'release dates'
  AND kt.kind IN ('movie')
  AND mi.note LIKE '%internet%'
  AND mi.info IS NOT NULL
  AND (mi.info LIKE 'USA:% 199%'
       OR mi.info LIKE 'USA:% 200%')
  AND t.production_year > 2000
  AND kt.id = t.kind_id
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND t.id = mc.movie_id
  AND t.id = cc.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mc.movie_id
  AND mk.movie_id = cc.movie_id
  AND mi.movie_id = mc.movie_id
  AND mi.movie_id = cc.movie_id
  AND mc.movie_id = cc.movie_id
  AND k.id = mk.keyword_id
  AND it1.id = mi.info_type_id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id
  AND cct1.id = cc.status_id;

DROP TABLE IF EXISTS cct1;
CREATE TABLE cct1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/23a/cct1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/23a/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/23a/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS kt;
CREATE TABLE kt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/23a/kt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/23a/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/23a/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(kt.kind) AS movie_kind, MIN(t.title) AS complete_us_internet_movie
 FROM it1, company_type AS ct, keyword AS k, movie_keyword AS mk, cct1, t, movie_companies AS mc, mi, cn, kt, complete_cast AS cc, 
WHERE kt.id = t.kind_id
AND t.id = mi.movie_id
AND t.id = mk.movie_id
AND t.id = mc.movie_id
AND t.id = cc.movie_id
AND mk.movie_id = mi.movie_id
AND mk.movie_id = mc.movie_id
AND mk.movie_id = cc.movie_id
AND mi.movie_id = mc.movie_id
AND mi.movie_id = cc.movie_id
AND mc.movie_id = cc.movie_id
AND k.id = mk.keyword_id
AND it1.id = mi.info_type_id
AND cn.id = mc.company_id
AND ct.id = mc.company_type_id
AND cct1.id = cc.status_id
;
.print 'Testing 23a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(kt.kind) AS movie_kind,
       MIN(t.title) AS complete_nerdy_internet_movie
FROM complete_cast AS cc,
     comp_cast_type AS cct1,
     company_name AS cn,
     company_type AS ct,
     info_type AS it1,
     keyword AS k,
     kind_type AS kt,
     movie_companies AS mc,
     movie_info AS mi,
     movie_keyword AS mk,
     title AS t
WHERE cct1.kind = 'complete+verified'
  AND cn.country_code = '[us]'
  AND it1.info = 'release dates'
  AND k.keyword IN ('nerd',
                    'loner',
                    'alienation',
                    'dignity')
  AND kt.kind IN ('movie')
  AND mi.note LIKE '%internet%'
  AND mi.info LIKE 'USA:% 200%'
  AND t.production_year > 2000
  AND kt.id = t.kind_id
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND t.id = mc.movie_id
  AND t.id = cc.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mc.movie_id
  AND mk.movie_id = cc.movie_id
  AND mi.movie_id = mc.movie_id
  AND mi.movie_id = cc.movie_id
  AND mc.movie_id = cc.movie_id
  AND k.id = mk.keyword_id
  AND it1.id = mi.info_type_id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id
  AND cct1.id = cc.status_id;

DROP TABLE IF EXISTS cct1;
CREATE TABLE cct1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/23b/cct1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/23b/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/23b/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/23b/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS kt;
CREATE TABLE kt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/23b/kt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/23b/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/23b/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(kt.kind) AS movie_kind, MIN(t.title) AS complete_nerdy_internet_movie
 FROM company_type AS ct, t, kt, it1, mi, cct1, cn, movie_companies AS mc, movie_keyword AS mk, k, complete_cast AS cc, 
WHERE kt.id = t.kind_id
AND t.id = mi.movie_id
AND t.id = mk.movie_id
AND t.id = mc.movie_id
AND t.id = cc.movie_id
AND mk.movie_id = mi.movie_id
AND mk.movie_id = mc.movie_id
AND mk.movie_id = cc.movie_id
AND mi.movie_id = mc.movie_id
AND mi.movie_id = cc.movie_id
AND mc.movie_id = cc.movie_id
AND k.id = mk.keyword_id
AND it1.id = mi.info_type_id
AND cn.id = mc.company_id
AND ct.id = mc.company_type_id
AND cct1.id = cc.status_id
;
.print 'Testing 23b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(kt.kind) AS movie_kind,
       MIN(t.title) AS complete_us_internet_movie
FROM complete_cast AS cc,
     comp_cast_type AS cct1,
     company_name AS cn,
     company_type AS ct,
     info_type AS it1,
     keyword AS k,
     kind_type AS kt,
     movie_companies AS mc,
     movie_info AS mi,
     movie_keyword AS mk,
     title AS t
WHERE cct1.kind = 'complete+verified'
  AND cn.country_code = '[us]'
  AND it1.info = 'release dates'
  AND kt.kind IN ('movie',
                  'tv movie',
                  'video movie',
                  'video game')
  AND mi.note LIKE '%internet%'
  AND mi.info IS NOT NULL
  AND (mi.info LIKE 'USA:% 199%'
       OR mi.info LIKE 'USA:% 200%')
  AND t.production_year > 1990
  AND kt.id = t.kind_id
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND t.id = mc.movie_id
  AND t.id = cc.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mc.movie_id
  AND mk.movie_id = cc.movie_id
  AND mi.movie_id = mc.movie_id
  AND mi.movie_id = cc.movie_id
  AND mc.movie_id = cc.movie_id
  AND k.id = mk.keyword_id
  AND it1.id = mi.info_type_id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id
  AND cct1.id = cc.status_id;

DROP TABLE IF EXISTS cct1;
CREATE TABLE cct1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/23c/cct1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/23c/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/23c/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS kt;
CREATE TABLE kt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/23c/kt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/23c/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/23c/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(kt.kind) AS movie_kind, MIN(t.title) AS complete_us_internet_movie
 FROM movie_companies AS mc, cn, cct1, kt, complete_cast AS cc, keyword AS k, it1, movie_keyword AS mk, t, mi, company_type AS ct, 
WHERE kt.id = t.kind_id
AND t.id = mi.movie_id
AND t.id = mk.movie_id
AND t.id = mc.movie_id
AND t.id = cc.movie_id
AND mk.movie_id = mi.movie_id
AND mk.movie_id = mc.movie_id
AND mk.movie_id = cc.movie_id
AND mi.movie_id = mc.movie_id
AND mi.movie_id = cc.movie_id
AND mc.movie_id = cc.movie_id
AND k.id = mk.keyword_id
AND it1.id = mi.info_type_id
AND cn.id = mc.company_id
AND ct.id = mc.company_type_id
AND cct1.id = cc.status_id
;
.print 'Testing 23c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(chn.name) AS voiced_char_name,
       MIN(n.name) AS voicing_actress_name,
       MIN(t.title) AS voiced_action_movie_jap_eng
FROM aka_name AS an,
     char_name AS chn,
     cast_info AS ci,
     company_name AS cn,
     info_type AS it,
     keyword AS k,
     movie_companies AS mc,
     movie_info AS mi,
     movie_keyword AS mk,
     name AS n,
     role_type AS rt,
     title AS t
WHERE ci.note IN ('(voice)',
                  '(voice: Japanese version)',
                  '(voice) (uncredited)',
                  '(voice: English version)')
  AND cn.country_code ='[us]'
  AND it.info = 'release dates'
  AND k.keyword IN ('hero',
                    'martial-arts',
                    'hand-to-hand-combat')
  AND mi.info IS NOT NULL
  AND (mi.info LIKE 'Japan:%201%'
       OR mi.info LIKE 'USA:%201%')
  AND n.gender ='f'
  AND n.name LIKE '%An%'
  AND rt.role ='actress'
  AND t.production_year > 2010
  AND t.id = mi.movie_id
  AND t.id = mc.movie_id
  AND t.id = ci.movie_id
  AND t.id = mk.movie_id
  AND mc.movie_id = ci.movie_id
  AND mc.movie_id = mi.movie_id
  AND mc.movie_id = mk.movie_id
  AND mi.movie_id = ci.movie_id
  AND mi.movie_id = mk.movie_id
  AND ci.movie_id = mk.movie_id
  AND cn.id = mc.company_id
  AND it.id = mi.info_type_id
  AND n.id = ci.person_id
  AND rt.id = ci.role_id
  AND n.id = an.person_id
  AND ci.person_id = an.person_id
  AND chn.id = ci.person_role_id
  AND k.id = mk.keyword_id;

DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/24a/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/24a/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it;
CREATE TABLE it AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/24a/it.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/24a/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/24a/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/24a/n.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS rt;
CREATE TABLE rt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/24a/rt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/24a/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(chn.name) AS voiced_char_name, MIN(n.name) AS voicing_actress_name, MIN(t.title) AS voiced_action_movie_jap_eng
 FROM char_name AS chn, movie_companies AS mc, rt, mi, it, aka_name AS an, cn, n, t, movie_keyword AS mk, k, ci, 
WHERE t.id = mi.movie_id
AND t.id = mc.movie_id
AND t.id = ci.movie_id
AND t.id = mk.movie_id
AND mc.movie_id = ci.movie_id
AND mc.movie_id = mi.movie_id
AND mc.movie_id = mk.movie_id
AND mi.movie_id = ci.movie_id
AND mi.movie_id = mk.movie_id
AND ci.movie_id = mk.movie_id
AND cn.id = mc.company_id
AND it.id = mi.info_type_id
AND n.id = ci.person_id
AND rt.id = ci.role_id
AND n.id = an.person_id
AND ci.person_id = an.person_id
AND chn.id = ci.person_role_id
AND k.id = mk.keyword_id
;
.print 'Testing 24a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(chn.name) AS voiced_char_name,
       MIN(n.name) AS voicing_actress_name,
       MIN(t.title) AS kung_fu_panda
FROM aka_name AS an,
     char_name AS chn,
     cast_info AS ci,
     company_name AS cn,
     info_type AS it,
     keyword AS k,
     movie_companies AS mc,
     movie_info AS mi,
     movie_keyword AS mk,
     name AS n,
     role_type AS rt,
     title AS t
WHERE ci.note IN ('(voice)',
                  '(voice: Japanese version)',
                  '(voice) (uncredited)',
                  '(voice: English version)')
  AND cn.country_code ='[us]'
  AND cn.name = 'DreamWorks Animation'
  AND it.info = 'release dates'
  AND k.keyword IN ('hero',
                    'martial-arts',
                    'hand-to-hand-combat',
                    'computer-animated-movie')
  AND mi.info IS NOT NULL
  AND (mi.info LIKE 'Japan:%201%'
       OR mi.info LIKE 'USA:%201%')
  AND n.gender ='f'
  AND n.name LIKE '%An%'
  AND rt.role ='actress'
  AND t.production_year > 2010
  AND t.title LIKE 'Kung Fu Panda%'
  AND t.id = mi.movie_id
  AND t.id = mc.movie_id
  AND t.id = ci.movie_id
  AND t.id = mk.movie_id
  AND mc.movie_id = ci.movie_id
  AND mc.movie_id = mi.movie_id
  AND mc.movie_id = mk.movie_id
  AND mi.movie_id = ci.movie_id
  AND mi.movie_id = mk.movie_id
  AND ci.movie_id = mk.movie_id
  AND cn.id = mc.company_id
  AND it.id = mi.info_type_id
  AND n.id = ci.person_id
  AND rt.id = ci.role_id
  AND n.id = an.person_id
  AND ci.person_id = an.person_id
  AND chn.id = ci.person_role_id
  AND k.id = mk.keyword_id;

DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/24b/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/24b/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it;
CREATE TABLE it AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/24b/it.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/24b/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/24b/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/24b/n.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS rt;
CREATE TABLE rt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/24b/rt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/24b/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(chn.name) AS voiced_char_name, MIN(n.name) AS voicing_actress_name, MIN(t.title) AS kung_fu_panda
 FROM k, movie_companies AS mc, ci, aka_name AS an, mi, it, n, movie_keyword AS mk, rt, char_name AS chn, t, cn, 
WHERE t.id = mi.movie_id
AND t.id = mc.movie_id
AND t.id = ci.movie_id
AND t.id = mk.movie_id
AND mc.movie_id = ci.movie_id
AND mc.movie_id = mi.movie_id
AND mc.movie_id = mk.movie_id
AND mi.movie_id = ci.movie_id
AND mi.movie_id = mk.movie_id
AND ci.movie_id = mk.movie_id
AND cn.id = mc.company_id
AND it.id = mi.info_type_id
AND n.id = ci.person_id
AND rt.id = ci.role_id
AND n.id = an.person_id
AND ci.person_id = an.person_id
AND chn.id = ci.person_role_id
AND k.id = mk.keyword_id
;
.print 'Testing 24b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(mi.info) AS movie_budget,
       MIN(mi_idx.info) AS movie_votes,
       MIN(n.name) AS male_writer,
       MIN(t.title) AS violent_movie_title
FROM cast_info AS ci,
     info_type AS it1,
     info_type AS it2,
     keyword AS k,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)')
  AND it1.info = 'genres'
  AND it2.info = 'votes'
  AND k.keyword IN ('murder',
                    'blood',
                    'gore',
                    'death',
                    'female-nudity')
  AND mi.info = 'Horror'
  AND n.gender = 'm'
  AND t.id = mi.movie_id
  AND t.id = mi_idx.movie_id
  AND t.id = ci.movie_id
  AND t.id = mk.movie_id
  AND ci.movie_id = mi.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND ci.movie_id = mk.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mk.movie_id
  AND mi_idx.movie_id = mk.movie_id
  AND n.id = ci.person_id
  AND it1.id = mi.info_type_id
  AND it2.id = mi_idx.info_type_id
  AND k.id = mk.keyword_id;

DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/25a/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/25a/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/25a/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/25a/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/25a/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/25a/n.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(mi.info) AS movie_budget, MIN(mi_idx.info) AS movie_votes, MIN(n.name) AS male_writer, MIN(t.title) AS violent_movie_title
 FROM n, it1, mi, movie_keyword AS mk, k, ci, it2, title AS t, movie_info_idx AS mi_idx, 
WHERE t.id = mi.movie_id
AND t.id = mi_idx.movie_id
AND t.id = ci.movie_id
AND t.id = mk.movie_id
AND ci.movie_id = mi.movie_id
AND ci.movie_id = mi_idx.movie_id
AND ci.movie_id = mk.movie_id
AND mi.movie_id = mi_idx.movie_id
AND mi.movie_id = mk.movie_id
AND mi_idx.movie_id = mk.movie_id
AND n.id = ci.person_id
AND it1.id = mi.info_type_id
AND it2.id = mi_idx.info_type_id
AND k.id = mk.keyword_id
;
.print 'Testing 25a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(mi.info) AS movie_budget,
       MIN(mi_idx.info) AS movie_votes,
       MIN(n.name) AS male_writer,
       MIN(t.title) AS violent_movie_title
FROM cast_info AS ci,
     info_type AS it1,
     info_type AS it2,
     keyword AS k,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)')
  AND it1.info = 'genres'
  AND it2.info = 'votes'
  AND k.keyword IN ('murder',
                    'blood',
                    'gore',
                    'death',
                    'female-nudity')
  AND mi.info = 'Horror'
  AND n.gender = 'm'
  AND t.production_year > 2010
  AND t.title LIKE 'Vampire%'
  AND t.id = mi.movie_id
  AND t.id = mi_idx.movie_id
  AND t.id = ci.movie_id
  AND t.id = mk.movie_id
  AND ci.movie_id = mi.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND ci.movie_id = mk.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mk.movie_id
  AND mi_idx.movie_id = mk.movie_id
  AND n.id = ci.person_id
  AND it1.id = mi.info_type_id
  AND it2.id = mi_idx.info_type_id
  AND k.id = mk.keyword_id;

DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/25b/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/25b/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/25b/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/25b/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/25b/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/25b/n.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/25b/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(mi.info) AS movie_budget, MIN(mi_idx.info) AS movie_votes, MIN(n.name) AS male_writer, MIN(t.title) AS violent_movie_title
 FROM ci, k, mi, t, movie_keyword AS mk, it1, movie_info_idx AS mi_idx, it2, n, 
WHERE t.id = mi.movie_id
AND t.id = mi_idx.movie_id
AND t.id = ci.movie_id
AND t.id = mk.movie_id
AND ci.movie_id = mi.movie_id
AND ci.movie_id = mi_idx.movie_id
AND ci.movie_id = mk.movie_id
AND mi.movie_id = mi_idx.movie_id
AND mi.movie_id = mk.movie_id
AND mi_idx.movie_id = mk.movie_id
AND n.id = ci.person_id
AND it1.id = mi.info_type_id
AND it2.id = mi_idx.info_type_id
AND k.id = mk.keyword_id
;
.print 'Testing 25b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(mi.info) AS movie_budget,
       MIN(mi_idx.info) AS movie_votes,
       MIN(n.name) AS male_writer,
       MIN(t.title) AS violent_movie_title
FROM cast_info AS ci,
     info_type AS it1,
     info_type AS it2,
     keyword AS k,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)')
  AND it1.info = 'genres'
  AND it2.info = 'votes'
  AND k.keyword IN ('murder',
                    'violence',
                    'blood',
                    'gore',
                    'death',
                    'female-nudity',
                    'hospital')
  AND mi.info IN ('Horror',
                  'Action',
                  'Sci-Fi',
                  'Thriller',
                  'Crime',
                  'War')
  AND n.gender = 'm'
  AND t.id = mi.movie_id
  AND t.id = mi_idx.movie_id
  AND t.id = ci.movie_id
  AND t.id = mk.movie_id
  AND ci.movie_id = mi.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND ci.movie_id = mk.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mk.movie_id
  AND mi_idx.movie_id = mk.movie_id
  AND n.id = ci.person_id
  AND it1.id = mi.info_type_id
  AND it2.id = mi_idx.info_type_id
  AND k.id = mk.keyword_id;

DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/25c/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/25c/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/25c/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/25c/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/25c/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/25c/n.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(mi.info) AS movie_budget, MIN(mi_idx.info) AS movie_votes, MIN(n.name) AS male_writer, MIN(t.title) AS violent_movie_title
 FROM k, title AS t, ci, n, it2, movie_keyword AS mk, it1, movie_info_idx AS mi_idx, mi, 
WHERE t.id = mi.movie_id
AND t.id = mi_idx.movie_id
AND t.id = ci.movie_id
AND t.id = mk.movie_id
AND ci.movie_id = mi.movie_id
AND ci.movie_id = mi_idx.movie_id
AND ci.movie_id = mk.movie_id
AND mi.movie_id = mi_idx.movie_id
AND mi.movie_id = mk.movie_id
AND mi_idx.movie_id = mk.movie_id
AND n.id = ci.person_id
AND it1.id = mi.info_type_id
AND it2.id = mi_idx.info_type_id
AND k.id = mk.keyword_id
;
.print 'Testing 25c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(chn.name) AS character_name,
       MIN(mi_idx.info) AS rating,
       MIN(n.name) AS playing_actor,
       MIN(t.title) AS complete_hero_movie
FROM complete_cast AS cc,
     comp_cast_type AS cct1,
     comp_cast_type AS cct2,
     char_name AS chn,
     cast_info AS ci,
     info_type AS it2,
     keyword AS k,
     kind_type AS kt,
     movie_info_idx AS mi_idx,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE cct1.kind = 'cast'
  AND cct2.kind LIKE '%complete%'
  AND chn.name IS NOT NULL
  AND (chn.name LIKE '%man%'
       OR chn.name LIKE '%Man%')
  AND it2.info = 'rating'
  AND k.keyword IN ('superhero',
                    'marvel-comics',
                    'based-on-comic',
                    'tv-special',
                    'fight',
                    'violence',
                    'magnet',
                    'web',
                    'claw',
                    'laser')
  AND kt.kind = 'movie'
  AND mi_idx.info > '7.0'
  AND t.production_year > 2000
  AND kt.id = t.kind_id
  AND t.id = mk.movie_id
  AND t.id = ci.movie_id
  AND t.id = cc.movie_id
  AND t.id = mi_idx.movie_id
  AND mk.movie_id = ci.movie_id
  AND mk.movie_id = cc.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND ci.movie_id = cc.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND cc.movie_id = mi_idx.movie_id
  AND chn.id = ci.person_role_id
  AND n.id = ci.person_id
  AND k.id = mk.keyword_id
  AND cct1.id = cc.subject_id
  AND cct2.id = cc.status_id
  AND it2.id = mi_idx.info_type_id;

DROP TABLE IF EXISTS cct1;
CREATE TABLE cct1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/26a/cct1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cct2;
CREATE TABLE cct2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/26a/cct2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS chn;
CREATE TABLE chn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/26a/chn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/26a/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/26a/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS kt;
CREATE TABLE kt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/26a/kt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi_idx;
.mode csv
CREATE TABLE mi_idx (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, info_type_id integer NOT NULL, info character varying NOT NULL, note character varying(1));
.import --csv --skip 1 '../../queries/join-order-benchmark-redux/data/26a/mi_idx.csv' mi_idx
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/26a/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(chn.name) AS character_name, MIN(mi_idx.info) AS rating, MIN(n.name) AS playing_actor, MIN(t.title) AS complete_hero_movie
 FROM mi_idx, name AS n, it2, movie_keyword AS mk, complete_cast AS cc, k, cct2, t, cct1, kt, chn, cast_info AS ci, 
WHERE kt.id = t.kind_id
AND t.id = mk.movie_id
AND t.id = ci.movie_id
AND t.id = cc.movie_id
AND t.id = mi_idx.movie_id
AND mk.movie_id = ci.movie_id
AND mk.movie_id = cc.movie_id
AND mk.movie_id = mi_idx.movie_id
AND ci.movie_id = cc.movie_id
AND ci.movie_id = mi_idx.movie_id
AND cc.movie_id = mi_idx.movie_id
AND chn.id = ci.person_role_id
AND n.id = ci.person_id
AND k.id = mk.keyword_id
AND cct1.id = cc.subject_id
AND cct2.id = cc.status_id
AND it2.id = mi_idx.info_type_id
;
.print 'Testing 26a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(chn.name) AS character_name,
       MIN(mi_idx.info) AS rating,
       MIN(t.title) AS complete_hero_movie
FROM complete_cast AS cc,
     comp_cast_type AS cct1,
     comp_cast_type AS cct2,
     char_name AS chn,
     cast_info AS ci,
     info_type AS it2,
     keyword AS k,
     kind_type AS kt,
     movie_info_idx AS mi_idx,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE cct1.kind = 'cast'
  AND cct2.kind LIKE '%complete%'
  AND chn.name IS NOT NULL
  AND (chn.name LIKE '%man%'
       OR chn.name LIKE '%Man%')
  AND it2.info = 'rating'
  AND k.keyword IN ('superhero',
                    'marvel-comics',
                    'based-on-comic',
                    'fight')
  AND kt.kind = 'movie'
  AND mi_idx.info > '8.0'
  AND t.production_year > 2005
  AND kt.id = t.kind_id
  AND t.id = mk.movie_id
  AND t.id = ci.movie_id
  AND t.id = cc.movie_id
  AND t.id = mi_idx.movie_id
  AND mk.movie_id = ci.movie_id
  AND mk.movie_id = cc.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND ci.movie_id = cc.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND cc.movie_id = mi_idx.movie_id
  AND chn.id = ci.person_role_id
  AND n.id = ci.person_id
  AND k.id = mk.keyword_id
  AND cct1.id = cc.subject_id
  AND cct2.id = cc.status_id
  AND it2.id = mi_idx.info_type_id;

DROP TABLE IF EXISTS cct1;
CREATE TABLE cct1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/26b/cct1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cct2;
CREATE TABLE cct2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/26b/cct2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS chn;
CREATE TABLE chn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/26b/chn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/26b/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/26b/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS kt;
CREATE TABLE kt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/26b/kt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi_idx;
.mode csv
CREATE TABLE mi_idx (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, info_type_id integer NOT NULL, info character varying NOT NULL, note character varying(1));
.import --csv --skip 1 '../../queries/join-order-benchmark-redux/data/26b/mi_idx.csv' mi_idx
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/26b/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(chn.name) AS character_name, MIN(mi_idx.info) AS rating, MIN(t.title) AS complete_hero_movie
 FROM cast_info AS ci, movie_keyword AS mk, chn, it2, kt, name AS n, mi_idx, cct1, t, k, cct2, complete_cast AS cc, 
WHERE kt.id = t.kind_id
AND t.id = mk.movie_id
AND t.id = ci.movie_id
AND t.id = cc.movie_id
AND t.id = mi_idx.movie_id
AND mk.movie_id = ci.movie_id
AND mk.movie_id = cc.movie_id
AND mk.movie_id = mi_idx.movie_id
AND ci.movie_id = cc.movie_id
AND ci.movie_id = mi_idx.movie_id
AND cc.movie_id = mi_idx.movie_id
AND chn.id = ci.person_role_id
AND n.id = ci.person_id
AND k.id = mk.keyword_id
AND cct1.id = cc.subject_id
AND cct2.id = cc.status_id
AND it2.id = mi_idx.info_type_id
;
.print 'Testing 26b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(chn.name) AS character_name,
       MIN(mi_idx.info) AS rating,
       MIN(t.title) AS complete_hero_movie
FROM complete_cast AS cc,
     comp_cast_type AS cct1,
     comp_cast_type AS cct2,
     char_name AS chn,
     cast_info AS ci,
     info_type AS it2,
     keyword AS k,
     kind_type AS kt,
     movie_info_idx AS mi_idx,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE cct1.kind = 'cast'
  AND cct2.kind LIKE '%complete%'
  AND chn.name IS NOT NULL
  AND (chn.name LIKE '%man%'
       OR chn.name LIKE '%Man%')
  AND it2.info = 'rating'
  AND k.keyword IN ('superhero',
                    'marvel-comics',
                    'based-on-comic',
                    'tv-special',
                    'fight',
                    'violence',
                    'magnet',
                    'web',
                    'claw',
                    'laser')
  AND kt.kind = 'movie'
  AND t.production_year > 2000
  AND kt.id = t.kind_id
  AND t.id = mk.movie_id
  AND t.id = ci.movie_id
  AND t.id = cc.movie_id
  AND t.id = mi_idx.movie_id
  AND mk.movie_id = ci.movie_id
  AND mk.movie_id = cc.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND ci.movie_id = cc.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND cc.movie_id = mi_idx.movie_id
  AND chn.id = ci.person_role_id
  AND n.id = ci.person_id
  AND k.id = mk.keyword_id
  AND cct1.id = cc.subject_id
  AND cct2.id = cc.status_id
  AND it2.id = mi_idx.info_type_id;

DROP TABLE IF EXISTS cct1;
CREATE TABLE cct1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/26c/cct1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cct2;
CREATE TABLE cct2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/26c/cct2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS chn;
CREATE TABLE chn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/26c/chn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/26c/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/26c/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS kt;
CREATE TABLE kt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/26c/kt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/26c/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(chn.name) AS character_name, MIN(mi_idx.info) AS rating, MIN(t.title) AS complete_hero_movie
 FROM complete_cast AS cc, kt, cct2, cast_info AS ci, chn, movie_info_idx AS mi_idx, k, it2, name AS n, t, cct1, movie_keyword AS mk, 
WHERE kt.id = t.kind_id
AND t.id = mk.movie_id
AND t.id = ci.movie_id
AND t.id = cc.movie_id
AND t.id = mi_idx.movie_id
AND mk.movie_id = ci.movie_id
AND mk.movie_id = cc.movie_id
AND mk.movie_id = mi_idx.movie_id
AND ci.movie_id = cc.movie_id
AND ci.movie_id = mi_idx.movie_id
AND cc.movie_id = mi_idx.movie_id
AND chn.id = ci.person_role_id
AND n.id = ci.person_id
AND k.id = mk.keyword_id
AND cct1.id = cc.subject_id
AND cct2.id = cc.status_id
AND it2.id = mi_idx.info_type_id
;
.print 'Testing 26c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(cn.name) AS producing_company,
       MIN(lt.link) AS link_type,
       MIN(t.title) AS complete_western_sequel
FROM complete_cast AS cc,
     comp_cast_type AS cct1,
     comp_cast_type AS cct2,
     company_name AS cn,
     company_type AS ct,
     keyword AS k,
     link_type AS lt,
     movie_companies AS mc,
     movie_info AS mi,
     movie_keyword AS mk,
     movie_link AS ml,
     title AS t
WHERE cct1.kind IN ('cast',
                    'crew')
  AND cct2.kind = 'complete'
  AND cn.country_code !='[pl]'
  AND (cn.name LIKE '%Film%'
       OR cn.name LIKE '%Warner%')
  AND ct.kind ='production companies'
  AND k.keyword ='sequel'
  AND lt.link LIKE '%follow%'
  AND mc.note IS NULL
  AND mi.info IN ('Sweden',
                  'Germany',
                  'Swedish',
                  'German')
  AND t.production_year BETWEEN 1950 AND 2000
  AND lt.id = ml.link_type_id
  AND ml.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND t.id = mc.movie_id
  AND mc.company_type_id = ct.id
  AND mc.company_id = cn.id
  AND mi.movie_id = t.id
  AND t.id = cc.movie_id
  AND cct1.id = cc.subject_id
  AND cct2.id = cc.status_id
  AND ml.movie_id = mk.movie_id
  AND ml.movie_id = mc.movie_id
  AND mk.movie_id = mc.movie_id
  AND ml.movie_id = mi.movie_id
  AND mk.movie_id = mi.movie_id
  AND mc.movie_id = mi.movie_id
  AND ml.movie_id = cc.movie_id
  AND mk.movie_id = cc.movie_id
  AND mc.movie_id = cc.movie_id
  AND mi.movie_id = cc.movie_id;

DROP TABLE IF EXISTS cct1;
CREATE TABLE cct1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/27a/cct1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cct2;
CREATE TABLE cct2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/27a/cct2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/27a/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS ct;
CREATE TABLE ct AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/27a/ct.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/27a/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS lt;
CREATE TABLE lt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/27a/lt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/27a/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/27a/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/27a/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(cn.name) AS producing_company, MIN(lt.link) AS link_type, MIN(t.title) AS complete_western_sequel
 FROM cn, mi, movie_link AS ml, cct1, complete_cast AS cc, cct2, mc, ct, k, lt, movie_keyword AS mk, t, 
WHERE lt.id = ml.link_type_id
AND ml.movie_id = t.id
AND t.id = mk.movie_id
AND mk.keyword_id = k.id
AND t.id = mc.movie_id
AND mc.company_type_id = ct.id
AND mc.company_id = cn.id
AND mi.movie_id = t.id
AND t.id = cc.movie_id
AND cct1.id = cc.subject_id
AND cct2.id = cc.status_id
AND ml.movie_id = mk.movie_id
AND ml.movie_id = mc.movie_id
AND mk.movie_id = mc.movie_id
AND ml.movie_id = mi.movie_id
AND mk.movie_id = mi.movie_id
AND mc.movie_id = mi.movie_id
AND ml.movie_id = cc.movie_id
AND mk.movie_id = cc.movie_id
AND mc.movie_id = cc.movie_id
AND mi.movie_id = cc.movie_id
;
.print 'Testing 27a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(cn.name) AS producing_company,
       MIN(lt.link) AS link_type,
       MIN(t.title) AS complete_western_sequel
FROM complete_cast AS cc,
     comp_cast_type AS cct1,
     comp_cast_type AS cct2,
     company_name AS cn,
     company_type AS ct,
     keyword AS k,
     link_type AS lt,
     movie_companies AS mc,
     movie_info AS mi,
     movie_keyword AS mk,
     movie_link AS ml,
     title AS t
WHERE cct1.kind IN ('cast',
                    'crew')
  AND cct2.kind = 'complete'
  AND cn.country_code !='[pl]'
  AND (cn.name LIKE '%Film%'
       OR cn.name LIKE '%Warner%')
  AND ct.kind ='production companies'
  AND k.keyword ='sequel'
  AND lt.link LIKE '%follow%'
  AND mc.note IS NULL
  AND mi.info IN ('Sweden',
                  'Germany',
                  'Swedish',
                  'German')
  AND t.production_year = 1998
  AND lt.id = ml.link_type_id
  AND ml.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND t.id = mc.movie_id
  AND mc.company_type_id = ct.id
  AND mc.company_id = cn.id
  AND mi.movie_id = t.id
  AND t.id = cc.movie_id
  AND cct1.id = cc.subject_id
  AND cct2.id = cc.status_id
  AND ml.movie_id = mk.movie_id
  AND ml.movie_id = mc.movie_id
  AND mk.movie_id = mc.movie_id
  AND ml.movie_id = mi.movie_id
  AND mk.movie_id = mi.movie_id
  AND mc.movie_id = mi.movie_id
  AND ml.movie_id = cc.movie_id
  AND mk.movie_id = cc.movie_id
  AND mc.movie_id = cc.movie_id
  AND mi.movie_id = cc.movie_id;

DROP TABLE IF EXISTS cct1;
CREATE TABLE cct1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/27b/cct1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cct2;
CREATE TABLE cct2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/27b/cct2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/27b/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS ct;
CREATE TABLE ct AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/27b/ct.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/27b/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS lt;
CREATE TABLE lt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/27b/lt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/27b/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/27b/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/27b/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(cn.name) AS producing_company, MIN(lt.link) AS link_type, MIN(t.title) AS complete_western_sequel
 FROM movie_link AS ml, t, lt, mi, complete_cast AS cc, ct, k, movie_keyword AS mk, cct2, cct1, cn, mc, 
WHERE lt.id = ml.link_type_id
AND ml.movie_id = t.id
AND t.id = mk.movie_id
AND mk.keyword_id = k.id
AND t.id = mc.movie_id
AND mc.company_type_id = ct.id
AND mc.company_id = cn.id
AND mi.movie_id = t.id
AND t.id = cc.movie_id
AND cct1.id = cc.subject_id
AND cct2.id = cc.status_id
AND ml.movie_id = mk.movie_id
AND ml.movie_id = mc.movie_id
AND mk.movie_id = mc.movie_id
AND ml.movie_id = mi.movie_id
AND mk.movie_id = mi.movie_id
AND mc.movie_id = mi.movie_id
AND ml.movie_id = cc.movie_id
AND mk.movie_id = cc.movie_id
AND mc.movie_id = cc.movie_id
AND mi.movie_id = cc.movie_id
;
.print 'Testing 27b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(cn.name) AS producing_company,
       MIN(lt.link) AS link_type,
       MIN(t.title) AS complete_western_sequel
FROM complete_cast AS cc,
     comp_cast_type AS cct1,
     comp_cast_type AS cct2,
     company_name AS cn,
     company_type AS ct,
     keyword AS k,
     link_type AS lt,
     movie_companies AS mc,
     movie_info AS mi,
     movie_keyword AS mk,
     movie_link AS ml,
     title AS t
WHERE cct1.kind = 'cast'
  AND cct2.kind LIKE 'complete%'
  AND cn.country_code !='[pl]'
  AND (cn.name LIKE '%Film%'
       OR cn.name LIKE '%Warner%')
  AND ct.kind ='production companies'
  AND k.keyword ='sequel'
  AND lt.link LIKE '%follow%'
  AND mc.note IS NULL
  AND mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Denish',
                  'Norwegian',
                  'German',
                  'English')
  AND t.production_year BETWEEN 1950 AND 2010
  AND lt.id = ml.link_type_id
  AND ml.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND t.id = mc.movie_id
  AND mc.company_type_id = ct.id
  AND mc.company_id = cn.id
  AND mi.movie_id = t.id
  AND t.id = cc.movie_id
  AND cct1.id = cc.subject_id
  AND cct2.id = cc.status_id
  AND ml.movie_id = mk.movie_id
  AND ml.movie_id = mc.movie_id
  AND mk.movie_id = mc.movie_id
  AND ml.movie_id = mi.movie_id
  AND mk.movie_id = mi.movie_id
  AND mc.movie_id = mi.movie_id
  AND ml.movie_id = cc.movie_id
  AND mk.movie_id = cc.movie_id
  AND mc.movie_id = cc.movie_id
  AND mi.movie_id = cc.movie_id;

DROP TABLE IF EXISTS cct1;
CREATE TABLE cct1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/27c/cct1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cct2;
CREATE TABLE cct2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/27c/cct2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/27c/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS ct;
CREATE TABLE ct AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/27c/ct.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/27c/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS lt;
CREATE TABLE lt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/27c/lt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/27c/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/27c/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/27c/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(cn.name) AS producing_company, MIN(lt.link) AS link_type, MIN(t.title) AS complete_western_sequel
 FROM mc, movie_keyword AS mk, complete_cast AS cc, cct2, cn, mi, movie_link AS ml, lt, t, k, ct, cct1, 
WHERE lt.id = ml.link_type_id
AND ml.movie_id = t.id
AND t.id = mk.movie_id
AND mk.keyword_id = k.id
AND t.id = mc.movie_id
AND mc.company_type_id = ct.id
AND mc.company_id = cn.id
AND mi.movie_id = t.id
AND t.id = cc.movie_id
AND cct1.id = cc.subject_id
AND cct2.id = cc.status_id
AND ml.movie_id = mk.movie_id
AND ml.movie_id = mc.movie_id
AND mk.movie_id = mc.movie_id
AND ml.movie_id = mi.movie_id
AND mk.movie_id = mi.movie_id
AND mc.movie_id = mi.movie_id
AND ml.movie_id = cc.movie_id
AND mk.movie_id = cc.movie_id
AND mc.movie_id = cc.movie_id
AND mi.movie_id = cc.movie_id
;
.print 'Testing 27c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(cn.name) AS movie_company,
       MIN(mi_idx.info) AS rating,
       MIN(t.title) AS complete_euro_dark_movie
FROM complete_cast AS cc,
     comp_cast_type AS cct1,
     comp_cast_type AS cct2,
     company_name AS cn,
     company_type AS ct,
     info_type AS it1,
     info_type AS it2,
     keyword AS k,
     kind_type AS kt,
     movie_companies AS mc,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     movie_keyword AS mk,
     title AS t
WHERE cct1.kind = 'crew'
  AND cct2.kind != 'complete+verified'
  AND cn.country_code != '[us]'
  AND it1.info = 'countries'
  AND it2.info = 'rating'
  AND k.keyword IN ('murder',
                    'murder-in-title',
                    'blood',
                    'violence')
  AND kt.kind IN ('movie',
                  'episode')
  AND mc.note NOT LIKE '%(USA)%'
  AND mc.note LIKE '%(200%)%'
  AND mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Danish',
                  'Norwegian',
                  'German',
                  'USA',
                  'American')
  AND mi_idx.info < '8.5'
  AND t.production_year > 2000
  AND kt.id = t.kind_id
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND t.id = mi_idx.movie_id
  AND t.id = mc.movie_id
  AND t.id = cc.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND mk.movie_id = mc.movie_id
  AND mk.movie_id = cc.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mc.movie_id
  AND mi.movie_id = cc.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND mc.movie_id = cc.movie_id
  AND mi_idx.movie_id = cc.movie_id
  AND k.id = mk.keyword_id
  AND it1.id = mi.info_type_id
  AND it2.id = mi_idx.info_type_id
  AND ct.id = mc.company_type_id
  AND cn.id = mc.company_id
  AND cct1.id = cc.subject_id
  AND cct2.id = cc.status_id;

DROP TABLE IF EXISTS cct1;
CREATE TABLE cct1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28a/cct1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cct2;
CREATE TABLE cct2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28a/cct2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28a/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28a/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28a/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28a/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS kt;
CREATE TABLE kt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28a/kt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28a/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28a/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi_idx;
.mode csv
CREATE TABLE mi_idx (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, info_type_id integer NOT NULL, info character varying NOT NULL, note character varying(1));
.import --csv --skip 1 '../../queries/join-order-benchmark-redux/data/28a/mi_idx.csv' mi_idx
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28a/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(cn.name) AS movie_company, MIN(mi_idx.info) AS rating, MIN(t.title) AS complete_euro_dark_movie
 FROM t, cct2, it2, cn, it1, k, kt, mc, cct1, company_type AS ct, mi, mi_idx, movie_keyword AS mk, complete_cast AS cc, 
WHERE kt.id = t.kind_id
AND t.id = mi.movie_id
AND t.id = mk.movie_id
AND t.id = mi_idx.movie_id
AND t.id = mc.movie_id
AND t.id = cc.movie_id
AND mk.movie_id = mi.movie_id
AND mk.movie_id = mi_idx.movie_id
AND mk.movie_id = mc.movie_id
AND mk.movie_id = cc.movie_id
AND mi.movie_id = mi_idx.movie_id
AND mi.movie_id = mc.movie_id
AND mi.movie_id = cc.movie_id
AND mc.movie_id = mi_idx.movie_id
AND mc.movie_id = cc.movie_id
AND mi_idx.movie_id = cc.movie_id
AND k.id = mk.keyword_id
AND it1.id = mi.info_type_id
AND it2.id = mi_idx.info_type_id
AND ct.id = mc.company_type_id
AND cn.id = mc.company_id
AND cct1.id = cc.subject_id
AND cct2.id = cc.status_id
;
.print 'Testing 28a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(cn.name) AS movie_company,
       MIN(mi_idx.info) AS rating,
       MIN(t.title) AS complete_euro_dark_movie
FROM complete_cast AS cc,
     comp_cast_type AS cct1,
     comp_cast_type AS cct2,
     company_name AS cn,
     company_type AS ct,
     info_type AS it1,
     info_type AS it2,
     keyword AS k,
     kind_type AS kt,
     movie_companies AS mc,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     movie_keyword AS mk,
     title AS t
WHERE cct1.kind = 'crew'
  AND cct2.kind != 'complete+verified'
  AND cn.country_code != '[us]'
  AND it1.info = 'countries'
  AND it2.info = 'rating'
  AND k.keyword IN ('murder',
                    'murder-in-title',
                    'blood',
                    'violence')
  AND kt.kind IN ('movie',
                  'episode')
  AND mc.note NOT LIKE '%(USA)%'
  AND mc.note LIKE '%(200%)%'
  AND mi.info IN ('Sweden',
                  'Germany',
                  'Swedish',
                  'German')
  AND mi_idx.info > '6.5'
  AND t.production_year > 2005
  AND kt.id = t.kind_id
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND t.id = mi_idx.movie_id
  AND t.id = mc.movie_id
  AND t.id = cc.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND mk.movie_id = mc.movie_id
  AND mk.movie_id = cc.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mc.movie_id
  AND mi.movie_id = cc.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND mc.movie_id = cc.movie_id
  AND mi_idx.movie_id = cc.movie_id
  AND k.id = mk.keyword_id
  AND it1.id = mi.info_type_id
  AND it2.id = mi_idx.info_type_id
  AND ct.id = mc.company_type_id
  AND cn.id = mc.company_id
  AND cct1.id = cc.subject_id
  AND cct2.id = cc.status_id;

DROP TABLE IF EXISTS cct1;
CREATE TABLE cct1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28b/cct1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cct2;
CREATE TABLE cct2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28b/cct2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28b/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28b/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28b/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28b/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS kt;
CREATE TABLE kt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28b/kt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28b/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28b/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi_idx;
.mode csv
CREATE TABLE mi_idx (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, info_type_id integer NOT NULL, info character varying NOT NULL, note character varying(1));
.import --csv --skip 1 '../../queries/join-order-benchmark-redux/data/28b/mi_idx.csv' mi_idx
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28b/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(cn.name) AS movie_company, MIN(mi_idx.info) AS rating, MIN(t.title) AS complete_euro_dark_movie
 FROM mi, movie_keyword AS mk, it1, t, cct2, it2, complete_cast AS cc, company_type AS ct, mi_idx, cct1, cn, kt, mc, k, 
WHERE kt.id = t.kind_id
AND t.id = mi.movie_id
AND t.id = mk.movie_id
AND t.id = mi_idx.movie_id
AND t.id = mc.movie_id
AND t.id = cc.movie_id
AND mk.movie_id = mi.movie_id
AND mk.movie_id = mi_idx.movie_id
AND mk.movie_id = mc.movie_id
AND mk.movie_id = cc.movie_id
AND mi.movie_id = mi_idx.movie_id
AND mi.movie_id = mc.movie_id
AND mi.movie_id = cc.movie_id
AND mc.movie_id = mi_idx.movie_id
AND mc.movie_id = cc.movie_id
AND mi_idx.movie_id = cc.movie_id
AND k.id = mk.keyword_id
AND it1.id = mi.info_type_id
AND it2.id = mi_idx.info_type_id
AND ct.id = mc.company_type_id
AND cn.id = mc.company_id
AND cct1.id = cc.subject_id
AND cct2.id = cc.status_id
;
.print 'Testing 28b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(cn.name) AS movie_company,
       MIN(mi_idx.info) AS rating,
       MIN(t.title) AS complete_euro_dark_movie
FROM complete_cast AS cc,
     comp_cast_type AS cct1,
     comp_cast_type AS cct2,
     company_name AS cn,
     company_type AS ct,
     info_type AS it1,
     info_type AS it2,
     keyword AS k,
     kind_type AS kt,
     movie_companies AS mc,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     movie_keyword AS mk,
     title AS t
WHERE cct1.kind = 'cast'
  AND cct2.kind = 'complete'
  AND cn.country_code != '[us]'
  AND it1.info = 'countries'
  AND it2.info = 'rating'
  AND k.keyword IN ('murder',
                    'murder-in-title',
                    'blood',
                    'violence')
  AND kt.kind IN ('movie',
                  'episode')
  AND mc.note NOT LIKE '%(USA)%'
  AND mc.note LIKE '%(200%)%'
  AND mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Danish',
                  'Norwegian',
                  'German',
                  'USA',
                  'American')
  AND mi_idx.info < '8.5'
  AND t.production_year > 2005
  AND kt.id = t.kind_id
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND t.id = mi_idx.movie_id
  AND t.id = mc.movie_id
  AND t.id = cc.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND mk.movie_id = mc.movie_id
  AND mk.movie_id = cc.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mc.movie_id
  AND mi.movie_id = cc.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND mc.movie_id = cc.movie_id
  AND mi_idx.movie_id = cc.movie_id
  AND k.id = mk.keyword_id
  AND it1.id = mi.info_type_id
  AND it2.id = mi_idx.info_type_id
  AND ct.id = mc.company_type_id
  AND cn.id = mc.company_id
  AND cct1.id = cc.subject_id
  AND cct2.id = cc.status_id;

DROP TABLE IF EXISTS cct1;
CREATE TABLE cct1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28c/cct1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cct2;
CREATE TABLE cct2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28c/cct2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28c/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28c/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28c/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28c/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS kt;
CREATE TABLE kt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28c/kt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28c/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28c/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi_idx;
.mode csv
CREATE TABLE mi_idx (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, info_type_id integer NOT NULL, info character varying NOT NULL, note character varying(1));
.import --csv --skip 1 '../../queries/join-order-benchmark-redux/data/28c/mi_idx.csv' mi_idx
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/28c/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(cn.name) AS movie_company, MIN(mi_idx.info) AS rating, MIN(t.title) AS complete_euro_dark_movie
 FROM cct2, company_type AS ct, complete_cast AS cc, it1, it2, cct1, mi, mi_idx, t, k, kt, cn, mc, movie_keyword AS mk, 
WHERE kt.id = t.kind_id
AND t.id = mi.movie_id
AND t.id = mk.movie_id
AND t.id = mi_idx.movie_id
AND t.id = mc.movie_id
AND t.id = cc.movie_id
AND mk.movie_id = mi.movie_id
AND mk.movie_id = mi_idx.movie_id
AND mk.movie_id = mc.movie_id
AND mk.movie_id = cc.movie_id
AND mi.movie_id = mi_idx.movie_id
AND mi.movie_id = mc.movie_id
AND mi.movie_id = cc.movie_id
AND mc.movie_id = mi_idx.movie_id
AND mc.movie_id = cc.movie_id
AND mi_idx.movie_id = cc.movie_id
AND k.id = mk.keyword_id
AND it1.id = mi.info_type_id
AND it2.id = mi_idx.info_type_id
AND ct.id = mc.company_type_id
AND cn.id = mc.company_id
AND cct1.id = cc.subject_id
AND cct2.id = cc.status_id
;
.print 'Testing 28c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(chn.name) AS voiced_char,
       MIN(n.name) AS voicing_actress,
       MIN(t.title) AS voiced_animation
FROM aka_name AS an,
     complete_cast AS cc,
     comp_cast_type AS cct1,
     comp_cast_type AS cct2,
     char_name AS chn,
     cast_info AS ci,
     company_name AS cn,
     info_type AS it,
     info_type AS it3,
     keyword AS k,
     movie_companies AS mc,
     movie_info AS mi,
     movie_keyword AS mk,
     name AS n,
     person_info AS pi,
     role_type AS rt,
     title AS t
WHERE cct1.kind ='cast'
  AND cct2.kind ='complete+verified'
  AND chn.name = 'Queen'
  AND ci.note IN ('(voice)',
                  '(voice) (uncredited)',
                  '(voice: English version)')
  AND cn.country_code ='[us]'
  AND it.info = 'release dates'
  AND it3.info = 'trivia'
  AND k.keyword = 'computer-animation'
  AND mi.info IS NOT NULL
  AND (mi.info LIKE 'Japan:%200%'
       OR mi.info LIKE 'USA:%200%')
  AND n.gender ='f'
  AND n.name LIKE '%An%'
  AND rt.role ='actress'
  AND t.title = 'Shrek 2'
  AND t.production_year BETWEEN 2000 AND 2010
  AND t.id = mi.movie_id
  AND t.id = mc.movie_id
  AND t.id = ci.movie_id
  AND t.id = mk.movie_id
  AND t.id = cc.movie_id
  AND mc.movie_id = ci.movie_id
  AND mc.movie_id = mi.movie_id
  AND mc.movie_id = mk.movie_id
  AND mc.movie_id = cc.movie_id
  AND mi.movie_id = ci.movie_id
  AND mi.movie_id = mk.movie_id
  AND mi.movie_id = cc.movie_id
  AND ci.movie_id = mk.movie_id
  AND ci.movie_id = cc.movie_id
  AND mk.movie_id = cc.movie_id
  AND cn.id = mc.company_id
  AND it.id = mi.info_type_id
  AND n.id = ci.person_id
  AND rt.id = ci.role_id
  AND n.id = an.person_id
  AND ci.person_id = an.person_id
  AND chn.id = ci.person_role_id
  AND n.id = pi.person_id
  AND ci.person_id = pi.person_id
  AND it3.id = pi.info_type_id
  AND k.id = mk.keyword_id
  AND cct1.id = cc.subject_id
  AND cct2.id = cc.status_id;

DROP TABLE IF EXISTS cct1;
CREATE TABLE cct1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29a/cct1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cct2;
CREATE TABLE cct2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29a/cct2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS chn;
CREATE TABLE chn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29a/chn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29a/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29a/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it3;
CREATE TABLE it3 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29a/it3.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it;
CREATE TABLE it AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29a/it.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29a/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29a/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29a/n.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS rt;
CREATE TABLE rt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29a/rt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29a/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(chn.name) AS voiced_char, MIN(n.name) AS voicing_actress, MIN(t.title) AS voiced_animation
 FROM ci, chn, person_info AS pi, it, k, movie_companies AS mc, movie_keyword AS mk, aka_name AS an, rt, n, complete_cast AS cc, cct2, mi, cct1, t, cn, it3, 
WHERE t.id = mi.movie_id
AND t.id = mc.movie_id
AND t.id = ci.movie_id
AND t.id = mk.movie_id
AND t.id = cc.movie_id
AND mc.movie_id = ci.movie_id
AND mc.movie_id = mi.movie_id
AND mc.movie_id = mk.movie_id
AND mc.movie_id = cc.movie_id
AND mi.movie_id = ci.movie_id
AND mi.movie_id = mk.movie_id
AND mi.movie_id = cc.movie_id
AND ci.movie_id = mk.movie_id
AND ci.movie_id = cc.movie_id
AND mk.movie_id = cc.movie_id
AND cn.id = mc.company_id
AND it.id = mi.info_type_id
AND n.id = ci.person_id
AND rt.id = ci.role_id
AND n.id = an.person_id
AND ci.person_id = an.person_id
AND chn.id = ci.person_role_id
AND n.id = pi.person_id
AND ci.person_id = pi.person_id
AND it3.id = pi.info_type_id
AND k.id = mk.keyword_id
AND cct1.id = cc.subject_id
AND cct2.id = cc.status_id
;
.print 'Testing 29a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(chn.name) AS voiced_char,
       MIN(n.name) AS voicing_actress,
       MIN(t.title) AS voiced_animation
FROM aka_name AS an,
     complete_cast AS cc,
     comp_cast_type AS cct1,
     comp_cast_type AS cct2,
     char_name AS chn,
     cast_info AS ci,
     company_name AS cn,
     info_type AS it,
     info_type AS it3,
     keyword AS k,
     movie_companies AS mc,
     movie_info AS mi,
     movie_keyword AS mk,
     name AS n,
     person_info AS pi,
     role_type AS rt,
     title AS t
WHERE cct1.kind ='cast'
  AND cct2.kind ='complete+verified'
  AND chn.name = 'Queen'
  AND ci.note IN ('(voice)',
                  '(voice) (uncredited)',
                  '(voice: English version)')
  AND cn.country_code ='[us]'
  AND it.info = 'release dates'
  AND it3.info = 'height'
  AND k.keyword = 'computer-animation'
  AND mi.info LIKE 'USA:%200%'
  AND n.gender ='f'
  AND n.name LIKE '%An%'
  AND rt.role ='actress'
  AND t.title = 'Shrek 2'
  AND t.production_year BETWEEN 2000 AND 2005
  AND t.id = mi.movie_id
  AND t.id = mc.movie_id
  AND t.id = ci.movie_id
  AND t.id = mk.movie_id
  AND t.id = cc.movie_id
  AND mc.movie_id = ci.movie_id
  AND mc.movie_id = mi.movie_id
  AND mc.movie_id = mk.movie_id
  AND mc.movie_id = cc.movie_id
  AND mi.movie_id = ci.movie_id
  AND mi.movie_id = mk.movie_id
  AND mi.movie_id = cc.movie_id
  AND ci.movie_id = mk.movie_id
  AND ci.movie_id = cc.movie_id
  AND mk.movie_id = cc.movie_id
  AND cn.id = mc.company_id
  AND it.id = mi.info_type_id
  AND n.id = ci.person_id
  AND rt.id = ci.role_id
  AND n.id = an.person_id
  AND ci.person_id = an.person_id
  AND chn.id = ci.person_role_id
  AND n.id = pi.person_id
  AND ci.person_id = pi.person_id
  AND it3.id = pi.info_type_id
  AND k.id = mk.keyword_id
  AND cct1.id = cc.subject_id
  AND cct2.id = cc.status_id;

DROP TABLE IF EXISTS cct1;
CREATE TABLE cct1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29b/cct1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cct2;
CREATE TABLE cct2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29b/cct2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS chn;
CREATE TABLE chn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29b/chn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29b/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29b/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it3;
CREATE TABLE it3 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29b/it3.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it;
CREATE TABLE it AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29b/it.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29b/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29b/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29b/n.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS rt;
CREATE TABLE rt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29b/rt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29b/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(chn.name) AS voiced_char, MIN(n.name) AS voicing_actress, MIN(t.title) AS voiced_animation
 FROM movie_companies AS mc, mi, cn, cct1, k, aka_name AS an, n, complete_cast AS cc, person_info AS pi, chn, it, cct2, ci, t, rt, movie_keyword AS mk, it3, 
WHERE t.id = mi.movie_id
AND t.id = mc.movie_id
AND t.id = ci.movie_id
AND t.id = mk.movie_id
AND t.id = cc.movie_id
AND mc.movie_id = ci.movie_id
AND mc.movie_id = mi.movie_id
AND mc.movie_id = mk.movie_id
AND mc.movie_id = cc.movie_id
AND mi.movie_id = ci.movie_id
AND mi.movie_id = mk.movie_id
AND mi.movie_id = cc.movie_id
AND ci.movie_id = mk.movie_id
AND ci.movie_id = cc.movie_id
AND mk.movie_id = cc.movie_id
AND cn.id = mc.company_id
AND it.id = mi.info_type_id
AND n.id = ci.person_id
AND rt.id = ci.role_id
AND n.id = an.person_id
AND ci.person_id = an.person_id
AND chn.id = ci.person_role_id
AND n.id = pi.person_id
AND ci.person_id = pi.person_id
AND it3.id = pi.info_type_id
AND k.id = mk.keyword_id
AND cct1.id = cc.subject_id
AND cct2.id = cc.status_id
;
.print 'Testing 29b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(chn.name) AS voiced_char,
       MIN(n.name) AS voicing_actress,
       MIN(t.title) AS voiced_animation
FROM aka_name AS an,
     complete_cast AS cc,
     comp_cast_type AS cct1,
     comp_cast_type AS cct2,
     char_name AS chn,
     cast_info AS ci,
     company_name AS cn,
     info_type AS it,
     info_type AS it3,
     keyword AS k,
     movie_companies AS mc,
     movie_info AS mi,
     movie_keyword AS mk,
     name AS n,
     person_info AS pi,
     role_type AS rt,
     title AS t
WHERE cct1.kind ='cast'
  AND cct2.kind ='complete+verified'
  AND ci.note IN ('(voice)',
                  '(voice: Japanese version)',
                  '(voice) (uncredited)',
                  '(voice: English version)')
  AND cn.country_code ='[us]'
  AND it.info = 'release dates'
  AND it3.info = 'trivia'
  AND k.keyword = 'computer-animation'
  AND mi.info IS NOT NULL
  AND (mi.info LIKE 'Japan:%200%'
       OR mi.info LIKE 'USA:%200%')
  AND n.gender ='f'
  AND n.name LIKE '%An%'
  AND rt.role ='actress'
  AND t.production_year BETWEEN 2000 AND 2010
  AND t.id = mi.movie_id
  AND t.id = mc.movie_id
  AND t.id = ci.movie_id
  AND t.id = mk.movie_id
  AND t.id = cc.movie_id
  AND mc.movie_id = ci.movie_id
  AND mc.movie_id = mi.movie_id
  AND mc.movie_id = mk.movie_id
  AND mc.movie_id = cc.movie_id
  AND mi.movie_id = ci.movie_id
  AND mi.movie_id = mk.movie_id
  AND mi.movie_id = cc.movie_id
  AND ci.movie_id = mk.movie_id
  AND ci.movie_id = cc.movie_id
  AND mk.movie_id = cc.movie_id
  AND cn.id = mc.company_id
  AND it.id = mi.info_type_id
  AND n.id = ci.person_id
  AND rt.id = ci.role_id
  AND n.id = an.person_id
  AND ci.person_id = an.person_id
  AND chn.id = ci.person_role_id
  AND n.id = pi.person_id
  AND ci.person_id = pi.person_id
  AND it3.id = pi.info_type_id
  AND k.id = mk.keyword_id
  AND cct1.id = cc.subject_id
  AND cct2.id = cc.status_id;

DROP TABLE IF EXISTS cct1;
CREATE TABLE cct1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29c/cct1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cct2;
CREATE TABLE cct2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29c/cct2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29c/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29c/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it3;
CREATE TABLE it3 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29c/it3.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it;
CREATE TABLE it AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29c/it.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29c/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29c/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29c/n.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS rt;
CREATE TABLE rt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29c/rt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/29c/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(chn.name) AS voiced_char, MIN(n.name) AS voicing_actress, MIN(t.title) AS voiced_animation
 FROM n, rt, mi, ci, cn, complete_cast AS cc, cct1, t, person_info AS pi, k, char_name AS chn, aka_name AS an, it, it3, movie_keyword AS mk, movie_companies AS mc, cct2, 
WHERE t.id = mi.movie_id
AND t.id = mc.movie_id
AND t.id = ci.movie_id
AND t.id = mk.movie_id
AND t.id = cc.movie_id
AND mc.movie_id = ci.movie_id
AND mc.movie_id = mi.movie_id
AND mc.movie_id = mk.movie_id
AND mc.movie_id = cc.movie_id
AND mi.movie_id = ci.movie_id
AND mi.movie_id = mk.movie_id
AND mi.movie_id = cc.movie_id
AND ci.movie_id = mk.movie_id
AND ci.movie_id = cc.movie_id
AND mk.movie_id = cc.movie_id
AND cn.id = mc.company_id
AND it.id = mi.info_type_id
AND n.id = ci.person_id
AND rt.id = ci.role_id
AND n.id = an.person_id
AND ci.person_id = an.person_id
AND chn.id = ci.person_role_id
AND n.id = pi.person_id
AND ci.person_id = pi.person_id
AND it3.id = pi.info_type_id
AND k.id = mk.keyword_id
AND cct1.id = cc.subject_id
AND cct2.id = cc.status_id
;
.print 'Testing 29c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(t.title) AS movie_title
FROM company_name AS cn,
     keyword AS k,
     movie_companies AS mc,
     movie_keyword AS mk,
     title AS t
WHERE cn.country_code ='[de]'
  AND k.keyword ='character-name-in-title'
  AND cn.id = mc.company_id
  AND mc.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND mc.movie_id = mk.movie_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/2a/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/2a/k.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(t.title) AS movie_title
 FROM k, movie_companies AS mc, cn, movie_keyword AS mk, title AS t, 
WHERE cn.id = mc.company_id
AND mc.movie_id = t.id
AND t.id = mk.movie_id
AND mk.keyword_id = k.id
AND mc.movie_id = mk.movie_id
;
.print 'Testing 2a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(t.title) AS movie_title
FROM company_name AS cn,
     keyword AS k,
     movie_companies AS mc,
     movie_keyword AS mk,
     title AS t
WHERE cn.country_code ='[nl]'
  AND k.keyword ='character-name-in-title'
  AND cn.id = mc.company_id
  AND mc.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND mc.movie_id = mk.movie_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/2b/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/2b/k.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(t.title) AS movie_title
 FROM cn, k, movie_companies AS mc, movie_keyword AS mk, title AS t, 
WHERE cn.id = mc.company_id
AND mc.movie_id = t.id
AND t.id = mk.movie_id
AND mk.keyword_id = k.id
AND mc.movie_id = mk.movie_id
;
.print 'Testing 2b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(t.title) AS movie_title
FROM company_name AS cn,
     keyword AS k,
     movie_companies AS mc,
     movie_keyword AS mk,
     title AS t
WHERE cn.country_code ='[sm]'
  AND k.keyword ='character-name-in-title'
  AND cn.id = mc.company_id
  AND mc.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND mc.movie_id = mk.movie_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/2c/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/2c/k.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(t.title) AS movie_title
 FROM title AS t, cn, movie_companies AS mc, movie_keyword AS mk, k, 
WHERE cn.id = mc.company_id
AND mc.movie_id = t.id
AND t.id = mk.movie_id
AND mk.keyword_id = k.id
AND mc.movie_id = mk.movie_id
;
.print 'Testing 2c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(t.title) AS movie_title
FROM company_name AS cn,
     keyword AS k,
     movie_companies AS mc,
     movie_keyword AS mk,
     title AS t
WHERE cn.country_code ='[us]'
  AND k.keyword ='character-name-in-title'
  AND cn.id = mc.company_id
  AND mc.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND mc.movie_id = mk.movie_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/2d/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/2d/k.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(t.title) AS movie_title
 FROM movie_companies AS mc, k, cn, movie_keyword AS mk, title AS t, 
WHERE cn.id = mc.company_id
AND mc.movie_id = t.id
AND t.id = mk.movie_id
AND mk.keyword_id = k.id
AND mc.movie_id = mk.movie_id
;
.print 'Testing 2d.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(mi.info) AS movie_budget,
       MIN(mi_idx.info) AS movie_votes,
       MIN(n.name) AS writer,
       MIN(t.title) AS complete_violent_movie
FROM complete_cast AS cc,
     comp_cast_type AS cct1,
     comp_cast_type AS cct2,
     cast_info AS ci,
     info_type AS it1,
     info_type AS it2,
     keyword AS k,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE cct1.kind IN ('cast',
                    'crew')
  AND cct2.kind ='complete+verified'
  AND ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)')
  AND it1.info = 'genres'
  AND it2.info = 'votes'
  AND k.keyword IN ('murder',
                    'violence',
                    'blood',
                    'gore',
                    'death',
                    'female-nudity',
                    'hospital')
  AND mi.info IN ('Horror',
                  'Thriller')
  AND n.gender = 'm'
  AND t.production_year > 2000
  AND t.id = mi.movie_id
  AND t.id = mi_idx.movie_id
  AND t.id = ci.movie_id
  AND t.id = mk.movie_id
  AND t.id = cc.movie_id
  AND ci.movie_id = mi.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND ci.movie_id = mk.movie_id
  AND ci.movie_id = cc.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mk.movie_id
  AND mi.movie_id = cc.movie_id
  AND mi_idx.movie_id = mk.movie_id
  AND mi_idx.movie_id = cc.movie_id
  AND mk.movie_id = cc.movie_id
  AND n.id = ci.person_id
  AND it1.id = mi.info_type_id
  AND it2.id = mi_idx.info_type_id
  AND k.id = mk.keyword_id
  AND cct1.id = cc.subject_id
  AND cct2.id = cc.status_id;

DROP TABLE IF EXISTS cct1;
CREATE TABLE cct1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/30a/cct1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cct2;
CREATE TABLE cct2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/30a/cct2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/30a/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/30a/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/30a/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/30a/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/30a/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/30a/n.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/30a/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(mi.info) AS movie_budget, MIN(mi_idx.info) AS movie_votes, MIN(n.name) AS writer, MIN(t.title) AS complete_violent_movie
 FROM mi, complete_cast AS cc, ci, it1, it2, k, movie_keyword AS mk, n, cct1, movie_info_idx AS mi_idx, cct2, t, 
WHERE t.id = mi.movie_id
AND t.id = mi_idx.movie_id
AND t.id = ci.movie_id
AND t.id = mk.movie_id
AND t.id = cc.movie_id
AND ci.movie_id = mi.movie_id
AND ci.movie_id = mi_idx.movie_id
AND ci.movie_id = mk.movie_id
AND ci.movie_id = cc.movie_id
AND mi.movie_id = mi_idx.movie_id
AND mi.movie_id = mk.movie_id
AND mi.movie_id = cc.movie_id
AND mi_idx.movie_id = mk.movie_id
AND mi_idx.movie_id = cc.movie_id
AND mk.movie_id = cc.movie_id
AND n.id = ci.person_id
AND it1.id = mi.info_type_id
AND it2.id = mi_idx.info_type_id
AND k.id = mk.keyword_id
AND cct1.id = cc.subject_id
AND cct2.id = cc.status_id
;
.print 'Testing 30a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(mi.info) AS movie_budget,
       MIN(mi_idx.info) AS movie_votes,
       MIN(n.name) AS writer,
       MIN(t.title) AS complete_gore_movie
FROM complete_cast AS cc,
     comp_cast_type AS cct1,
     comp_cast_type AS cct2,
     cast_info AS ci,
     info_type AS it1,
     info_type AS it2,
     keyword AS k,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE cct1.kind IN ('cast',
                    'crew')
  AND cct2.kind ='complete+verified'
  AND ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)')
  AND it1.info = 'genres'
  AND it2.info = 'votes'
  AND k.keyword IN ('murder',
                    'violence',
                    'blood',
                    'gore',
                    'death',
                    'female-nudity',
                    'hospital')
  AND mi.info IN ('Horror',
                  'Thriller')
  AND n.gender = 'm'
  AND t.production_year > 2000
  AND (t.title LIKE '%Freddy%'
       OR t.title LIKE '%Jason%'
       OR t.title LIKE 'Saw%')
  AND t.id = mi.movie_id
  AND t.id = mi_idx.movie_id
  AND t.id = ci.movie_id
  AND t.id = mk.movie_id
  AND t.id = cc.movie_id
  AND ci.movie_id = mi.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND ci.movie_id = mk.movie_id
  AND ci.movie_id = cc.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mk.movie_id
  AND mi.movie_id = cc.movie_id
  AND mi_idx.movie_id = mk.movie_id
  AND mi_idx.movie_id = cc.movie_id
  AND mk.movie_id = cc.movie_id
  AND n.id = ci.person_id
  AND it1.id = mi.info_type_id
  AND it2.id = mi_idx.info_type_id
  AND k.id = mk.keyword_id
  AND cct1.id = cc.subject_id
  AND cct2.id = cc.status_id;

DROP TABLE IF EXISTS cct1;
CREATE TABLE cct1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/30b/cct1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cct2;
CREATE TABLE cct2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/30b/cct2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/30b/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/30b/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/30b/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/30b/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/30b/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/30b/n.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/30b/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(mi.info) AS movie_budget, MIN(mi_idx.info) AS movie_votes, MIN(n.name) AS writer, MIN(t.title) AS complete_gore_movie
 FROM n, complete_cast AS cc, t, ci, k, movie_info_idx AS mi_idx, it1, cct2, cct1, mi, it2, movie_keyword AS mk, 
WHERE t.id = mi.movie_id
AND t.id = mi_idx.movie_id
AND t.id = ci.movie_id
AND t.id = mk.movie_id
AND t.id = cc.movie_id
AND ci.movie_id = mi.movie_id
AND ci.movie_id = mi_idx.movie_id
AND ci.movie_id = mk.movie_id
AND ci.movie_id = cc.movie_id
AND mi.movie_id = mi_idx.movie_id
AND mi.movie_id = mk.movie_id
AND mi.movie_id = cc.movie_id
AND mi_idx.movie_id = mk.movie_id
AND mi_idx.movie_id = cc.movie_id
AND mk.movie_id = cc.movie_id
AND n.id = ci.person_id
AND it1.id = mi.info_type_id
AND it2.id = mi_idx.info_type_id
AND k.id = mk.keyword_id
AND cct1.id = cc.subject_id
AND cct2.id = cc.status_id
;
.print 'Testing 30b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(mi.info) AS movie_budget,
       MIN(mi_idx.info) AS movie_votes,
       MIN(n.name) AS writer,
       MIN(t.title) AS complete_violent_movie
FROM complete_cast AS cc,
     comp_cast_type AS cct1,
     comp_cast_type AS cct2,
     cast_info AS ci,
     info_type AS it1,
     info_type AS it2,
     keyword AS k,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE cct1.kind = 'cast'
  AND cct2.kind ='complete+verified'
  AND ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)')
  AND it1.info = 'genres'
  AND it2.info = 'votes'
  AND k.keyword IN ('murder',
                    'violence',
                    'blood',
                    'gore',
                    'death',
                    'female-nudity',
                    'hospital')
  AND mi.info IN ('Horror',
                  'Action',
                  'Sci-Fi',
                  'Thriller',
                  'Crime',
                  'War')
  AND n.gender = 'm'
  AND t.id = mi.movie_id
  AND t.id = mi_idx.movie_id
  AND t.id = ci.movie_id
  AND t.id = mk.movie_id
  AND t.id = cc.movie_id
  AND ci.movie_id = mi.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND ci.movie_id = mk.movie_id
  AND ci.movie_id = cc.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mk.movie_id
  AND mi.movie_id = cc.movie_id
  AND mi_idx.movie_id = mk.movie_id
  AND mi_idx.movie_id = cc.movie_id
  AND mk.movie_id = cc.movie_id
  AND n.id = ci.person_id
  AND it1.id = mi.info_type_id
  AND it2.id = mi_idx.info_type_id
  AND k.id = mk.keyword_id
  AND cct1.id = cc.subject_id
  AND cct2.id = cc.status_id;

DROP TABLE IF EXISTS cct1;
CREATE TABLE cct1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/30c/cct1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cct2;
CREATE TABLE cct2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/30c/cct2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/30c/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/30c/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/30c/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/30c/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/30c/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/30c/n.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(mi.info) AS movie_budget, MIN(mi_idx.info) AS movie_votes, MIN(n.name) AS writer, MIN(t.title) AS complete_violent_movie
 FROM it2, k, movie_info_idx AS mi_idx, cct2, movie_keyword AS mk, cct1, n, title AS t, mi, complete_cast AS cc, it1, ci, 
WHERE t.id = mi.movie_id
AND t.id = mi_idx.movie_id
AND t.id = ci.movie_id
AND t.id = mk.movie_id
AND t.id = cc.movie_id
AND ci.movie_id = mi.movie_id
AND ci.movie_id = mi_idx.movie_id
AND ci.movie_id = mk.movie_id
AND ci.movie_id = cc.movie_id
AND mi.movie_id = mi_idx.movie_id
AND mi.movie_id = mk.movie_id
AND mi.movie_id = cc.movie_id
AND mi_idx.movie_id = mk.movie_id
AND mi_idx.movie_id = cc.movie_id
AND mk.movie_id = cc.movie_id
AND n.id = ci.person_id
AND it1.id = mi.info_type_id
AND it2.id = mi_idx.info_type_id
AND k.id = mk.keyword_id
AND cct1.id = cc.subject_id
AND cct2.id = cc.status_id
;
.print 'Testing 30c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(mi.info) AS movie_budget,
       MIN(mi_idx.info) AS movie_votes,
       MIN(n.name) AS writer,
       MIN(t.title) AS violent_liongate_movie
FROM cast_info AS ci,
     company_name AS cn,
     info_type AS it1,
     info_type AS it2,
     keyword AS k,
     movie_companies AS mc,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)')
  AND cn.name LIKE 'Lionsgate%'
  AND it1.info = 'genres'
  AND it2.info = 'votes'
  AND k.keyword IN ('murder',
                    'violence',
                    'blood',
                    'gore',
                    'death',
                    'female-nudity',
                    'hospital')
  AND mi.info IN ('Horror',
                  'Thriller')
  AND n.gender = 'm'
  AND t.id = mi.movie_id
  AND t.id = mi_idx.movie_id
  AND t.id = ci.movie_id
  AND t.id = mk.movie_id
  AND t.id = mc.movie_id
  AND ci.movie_id = mi.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND ci.movie_id = mk.movie_id
  AND ci.movie_id = mc.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mk.movie_id
  AND mi.movie_id = mc.movie_id
  AND mi_idx.movie_id = mk.movie_id
  AND mi_idx.movie_id = mc.movie_id
  AND mk.movie_id = mc.movie_id
  AND n.id = ci.person_id
  AND it1.id = mi.info_type_id
  AND it2.id = mi_idx.info_type_id
  AND k.id = mk.keyword_id
  AND cn.id = mc.company_id;

DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/31a/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/31a/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/31a/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/31a/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/31a/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/31a/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/31a/n.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(mi.info) AS movie_budget, MIN(mi_idx.info) AS movie_votes, MIN(n.name) AS writer, MIN(t.title) AS violent_liongate_movie
 FROM n, movie_keyword AS mk, movie_info_idx AS mi_idx, cn, it2, k, ci, mi, movie_companies AS mc, title AS t, it1, 
WHERE t.id = mi.movie_id
AND t.id = mi_idx.movie_id
AND t.id = ci.movie_id
AND t.id = mk.movie_id
AND t.id = mc.movie_id
AND ci.movie_id = mi.movie_id
AND ci.movie_id = mi_idx.movie_id
AND ci.movie_id = mk.movie_id
AND ci.movie_id = mc.movie_id
AND mi.movie_id = mi_idx.movie_id
AND mi.movie_id = mk.movie_id
AND mi.movie_id = mc.movie_id
AND mi_idx.movie_id = mk.movie_id
AND mi_idx.movie_id = mc.movie_id
AND mk.movie_id = mc.movie_id
AND n.id = ci.person_id
AND it1.id = mi.info_type_id
AND it2.id = mi_idx.info_type_id
AND k.id = mk.keyword_id
AND cn.id = mc.company_id
;
.print 'Testing 31a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(mi.info) AS movie_budget,
       MIN(mi_idx.info) AS movie_votes,
       MIN(n.name) AS writer,
       MIN(t.title) AS violent_liongate_movie
FROM cast_info AS ci,
     company_name AS cn,
     info_type AS it1,
     info_type AS it2,
     keyword AS k,
     movie_companies AS mc,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)')
  AND cn.name LIKE 'Lionsgate%'
  AND it1.info = 'genres'
  AND it2.info = 'votes'
  AND k.keyword IN ('murder',
                    'violence',
                    'blood',
                    'gore',
                    'death',
                    'female-nudity',
                    'hospital')
  AND mc.note LIKE '%(Blu-ray)%'
  AND mi.info IN ('Horror',
                  'Thriller')
  AND n.gender = 'm'
  AND t.production_year > 2000
  AND (t.title LIKE '%Freddy%'
       OR t.title LIKE '%Jason%'
       OR t.title LIKE 'Saw%')
  AND t.id = mi.movie_id
  AND t.id = mi_idx.movie_id
  AND t.id = ci.movie_id
  AND t.id = mk.movie_id
  AND t.id = mc.movie_id
  AND ci.movie_id = mi.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND ci.movie_id = mk.movie_id
  AND ci.movie_id = mc.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mk.movie_id
  AND mi.movie_id = mc.movie_id
  AND mi_idx.movie_id = mk.movie_id
  AND mi_idx.movie_id = mc.movie_id
  AND mk.movie_id = mc.movie_id
  AND n.id = ci.person_id
  AND it1.id = mi.info_type_id
  AND it2.id = mi_idx.info_type_id
  AND k.id = mk.keyword_id
  AND cn.id = mc.company_id;

DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/31b/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/31b/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/31b/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/31b/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/31b/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/31b/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/31b/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/31b/n.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/31b/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(mi.info) AS movie_budget, MIN(mi_idx.info) AS movie_votes, MIN(n.name) AS writer, MIN(t.title) AS violent_liongate_movie
 FROM t, ci, movie_keyword AS mk, cn, n, mc, it1, it2, mi, movie_info_idx AS mi_idx, k, 
WHERE t.id = mi.movie_id
AND t.id = mi_idx.movie_id
AND t.id = ci.movie_id
AND t.id = mk.movie_id
AND t.id = mc.movie_id
AND ci.movie_id = mi.movie_id
AND ci.movie_id = mi_idx.movie_id
AND ci.movie_id = mk.movie_id
AND ci.movie_id = mc.movie_id
AND mi.movie_id = mi_idx.movie_id
AND mi.movie_id = mk.movie_id
AND mi.movie_id = mc.movie_id
AND mi_idx.movie_id = mk.movie_id
AND mi_idx.movie_id = mc.movie_id
AND mk.movie_id = mc.movie_id
AND n.id = ci.person_id
AND it1.id = mi.info_type_id
AND it2.id = mi_idx.info_type_id
AND k.id = mk.keyword_id
AND cn.id = mc.company_id
;
.print 'Testing 31b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(mi.info) AS movie_budget,
       MIN(mi_idx.info) AS movie_votes,
       MIN(n.name) AS writer,
       MIN(t.title) AS violent_liongate_movie
FROM cast_info AS ci,
     company_name AS cn,
     info_type AS it1,
     info_type AS it2,
     keyword AS k,
     movie_companies AS mc,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)')
  AND cn.name LIKE 'Lionsgate%'
  AND it1.info = 'genres'
  AND it2.info = 'votes'
  AND k.keyword IN ('murder',
                    'violence',
                    'blood',
                    'gore',
                    'death',
                    'female-nudity',
                    'hospital')
  AND mi.info IN ('Horror',
                  'Action',
                  'Sci-Fi',
                  'Thriller',
                  'Crime',
                  'War')
  AND t.id = mi.movie_id
  AND t.id = mi_idx.movie_id
  AND t.id = ci.movie_id
  AND t.id = mk.movie_id
  AND t.id = mc.movie_id
  AND ci.movie_id = mi.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND ci.movie_id = mk.movie_id
  AND ci.movie_id = mc.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mk.movie_id
  AND mi.movie_id = mc.movie_id
  AND mi_idx.movie_id = mk.movie_id
  AND mi_idx.movie_id = mc.movie_id
  AND mk.movie_id = mc.movie_id
  AND n.id = ci.person_id
  AND it1.id = mi.info_type_id
  AND it2.id = mi_idx.info_type_id
  AND k.id = mk.keyword_id
  AND cn.id = mc.company_id;

DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/31c/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/31c/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/31c/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/31c/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/31c/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/31c/mi.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(mi.info) AS movie_budget, MIN(mi_idx.info) AS movie_votes, MIN(n.name) AS writer, MIN(t.title) AS violent_liongate_movie
 FROM cn, movie_keyword AS mk, k, mi, title AS t, name AS n, it2, movie_info_idx AS mi_idx, ci, it1, movie_companies AS mc, 
WHERE t.id = mi.movie_id
AND t.id = mi_idx.movie_id
AND t.id = ci.movie_id
AND t.id = mk.movie_id
AND t.id = mc.movie_id
AND ci.movie_id = mi.movie_id
AND ci.movie_id = mi_idx.movie_id
AND ci.movie_id = mk.movie_id
AND ci.movie_id = mc.movie_id
AND mi.movie_id = mi_idx.movie_id
AND mi.movie_id = mk.movie_id
AND mi.movie_id = mc.movie_id
AND mi_idx.movie_id = mk.movie_id
AND mi_idx.movie_id = mc.movie_id
AND mk.movie_id = mc.movie_id
AND n.id = ci.person_id
AND it1.id = mi.info_type_id
AND it2.id = mi_idx.info_type_id
AND k.id = mk.keyword_id
AND cn.id = mc.company_id
;
.print 'Testing 31c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(lt.link) AS link_type,
       MIN(t1.title) AS first_movie,
       MIN(t2.title) AS second_movie
FROM keyword AS k,
     link_type AS lt,
     movie_keyword AS mk,
     movie_link AS ml,
     title AS t1,
     title AS t2
WHERE k.keyword ='10,000-mile-club'
  AND mk.keyword_id = k.id
  AND t1.id = mk.movie_id
  AND ml.movie_id = t1.id
  AND ml.linked_movie_id = t2.id
  AND lt.id = ml.link_type_id
  AND mk.movie_id = t1.id;

DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/32a/k.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(lt.link) AS link_type, MIN(t1.title) AS first_movie, MIN(t2.title) AS second_movie
 FROM movie_link AS ml, link_type AS lt, k, movie_keyword AS mk, title AS t2, title AS t1, 
WHERE mk.keyword_id = k.id
AND t1.id = mk.movie_id
AND ml.movie_id = t1.id
AND ml.linked_movie_id = t2.id
AND lt.id = ml.link_type_id
AND mk.movie_id = t1.id
;
.print 'Testing 32a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(lt.link) AS link_type,
       MIN(t1.title) AS first_movie,
       MIN(t2.title) AS second_movie
FROM keyword AS k,
     link_type AS lt,
     movie_keyword AS mk,
     movie_link AS ml,
     title AS t1,
     title AS t2
WHERE k.keyword ='character-name-in-title'
  AND mk.keyword_id = k.id
  AND t1.id = mk.movie_id
  AND ml.movie_id = t1.id
  AND ml.linked_movie_id = t2.id
  AND lt.id = ml.link_type_id
  AND mk.movie_id = t1.id;

DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/32b/k.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(lt.link) AS link_type, MIN(t1.title) AS first_movie, MIN(t2.title) AS second_movie
 FROM link_type AS lt, movie_link AS ml, title AS t1, k, movie_keyword AS mk, title AS t2, 
WHERE mk.keyword_id = k.id
AND t1.id = mk.movie_id
AND ml.movie_id = t1.id
AND ml.linked_movie_id = t2.id
AND lt.id = ml.link_type_id
AND mk.movie_id = t1.id
;
.print 'Testing 32b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(cn1.name) AS first_company,
       MIN(cn2.name) AS second_company,
       MIN(mi_idx1.info) AS first_rating,
       MIN(mi_idx2.info) AS second_rating,
       MIN(t1.title) AS first_movie,
       MIN(t2.title) AS second_movie
FROM company_name AS cn1,
     company_name AS cn2,
     info_type AS it1,
     info_type AS it2,
     kind_type AS kt1,
     kind_type AS kt2,
     link_type AS lt,
     movie_companies AS mc1,
     movie_companies AS mc2,
     movie_info_idx AS mi_idx1,
     movie_info_idx AS mi_idx2,
     movie_link AS ml,
     title AS t1,
     title AS t2
WHERE cn1.country_code = '[us]'
  AND it1.info = 'rating'
  AND it2.info = 'rating'
  AND kt1.kind IN ('tv series')
  AND kt2.kind IN ('tv series')
  AND lt.link IN ('sequel',
                  'follows',
                  'followed by')
  AND mi_idx2.info < '3.0'
  AND t2.production_year BETWEEN 2005 AND 2008
  AND lt.id = ml.link_type_id
  AND t1.id = ml.movie_id
  AND t2.id = ml.linked_movie_id
  AND it1.id = mi_idx1.info_type_id
  AND t1.id = mi_idx1.movie_id
  AND kt1.id = t1.kind_id
  AND cn1.id = mc1.company_id
  AND t1.id = mc1.movie_id
  AND ml.movie_id = mi_idx1.movie_id
  AND ml.movie_id = mc1.movie_id
  AND mi_idx1.movie_id = mc1.movie_id
  AND it2.id = mi_idx2.info_type_id
  AND t2.id = mi_idx2.movie_id
  AND kt2.id = t2.kind_id
  AND cn2.id = mc2.company_id
  AND t2.id = mc2.movie_id
  AND ml.linked_movie_id = mi_idx2.movie_id
  AND ml.linked_movie_id = mc2.movie_id
  AND mi_idx2.movie_id = mc2.movie_id;

DROP TABLE IF EXISTS cn1;
CREATE TABLE cn1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/33a/cn1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/33a/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/33a/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS kt1;
CREATE TABLE kt1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/33a/kt1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS kt2;
CREATE TABLE kt2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/33a/kt2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS lt;
CREATE TABLE lt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/33a/lt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi_idx2;
.mode csv
CREATE TABLE mi_idx2 (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, info_type_id integer NOT NULL, info character varying NOT NULL, note character varying(1));
.import --csv --skip 1 '../../queries/join-order-benchmark-redux/data/33a/mi_idx2.csv' mi_idx2
DROP TABLE IF EXISTS t2;
CREATE TABLE t2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/33a/t2.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(cn1.name) AS first_company, MIN(cn2.name) AS second_company, MIN(mi_idx1.info) AS first_rating, MIN(mi_idx2.info) AS second_rating, MIN(t1.title) AS first_movie, MIN(t2.title) AS second_movie
 FROM company_name AS cn2, it2, movie_companies AS mc1, mi_idx2, kt2, movie_link AS ml, lt, cn1, kt1, movie_info_idx AS mi_idx1, t2, movie_companies AS mc2, it1, title AS t1, 
WHERE lt.id = ml.link_type_id
AND t1.id = ml.movie_id
AND t2.id = ml.linked_movie_id
AND it1.id = mi_idx1.info_type_id
AND t1.id = mi_idx1.movie_id
AND kt1.id = t1.kind_id
AND cn1.id = mc1.company_id
AND t1.id = mc1.movie_id
AND ml.movie_id = mi_idx1.movie_id
AND ml.movie_id = mc1.movie_id
AND mi_idx1.movie_id = mc1.movie_id
AND it2.id = mi_idx2.info_type_id
AND t2.id = mi_idx2.movie_id
AND kt2.id = t2.kind_id
AND cn2.id = mc2.company_id
AND t2.id = mc2.movie_id
AND ml.linked_movie_id = mi_idx2.movie_id
AND ml.linked_movie_id = mc2.movie_id
AND mi_idx2.movie_id = mc2.movie_id
;
.print 'Testing 33a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(cn1.name) AS first_company,
       MIN(cn2.name) AS second_company,
       MIN(mi_idx1.info) AS first_rating,
       MIN(mi_idx2.info) AS second_rating,
       MIN(t1.title) AS first_movie,
       MIN(t2.title) AS second_movie
FROM company_name AS cn1,
     company_name AS cn2,
     info_type AS it1,
     info_type AS it2,
     kind_type AS kt1,
     kind_type AS kt2,
     link_type AS lt,
     movie_companies AS mc1,
     movie_companies AS mc2,
     movie_info_idx AS mi_idx1,
     movie_info_idx AS mi_idx2,
     movie_link AS ml,
     title AS t1,
     title AS t2
WHERE cn1.country_code = '[nl]'
  AND it1.info = 'rating'
  AND it2.info = 'rating'
  AND kt1.kind IN ('tv series')
  AND kt2.kind IN ('tv series')
  AND lt.link LIKE '%follow%'
  AND mi_idx2.info < '3.0'
  AND t2.production_year = 2007
  AND lt.id = ml.link_type_id
  AND t1.id = ml.movie_id
  AND t2.id = ml.linked_movie_id
  AND it1.id = mi_idx1.info_type_id
  AND t1.id = mi_idx1.movie_id
  AND kt1.id = t1.kind_id
  AND cn1.id = mc1.company_id
  AND t1.id = mc1.movie_id
  AND ml.movie_id = mi_idx1.movie_id
  AND ml.movie_id = mc1.movie_id
  AND mi_idx1.movie_id = mc1.movie_id
  AND it2.id = mi_idx2.info_type_id
  AND t2.id = mi_idx2.movie_id
  AND kt2.id = t2.kind_id
  AND cn2.id = mc2.company_id
  AND t2.id = mc2.movie_id
  AND ml.linked_movie_id = mi_idx2.movie_id
  AND ml.linked_movie_id = mc2.movie_id
  AND mi_idx2.movie_id = mc2.movie_id;

DROP TABLE IF EXISTS cn1;
CREATE TABLE cn1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/33b/cn1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/33b/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/33b/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS kt1;
CREATE TABLE kt1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/33b/kt1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS kt2;
CREATE TABLE kt2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/33b/kt2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS lt;
CREATE TABLE lt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/33b/lt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi_idx2;
.mode csv
CREATE TABLE mi_idx2 (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, info_type_id integer NOT NULL, info character varying NOT NULL, note character varying(1));
.import --csv --skip 1 '../../queries/join-order-benchmark-redux/data/33b/mi_idx2.csv' mi_idx2
DROP TABLE IF EXISTS t2;
CREATE TABLE t2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/33b/t2.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(cn1.name) AS first_company, MIN(cn2.name) AS second_company, MIN(mi_idx1.info) AS first_rating, MIN(mi_idx2.info) AS second_rating, MIN(t1.title) AS first_movie, MIN(t2.title) AS second_movie
 FROM title AS t1, t2, kt1, cn1, it1, it2, movie_companies AS mc2, kt2, movie_companies AS mc1, lt, movie_info_idx AS mi_idx1, company_name AS cn2, mi_idx2, movie_link AS ml, 
WHERE lt.id = ml.link_type_id
AND t1.id = ml.movie_id
AND t2.id = ml.linked_movie_id
AND it1.id = mi_idx1.info_type_id
AND t1.id = mi_idx1.movie_id
AND kt1.id = t1.kind_id
AND cn1.id = mc1.company_id
AND t1.id = mc1.movie_id
AND ml.movie_id = mi_idx1.movie_id
AND ml.movie_id = mc1.movie_id
AND mi_idx1.movie_id = mc1.movie_id
AND it2.id = mi_idx2.info_type_id
AND t2.id = mi_idx2.movie_id
AND kt2.id = t2.kind_id
AND cn2.id = mc2.company_id
AND t2.id = mc2.movie_id
AND ml.linked_movie_id = mi_idx2.movie_id
AND ml.linked_movie_id = mc2.movie_id
AND mi_idx2.movie_id = mc2.movie_id
;
.print 'Testing 33b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(cn1.name) AS first_company,
       MIN(cn2.name) AS second_company,
       MIN(mi_idx1.info) AS first_rating,
       MIN(mi_idx2.info) AS second_rating,
       MIN(t1.title) AS first_movie,
       MIN(t2.title) AS second_movie
FROM company_name AS cn1,
     company_name AS cn2,
     info_type AS it1,
     info_type AS it2,
     kind_type AS kt1,
     kind_type AS kt2,
     link_type AS lt,
     movie_companies AS mc1,
     movie_companies AS mc2,
     movie_info_idx AS mi_idx1,
     movie_info_idx AS mi_idx2,
     movie_link AS ml,
     title AS t1,
     title AS t2
WHERE cn1.country_code != '[us]'
  AND it1.info = 'rating'
  AND it2.info = 'rating'
  AND kt1.kind IN ('tv series',
                   'episode')
  AND kt2.kind IN ('tv series',
                   'episode')
  AND lt.link IN ('sequel',
                  'follows',
                  'followed by')
  AND mi_idx2.info < '3.5'
  AND t2.production_year BETWEEN 2000 AND 2010
  AND lt.id = ml.link_type_id
  AND t1.id = ml.movie_id
  AND t2.id = ml.linked_movie_id
  AND it1.id = mi_idx1.info_type_id
  AND t1.id = mi_idx1.movie_id
  AND kt1.id = t1.kind_id
  AND cn1.id = mc1.company_id
  AND t1.id = mc1.movie_id
  AND ml.movie_id = mi_idx1.movie_id
  AND ml.movie_id = mc1.movie_id
  AND mi_idx1.movie_id = mc1.movie_id
  AND it2.id = mi_idx2.info_type_id
  AND t2.id = mi_idx2.movie_id
  AND kt2.id = t2.kind_id
  AND cn2.id = mc2.company_id
  AND t2.id = mc2.movie_id
  AND ml.linked_movie_id = mi_idx2.movie_id
  AND ml.linked_movie_id = mc2.movie_id
  AND mi_idx2.movie_id = mc2.movie_id;

DROP TABLE IF EXISTS cn1;
CREATE TABLE cn1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/33c/cn1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it1;
CREATE TABLE it1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/33c/it1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it2;
CREATE TABLE it2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/33c/it2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS kt1;
CREATE TABLE kt1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/33c/kt1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS kt2;
CREATE TABLE kt2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/33c/kt2.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS lt;
CREATE TABLE lt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/33c/lt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi_idx2;
.mode csv
CREATE TABLE mi_idx2 (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, info_type_id integer NOT NULL, info character varying NOT NULL, note character varying(1));
.import --csv --skip 1 '../../queries/join-order-benchmark-redux/data/33c/mi_idx2.csv' mi_idx2
DROP TABLE IF EXISTS t2;
CREATE TABLE t2 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/33c/t2.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(cn1.name) AS first_company, MIN(cn2.name) AS second_company, MIN(mi_idx1.info) AS first_rating, MIN(mi_idx2.info) AS second_rating, MIN(t1.title) AS first_movie, MIN(t2.title) AS second_movie
 FROM kt1, movie_companies AS mc1, movie_companies AS mc2, company_name AS cn2, mi_idx2, movie_link AS ml, title AS t1, kt2, cn1, movie_info_idx AS mi_idx1, t2, lt, it1, it2, 
WHERE lt.id = ml.link_type_id
AND t1.id = ml.movie_id
AND t2.id = ml.linked_movie_id
AND it1.id = mi_idx1.info_type_id
AND t1.id = mi_idx1.movie_id
AND kt1.id = t1.kind_id
AND cn1.id = mc1.company_id
AND t1.id = mc1.movie_id
AND ml.movie_id = mi_idx1.movie_id
AND ml.movie_id = mc1.movie_id
AND mi_idx1.movie_id = mc1.movie_id
AND it2.id = mi_idx2.info_type_id
AND t2.id = mi_idx2.movie_id
AND kt2.id = t2.kind_id
AND cn2.id = mc2.company_id
AND t2.id = mc2.movie_id
AND ml.linked_movie_id = mi_idx2.movie_id
AND ml.linked_movie_id = mc2.movie_id
AND mi_idx2.movie_id = mc2.movie_id
;
.print 'Testing 33c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(t.title) AS movie_title
FROM keyword AS k,
     movie_info AS mi,
     movie_keyword AS mk,
     title AS t
WHERE k.keyword LIKE '%sequel%'
  AND mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Denish',
                  'Norwegian',
                  'German')
  AND t.production_year > 2005
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND mk.movie_id = mi.movie_id
  AND k.id = mk.keyword_id;

DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/3a/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/3a/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/3a/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(t.title) AS movie_title
 FROM movie_keyword AS mk, k, mi, t, 
WHERE t.id = mi.movie_id
AND t.id = mk.movie_id
AND mk.movie_id = mi.movie_id
AND k.id = mk.keyword_id
;
.print 'Testing 3a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(t.title) AS movie_title
FROM keyword AS k,
     movie_info AS mi,
     movie_keyword AS mk,
     title AS t
WHERE k.keyword LIKE '%sequel%'
  AND mi.info IN ('Bulgaria')
  AND t.production_year > 2010
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND mk.movie_id = mi.movie_id
  AND k.id = mk.keyword_id;

DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/3b/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/3b/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/3b/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(t.title) AS movie_title
 FROM k, movie_keyword AS mk, t, mi, 
WHERE t.id = mi.movie_id
AND t.id = mk.movie_id
AND mk.movie_id = mi.movie_id
AND k.id = mk.keyword_id
;
.print 'Testing 3b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(t.title) AS movie_title
FROM keyword AS k,
     movie_info AS mi,
     movie_keyword AS mk,
     title AS t
WHERE k.keyword LIKE '%sequel%'
  AND mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Denish',
                  'Norwegian',
                  'German',
                  'USA',
                  'American')
  AND t.production_year > 1990
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND mk.movie_id = mi.movie_id
  AND k.id = mk.keyword_id;

DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/3c/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/3c/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/3c/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(t.title) AS movie_title
 FROM mi, movie_keyword AS mk, t, k, 
WHERE t.id = mi.movie_id
AND t.id = mk.movie_id
AND mk.movie_id = mi.movie_id
AND k.id = mk.keyword_id
;
.print 'Testing 3c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(mi_idx.info) AS rating,
       MIN(t.title) AS movie_title
FROM info_type AS it,
     keyword AS k,
     movie_info_idx AS mi_idx,
     movie_keyword AS mk,
     title AS t
WHERE it.info ='rating'
  AND k.keyword LIKE '%sequel%'
  AND mi_idx.info > '5.0'
  AND t.production_year > 2005
  AND t.id = mi_idx.movie_id
  AND t.id = mk.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND k.id = mk.keyword_id
  AND it.id = mi_idx.info_type_id;

DROP TABLE IF EXISTS it;
CREATE TABLE it AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/4a/it.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/4a/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi_idx;
.mode csv
CREATE TABLE mi_idx (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, info_type_id integer NOT NULL, info character varying NOT NULL, note character varying(1));
.import --csv --skip 1 '../../queries/join-order-benchmark-redux/data/4a/mi_idx.csv' mi_idx
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/4a/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(mi_idx.info) AS rating, MIN(t.title) AS movie_title
 FROM it, movie_keyword AS mk, k, t, mi_idx, 
WHERE t.id = mi_idx.movie_id
AND t.id = mk.movie_id
AND mk.movie_id = mi_idx.movie_id
AND k.id = mk.keyword_id
AND it.id = mi_idx.info_type_id
;
.print 'Testing 4a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(mi_idx.info) AS rating,
       MIN(t.title) AS movie_title
FROM info_type AS it,
     keyword AS k,
     movie_info_idx AS mi_idx,
     movie_keyword AS mk,
     title AS t
WHERE it.info ='rating'
  AND k.keyword LIKE '%sequel%'
  AND mi_idx.info > '9.0'
  AND t.production_year > 2010
  AND t.id = mi_idx.movie_id
  AND t.id = mk.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND k.id = mk.keyword_id
  AND it.id = mi_idx.info_type_id;

DROP TABLE IF EXISTS it;
CREATE TABLE it AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/4b/it.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/4b/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi_idx;
.mode csv
CREATE TABLE mi_idx (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, info_type_id integer NOT NULL, info character varying NOT NULL, note character varying(1));
.import --csv --skip 1 '../../queries/join-order-benchmark-redux/data/4b/mi_idx.csv' mi_idx
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/4b/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(mi_idx.info) AS rating, MIN(t.title) AS movie_title
 FROM k, mi_idx, t, it, movie_keyword AS mk, 
WHERE t.id = mi_idx.movie_id
AND t.id = mk.movie_id
AND mk.movie_id = mi_idx.movie_id
AND k.id = mk.keyword_id
AND it.id = mi_idx.info_type_id
;
.print 'Testing 4b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(mi_idx.info) AS rating,
       MIN(t.title) AS movie_title
FROM info_type AS it,
     keyword AS k,
     movie_info_idx AS mi_idx,
     movie_keyword AS mk,
     title AS t
WHERE it.info ='rating'
  AND k.keyword LIKE '%sequel%'
  AND mi_idx.info > '2.0'
  AND t.production_year > 1990
  AND t.id = mi_idx.movie_id
  AND t.id = mk.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND k.id = mk.keyword_id
  AND it.id = mi_idx.info_type_id;

DROP TABLE IF EXISTS it;
CREATE TABLE it AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/4c/it.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/4c/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi_idx;
.mode csv
CREATE TABLE mi_idx (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, info_type_id integer NOT NULL, info character varying NOT NULL, note character varying(1));
.import --csv --skip 1 '../../queries/join-order-benchmark-redux/data/4c/mi_idx.csv' mi_idx
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/4c/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(mi_idx.info) AS rating, MIN(t.title) AS movie_title
 FROM k, t, it, mi_idx, movie_keyword AS mk, 
WHERE t.id = mi_idx.movie_id
AND t.id = mk.movie_id
AND mk.movie_id = mi_idx.movie_id
AND k.id = mk.keyword_id
AND it.id = mi_idx.info_type_id
;
.print 'Testing 4c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(t.title) AS typical_european_movie
FROM company_type AS ct,
     info_type AS it,
     movie_companies AS mc,
     movie_info AS mi,
     title AS t
WHERE ct.kind = 'production companies'
  AND mc.note LIKE '%(theatrical)%'
  AND mc.note LIKE '%(France)%'
  AND mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Denish',
                  'Norwegian',
                  'German')
  AND t.production_year > 2005
  AND t.id = mi.movie_id
  AND t.id = mc.movie_id
  AND mc.movie_id = mi.movie_id
  AND ct.id = mc.company_type_id
  AND it.id = mi.info_type_id;

DROP TABLE IF EXISTS ct;
CREATE TABLE ct AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/5a/ct.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/5a/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/5a/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/5a/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(t.title) AS typical_european_movie
 FROM ct, info_type AS it, mi, t, mc, 
WHERE t.id = mi.movie_id
AND t.id = mc.movie_id
AND mc.movie_id = mi.movie_id
AND ct.id = mc.company_type_id
AND it.id = mi.info_type_id
;
.print 'Testing 5a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(t.title) AS american_vhs_movie
FROM company_type AS ct,
     info_type AS it,
     movie_companies AS mc,
     movie_info AS mi,
     title AS t
WHERE ct.kind = 'production companies'
  AND mc.note LIKE '%(VHS)%'
  AND mc.note LIKE '%(USA)%'
  AND mc.note LIKE '%(1994)%'
  AND mi.info IN ('USA',
                  'America')
  AND t.production_year > 2010
  AND t.id = mi.movie_id
  AND t.id = mc.movie_id
  AND mc.movie_id = mi.movie_id
  AND ct.id = mc.company_type_id
  AND it.id = mi.info_type_id;

DROP TABLE IF EXISTS ct;
CREATE TABLE ct AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/5b/ct.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/5b/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/5b/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/5b/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(t.title) AS american_vhs_movie
 FROM mc, info_type AS it, t, ct, mi, 
WHERE t.id = mi.movie_id
AND t.id = mc.movie_id
AND mc.movie_id = mi.movie_id
AND ct.id = mc.company_type_id
AND it.id = mi.info_type_id
;
.print 'Testing 5b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(t.title) AS american_movie
FROM company_type AS ct,
     info_type AS it,
     movie_companies AS mc,
     movie_info AS mi,
     title AS t
WHERE ct.kind = 'production companies'
  AND mc.note NOT LIKE '%(TV)%'
  AND mc.note LIKE '%(USA)%'
  AND mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Denish',
                  'Norwegian',
                  'German',
                  'USA',
                  'American')
  AND t.production_year > 1990
  AND t.id = mi.movie_id
  AND t.id = mc.movie_id
  AND mc.movie_id = mi.movie_id
  AND ct.id = mc.company_type_id
  AND it.id = mi.info_type_id;

DROP TABLE IF EXISTS ct;
CREATE TABLE ct AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/5c/ct.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/5c/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mi;
CREATE TABLE mi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/5c/mi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/5c/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(t.title) AS american_movie
 FROM mc, mi, ct, info_type AS it, t, 
WHERE t.id = mi.movie_id
AND t.id = mc.movie_id
AND mc.movie_id = mi.movie_id
AND ct.id = mc.company_type_id
AND it.id = mi.info_type_id
;
.print 'Testing 5c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(k.keyword) AS movie_keyword,
       MIN(n.name) AS actor_name,
       MIN(t.title) AS marvel_movie
FROM cast_info AS ci,
     keyword AS k,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE k.keyword = 'marvel-cinematic-universe'
  AND n.name LIKE '%Downey%Robert%'
  AND t.production_year > 2010
  AND k.id = mk.keyword_id
  AND t.id = mk.movie_id
  AND t.id = ci.movie_id
  AND ci.movie_id = mk.movie_id
  AND n.id = ci.person_id;

DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/6a/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/6a/n.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/6a/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(k.keyword) AS movie_keyword, MIN(n.name) AS actor_name, MIN(t.title) AS marvel_movie
 FROM t, k, n, movie_keyword AS mk, cast_info AS ci, 
WHERE k.id = mk.keyword_id
AND t.id = mk.movie_id
AND t.id = ci.movie_id
AND ci.movie_id = mk.movie_id
AND n.id = ci.person_id
;
.print 'Testing 6a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(k.keyword) AS movie_keyword,
       MIN(n.name) AS actor_name,
       MIN(t.title) AS hero_movie
FROM cast_info AS ci,
     keyword AS k,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE k.keyword IN ('superhero',
                    'sequel',
                    'second-part',
                    'marvel-comics',
                    'based-on-comic',
                    'tv-special',
                    'fight',
                    'violence')
  AND n.name LIKE '%Downey%Robert%'
  AND t.production_year > 2014
  AND k.id = mk.keyword_id
  AND t.id = mk.movie_id
  AND t.id = ci.movie_id
  AND ci.movie_id = mk.movie_id
  AND n.id = ci.person_id;

DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/6b/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/6b/n.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/6b/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(k.keyword) AS movie_keyword, MIN(n.name) AS actor_name, MIN(t.title) AS hero_movie
 FROM k, movie_keyword AS mk, cast_info AS ci, t, n, 
WHERE k.id = mk.keyword_id
AND t.id = mk.movie_id
AND t.id = ci.movie_id
AND ci.movie_id = mk.movie_id
AND n.id = ci.person_id
;
.print 'Testing 6b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(k.keyword) AS movie_keyword,
       MIN(n.name) AS actor_name,
       MIN(t.title) AS marvel_movie
FROM cast_info AS ci,
     keyword AS k,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE k.keyword = 'marvel-cinematic-universe'
  AND n.name LIKE '%Downey%Robert%'
  AND t.production_year > 2014
  AND k.id = mk.keyword_id
  AND t.id = mk.movie_id
  AND t.id = ci.movie_id
  AND ci.movie_id = mk.movie_id
  AND n.id = ci.person_id;

DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/6c/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/6c/n.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/6c/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(k.keyword) AS movie_keyword, MIN(n.name) AS actor_name, MIN(t.title) AS marvel_movie
 FROM n, t, movie_keyword AS mk, k, cast_info AS ci, 
WHERE k.id = mk.keyword_id
AND t.id = mk.movie_id
AND t.id = ci.movie_id
AND ci.movie_id = mk.movie_id
AND n.id = ci.person_id
;
.print 'Testing 6c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(k.keyword) AS movie_keyword,
       MIN(n.name) AS actor_name,
       MIN(t.title) AS hero_movie
FROM cast_info AS ci,
     keyword AS k,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE k.keyword IN ('superhero',
                    'sequel',
                    'second-part',
                    'marvel-comics',
                    'based-on-comic',
                    'tv-special',
                    'fight',
                    'violence')
  AND n.name LIKE '%Downey%Robert%'
  AND t.production_year > 2000
  AND k.id = mk.keyword_id
  AND t.id = mk.movie_id
  AND t.id = ci.movie_id
  AND ci.movie_id = mk.movie_id
  AND n.id = ci.person_id;

DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/6d/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/6d/n.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/6d/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(k.keyword) AS movie_keyword, MIN(n.name) AS actor_name, MIN(t.title) AS hero_movie
 FROM movie_keyword AS mk, t, cast_info AS ci, n, k, 
WHERE k.id = mk.keyword_id
AND t.id = mk.movie_id
AND t.id = ci.movie_id
AND ci.movie_id = mk.movie_id
AND n.id = ci.person_id
;
.print 'Testing 6d.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(k.keyword) AS movie_keyword,
       MIN(n.name) AS actor_name,
       MIN(t.title) AS marvel_movie
FROM cast_info AS ci,
     keyword AS k,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE k.keyword = 'marvel-cinematic-universe'
  AND n.name LIKE '%Downey%Robert%'
  AND t.production_year > 2000
  AND k.id = mk.keyword_id
  AND t.id = mk.movie_id
  AND t.id = ci.movie_id
  AND ci.movie_id = mk.movie_id
  AND n.id = ci.person_id;

DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/6e/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/6e/n.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/6e/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(k.keyword) AS movie_keyword, MIN(n.name) AS actor_name, MIN(t.title) AS marvel_movie
 FROM t, cast_info AS ci, movie_keyword AS mk, n, k, 
WHERE k.id = mk.keyword_id
AND t.id = mk.movie_id
AND t.id = ci.movie_id
AND ci.movie_id = mk.movie_id
AND n.id = ci.person_id
;
.print 'Testing 6e.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(k.keyword) AS movie_keyword,
       MIN(n.name) AS actor_name,
       MIN(t.title) AS hero_movie
FROM cast_info AS ci,
     keyword AS k,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE k.keyword IN ('superhero',
                    'sequel',
                    'second-part',
                    'marvel-comics',
                    'based-on-comic',
                    'tv-special',
                    'fight',
                    'violence')
  AND t.production_year > 2000
  AND k.id = mk.keyword_id
  AND t.id = mk.movie_id
  AND t.id = ci.movie_id
  AND ci.movie_id = mk.movie_id
  AND n.id = ci.person_id;

DROP TABLE IF EXISTS k;
CREATE TABLE k AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/6f/k.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/6f/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(k.keyword) AS movie_keyword, MIN(n.name) AS actor_name, MIN(t.title) AS hero_movie
 FROM cast_info AS ci, movie_keyword AS mk, t, k, name AS n, 
WHERE k.id = mk.keyword_id
AND t.id = mk.movie_id
AND t.id = ci.movie_id
AND ci.movie_id = mk.movie_id
AND n.id = ci.person_id
;
.print 'Testing 6f.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(n.name) AS of_person,
       MIN(t.title) AS biography_movie
FROM aka_name AS an,
     cast_info AS ci,
     info_type AS it,
     link_type AS lt,
     movie_link AS ml,
     name AS n,
     person_info AS pi,
     title AS t
WHERE an.name LIKE '%a%'
  AND it.info ='mini biography'
  AND lt.link ='features'
  AND n.name_pcode_cf BETWEEN 'A' AND 'F'
  AND (n.gender='m'
       OR (n.gender = 'f'
           AND n.name LIKE 'B%'))
  AND pi.note ='Volker Boehm'
  AND t.production_year BETWEEN 1980 AND 1995
  AND n.id = an.person_id
  AND n.id = pi.person_id
  AND ci.person_id = n.id
  AND t.id = ci.movie_id
  AND ml.linked_movie_id = t.id
  AND lt.id = ml.link_type_id
  AND it.id = pi.info_type_id
  AND pi.person_id = an.person_id
  AND pi.person_id = ci.person_id
  AND an.person_id = ci.person_id
  AND ci.movie_id = ml.linked_movie_id;

DROP TABLE IF EXISTS an;
CREATE TABLE an AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/7a/an.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it;
CREATE TABLE it AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/7a/it.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS lt;
CREATE TABLE lt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/7a/lt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/7a/n.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS pi;
CREATE TABLE pi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/7a/pi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/7a/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(n.name) AS of_person, MIN(t.title) AS biography_movie
 FROM pi, movie_link AS ml, cast_info AS ci, an, it, n, t, lt, 
WHERE n.id = an.person_id
AND n.id = pi.person_id
AND ci.person_id = n.id
AND t.id = ci.movie_id
AND ml.linked_movie_id = t.id
AND lt.id = ml.link_type_id
AND it.id = pi.info_type_id
AND pi.person_id = an.person_id
AND pi.person_id = ci.person_id
AND an.person_id = ci.person_id
AND ci.movie_id = ml.linked_movie_id
;
.print 'Testing 7a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(n.name) AS of_person,
       MIN(t.title) AS biography_movie
FROM aka_name AS an,
     cast_info AS ci,
     info_type AS it,
     link_type AS lt,
     movie_link AS ml,
     name AS n,
     person_info AS pi,
     title AS t
WHERE an.name LIKE '%a%'
  AND it.info ='mini biography'
  AND lt.link ='features'
  AND n.name_pcode_cf LIKE 'D%'
  AND n.gender='m'
  AND pi.note ='Volker Boehm'
  AND t.production_year BETWEEN 1980 AND 1984
  AND n.id = an.person_id
  AND n.id = pi.person_id
  AND ci.person_id = n.id
  AND t.id = ci.movie_id
  AND ml.linked_movie_id = t.id
  AND lt.id = ml.link_type_id
  AND it.id = pi.info_type_id
  AND pi.person_id = an.person_id
  AND pi.person_id = ci.person_id
  AND an.person_id = ci.person_id
  AND ci.movie_id = ml.linked_movie_id;

DROP TABLE IF EXISTS an;
CREATE TABLE an AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/7b/an.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it;
CREATE TABLE it AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/7b/it.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS lt;
CREATE TABLE lt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/7b/lt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/7b/n.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS pi;
CREATE TABLE pi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/7b/pi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/7b/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(n.name) AS of_person, MIN(t.title) AS biography_movie
 FROM an, it, n, pi, movie_link AS ml, lt, t, cast_info AS ci, 
WHERE n.id = an.person_id
AND n.id = pi.person_id
AND ci.person_id = n.id
AND t.id = ci.movie_id
AND ml.linked_movie_id = t.id
AND lt.id = ml.link_type_id
AND it.id = pi.info_type_id
AND pi.person_id = an.person_id
AND pi.person_id = ci.person_id
AND an.person_id = ci.person_id
AND ci.movie_id = ml.linked_movie_id
;
.print 'Testing 7b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(n.name) AS cast_member_name,
       MIN(pi.info) AS cast_member_info
FROM aka_name AS an,
     cast_info AS ci,
     info_type AS it,
     link_type AS lt,
     movie_link AS ml,
     name AS n,
     person_info AS pi,
     title AS t
WHERE an.name IS NOT NULL
  AND (an.name LIKE '%a%'
       OR an.name LIKE 'A%')
  AND it.info ='mini biography'
  AND lt.link IN ('references',
                  'referenced in',
                  'features',
                  'featured in')
  AND n.name_pcode_cf BETWEEN 'A' AND 'F'
  AND (n.gender='m'
       OR (n.gender = 'f'
           AND n.name LIKE 'A%'))
  AND pi.note IS NOT NULL
  AND t.production_year BETWEEN 1980 AND 2010
  AND n.id = an.person_id
  AND n.id = pi.person_id
  AND ci.person_id = n.id
  AND t.id = ci.movie_id
  AND ml.linked_movie_id = t.id
  AND lt.id = ml.link_type_id
  AND it.id = pi.info_type_id
  AND pi.person_id = an.person_id
  AND pi.person_id = ci.person_id
  AND an.person_id = ci.person_id
  AND ci.movie_id = ml.linked_movie_id;

DROP TABLE IF EXISTS an;
CREATE TABLE an AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/7c/an.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS it;
CREATE TABLE it AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/7c/it.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS lt;
CREATE TABLE lt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/7c/lt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/7c/n.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS pi;
CREATE TABLE pi AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/7c/pi.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/7c/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(n.name) AS cast_member_name, MIN(pi.info) AS cast_member_info
 FROM cast_info AS ci, n, movie_link AS ml, pi, an, it, t, lt, 
WHERE n.id = an.person_id
AND n.id = pi.person_id
AND ci.person_id = n.id
AND t.id = ci.movie_id
AND ml.linked_movie_id = t.id
AND lt.id = ml.link_type_id
AND it.id = pi.info_type_id
AND pi.person_id = an.person_id
AND pi.person_id = ci.person_id
AND an.person_id = ci.person_id
AND ci.movie_id = ml.linked_movie_id
;
.print 'Testing 7c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(an1.name) AS actress_pseudonym,
       MIN(t.title) AS japanese_movie_dubbed
FROM aka_name AS an1,
     cast_info AS ci,
     company_name AS cn,
     movie_companies AS mc,
     name AS n1,
     role_type AS rt,
     title AS t
WHERE ci.note ='(voice: English version)'
  AND cn.country_code ='[jp]'
  AND mc.note LIKE '%(Japan)%'
  AND mc.note NOT LIKE '%(USA)%'
  AND n1.name LIKE '%Yo%'
  AND n1.name NOT LIKE '%Yu%'
  AND rt.role ='actress'
  AND an1.person_id = n1.id
  AND n1.id = ci.person_id
  AND ci.movie_id = t.id
  AND t.id = mc.movie_id
  AND mc.company_id = cn.id
  AND ci.role_id = rt.id
  AND an1.person_id = ci.person_id
  AND ci.movie_id = mc.movie_id;

DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/8a/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/8a/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/8a/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n1;
CREATE TABLE n1 AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/8a/n1.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS rt;
CREATE TABLE rt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/8a/rt.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(an1.name) AS actress_pseudonym, MIN(t.title) AS japanese_movie_dubbed
 FROM title AS t, aka_name AS an1, rt, ci, mc, cn, n1, 
WHERE an1.person_id = n1.id
AND n1.id = ci.person_id
AND ci.movie_id = t.id
AND t.id = mc.movie_id
AND mc.company_id = cn.id
AND ci.role_id = rt.id
AND an1.person_id = ci.person_id
AND ci.movie_id = mc.movie_id
;
.print 'Testing 8a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(an.name) AS acress_pseudonym,
       MIN(t.title) AS japanese_anime_movie
FROM aka_name AS an,
     cast_info AS ci,
     company_name AS cn,
     movie_companies AS mc,
     name AS n,
     role_type AS rt,
     title AS t
WHERE ci.note ='(voice: English version)'
  AND cn.country_code ='[jp]'
  AND mc.note LIKE '%(Japan)%'
  AND mc.note NOT LIKE '%(USA)%'
  AND (mc.note LIKE '%(2006)%'
       OR mc.note LIKE '%(2007)%')
  AND n.name LIKE '%Yo%'
  AND n.name NOT LIKE '%Yu%'
  AND rt.role ='actress'
  AND t.production_year BETWEEN 2006 AND 2007
  AND (t.title LIKE 'One Piece%'
       OR t.title LIKE 'Dragon Ball Z%')
  AND an.person_id = n.id
  AND n.id = ci.person_id
  AND ci.movie_id = t.id
  AND t.id = mc.movie_id
  AND mc.company_id = cn.id
  AND ci.role_id = rt.id
  AND an.person_id = ci.person_id
  AND ci.movie_id = mc.movie_id;

DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/8b/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/8b/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/8b/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/8b/n.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS rt;
CREATE TABLE rt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/8b/rt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/8b/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(an.name) AS acress_pseudonym, MIN(t.title) AS japanese_anime_movie
 FROM aka_name AS an, ci, cn, rt, n, t, mc, 
WHERE an.person_id = n.id
AND n.id = ci.person_id
AND ci.movie_id = t.id
AND t.id = mc.movie_id
AND mc.company_id = cn.id
AND ci.role_id = rt.id
AND an.person_id = ci.person_id
AND ci.movie_id = mc.movie_id
;
.print 'Testing 8b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(a1.name) AS writer_pseudo_name,
       MIN(t.title) AS movie_title
FROM aka_name AS a1,
     cast_info AS ci,
     company_name AS cn,
     movie_companies AS mc,
     name AS n1,
     role_type AS rt,
     title AS t
WHERE cn.country_code ='[us]'
  AND rt.role ='writer'
  AND a1.person_id = n1.id
  AND n1.id = ci.person_id
  AND ci.movie_id = t.id
  AND t.id = mc.movie_id
  AND mc.company_id = cn.id
  AND ci.role_id = rt.id
  AND a1.person_id = ci.person_id
  AND ci.movie_id = mc.movie_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/8c/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS rt;
CREATE TABLE rt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/8c/rt.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(a1.name) AS writer_pseudo_name, MIN(t.title) AS movie_title
 FROM name AS n1, rt, title AS t, cn, movie_companies AS mc, cast_info AS ci, aka_name AS a1, 
WHERE a1.person_id = n1.id
AND n1.id = ci.person_id
AND ci.movie_id = t.id
AND t.id = mc.movie_id
AND mc.company_id = cn.id
AND ci.role_id = rt.id
AND a1.person_id = ci.person_id
AND ci.movie_id = mc.movie_id
;
.print 'Testing 8c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(an1.name) AS costume_designer_pseudo,
       MIN(t.title) AS movie_with_costumes
FROM aka_name AS an1,
     cast_info AS ci,
     company_name AS cn,
     movie_companies AS mc,
     name AS n1,
     role_type AS rt,
     title AS t
WHERE cn.country_code ='[us]'
  AND rt.role ='costume designer'
  AND an1.person_id = n1.id
  AND n1.id = ci.person_id
  AND ci.movie_id = t.id
  AND t.id = mc.movie_id
  AND mc.company_id = cn.id
  AND ci.role_id = rt.id
  AND an1.person_id = ci.person_id
  AND ci.movie_id = mc.movie_id;

DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/8d/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS rt;
CREATE TABLE rt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/8d/rt.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(an1.name) AS costume_designer_pseudo, MIN(t.title) AS movie_with_costumes
 FROM title AS t, cn, rt, name AS n1, aka_name AS an1, cast_info AS ci, movie_companies AS mc, 
WHERE an1.person_id = n1.id
AND n1.id = ci.person_id
AND ci.movie_id = t.id
AND t.id = mc.movie_id
AND mc.company_id = cn.id
AND ci.role_id = rt.id
AND an1.person_id = ci.person_id
AND ci.movie_id = mc.movie_id
;
.print 'Testing 8d.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(an.name) AS alternative_name,
       MIN(chn.name) AS character_name,
       MIN(t.title) AS movie
FROM aka_name AS an,
     char_name AS chn,
     cast_info AS ci,
     company_name AS cn,
     movie_companies AS mc,
     name AS n,
     role_type AS rt,
     title AS t
WHERE ci.note IN ('(voice)',
                  '(voice: Japanese version)',
                  '(voice) (uncredited)',
                  '(voice: English version)')
  AND cn.country_code ='[us]'
  AND mc.note IS NOT NULL
  AND (mc.note LIKE '%(USA)%'
       OR mc.note LIKE '%(worldwide)%')
  AND n.gender ='f'
  AND n.name LIKE '%Ang%'
  AND rt.role ='actress'
  AND t.production_year BETWEEN 2005 AND 2015
  AND ci.movie_id = t.id
  AND t.id = mc.movie_id
  AND ci.movie_id = mc.movie_id
  AND mc.company_id = cn.id
  AND ci.role_id = rt.id
  AND n.id = ci.person_id
  AND chn.id = ci.person_role_id
  AND an.person_id = n.id
  AND an.person_id = ci.person_id;

DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/9a/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/9a/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/9a/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/9a/n.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS rt;
CREATE TABLE rt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/9a/rt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/9a/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(an.name) AS alternative_name, MIN(chn.name) AS character_name, MIN(t.title) AS movie
 FROM t, cn, aka_name AS an, char_name AS chn, mc, ci, n, rt, 
WHERE ci.movie_id = t.id
AND t.id = mc.movie_id
AND ci.movie_id = mc.movie_id
AND mc.company_id = cn.id
AND ci.role_id = rt.id
AND n.id = ci.person_id
AND chn.id = ci.person_role_id
AND an.person_id = n.id
AND an.person_id = ci.person_id
;
.print 'Testing 9a.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(an.name) AS alternative_name,
       MIN(chn.name) AS voiced_character,
       MIN(n.name) AS voicing_actress,
       MIN(t.title) AS american_movie
FROM aka_name AS an,
     char_name AS chn,
     cast_info AS ci,
     company_name AS cn,
     movie_companies AS mc,
     name AS n,
     role_type AS rt,
     title AS t
WHERE ci.note = '(voice)'
  AND cn.country_code ='[us]'
  AND mc.note LIKE '%(200%)%'
  AND (mc.note LIKE '%(USA)%'
       OR mc.note LIKE '%(worldwide)%')
  AND n.gender ='f'
  AND n.name LIKE '%Angel%'
  AND rt.role ='actress'
  AND t.production_year BETWEEN 2007 AND 2010
  AND ci.movie_id = t.id
  AND t.id = mc.movie_id
  AND ci.movie_id = mc.movie_id
  AND mc.company_id = cn.id
  AND ci.role_id = rt.id
  AND n.id = ci.person_id
  AND chn.id = ci.person_role_id
  AND an.person_id = n.id
  AND an.person_id = ci.person_id;

DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/9b/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/9b/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS mc;
CREATE TABLE mc AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/9b/mc.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/9b/n.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS rt;
CREATE TABLE rt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/9b/rt.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS t;
CREATE TABLE t AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/9b/t.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(an.name) AS alternative_name, MIN(chn.name) AS voiced_character, MIN(n.name) AS voicing_actress, MIN(t.title) AS american_movie
 FROM t, n, cn, char_name AS chn, aka_name AS an, ci, rt, mc, 
WHERE ci.movie_id = t.id
AND t.id = mc.movie_id
AND ci.movie_id = mc.movie_id
AND mc.company_id = cn.id
AND ci.role_id = rt.id
AND n.id = ci.person_id
AND chn.id = ci.person_role_id
AND an.person_id = n.id
AND an.person_id = ci.person_id
;
.print 'Testing 9b.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(an.name) AS alternative_name,
       MIN(chn.name) AS voiced_character_name,
       MIN(n.name) AS voicing_actress,
       MIN(t.title) AS american_movie
FROM aka_name AS an,
     char_name AS chn,
     cast_info AS ci,
     company_name AS cn,
     movie_companies AS mc,
     name AS n,
     role_type AS rt,
     title AS t
WHERE ci.note IN ('(voice)',
                  '(voice: Japanese version)',
                  '(voice) (uncredited)',
                  '(voice: English version)')
  AND cn.country_code ='[us]'
  AND n.gender ='f'
  AND n.name LIKE '%An%'
  AND rt.role ='actress'
  AND ci.movie_id = t.id
  AND t.id = mc.movie_id
  AND ci.movie_id = mc.movie_id
  AND mc.company_id = cn.id
  AND ci.role_id = rt.id
  AND n.id = ci.person_id
  AND chn.id = ci.person_role_id
  AND an.person_id = n.id
  AND an.person_id = ci.person_id;

DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/9c/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/9c/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/9c/n.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS rt;
CREATE TABLE rt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/9c/rt.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(an.name) AS alternative_name, MIN(chn.name) AS voiced_character_name, MIN(n.name) AS voicing_actress, MIN(t.title) AS american_movie
 FROM cn, rt, title AS t, movie_companies AS mc, char_name AS chn, aka_name AS an, ci, n, 
WHERE ci.movie_id = t.id
AND t.id = mc.movie_id
AND ci.movie_id = mc.movie_id
AND mc.company_id = cn.id
AND ci.role_id = rt.id
AND n.id = ci.person_id
AND chn.id = ci.person_role_id
AND an.person_id = n.id
AND an.person_id = ci.person_id
;
.print 'Testing 9c.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;
CREATE TABLE before AS 
SELECT MIN(an.name) AS alternative_name,
       MIN(chn.name) AS voiced_char_name,
       MIN(n.name) AS voicing_actress,
       MIN(t.title) AS american_movie
FROM aka_name AS an,
     char_name AS chn,
     cast_info AS ci,
     company_name AS cn,
     movie_companies AS mc,
     name AS n,
     role_type AS rt,
     title AS t
WHERE ci.note IN ('(voice)',
                  '(voice: Japanese version)',
                  '(voice) (uncredited)',
                  '(voice: English version)')
  AND cn.country_code ='[us]'
  AND n.gender ='f'
  AND rt.role ='actress'
  AND ci.movie_id = t.id
  AND t.id = mc.movie_id
  AND ci.movie_id = mc.movie_id
  AND mc.company_id = cn.id
  AND ci.role_id = rt.id
  AND n.id = ci.person_id
  AND chn.id = ci.person_role_id
  AND an.person_id = n.id
  AND an.person_id = ci.person_id;

DROP TABLE IF EXISTS ci;
CREATE TABLE ci AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/9d/ci.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS cn;
CREATE TABLE cn AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/9d/cn.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS n;
CREATE TABLE n AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/9d/n.csv', header=True, delim=',', escape='\');
DROP TABLE IF EXISTS rt;
CREATE TABLE rt AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/9d/rt.csv', header=True, delim=',', escape='\');
CREATE TABLE after AS 
SELECT MIN(an.name) AS alternative_name, MIN(chn.name) AS voiced_char_name, MIN(n.name) AS voicing_actress, MIN(t.title) AS american_movie
 FROM char_name AS chn, n, ci, movie_companies AS mc, rt, cn, title AS t, aka_name AS an, 
WHERE ci.movie_id = t.id
AND t.id = mc.movie_id
AND ci.movie_id = mc.movie_id
AND mc.company_id = cn.id
AND ci.role_id = rt.id
AND n.id = ci.person_id
AND chn.id = ci.person_role_id
AND an.person_id = n.id
AND an.person_id = ci.person_id
;
.print 'Testing 9d.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
DROP TABLE IF EXISTS before;
DROP TABLE IF EXISTS after;

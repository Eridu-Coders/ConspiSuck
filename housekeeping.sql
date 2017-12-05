drop table if exists "TB_USER_UNIQUE";
create table "TB_USER_UNIQUE" as
select 
	"ID"
	, max("ST_NAME") "ST_NAME"
	, min("DT_CRE") "DT_CRE"
	, min("DT_MSG") "DT_MSG"
	, min("ID_INTERNAL") "ID_INTERNAL"
	, min("ST_USER_ID") "ST_USER_ID"
	, min("ST_TYPE") "ST_TYPE"
	, count(1) as "CHANGES_COUNT"
from "TB_USER"
group by "ID";
ALTER TABLE public."TB_USER_UNIQUE" ADD PRIMARY KEY ("ID");
CREATE UNIQUE INDEX "TB_USER_UNIQUE_id_internal" ON "TB_USER_UNIQUE" USING btree("ID_INTERNAL");

drop table if exists "TMP_UCOUNTS_1";
create table "TMP_UCOUNTS_1" as
select
	"U"."ID"
	, "U"."ID_INTERNAL"
	, "U"."ST_NAME"
	, "L"."LIKES_COUNT"
	, "P"."POSTS_COUNT"
	, "C"."COMMENTS_COUNT"
	, case when "C"."COMMENTS_COUNT" is null then 0 else "C"."COMMENTS_COUNT" end + 
		case when "P"."POSTS_COUNT" is null then 0 else "P"."POSTS_COUNT" end + 
		case when "L"."LIKES_COUNT" is null then 0 else "L"."LIKES_COUNT" end 
	as "TOTAL_COUNT"
from
	"TB_USER_UNIQUE" as "U"
	left outer join (
		select "ID_USER_INTERNAL", count(1) as "LIKES_COUNT"
		from "TB_LIKE"	
		group by "ID_USER_INTERNAL"
	) as "L" on "U"."ID_INTERNAL" = "L"."ID_USER_INTERNAL"
	left outer join (
		select "ID_USER", count(1) as "POSTS_COUNT"
		from "TB_OBJ"
		where "ST_TYPE" = 'Post'	
		group by "ID_USER"
	) as "P" on "U"."ID" = "P"."ID_USER"
	left outer join (
		select "ID_USER", count(1) as "COMMENTS_COUNT"
		from "TB_OBJ"
		where "ST_TYPE" = 'Comm'	
		group by "ID_USER"
	) as "C" on "U"."ID" = "C"."ID_USER"
order by case when "C"."COMMENTS_COUNT" is null then 0 else "C"."COMMENTS_COUNT" end + 
		case when "P"."POSTS_COUNT" is null then 0 else "P"."POSTS_COUNT" end + 
		case when "L"."LIKES_COUNT" is null then 0 else "L"."LIKES_COUNT" end desc;
ALTER TABLE public."TMP_UCOUNTS_1" ADD PRIMARY KEY ("ID");

drop table if exists "TMP_UCOUNTS_2";
create table "TMP_UCOUNTS_2" as
select "ID", count(distinct "ID_PAGE") as "COUNT_PAGE"
from (
	select
		"U"."ID"
		, "O"."ID_PAGE"
	from
		"TB_USER" as "U"
		join "TB_LIKE" as "L" on "U"."ID_INTERNAL" = "L"."ID_USER_INTERNAL"
		join "TB_OBJ" as "O" on "L"."ID_OBJ_INTERNAL" = "O"."ID_INTERNAL"
	union 
		select "ID_USER", "ID_PAGE"
		from "TB_OBJ"
		where "ST_TYPE" = 'Post'	
	union 
		select "ID_USER", "ID_PAGE"
		from "TB_OBJ"
		where "ST_TYPE" = 'Comm'	
) as "A"
group by "ID";
ALTER TABLE public."TMP_UCOUNTS_2" ADD PRIMARY KEY ("ID");

drop table if exists "TB_USER_STATS";
create table "TB_USER_STATS" as
select
	"A".*, "B"."COUNT_PAGE"
from
	"TMP_UCOUNTS_1" as "A"
	join "TMP_UCOUNTS_2" as "B" on "A"."ID" = "B"."ID"
order by "B"."COUNT_PAGE" desc;
ALTER TABLE public."TB_USER_STATS" ADD PRIMARY KEY ("ID");	

drop table if exists "TB_STATS_DAY";
create table "TB_STATS_DAY" as
select * from (
	select "OPT"."DAY", "P"."ID", "P"."TX_NAME",
		"OPT"."COUNT_POST" as "PD_COUNT",
		case when "OCT"."COUNT_COMM" is null then 0 else "OCT"."COUNT_COMM" end "CD COUNT"
	from
		"TB_PAGES" as "P"
		join (
			select "ID_PAGE", count (1) as "COUNT_POST", date_trunc('day', "DT_CRE") "DAY"
			from "TB_OBJ"
			where "ST_TYPE" = 'Post' and "ID_PAGE" = "ID_USER"
			group by "ID_PAGE", date_trunc('day' , "DT_CRE")
		) as "OPT" on "P"."ID" = "OPT"."ID_PAGE"
		left outer join (
			select "ID_PAGE", count (1) as "COUNT_COMM", date_trunc('day', "DT_CRE") "DAY"
			from "TB_OBJ"
			where "ST_TYPE" = 'Comm' 
			group by "ID_PAGE", date_trunc('day' , "DT_CRE")
		) as "OCT" on "P"."ID" = "OCT"."ID_PAGE" and "OCT"."DAY" = "OPT"."DAY"
) as "R"
order by "DAY" desc, "PD_COUNT" desc;
ALTER TABLE public."TB_STATS_DAY" ADD PRIMARY KEY ("DAY", "ID");

update "TB_OBJ" as "O"
set "N_COMM" = "C"."COMM_COUNT"
from (
	select "ID_FATHER", count(1) as "COMM_COUNT"
	from "TB_OBJ"
	where "ST_TYPE" = 'Comm'
	group by "ID_FATHER"
) as "C"
where "O"."ID" = "C"."ID_FATHER" and DATE_PART('day', now()::date - "O"."DT_CRE") <= 30;
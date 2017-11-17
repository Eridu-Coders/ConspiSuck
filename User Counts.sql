drop table "TMP_UCOUNTS_1";
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

drop table "TMP_UCOUNTS_2";
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

drop table "TB_USER_STATS";
create table "TB_USER_STATS" as
select
	"A".*, "B"."COUNT_PAGE"
from
	"TMP_UCOUNTS_1" as "A"
	join "TMP_UCOUNTS_2" as "B" on "A"."ID" = "B"."ID"
order by "B"."COUNT_PAGE" desc;
ALTER TABLE public."TB_USER_STATS" ADD PRIMARY KEY ("ID");	
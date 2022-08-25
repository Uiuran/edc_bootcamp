-- Create table
create table public."Billboard"(
   "date" date null,
   "rank" int4 null,
   song varchar(300) null,
   artist varchar(300) null,
   "last-week" int4 null,
   "peak-rank" int4 null,
   "weeks-on-board" int4 null
);

select count(*) as quantidade
from public."Billboard" b
limit 100;

select 
b."date",
b."rank",
b."artist",
b."song"
from public."Billboard" b 
limit 20;

select 
b."artist",
b."song",
count(*) as quantity
from public."Billboard" b
where b.artist in('Chuck Berry','Frankie Vaughan')
-- b.artist = 'Chuck Berry' or b.artist = 'Frankie Vaughan'
group by b.artist,b.song;


with cte_billboard(StyleID, ID, Nome)
as (
select 1,1,'Rhuan' union all
select 1,1,'André' union all
select 1,2,'Ana' union all
select 1,2,'Gustavo' union all
select 1,3,'Caio' union all
select 1,4,'Cassio' union all
select 1,4,'Jair' union all
select 1,5,'Horácio' union all
select 1,5,'Santana' union all
select 1,6,'Juliana' union all
select 1,6,'Marina'
)
select *,
row_number() over(partition by StyleID order by ID) as "row_number",
rank() over(partition by StyleID order by ID) as "rank",
dense_rank() over(partition by StyleID order by ID) as "dense_rank",
percent_rank() over(partition by StyleID order by ID) as "percent_rank",
cume_dist() over(partition by StyleId order by ID) as "Cume_Dist",
cume_dist() over(partition by StyleId order by ID desc) as "cume_dist_desc",
first_value(Nome) over(partition by StyleID order by ID) as "first_value",
last_value(Nome) over(partition by StyleID order by ID) as "last_value",
nth_value(Nome,5) over(partition by StyleID order by ID) as "nth_value",
NTILE(5) over(order by StyleID) as "ntile_5",
lag(Nome,1) over(order by ID) as "lag_nome",
lead(Nome,1) over(order by ID) as "lead_nome"
from cte_billboard;

with cte_billboard as (
select 
b."date",
b.artist,
b.rank,
row_number() over (partition by b.artist order by b.artist,b."date") as "row_artist"
from public."Billboard" b
order by b.artist
)
select * 
from cte_billboard as cbb
where cbb.row_artist = 1
order by cbb.artist,cbb."date";

create table tb_dedup as(
with cte_dedup_billboard as (
select 
b."date",
b.artist,
b.rank,
row_number() over (partition by b.artist order by b.artist,b."date") as "dedup"
from public."Billboard" b
order by b.artist
)
select 
ctededup."date",
ctededup.artist,
ctededup.rank
from cte_dedup_billboard as ctededup
where ctededup.dedup = 1)

select * from tb_dedup td;

create view vw_dedup as(
select * from tb_dedup
)

select * from vw_dedup;

insert into tb_dedup(
select 
b."date",
b.artist,
b."rank" 
from public."Billboard" b
where b.artist like '%AC/DC%'
);

select * 
from vw_dedup
where vw_dedup.artist like '%AC/DC%';

truncate table tb_dedup;

select * from vw_dedup;

insert into tb_dedup(
select 
b."date",
b.artist,
b."rank" 
from public."Billboard" b
where b.artist like '%AC/DC%'
);

select * from vw_dedup;

drop view if exists vw_dedup;

create or replace view vw_dedup as(
with cte as (
select
td."date",
td.artist,
b.song,
td."rank",
b."peak-rank",
row_number() over(partition by b."song" order by b.song) as "row_number"
from tb_dedup td
left join public."Billboard" b
on td.artist = b.artist)
select
cte."date",
cte.artist,
cte.song,
cte."rank",
cte."peak-rank"
from cte
where cte."row_number" < 2
)

select * from vw_dedup;
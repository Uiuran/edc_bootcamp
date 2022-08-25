from sqlalchemy import create_engine
import psycopg2
import pandas as pd

session = create_engine('postgresql+psycopg2://root:root@localhost:5432/test_db')

query = '''
select * from vw_dedup;
'''

df = pd.read_sql(query,session)

query = ''' create table mock as (
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
from cte_billboard
);
'''

session.execute(query)

df_mock = pd.read_sql('select * from mock;',session)

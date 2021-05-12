---QA
--Q1
--In the last 5 years, how many titles were made per year?
select startYear, count(*)
from title
group by startYear
order by startYear desc 
limit 5;

--Q2
--In the last 5 years what’s the name of the top three actors/actresses that made the most titles? 
--And how many titles did they make?
with top_actors as (
    select personid, count(*) as antal_filmer
    from cast
    where category in ('actor','actress')
    group by personid
    order by count(distinct titleid) desc
    limit 1000 --till 3 såklart...
)
select p.primaryName, ta.*
from top_actors ta 
inner join person p on 
ta.personid = p.personid

--Other possible queries of interest.ADD 
--E1 -- Based on movie get 10 recommendations

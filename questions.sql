---QA
--Q1
--In the last 5 years, how many titles were made per year?
--Report model only title table
select startYear, count(*)
from title
group by startYear
order by startYear desc 
limit 5;

--Q2
--In the last 5 years what’s the name of the top three actors/actresses that made the most titles? 
--And how many titles did they make?
--Report model cast and person 

with top_actors as (
    select personid, count(*) as antal_filmer
    from cast
    where category in ('actor','actress')
    group by personid
    order by count(*) desc
    limit 1000 
)
select p.primaryName, ta.*
from top_actors ta 
inner join person p on 
ta.personid = p.personid

--Other possible queries of interest.ADD 
--E1 -- Based on movie get 10 recommendations based on rating of same genres
with movie_watched as (
    select titleId
    from title
    where primaryTitle = 'The Pony Express Girl'
),
get_genres as (
    select tg.genre
    from title_genre tg
    inner join movie_watched mv ON 
    tg.titleId = mv.titleId
),

get_suggestions as (
    select distinct r.*
    from ratings r 
    inner join title_genre gr ON 
    r.titleId = gr.titleId 
    inner join get_genres gg ON 
    gr.genre in (gg.genre)
    where r.titleId not in (select titleid from movie_watched)
    order by averageRating desc
    limit 10
)

--Results
select t.primaryTitle, gs.averageRating, t.titleId
from get_suggestions gs
join title t ON 
gs.titleId = t.titleId



--Version 2 (based on exactly same genres)
with movie_watched as (
    select titleId
    from title
    where primaryTitle = 'The Pony Express Girl'
),
get_genres as (
    select tg.genre
    from title_genre tg
    inner join movie_watched mv ON 
    tg.titleId = mv.titleId
),

concat_genres as (
select GROUP_CONCAT(genre order by genre asc) as genre_combo
from get_genres
),

get_suggestions as (
    
    select sub2.titleId, sub2.genre_combo
    from (
            select 
            titleId, GROUP_CONCAT(genre order by genre asc) as genre_combo
            from (
                select titleId, genre
                from title_genre tg
                where tg.genre in (select genre from get_genres)
            ) sub1
            group by titleId
        ) sub2
    inner join concat_genres cg ON 
    cg.genre_combo =  sub2.genre_combo
)

--Results
select t.titleId, r.averageRating
from get_suggestions gs
inner join title t ON 
gs.titleId = t.titleId
inner join ratings r ON 
gs.titleId = r.titleId
where r.titleId not in (select titleid from movie_watched)
order by r.averageRating DESC 
limit 10;






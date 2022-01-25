# Sparkify Datawarehouse
***

## Purpose
The purpose of this datawarehouse system is to have a single source of truth db for analysts to be able to run different analytical SQL queries and generate reports and insights in an efficient manner. 

## About The Data
The data is stored in JSON format in multiple files, which are mainly of two types, songs data and logs data.

The song data files contain all the information related to songs and artists.
The log data files store the information related to events streamed from a music app and contain relevent information
related to users, play sesssions, timestamp of event, etc.

## Schema Design
- This datawarehouse follows **Dimensional Modeling** ***(Star Schema)*** approach where the data is stored in **facts** and **dimension** tables.
- All the tables are just in the first normal form to enable faster retrieval of the data and reduce multiple complex joins that could cause a delay in data retrieval.
- At the centre of the schema design lies `songplays` table that stores information depicting an event of a song play. 
  This table holds primary keys of the fact tables and forms a grain by adding event details such as session_id, location, user_agent.
- The fact tables are `songs`, `artists`, `users`, and `time`.

## Pipeline Flow
- Fact tables need to be created first as their keys will be required in the dimension table i.e. `songplays` table.
- We therefore, start with song data files and populate `songs` & `artists` tables.
- We then proceed with log data files to populate `users` & `time` tables.
- Finally, the `songplays` table is populated by using the primary keys of the above tables.

## Sample Queries
- This database can be used to get the answer to various anlytical queries, for example,

- To get the top five songs played overall and userwise (uncomment the where clause for userwise data)
> SELECT song_id, count(songplay_id) play_count
> FROM songplays sp
> INNER JOIN songs s USING(song_id)
> -- WHERE user_id = 16 
> ORDER BY 2 DESC 
> LIMIT 5

- To get the part of the day when user is most active (morning/afternoon/evening/late_night)
> WITH user_data AS (
>     SELECT 
>         user_id, 
>         'morning' part_of_the_day,
>         COUNT(songplay_id) FILTER(WHERE DATE_PART('hour', start_time) BETWEEN 4 AND 11) song_count 
>     FROM songplays
>     INNER JOIN users u USING(user_id)
>     GROUP BY 1
>     UNION ALL
>     SELECT 
>         user_id, 
>         'afternoon' part_of_the_day,
>         COUNT(songplay_id) FILTER(WHERE DATE_PART('hour', start_time) BETWEEN 11 AND 16) song_count 
>     FROM songplays
>     INNER JOIN users u USING(user_id)
>     GROUP BY 1
>     UNION ALL
>     SELECT 
>         user_id, 
>         'evening' part_of_the_day,
>         COUNT(songplay_id) FILTER(WHERE DATE_PART('hour', start_time) BETWEEN 16 AND 22) song_count 
>     FROM songplays
>     INNER JOIN users u USING(user_id)
>     GROUP BY 1
>     UNION ALL
>     SELECT 
>         user_id, 
>         'late_night' part_of_the_day,
>         COUNT(songplay_id) FILTER(WHERE DATE_PART('hour', start_time) BETWEEN 22 AND 4) song_count 
>     FROM songplays
>     INNER JOIN users u USING(user_id)
>     GROUP BY 1
> )
> SELECT DISTINCT ON (user_id)
>     user_id, 
>     CONCAT_WS(' ', u.first_name, u.last_name) user_name,
>     part_of_the_day
> FROM user_data
> INNER JOIN users u USING(user_id)
> WHERE song_count > 0
> ORDER BY 1, 2 DESC
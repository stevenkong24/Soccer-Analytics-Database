# Soccer Analytics Database
### Steven Kong

To use json_loader.py, the directory containing the StatsBomb open data, named "data", must be in the same directory as the json_loader directory.

The inside of the "data" folder should look like the following:
![image](https://github.com/stevenkong24/3005FinalProject/assets/99783492/f818523f-9308-478c-a0c1-21baf4bb9b56)

## Example Queries
### Query 1: Returning the players with the largest xG (expected goals) per game in the 2020/2021 La Liga season in descending order
```
SELECT player_name, SUM(statsbomb_xg)/COUNT(*) as sum_xg
    FROM events INNER JOIN matches ON events.match_id = matches.match_id
    WHERE competition_name = 'La Liga' AND season_name = '2020/2021' AND type_name = 'Shot'
    GROUP BY player_name
    ORDER BY sum_xg DESC
```

### Query 2: Returning the players with the most shots in the 2020/2021 La Liga season in descending order
```
SELECT player_name, COUNT(*)
    FROM events INNER JOIN matches ON events.match_id = matches.match_id
    WHERE competition_name = 'La Liga' AND season_name = '2020/2021' AND events.type_name = 'Shot'
    GROUP BY player_name
    ORDER BY COUNT(*) DESC
```

### Query 3: Returning the players with the most first-time shots in the 2018/2019-2020/2021 La Liga seasons in descending order
```
SELECT player_name, COUNT(*)
    FROM events INNER JOIN matches ON events.match_id = matches.match_id
    WHERE competition_name = 'La Liga' AND (season_name = '2020/2021' OR season_name = '2019/2020' OR season_name = '2018/2019') AND events.first_time = TRUE
    GROUP BY player_name
    ORDER BY COUNT(*) DESC
```

### Query 4: Returning the teams with the most passes made in the 2020/2021 La Liga season in descending order
```
SELECT team_name, COUNT(*)
    FROM events INNER JOIN matches ON events.match_id = matches.match_id
    WHERE competition_name = 'La Liga' AND season_name = '2020/2021' AND events.type_name = 'Pass'
    GROUP BY team_name
    ORDER BY COUNT(*) DESC
```

### Query 5: Returning the players who were the most intended recipients of passes in the 2003/2004 Premier League season in descending order
```
SELECT recipient_name, count(*) 
    FROM events INNER JOIN matches ON events.match_id = matches.match_id
    WHERE competition_name = 'Premier League' AND season_name = '2003/2004' AND recipient_name IS NOT NULL
    GROUP BY recipient_name
    ORDER BY COUNT(*) DESC
```

### Query 6: Returning the players who made the most through balls in the 2020/2021 La Liga season in descending order
```
SELECT player_name, COUNT(*)
    FROM events INNER JOIN matches ON events.match_id = matches.match_id
    WHERE competition_name = 'La Liga' AND season_name = '2020/2021' AND technique_name = 'Through Ball'
    GROUP BY player_name
    ORDER BY COUNT(*) DESC
```

### Query 7: Returning the players with the most dribbles completed in the 2018/2019-2020/2021 La Liga seasons in descending order
```
SELECT player_name, COUNT(*)
    FROM events INNER JOIN matches ON events.match_id = matches.match_id
    WHERE competition_name = 'La Liga' AND (season_name = '2020/2021' OR season_name = '2019/2020' OR season_name = '2018/2019') AND outcome_name = 'Complete' AND events.type_name = 'Dribble'
    GROUP BY player_name
    ORDER BY COUNT(*) DESC
```

### Query 8: Returning the players who were dribbled past the least in the 2020/2021 La Liga season in descending order
```
SELECT player_name, COUNT(*)
    FROM events INNER JOIN matches ON events.match_id = matches.match_id
    WHERE competition_name = 'La Liga' AND season_name = '2020/2021' AND type_name = 'Dribbled Past' 
    GROUP BY player_name
    ORDER BY COUNT(*) DESC
```

### Query 9: Returning the players who shot the most in the top left or top right corners in the 2018/2019-2020/2021 La Liga seasons in descending order
```
SELECT player_name, COUNT(*)
    FROM events INNER JOIN matches ON events.match_id = matches.match_id
    WHERE competition_name = 'La Liga' AND (season_name = '2020/2021' OR season_name = '2019/2020' OR season_name = '2018/2019') AND type_name = 'Shot'
    AND end_location[3] > 1.33 AND end_location[3] < 2.67 AND ((end_location[2] > 36 AND end_location[2] < 38.67) OR (end_location[2] < 44 AND end_location[2] > 41.33))    
    GROUP BY player_name
    ORDER BY COUNT(*) DESC
```

### Query 10: Returning the teams who had the most successful passes into the opponents box in the 2020/2021 La Liga season in descending order
```
SELECT team_name, COUNT(*)
    FROM events INNER JOIN matches ON events.match_id = matches.match_id 
    WHERE competition_name = 'La Liga' AND season_name = '2020/2021' AND type_name = 'Pass' AND outcome_name IS NULL AND 
    ((end_location[1] >= 102 AND end_location[1] <= 120 AND end_location[2] >= 18 AND end_location[2] <= 62 AND (location[1] > 18 OR location[2] > 62 OR location[2] < 18))
    OR (end_location[1] >= 0 AND end_location[1] <= 18 AND end_location[2] >= 18 AND end_location[2] <= 62 AND (location[1] < 102 OR location[2] > 62 OR location[2] < 18)))
    GROUP BY team_name
    ORDER BY COUNT(*) DESC
```

import psycopg
import json
import os
from decimal import Decimal

connection = "host = 'localhost' \
dbname = 'project_database' user = 'postgres' \
password = '1234'"
connect = psycopg.connect(connection)

connect.autocommit = False
cursor = connect.cursor()

cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'project_database'")
exists = cursor.fetchone()
if not exists:
    sql = '''CREATE DATABASE "project_database"'''
    cursor.execute(sql)
    connect.commit()

def createTables():
    sql = '''CREATE TABLE matches(
        match_id INT NOT NULL UNIQUE,
        competition_id INT NOT NULL,
        competition_name VARCHAR(20) NOT NULL,
        country_name VARCHAR(20) NOT NULL,
        season_id INT NOT NULL,
        season_name VARCHAR(10) NOT NULL,
        match_date DATE NOT NULL,
        kick_off TIME NOT NULL,
        stadium_id INT,
        stadium_name VARCHAR(40),
        stadium_country_id INT,
        stadium_country_name VARCHAR(20),
        referee_name VARCHAR(40),
        referee_id INT,
        referee_country_id INT,
        referee_country_name VARCHAR(20),
        home_team_id INT NOT NULL,
        home_team_name VARCHAR(30) NOT NULL,
        home_team_gender VARCHAR(10) NOT NULL,
        home_team_manager_id INT,
        home_team_manager_name VARCHAR(40),
        home_team_manager_nickname VARCHAR(20),
        home_team_manager_dob DATE,
        home_team_manager_country_id INT,
        home_team_manager_country_name VARCHAR(20),
        home_team_group VARCHAR(20),
        home_team_country_id INT NOT NULL,
        home_team_country_name VARCHAR(20) NOT NULL,
        away_team_id INT NOT NULL,
        away_team_name VARCHAR(30) NOT NULL,
        away_team_gender VARCHAR(10) NOT NULL,
        away_team_manager_id INT,
        away_team_manager_name VARCHAR(40),
        away_team_manager_nickname VARCHAR(20),
        away_team_manager_dob DATE,
        away_team_manager_country_id INT,
        away_team_manager_country_name VARCHAR(20),
        away_team_group VARCHAR(20),
        away_team_country_id INT NOT NULL,
        away_team_country_name VARCHAR(20) NOT NULL,     
        home_score INT NOT NULL,
        away_score INT NOT NULL,
        match_week INT NOT NULL,
        competition_stage_id INT NOT NULL,
        competition_stage_name VARCHAR(20) NOT NULL,        
        PRIMARY KEY (match_id)
    )
    '''

    cursor.execute(sql)
    connect.commit()

    sql ='''CREATE TABLE events(
        id UUID NOT NULL UNIQUE,
        match_id INT NOT NULL,
        index INT NOT NULL,
        period INT NOT NULL,
        timestamp TIME NOT NULL,
        minute INT NOT NULL,
        second INT NOT NULL,
        type_id INT NOT NULL,
        type_name VARCHAR(20) NOT NULL,
        possession INT NOT NULL,
        possession_team_id INT NOT NULL,
        possession_team_name VARCHAR(30) NOT NULL,
        play_pattern_id INT NOT NULL,
        play_pattern_name VARCHAR(20) NOT NULL,
        team_id INT NOT NULL,
        team_name VARCHAR(30) NOT NULL,
        player_id INT,
        player_name VARCHAR(60),
        position_id INT,
        position_name VARCHAR(30),
        location DECIMAL(7, 3)[],
        duration DECIMAL(6, 3),
        under_pressure BOOLEAN,
        off_camera BOOLEAN,
        out BOOLEAN,
        formation INT[],
        outcome_id INT,
        outcome_name VARCHAR(25),
        counterpress BOOLEAN,
        card_id INT,
        card_name VARCHAR(20),
        offensive BOOLEAN,
        recovery_failure BOOLEAN,
        deflection BOOLEAN,
        save_block BOOLEAN,
        end_location DECIMAL(7, 3)[],
        aerial_won BOOLEAN,
        body_part_id INT,
        body_part_name VARCHAR(10),
        overrun BOOLEAN,
        nutmeg BOOLEAN,
        no_touch BOOLEAN,
        advantage BOOLEAN,
        penalty BOOLEAN,
        defensive BOOLEAN,
        gk_position_id INT,
        gk_position_name VARCHAR(10),
        technique_id INT,
        technique_name VARCHAR(20),
        specifying_type_id INT,
        specifying_type_name VARCHAR(25),
        early_video_end BOOLEAN,
        match_suspended BOOLEAN,
        late_video_start BOOLEAN,
        in_chain BOOLEAN,
        recipient_id INT,
        recipient_name VARCHAR(60),
        length DECIMAL(10, 7),
        angle DECIMAL(10, 7),
        height_id INT,
        height_name VARCHAR(15),
        assisted_shot_id UUID,
        backheel BOOLEAN,
        deflected BOOLEAN,
        miscommunication BOOLEAN,
        cross_bool BOOLEAN,
        cut_back BOOLEAN,
        switch BOOLEAN,
        shot_assist BOOLEAN,
        goal_assist BOOLEAN,
        permanent BOOLEAN,
        key_pass_id UUID,
        follows_dribble BOOLEAN,
        first_time BOOLEAN,
        open_goal BOOLEAN,
        statsbomb_xg DECIMAL(16, 15),
        replacement_id INT,
        replacement_name VARCHAR(60),        
         
        PRIMARY KEY (id),
  		FOREIGN KEY (match_id) REFERENCES matches(match_id)
    )
    '''
    cursor.execute(sql)
    connect.commit()

    sql ='''CREATE TABLE game_played(
        match_id INT NOT NULL,
        team_id INT NOT NULL,
        team_name VARCHAR(30) NOT NULL,
        player_id INT NOT NULL,
        player_name VARCHAR(50) NOT NULL,
        player_nickname VARCHAR(25),
        jersey_number INT NOT NULL,
        cards_time VARCHAR(10)[], 
        cards_type VARCHAR(15)[],
        cards_reason VARCHAR(20)[],
        cards_period INT[],
        positions_id INT[] NOT NULL,
        positions_name VARCHAR(40)[] NOT NULL, 
        positions_from VARCHAR(20)[],
        positions_to VARCHAR(20)[],
        positions_from_period INT[],
        positions_to_period INT[],
        positions_start_reason VARCHAR(40)[] NOT NULL,
        positions_end_reason VARCHAR(40)[] NOT NULL,
        
        FOREIGN KEY (match_id) REFERENCES matches(match_id)
    )
    '''
    cursor.execute(sql)
    connect.commit()
    for f in os.listdir("../data/data/matches"):
        
        for fi in os.listdir("../data/data/matches/"+f):
            path = "../data/data/matches/"+f+"/"+fi
            file = open(path, encoding="utf8")
            data = json.load(file)
            for x in data:
                if (x["competition"]["competition_name"] != "La Liga" and x["competition"]["competition_name"] != "Premier League"):
                    break
                elif (x["competition"]["competition_name"] == "La Liga" and x["season"]["season_name"] != "2018/2019" and x["season"]["season_name"] != "2019/2020" and x["season"]["season_name"] != "2020/2021"):
                    continue
                elif (x["competition"]["competition_name"] == "Premier League" and x["season"]["season_name"] != "2003/2004"):
                    continue
                sql= '''INSERT INTO matches(match_id, competition_id, competition_name, country_name, season_id, season_name,
                match_date, kick_off, stadium_id, stadium_name, stadium_country_id, stadium_country_name, referee_name, referee_id, referee_country_id,
                referee_country_name, home_team_id, home_team_name, home_team_gender, home_team_manager_id, home_team_manager_name,
                home_team_manager_nickname, home_team_manager_dob, home_team_manager_country_id, home_team_manager_country_name,
                home_team_group, home_team_country_id, home_team_country_name, away_team_id, away_team_name, away_team_gender, 
                away_team_manager_id, away_team_manager_name, away_team_manager_nickname, away_team_manager_dob, 
                away_team_manager_country_id, away_team_manager_country_name, away_team_group, away_team_country_id, 
                away_team_country_name, home_score, away_score, match_week, competition_stage_id, competition_stage_name)                
                VALUES (%s, %s, %s, %s, %s, %s, %s,  %s, %s, %s, %s, %s, %s, %s,  %s, %s, %s, %s, %s, %s, %s,  %s, %s, %s, %s, %s, %s, %s,  
                %s, %s, %s, %s, %s, %s, %s,  %s, %s, %s, %s, %s, %s, %s,  %s, %s, %s);'''
                newData = []
                
                newData.append(x["match_id"])
                newData.append(x["competition"]["competition_id"])
                newData.append(x["competition"]["competition_name"])
                newData.append(x["competition"]["country_name"])
                newData.append(x["season"]["season_id"])
                newData.append(x["season"]["season_name"])
                newData.append(x["match_date"])
                newData.append(x["kick_off"])
                if ("stadium" in x.keys()):
                    newData.append(x["stadium"]["id"])
                    newData.append(x["stadium"]["name"])
                    newData.append(x["stadium"]["country"]["id"])
                    newData.append(x["stadium"]["country"]["name"])
                else:
                    newData.append(None)
                    newData.append(None)
                    newData.append(None)
                    newData.append(None)
                    
                if ("referee" in x.keys()):
                    newData.append(x["referee"]["name"])
                    newData.append(x["referee"]["id"])
                    newData.append(x["referee"]["country"]["id"])
                    newData.append(x["referee"]["country"]["name"])
                else:
                    newData.append(None)
                    newData.append(None)
                    newData.append(None)
                    newData.append(None)
                newData.append(x["home_team"]["home_team_id"])
                newData.append(x["home_team"]["home_team_name"])
                newData.append(x["home_team"]["home_team_gender"])
                if ("manager" in x["home_team"].keys()):
                    newData.append(x["home_team"]["managers"][0]["id"])
                    newData.append(x["home_team"]["managers"][0]["name"])
                    newData.append(x["home_team"]["managers"][0]["nickname"])
                    newData.append(x["home_team"]["managers"][0]["dob"])
                    newData.append(x["home_team"]["managers"][0]["country"]["id"])
                    newData.append(x["home_team"]["managers"][0]["country"]["name"])
                else:
                    newData.append(None)
                    newData.append(None)
                    newData.append(None)
                    newData.append(None)
                    newData.append(None)
                    newData.append(None) 
                newData.append(x["away_team"]["away_team_group"])
                newData.append(x["away_team"]["country"]["id"])
                newData.append(x["away_team"]["country"]["name"])
                newData.append(x["away_team"]["away_team_id"])
                newData.append(x["away_team"]["away_team_name"])
                newData.append(x["away_team"]["away_team_gender"])
                if ("manager" in x["away_team"].keys()):
                    newData.append(x["away_team"]["managers"][0]["id"])
                    newData.append(x["away_team"]["managers"][0]["name"])
                    newData.append(x["away_team"]["managers"][0]["nickname"])
                    newData.append(x["away_team"]["managers"][0]["dob"])
                    newData.append(x["away_team"]["managers"][0]["country"]["id"])
                    newData.append(x["away_team"]["managers"][0]["country"]["name"])
                else:
                    newData.append(None)
                    newData.append(None)
                    newData.append(None)
                    newData.append(None)
                    newData.append(None)
                    newData.append(None) 
                newData.append(x["away_team"]["away_team_group"])
                newData.append(x["away_team"]["country"]["id"])
                newData.append(x["away_team"]["country"]["name"])
                newData.append(x["home_score"])
                newData.append(x["away_score"])
                newData.append(x["match_week"])
                newData.append(x["competition_stage"]["id"])
                newData.append(x["competition_stage"]["name"])
                
                cursor.execute(sql, newData)
                connect.commit()
                
                path = "../data/data/lineups/"+ str(x["match_id"]) +".json"
                file = open(path, encoding="utf8")
                data2 = json.load(file)
                for y in data2:
                    for z in y["lineup"]:  
                        newData = []
                        newData.append(x["match_id"])
                        newData.append(y["team_id"])
                        newData.append(y["team_name"])
                        newData.append(z["player_id"])
                        newData.append(z["player_name"])
                        if ("player_nickname" in z.keys()):
                            newData.append(z["player_nickname"])
                        else:
                            newData.append(None)
                        newData.append(z["jersey_number"])
                        if (len(z["cards"]) == 0):
                            newData.append([])
                            newData.append([])
                            newData.append([])
                            newData.append([])
                        else:
                            cards_time = []
                            cards_card_type = []
                            cards_reason = []
                            cards_period = []
                            for a in z["cards"]:
                                cards_time.append(a["time"])
                                cards_card_type.append(a["card_type"])
                                cards_reason.append(a["reason"])
                                cards_period.append(a["period"])
                            newData.append(cards_time)
                            newData.append(cards_card_type)
                            newData.append(cards_reason)
                            newData.append(cards_period)
                        position_id = []
                        position = []
                        from_time = []
                        to = []
                        from_period = []
                        to_period = []
                        start_reason = []
                        end_reason = []
                        
                        for a in z["positions"]:
                            position_id.append(a["position_id"])
                            position.append(a["position"])
                            if ("from" in a.keys()):
                                from_time.append(a["from"])
                                from_period.append(a["from_period"])
                            else:
                                from_time.append(None)
                                from_period.append(None)
                                
                            if ("to" in a.keys()):
                                to.append(a["to"])
                                to_period.append(a["to_period"])
                            else:
                                to.append(None)
                                to_period.append(None)
                            
                            start_reason.append(a["start_reason"])
                            end_reason.append(a["end_reason"])
                        
                                
                        newData.append(position_id)
                        newData.append(position)
                        newData.append(from_time)
                        newData.append(to)
                        newData.append(from_period)
                        newData.append(to_period)
                        newData.append(start_reason)
                        newData.append(end_reason)
                        sql = '''INSERT INTO game_played(match_id, team_id, team_name, player_id, player_name, player_nickname, jersey_number, cards_time, cards_type,
                        cards_reason, cards_period, positions_id, positions_name, positions_from, positions_to, positions_from_period, positions_to_period,
                        positions_start_reason, positions_end_reason) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        '''
                        cursor.execute(sql, newData)
                        connect.commit()
                       
                        
                        
                path = "../data/data/events/"+ str(x["match_id"]) +".json"
                file = open(path, encoding="utf8")
                data2 = json.load(file)
                for y in data2:
                    if (y["type"]["name"] == "Shot"):
                        sql = '''INSERT INTO events(id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession, possession_team_id,
                        possession_team_name, play_pattern_id, play_pattern_name, team_id, team_name, player_id, player_name, position_id, position_name, location, duration, under_pressure, off_camera, out,
                        statsbomb_xg, end_location, body_part_id, body_part_name, technique_id, technique_name, specifying_type_id, specifying_type_name, outcome_id, outcome_name, key_pass_id, aerial_won,
                        follows_dribble, first_time, open_goal, deflected) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        '''
                        newData = []
                        newData.append(y["id"])
                        newData.append(x["match_id"])
                        newData.append(y["index"])
                        newData.append(y["period"])
                        newData.append(y["timestamp"])
                        newData.append(y["minute"])
                        newData.append(y["second"])
                        newData.append(y["type"]["id"])
                        newData.append(y["type"]["name"])
                        newData.append(y["possession"])
                        newData.append(y["possession_team"]["id"])
                        newData.append(y["possession_team"]["name"])
                        newData.append(y["play_pattern"]["id"])
                        newData.append(y["play_pattern"]["name"])
                        newData.append(y["team"]["id"])
                        newData.append(y["team"]["name"])
                        newData.append(y["player"]["id"])
                        newData.append(y["player"]["name"])
                        newData.append(y["position"]["id"])
                        newData.append(y["position"]["name"])
                        newData.append(y["location"])
                        newData.append(y["duration"])
                        
                        if ("under_pressure" in y.keys()):
                            newData.append(y["under_pressure"])
                        else:
                            newData.append(None)
                            
                        if ("off_camera" in y.keys()):
                            newData.append(y["off_camera"])
                        else:
                            newData.append(None)
                        
                        if ("out" in y.keys()):
                            newData.append(y["out"])
                        else:
                            newData.append(None)
                        
                        newData.append(y["shot"]["statsbomb_xg"])
                        newData.append(y["shot"]["end_location"])
                        newData.append(y["shot"]["body_part"]["id"])
                        newData.append(y["shot"]["body_part"]["name"])
                        newData.append(y["shot"]["technique"]["id"])
                        newData.append(y["shot"]["technique"]["name"])
                        newData.append(y["shot"]["type"]["id"])
                        newData.append(y["shot"]["type"]["name"])
                        newData.append(y["shot"]["outcome"]["id"])
                        newData.append(y["shot"]["outcome"]["name"])
                        if ("key_pass_id" in y["shot"].keys()):
                            newData.append(y["shot"]["key_pass_id"])
                        else:
                            newData.append(None)
                            
                        if ("aerial_won" in y["shot"].keys()):
                            newData.append(y["shot"]["aerial_won"])
                        else:
                            newData.append(None)
                            
                        if ("follows_dribble" in y["shot"].keys()):
                            newData.append(y["shot"]["follows_dribble"])
                        else:
                            newData.append(None)
                        
                        if ("first_time" in y["shot"].keys()):
                            newData.append(y["shot"]["first_time"])
                        else:
                            newData.append(None)
                        
                        if ("open_goal" in y["shot"].keys()):
                            newData.append(y["shot"]["open_goal"])
                        else:
                            newData.append(None)

                        if ("deflected" in y["shot"].keys()):
                            newData.append(y["shot"]["deflected"])
                        else:
                            newData.append(None)
                        cursor.execute(sql, newData)
                        connect.commit()
                    
                    elif (y["type"]["name"] == "Dribble"):
                        sql = '''INSERT INTO events(id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession, possession_team_id,
                        possession_team_name, play_pattern_id, play_pattern_name, team_id, team_name, player_id, player_name, position_id, position_name, location, duration, 
                        under_pressure, off_camera, out, overrun, nutmeg, outcome_id, outcome_name, no_touch)                       
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        '''
                        newData = []
                        newData.append(y["id"])
                        newData.append(x["match_id"])
                        newData.append(y["index"])
                        newData.append(y["period"])
                        newData.append(y["timestamp"])
                        newData.append(y["minute"])
                        newData.append(y["second"])
                        newData.append(y["type"]["id"])
                        newData.append(y["type"]["name"])
                        newData.append(y["possession"])
                        newData.append(y["possession_team"]["id"])
                        newData.append(y["possession_team"]["name"])
                        newData.append(y["play_pattern"]["id"])
                        newData.append(y["play_pattern"]["name"])
                        newData.append(y["team"]["id"])
                        newData.append(y["team"]["name"])
                        newData.append(y["player"]["id"])
                        newData.append(y["player"]["name"])
                        newData.append(y["position"]["id"])
                        newData.append(y["position"]["name"])
                        newData.append(y["location"])
                        newData.append(y["duration"])
                        
                        if ("under_pressure" in y.keys()):
                            newData.append(y["under_pressure"])
                        else:
                            newData.append(None)
                            
                        if ("off_camera" in y.keys()):
                            newData.append(y["off_camera"])
                        else:
                            newData.append(None)
                        
                        if ("out" in y.keys()):
                            newData.append(y["out"])
                        else:
                            newData.append(None)
                        
                        if ("overrun" in y["dribble"].keys()):
                            newData.append(y["dribble"]["overrun"])
                        else:
                            newData.append(None)
                        
                        if ("nutmeg" in y["dribble"].keys()):
                            newData.append(y["dribble"]["nutmeg"])
                        else:
                            newData.append(None)
                        
                        newData.append(y["dribble"]["outcome"]["id"])  
                        newData.append(y["dribble"]["outcome"]["name"])
                        
                        if ("no_touch" in y["dribble"].keys()):
                            newData.append(y["dribble"]["no_touch"])
                        else:
                            newData.append(None)
                            
                        cursor.execute(sql, newData)    
                        connect.commit()
                    
                    elif (y["type"]["name"] == "Dribbled Past"):
                        sql = '''INSERT INTO events(id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession, possession_team_id,
                        possession_team_name, play_pattern_id, play_pattern_name, team_id, team_name, player_id, player_name, position_id, position_name, location, duration, 
                        under_pressure, off_camera, out, counterpress)                       
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        '''
                        newData = []
                        newData.append(y["id"])
                        newData.append(x["match_id"])
                        newData.append(y["index"])
                        newData.append(y["period"])
                        newData.append(y["timestamp"])
                        newData.append(y["minute"])
                        newData.append(y["second"])
                        newData.append(y["type"]["id"])
                        newData.append(y["type"]["name"])
                        newData.append(y["possession"])
                        newData.append(y["possession_team"]["id"])
                        newData.append(y["possession_team"]["name"])
                        newData.append(y["play_pattern"]["id"])
                        newData.append(y["play_pattern"]["name"])
                        newData.append(y["team"]["id"])
                        newData.append(y["team"]["name"])
                        newData.append(y["player"]["id"])
                        newData.append(y["player"]["name"])
                        newData.append(y["position"]["id"])
                        newData.append(y["position"]["name"])
                        newData.append(y["location"])
                        newData.append(y["duration"])
                        
                        if ("under_pressure" in y.keys()):
                            newData.append(y["under_pressure"])
                        else:
                            newData.append(None)
                            
                        if ("off_camera" in y.keys()):
                            newData.append(y["off_camera"])
                        else:
                            newData.append(None)
                        
                        if ("out" in y.keys()):
                            newData.append(y["out"])
                        else:
                            newData.append(None)
                        
                        if ("counterpress" in y.keys()):
                            newData.append(y["counterpress"])
                        else:
                            newData.append(None)
                            
                        cursor.execute(sql, newData)
                        connect.commit()
                        
                    elif (y["type"]["name"] == "Pass"):
                        sql = '''INSERT INTO events(id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession, possession_team_id,
                        possession_team_name, play_pattern_id, play_pattern_name, team_id, team_name, player_id, player_name, position_id, position_name, location, duration, under_pressure,
                        off_camera, out, recipient_id, recipient_name, length, angle, height_id, height_name, end_location, assisted_shot_id, body_part_id, body_part_name, technique_id, technique_name, 
                        specifying_type_id, specifying_type_name, outcome_id, outcome_name, backheel, deflected, miscommunication, cross_bool, cut_back, switch, shot_assist, goal_assist, no_touch)                       
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        '''
                        newData = []
                        newData.append(y["id"])
                        newData.append(x["match_id"])
                        newData.append(y["index"])
                        newData.append(y["period"])
                        newData.append(y["timestamp"])
                        newData.append(y["minute"])
                        newData.append(y["second"])
                        newData.append(y["type"]["id"])
                        newData.append(y["type"]["name"])
                        newData.append(y["possession"])
                        newData.append(y["possession_team"]["id"])
                        newData.append(y["possession_team"]["name"])
                        newData.append(y["play_pattern"]["id"])
                        newData.append(y["play_pattern"]["name"])
                        newData.append(y["team"]["id"])
                        newData.append(y["team"]["name"])
                        newData.append(y["player"]["id"])
                        newData.append(y["player"]["name"])
                        newData.append(y["position"]["id"])
                        newData.append(y["position"]["name"])
                        newData.append(y["location"])
                        newData.append(y["duration"])
                        
                        if ("under_pressure" in y.keys()):
                            newData.append(y["under_pressure"])
                        else:
                            newData.append(None)
                            
                        if ("off_camera" in y.keys()):
                            newData.append(y["off_camera"])
                        else:
                            newData.append(None)
                        
                        if ("out" in y.keys()):
                            newData.append(y["out"])
                        else:
                            newData.append(None)
                        
                        if ("recipient" in y["pass"].keys()):
                            newData.append(y["pass"]["recipient"]["id"])
                            newData.append(y["pass"]["recipient"]["name"])

                        else:
                            newData.append(None)
                            newData.append(None)
                            
                        newData.append(y["pass"]["length"])
                        newData.append(y["pass"]["angle"])
                        newData.append(y["pass"]["height"]["id"])
                        newData.append(y["pass"]["height"]["name"])
                        newData.append(y["pass"]["end_location"])
                        if ("assisted_shot_id" in y["pass"].keys()):
                            newData.append(y["pass"]["assisted_shot_id"])
                        else:
                            newData.append(None)
                        
                        if ("body_part" in y["pass"].keys()):
                            newData.append(y["pass"]["body_part"]["id"])
                            newData.append(y["pass"]["body_part"]["name"])
                        else:
                            newData.append(None)
                            newData.append(None)
                            
                        if ("technique" in y["pass"].keys()):
                            newData.append(y["pass"]["technique"]["id"])
                            newData.append(y["pass"]["technique"]["name"])
                        else:
                            newData.append(None)
                            newData.append(None)
                        
                        if ("type" in y["pass"].keys()):
                            newData.append(y["pass"]["type"]["id"])
                            newData.append(y["pass"]["type"]["name"])
                        else:
                            newData.append(None)
                            newData.append(None)
                        
                        if ("outcome" in y["pass"].keys()):
                            newData.append(y["pass"]["outcome"]["id"])
                            newData.append(y["pass"]["outcome"]["name"])
                        else:
                            newData.append(None)
                            newData.append(None)
                            
                        if ("backheel" in y["pass"].keys()):
                            newData.append(y["pass"]["backheel"])
                        else:
                            newData.append(None)
                            
                        if ("deflected" in y["pass"].keys()):
                            newData.append(y["pass"]["deflected"])
                        else:
                            newData.append(None)
                            
                        if ("miscommunication" in y["pass"].keys()):
                            newData.append(y["pass"]["miscommunication"])
                        else:
                            newData.append(None)
                        
                        if ("cross" in y["pass"].keys()):
                            newData.append(y["pass"]["cross"])
                        else:
                            newData.append(None)
                        
                        if ("cut_back" in y["pass"].keys()):
                            newData.append(y["pass"]["cut_back"])
                        else:
                            newData.append(None)

                        if ("switch" in y["pass"].keys()):
                            newData.append(y["pass"]["switch"])
                        else:
                            newData.append(None)
                        
                        if ("shot_assist" in y["pass"].keys()):
                            newData.append(y["pass"]["shot_assist"])
                        else:
                            newData.append(None)

                        if ("goal_assist" in y["pass"].keys()):
                            newData.append(y["pass"]["goal_assist"])
                        else:
                            newData.append(None)
                            
                        if ("no_touch" in y["pass"].keys()):
                            newData.append(y["pass"]["no_touch"])
                        else:
                            newData.append(None)
                        
                        cursor.execute(sql, newData)
                        connect.commit()
                    
                    elif (y["type"]["name"] == "50/50"):
                        sql = '''INSERT INTO events(id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession, possession_team_id,
                        possession_team_name, play_pattern_id, play_pattern_name, team_id, team_name, player_id, player_name, position_id, position_name, location, duration,
                        outcome_id, outcome_name, counterpress) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                        
                        newData = []
                        newData.append(y["id"])
                        newData.append(x["match_id"])
                        newData.append(y["index"])
                        newData.append(y["period"])
                        newData.append(y["timestamp"])
                        newData.append(y["minute"])
                        newData.append(y["second"])
                        newData.append(y["type"]["id"])
                        newData.append(y["type"]["name"])
                        newData.append(y["possession"])
                        newData.append(y["possession_team"]["id"])
                        newData.append(y["possession_team"]["name"])
                        newData.append(y["play_pattern"]["id"])
                        newData.append(y["play_pattern"]["name"])
                        newData.append(y["team"]["id"])
                        newData.append(y["team"]["name"])
                        newData.append(y["player"]["id"])
                        newData.append(y["player"]["name"])
                        newData.append(y["position"]["id"])
                        newData.append(y["position"]["name"])
                        newData.append(y["location"])
                        newData.append(y["duration"])

                        newData.append(y["50_50"]["outcome"]["id"])
                        newData.append(y["50_50"]["outcome"]["name"])

                        
                        if ("counterpress" in y.keys()):
                            newData.append(y["counterpress"])
                        else:
                            newData.append(None)
                        
                        cursor.execute(sql, newData)
                        connect.commit()
                        
                    elif (y["type"]["name"] == "Bad Behaviour"):
                        sql = '''INSERT INTO events(id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession, possession_team_id,
                        possession_team_name, play_pattern_id, play_pattern_name, team_id, team_name, player_id, player_name, position_id, position_name, location, duration,
                        card_id, card_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                        
                        newData = []
                        newData.append(y["id"])
                        newData.append(x["match_id"])                        
                        newData.append(y["index"])
                        newData.append(y["period"])
                        newData.append(y["timestamp"])
                        newData.append(y["minute"])
                        newData.append(y["second"])
                        newData.append(y["type"]["id"])
                        newData.append(y["type"]["name"])
                        newData.append(y["possession"])
                        newData.append(y["possession_team"]["id"])
                        newData.append(y["possession_team"]["name"])
                        newData.append(y["play_pattern"]["id"])
                        newData.append(y["play_pattern"]["name"])
                        newData.append(y["team"]["id"])
                        newData.append(y["team"]["name"])
                        newData.append(y["player"]["id"])
                        newData.append(y["player"]["name"])
                        newData.append(y["position"]["id"])
                        newData.append(y["position"]["name"])
                        
                        if ("location" in y.keys()):
                            newData.append(y["location"])
                        else:
                            newData.append(None)
                            
                        newData.append(y["duration"])

                        newData.append(y["bad_behaviour"]["card"]["id"])
                        newData.append(y["bad_behaviour"]["card"]["name"])
                        
                        cursor.execute(sql, newData)
                        connect.commit()
                    
                    elif (y["type"]["name"] == "Ball Receipt*"):
                        sql = '''INSERT INTO events(id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession, possession_team_id,
                        possession_team_name, play_pattern_id, play_pattern_name, team_id, team_name, player_id, player_name, position_id, position_name, location, duration,
                        outcome_id, outcome_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                        
                        newData = []
                        newData.append(y["id"])
                        newData.append(x["match_id"])
                        newData.append(y["index"])
                        newData.append(y["period"])
                        newData.append(y["timestamp"])
                        newData.append(y["minute"])
                        newData.append(y["second"])
                        newData.append(y["type"]["id"])
                        newData.append(y["type"]["name"])
                        newData.append(y["possession"])
                        newData.append(y["possession_team"]["id"])
                        newData.append(y["possession_team"]["name"])
                        newData.append(y["play_pattern"]["id"])
                        newData.append(y["play_pattern"]["name"])
                        newData.append(y["team"]["id"])
                        newData.append(y["team"]["name"])
                        newData.append(y["player"]["id"])
                        newData.append(y["player"]["name"])
                        newData.append(y["position"]["id"])
                        newData.append(y["position"]["name"])
                        newData.append(y["location"])
                        if ("duration" in y.keys()):
                            newData.append(y["duration"])
                        else:
                            newData.append(None)
                        
                        if ("ball_receipt" in y.keys()):
                            newData.append(y["ball_receipt"]["outcome"]["id"])
                            newData.append(y["ball_receipt"]["outcome"]["name"])
                        else:
                            newData.append(None)
                            newData.append(None)
                            
                        cursor.execute(sql, newData)
                        connect.commit()
                        
                    elif (y["type"]["name"] == "Ball Recovery"):
                        sql = '''INSERT INTO events(id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession, possession_team_id,
                        possession_team_name, play_pattern_id, play_pattern_name, team_id, team_name, player_id, player_name, position_id, position_name, location, duration, offensive, recovery_failure)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                        
                        newData = []
                        newData.append(y["id"])
                        newData.append(x["match_id"])
                        newData.append(y["index"])
                        newData.append(y["period"])
                        newData.append(y["timestamp"])
                        newData.append(y["minute"])
                        newData.append(y["second"])
                        newData.append(y["type"]["id"])
                        newData.append(y["type"]["name"])
                        newData.append(y["possession"])
                        newData.append(y["possession_team"]["id"])
                        newData.append(y["possession_team"]["name"])
                        newData.append(y["play_pattern"]["id"])
                        newData.append(y["play_pattern"]["name"])
                        newData.append(y["team"]["id"])
                        newData.append(y["team"]["name"])
                        newData.append(y["player"]["id"])
                        newData.append(y["player"]["name"])
                        newData.append(y["position"]["id"])
                        newData.append(y["position"]["name"])
                        newData.append(y["location"])
                        newData.append(y["duration"])
                        
                        if ("ball_recovery" in y.keys()):
                            if ("recovery_failure" in y["ball_recovery"].keys()):
                                newData.append(y["ball_recovery"]["recovery_failure"])
                            else:
                                newData.append(None)
                                
                            if ("offensive" in y["ball_recovery"].keys()):
                                newData.append(y["ball_recovery"]["offensive"])                                
                            else:
                                newData.append(None)
                        else:
                            newData.append(None)
                            newData.append(None)
                        
                        cursor.execute(sql, newData)
                        connect.commit()
                    
                    elif (y["type"]["name"] == "Block"):
                        sql = '''INSERT INTO events(id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession, possession_team_id,
                        possession_team_name, play_pattern_id, play_pattern_name, team_id, team_name, player_id, player_name, position_id, position_name, location, duration, 
                        deflection, offensive, save_block, counterpress)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                        
                        newData = []
                        newData.append(y["id"])
                        newData.append(x["match_id"])
                        newData.append(y["index"])
                        newData.append(y["period"])
                        newData.append(y["timestamp"])
                        newData.append(y["minute"])
                        newData.append(y["second"])
                        newData.append(y["type"]["id"])
                        newData.append(y["type"]["name"])
                        newData.append(y["possession"])
                        newData.append(y["possession_team"]["id"])
                        newData.append(y["possession_team"]["name"])
                        newData.append(y["play_pattern"]["id"])
                        newData.append(y["play_pattern"]["name"])
                        newData.append(y["team"]["id"])
                        newData.append(y["team"]["name"])
                        newData.append(y["player"]["id"])
                        newData.append(y["player"]["name"])
                        newData.append(y["position"]["id"])
                        newData.append(y["position"]["name"])
                        newData.append(y["location"])
                        newData.append(y["duration"])
                        if ("block" in y.keys()):
                            if ("deflection" in y["block"].keys()):
                                newData.append(y["block"]["deflection"])
                            else:
                                newData.append(None)
                                
                            if ("offensive" in y["block"].keys()):
                                newData.append(y["block"]["offensive"])                                
                            else:
                                newData.append(None)
                            
                            if ("save_block" in y["block"].keys()):
                                newData.append(y["block"]["save_block"])                                
                            else:
                                newData.append(None)
                                
                            if ("counterpress" in y["block"].keys()):
                                newData.append(y["block"]["counterpress"])                                
                            else:
                                newData.append(None)
                                
                        else:
                            newData.append(None)
                            newData.append(None)
                            newData.append(None)
                            newData.append(None)
                        
                        cursor.execute(sql, newData)
                        connect.commit()
                        
                    elif (y["type"]["name"] == "Carry"):
                        sql = '''INSERT INTO events(id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession, possession_team_id,
                        possession_team_name, play_pattern_id, play_pattern_name, team_id, team_name, player_id, player_name, position_id, position_name, location, duration,
                        end_location) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                        
                        newData = []
                        newData.append(y["id"])
                        newData.append(x["match_id"])
                        newData.append(y["index"])
                        newData.append(y["period"])
                        newData.append(y["timestamp"])
                        newData.append(y["minute"])
                        newData.append(y["second"])
                        newData.append(y["type"]["id"])
                        newData.append(y["type"]["name"])
                        newData.append(y["possession"])
                        newData.append(y["possession_team"]["id"])
                        newData.append(y["possession_team"]["name"])
                        newData.append(y["play_pattern"]["id"])
                        newData.append(y["play_pattern"]["name"])
                        newData.append(y["team"]["id"])
                        newData.append(y["team"]["name"])
                        newData.append(y["player"]["id"])
                        newData.append(y["player"]["name"])
                        newData.append(y["position"]["id"])
                        newData.append(y["position"]["name"])
                        newData.append(y["location"])
                        newData.append(y["duration"])

                        newData.append(y["carry"]["end_location"])
                        
                        cursor.execute(sql, newData)
                        connect.commit()
                    
                    elif (y["type"]["name"] == "Clearance"):
                        sql = '''INSERT INTO events(id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession, possession_team_id,
                        possession_team_name, play_pattern_id, play_pattern_name, team_id, team_name, player_id, player_name, position_id, position_name, location, duration,
                        aerial_won, body_part_id, body_part_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                        
                        newData = []
                        newData.append(y["id"])
                        newData.append(x["match_id"])
                        newData.append(y["index"])
                        newData.append(y["period"])
                        newData.append(y["timestamp"])
                        newData.append(y["minute"])
                        newData.append(y["second"])
                        newData.append(y["type"]["id"])
                        newData.append(y["type"]["name"])
                        newData.append(y["possession"])
                        newData.append(y["possession_team"]["id"])
                        newData.append(y["possession_team"]["name"])
                        newData.append(y["play_pattern"]["id"])
                        newData.append(y["play_pattern"]["name"])
                        newData.append(y["team"]["id"])
                        newData.append(y["team"]["name"])
                        newData.append(y["player"]["id"])
                        newData.append(y["player"]["name"])
                        newData.append(y["position"]["id"])
                        newData.append(y["position"]["name"])
                        newData.append(y["location"])
                        newData.append(y["duration"])

                        if ("aerial_won" in y["clearance"].keys()):
                            newData.append(y["clearance"]["aerial_won"])
                        else:
                            newData.append(None)
                        
                        newData.append(y["clearance"]["body_part"]["id"])
                        newData.append(y["clearance"]["body_part"]["name"])
                        
                        cursor.execute(sql, newData)
                        connect.commit()
                        
                    elif (y["type"]["name"] == "Duel"):
                        sql = '''INSERT INTO events(id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession, possession_team_id,
                        possession_team_name, play_pattern_id, play_pattern_name, team_id, team_name, player_id, player_name, position_id, position_name, location, duration,
                        counterpress, specifying_type_id, specifying_type_name, outcome_id, outcome_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                        
                        newData = []
                        newData.append(y["id"])
                        newData.append(x["match_id"])
                        newData.append(y["index"])
                        newData.append(y["period"])
                        newData.append(y["timestamp"])
                        newData.append(y["minute"])
                        newData.append(y["second"])
                        newData.append(y["type"]["id"])
                        newData.append(y["type"]["name"])
                        newData.append(y["possession"])
                        newData.append(y["possession_team"]["id"])
                        newData.append(y["possession_team"]["name"])
                        newData.append(y["play_pattern"]["id"])
                        newData.append(y["play_pattern"]["name"])
                        newData.append(y["team"]["id"])
                        newData.append(y["team"]["name"])
                        newData.append(y["player"]["id"])
                        newData.append(y["player"]["name"])
                        newData.append(y["position"]["id"])
                        newData.append(y["position"]["name"])
                        newData.append(y["location"])
                        newData.append(y["duration"])

                        if ("counterpress" in y["duel"].keys()):
                            newData.append(y["duel"]["counterpress"])
                        else:
                            newData.append(None)
                        
                        newData.append(y["duel"]["type"]["id"])
                        newData.append(y["duel"]["type"]["name"])
                        
                        if ("outcome" in y["duel"].keys()):
                            newData.append(y["duel"]["outcome"]["id"])
                            newData.append(y["duel"]["outcome"]["name"])
                        else:
                            newData.append(None)
                            newData.append(None)
                        
                        cursor.execute(sql, newData)
                        connect.commit()
                        
                    elif (y["type"]["name"] == "Foul Committed"):
                        sql = '''INSERT INTO events(id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession, possession_team_id,
                        possession_team_name, play_pattern_id, play_pattern_name, team_id, team_name, player_id, player_name, position_id, position_name, location, duration,
                        counterpress, offensive, specifying_type_id, specifying_type_name, advantage, penalty, card_id, card_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                        
                        newData = []
                        newData.append(y["id"])
                        newData.append(x["match_id"])
                        newData.append(y["index"])
                        newData.append(y["period"])
                        newData.append(y["timestamp"])
                        newData.append(y["minute"])
                        newData.append(y["second"])
                        newData.append(y["type"]["id"])
                        newData.append(y["type"]["name"])
                        newData.append(y["possession"])
                        newData.append(y["possession_team"]["id"])
                        newData.append(y["possession_team"]["name"])
                        newData.append(y["play_pattern"]["id"])
                        newData.append(y["play_pattern"]["name"])
                        newData.append(y["team"]["id"])
                        newData.append(y["team"]["name"])
                        newData.append(y["player"]["id"])
                        newData.append(y["player"]["name"])
                        newData.append(y["position"]["id"])
                        newData.append(y["position"]["name"])
                        newData.append(y["location"])
                        newData.append(y["duration"])

                        
                        if ("foul_committed" in y.keys()):
                            
                            if ("counterpress" in y["foul_committed"].keys()):
                                newData.append(y["foul_committed"]["counterpress"])
                            else:
                                newData.append(None)
                            
                            if ("offensive" in  y["foul_committed"].keys()):
                                newData.append(y["foul_committed"]["offensive"])
                            else:
                                newData.append(None)
                                
                            if ("type" in  y["foul_committed"].keys()):
                                newData.append(y["foul_committed"]["type"]["id"])
                                newData.append(y["foul_committed"]["type"]["name"])
                            else:
                                newData.append(None)
                                newData.append(None)
                                
                            if ("advantage" in  y["foul_committed"].keys()):
                                newData.append(y["foul_committed"]["advantage"])
                            else:
                                newData.append(None)
                            
                            if ("penalty" in  y["foul_committed"].keys()):
                                newData.append(y["foul_committed"]["penalty"])
                            else:
                                newData.append(None)
                                
                            if ("card" in  y["foul_committed"].keys()):
                                newData.append(y["foul_committed"]["card"]["id"])
                                newData.append(y["foul_committed"]["card"]["name"])
                            else:
                                newData.append(None)
                                newData.append(None)
                        else:
                            newData.append(None)
                            newData.append(None)
                            newData.append(None)
                            newData.append(None)
                            newData.append(None)
                            newData.append(None)
                            newData.append(None)
                            newData.append(None)
                        
                        cursor.execute(sql, newData)
                        connect.commit()
                        
                    elif (y["type"]["name"] == "Foul Won"):
                        sql = '''INSERT INTO events(id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession, possession_team_id,
                        possession_team_name, play_pattern_id, play_pattern_name, team_id, team_name, player_id, player_name, position_id, position_name, location, duration,
                        defensive, advantage, penalty) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                        
                        newData = []
                        newData.append(y["id"])
                        newData.append(x["match_id"])
                        newData.append(y["index"])
                        newData.append(y["period"])
                        newData.append(y["timestamp"])
                        newData.append(y["minute"])
                        newData.append(y["second"])
                        newData.append(y["type"]["id"])
                        newData.append(y["type"]["name"])
                        newData.append(y["possession"])
                        newData.append(y["possession_team"]["id"])
                        newData.append(y["possession_team"]["name"])
                        newData.append(y["play_pattern"]["id"])
                        newData.append(y["play_pattern"]["name"])
                        newData.append(y["team"]["id"])
                        newData.append(y["team"]["name"])
                        newData.append(y["player"]["id"])
                        newData.append(y["player"]["name"])
                        newData.append(y["position"]["id"])
                        newData.append(y["position"]["name"])
                        
                        newData.append(y["location"])
                        newData.append(y["duration"])

                        if ("foul_won" in y.keys()):
                            if ("defensive" in y["foul_won"].keys()):
                                newData.append(y["foul_won"]["defensive"])
                            else:
                                newData.append(None)
                            
                            if ("advantage" in y["foul_won"].keys()):
                                newData.append(y["foul_won"]["advantage"])
                            else:
                                newData.append(None)
                        
                            if ("penalty" in y["foul_won"].keys()):
                                newData.append(y["foul_won"]["penalty"])
                            else:
                                newData.append(None)
                                
                        else:
                                newData.append(None)
                                newData.append(None)
                                newData.append(None)

                        cursor.execute(sql, newData)
                        connect.commit()
                        
                    elif (y["type"]["name"] == "Goal Keeper"):
                        sql = '''INSERT INTO events(id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession, possession_team_id,
                        possession_team_name, play_pattern_id, play_pattern_name, team_id, team_name, player_id, player_name, position_id, position_name, location, duration,
                        gk_position_id, gk_position_name, technique_id, technique_name, body_part_id, body_part_name, specifying_type_id, specifying_type_name, outcome_id, outcome_name, end_location) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                        
                        newData = []
                        newData.append(y["id"])
                        newData.append(x["match_id"])
                        newData.append(y["index"])
                        newData.append(y["period"])
                        newData.append(y["timestamp"])
                        newData.append(y["minute"])
                        newData.append(y["second"])
                        newData.append(y["type"]["id"])
                        newData.append(y["type"]["name"])
                        newData.append(y["possession"])
                        newData.append(y["possession_team"]["id"])
                        newData.append(y["possession_team"]["name"])
                        newData.append(y["play_pattern"]["id"])
                        newData.append(y["play_pattern"]["name"])
                        newData.append(y["team"]["id"])
                        newData.append(y["team"]["name"])
                        newData.append(y["player"]["id"])
                        newData.append(y["player"]["name"])
                        newData.append(y["position"]["id"])
                        newData.append(y["position"]["name"])
                        
                        if ("location" in y.keys()):
                            newData.append(y["location"])
                        else:
                            newData.append(None)
                            
                        newData.append(y["duration"])
                        
                        if ("position" in y["goalkeeper"].keys()):
                            newData.append(y["goalkeeper"]["position"]["id"])
                            newData.append(y["goalkeeper"]["position"]["name"])
                        else:
                            newData.append(None)
                            newData.append(None)
                        
                        if ("technique" in y["goalkeeper"].keys()):
                            newData.append(y["goalkeeper"]["technique"]["id"])
                            newData.append(y["goalkeeper"]["technique"]["name"])
                        else:
                            newData.append(None)
                            newData.append(None)
                        
                        if ("body_part" in y["goalkeeper"].keys()):
                            newData.append(y["goalkeeper"]["body_part"]["id"])
                            newData.append(y["goalkeeper"]["body_part"]["name"])
                        else:
                            newData.append(None)
                            newData.append(None)
                        
                        if ("type" in y["goalkeeper"].keys()):
                            newData.append(y["goalkeeper"]["type"]["id"])
                            newData.append(y["goalkeeper"]["type"]["name"])
                        else:
                            newData.append(None)
                            newData.append(None)
                        
                        if ("outcome" in y["goalkeeper"].keys()):
                            newData.append(y["goalkeeper"]["outcome"]["id"])
                            newData.append(y["goalkeeper"]["outcome"]["name"])
                        else:
                            newData.append(None)
                            newData.append(None)
                        
                        if ("end_location" in y["goalkeeper"].keys()):
                            newData.append(y["goalkeeper"]["end_location"])
                        else:
                            newData.append(None)
                            
                        cursor.execute(sql, newData)
                        connect.commit()
                        
                    elif (y["type"]["name"] == "Half End"):
                        sql = '''INSERT INTO events(id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession, possession_team_id,
                        possession_team_name, play_pattern_id, play_pattern_name, team_id, team_name, duration,
                        early_video_end, match_suspended) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                        
                        newData = []
                        newData.append(y["id"])
                        newData.append(x["match_id"])
                        newData.append(y["index"])
                        newData.append(y["period"])
                        newData.append(y["timestamp"])
                        newData.append(y["minute"])
                        newData.append(y["second"])
                        newData.append(y["type"]["id"])
                        newData.append(y["type"]["name"])
                        newData.append(y["possession"])
                        newData.append(y["possession_team"]["id"])
                        newData.append(y["possession_team"]["name"])
                        newData.append(y["play_pattern"]["id"])
                        newData.append(y["play_pattern"]["name"])
                        newData.append(y["team"]["id"])
                        newData.append(y["team"]["name"])
                        
                        newData.append(y["duration"])
                        
                        if ("half_end" in y.keys()):
                            if ("early_video_end" in y["half_end"].keys()):
                                newData.append(y["half_end"]["early_video_end"])
                            else:
                                newData.append(None)
                                
                            if ("match_suspended" in y["half_end"].keys()):
                                newData.append(y["half_end"]["match_suspended"])
                            else:
                                newData.append(None)
                            
                        else:
                            newData.append(None)
                            newData.append(None)
                        
                        
                        cursor.execute(sql, newData)
                        connect.commit()
                        
                    elif (y["type"]["name"] == "Half Start"):
                        sql = '''INSERT INTO events(id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession, possession_team_id,
                        possession_team_name, play_pattern_id, play_pattern_name, team_id, team_name, duration,
                        late_video_start) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                        
                        newData = []
                        newData.append(y["id"])
                        newData.append(x["match_id"])
                        newData.append(y["index"])
                        newData.append(y["period"])
                        newData.append(y["timestamp"])
                        newData.append(y["minute"])
                        newData.append(y["second"])
                        newData.append(y["type"]["id"])
                        newData.append(y["type"]["name"])
                        newData.append(y["possession"])
                        newData.append(y["possession_team"]["id"])
                        newData.append(y["possession_team"]["name"])
                        newData.append(y["play_pattern"]["id"])
                        newData.append(y["play_pattern"]["name"])
                        newData.append(y["team"]["id"])
                        newData.append(y["team"]["name"])
                        
                        newData.append(y["duration"])
                        
                        if ("half_start" in y.keys()):
                            if ("late_video_start" in y["half_start"].keys()):
                                newData.append(y["half_start"]["late_video_start"])
                            else:
                                newData.append(None)
                            
                        else:
                            newData.append(None)
                        
                        cursor.execute(sql, newData)
                        connect.commit()
                        
                    elif (y["type"]["name"] == "Injury Stoppage"):
                        sql = '''INSERT INTO events(id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession, possession_team_id,
                        possession_team_name, play_pattern_id, play_pattern_name, team_id, team_name, player_id, player_name, position_id, position_name, location, duration,
                        aerial_won) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                        
                        newData = []
                        newData.append(y["id"])
                        newData.append(x["match_id"])
                        newData.append(y["index"])
                        newData.append(y["period"])
                        newData.append(y["timestamp"])
                        newData.append(y["minute"])
                        newData.append(y["second"])
                        newData.append(y["type"]["id"])
                        newData.append(y["type"]["name"])
                        newData.append(y["possession"])
                        newData.append(y["possession_team"]["id"])
                        newData.append(y["possession_team"]["name"])
                        newData.append(y["play_pattern"]["id"])
                        newData.append(y["play_pattern"]["name"])
                        newData.append(y["team"]["id"])
                        newData.append(y["team"]["name"])
                        newData.append(y["player"]["id"])
                        newData.append(y["player"]["name"])
                        newData.append(y["position"]["id"])
                        newData.append(y["position"]["name"])
                        if ("location" in y.keys()):
                            newData.append(y["location"])
                        else:
                            newData.append(None)
                        newData.append(y["duration"])

                        if ("injury_stoppage" in y.keys()):
                            if ("in_chain" in y["injury_stoppage"].keys()):
                                newData.append(y["injury_stoppage"]["in_chain"])
                            else:
                                newData.append(None) 
                        else:
                            newData.append(None)
                        
                        cursor.execute(sql, newData)
                        connect.commit()
                        
                    elif (y["type"]["name"] == "Interception"):
                        sql = '''INSERT INTO events(id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession, possession_team_id,
                        possession_team_name, play_pattern_id, play_pattern_name, team_id, team_name, player_id, player_name, position_id, position_name, location, duration,
                        outcome_id, outcome_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                        
                        newData = []
                        newData.append(y["id"])
                        newData.append(x["match_id"])
                        newData.append(y["index"])
                        newData.append(y["period"])
                        newData.append(y["timestamp"])
                        newData.append(y["minute"])
                        newData.append(y["second"])
                        newData.append(y["type"]["id"])
                        newData.append(y["type"]["name"])
                        newData.append(y["possession"])
                        newData.append(y["possession_team"]["id"])
                        newData.append(y["possession_team"]["name"])
                        newData.append(y["play_pattern"]["id"])
                        newData.append(y["play_pattern"]["name"])
                        newData.append(y["team"]["id"])
                        newData.append(y["team"]["name"])
                        newData.append(y["player"]["id"])
                        newData.append(y["player"]["name"])
                        newData.append(y["position"]["id"])
                        newData.append(y["position"]["name"])
                        newData.append(y["location"])
                        newData.append(y["duration"])

                        newData.append(y["interception"]["outcome"]["id"])
                        newData.append(y["interception"]["outcome"]["name"])
                        
                        cursor.execute(sql, newData)
                        connect.commit()
                    
                    elif (y["type"]["name"] == "Miscontrol"):
                        sql = '''INSERT INTO events(id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession, possession_team_id,
                        possession_team_name, play_pattern_id, play_pattern_name, team_id, team_name, player_id, player_name, position_id, position_name, location, duration,
                        aerial_won) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                        
                        newData = []
                        newData.append(y["id"])
                        newData.append(x["match_id"])
                        newData.append(y["index"])
                        newData.append(y["period"])
                        newData.append(y["timestamp"])
                        newData.append(y["minute"])
                        newData.append(y["second"])
                        newData.append(y["type"]["id"])
                        newData.append(y["type"]["name"])
                        newData.append(y["possession"])
                        newData.append(y["possession_team"]["id"])
                        newData.append(y["possession_team"]["name"])
                        newData.append(y["play_pattern"]["id"])
                        newData.append(y["play_pattern"]["name"])
                        newData.append(y["team"]["id"])
                        newData.append(y["team"]["name"])
                        newData.append(y["player"]["id"])
                        newData.append(y["player"]["name"])
                        newData.append(y["position"]["id"])
                        newData.append(y["position"]["name"])
                        newData.append(y["location"])
                        newData.append(y["duration"])

                        if ("miscontrol" in y.keys()):
                            newData.append(y["miscontrol"]["aerial_won"])
                        else:
                            newData.append(None)
                        
                        cursor.execute(sql, newData)
                        connect.commit()
                        
                    elif (y["type"]["name"] == "Player Off"):
                        sql = '''INSERT INTO events(id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession, possession_team_id,
                        possession_team_name, play_pattern_id, play_pattern_name, team_id, team_name, player_id, player_name, position_id, position_name, location, duration,
                        permanent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                        
                        newData = []
                        newData.append(y["id"])
                        newData.append(x["match_id"])
                        newData.append(y["index"])
                        newData.append(y["period"])
                        newData.append(y["timestamp"])
                        newData.append(y["minute"])
                        newData.append(y["second"])
                        newData.append(y["type"]["id"])
                        newData.append(y["type"]["name"])
                        newData.append(y["possession"])
                        newData.append(y["possession_team"]["id"])
                        newData.append(y["possession_team"]["name"])
                        newData.append(y["play_pattern"]["id"])
                        newData.append(y["play_pattern"]["name"])
                        newData.append(y["team"]["id"])
                        newData.append(y["team"]["name"])
                        newData.append(y["player"]["id"])
                        newData.append(y["player"]["name"])
                        newData.append(y["position"]["id"])
                        newData.append(y["position"]["name"])
                        if ("location" in y.keys()):
                            newData.append(y["location"])
                        else:
                            newData.append(None)
                        newData.append(y["duration"])

                        if ("player_off" in y.keys()):
                            newData.append(y["miscontrol"]["permanent"])
                        else:
                            newData.append(None)
                        
                        cursor.execute(sql, newData)
                        connect.commit()
                        
                    elif (y["type"]["name"] == "Pressure"):
                        sql = '''INSERT INTO events(id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession, possession_team_id,
                        possession_team_name, play_pattern_id, play_pattern_name, team_id, team_name, player_id, player_name, position_id, position_name, location, duration,
                        counterpress) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                        
                        newData = []
                        newData.append(y["id"])
                        newData.append(x["match_id"])
                        newData.append(y["index"])
                        newData.append(y["period"])
                        newData.append(y["timestamp"])
                        newData.append(y["minute"])
                        newData.append(y["second"])
                        newData.append(y["type"]["id"])
                        newData.append(y["type"]["name"])
                        newData.append(y["possession"])
                        newData.append(y["possession_team"]["id"])
                        newData.append(y["possession_team"]["name"])
                        newData.append(y["play_pattern"]["id"])
                        newData.append(y["play_pattern"]["name"])
                        newData.append(y["team"]["id"])
                        newData.append(y["team"]["name"])
                        newData.append(y["player"]["id"])
                        newData.append(y["player"]["name"])
                        newData.append(y["position"]["id"])
                        newData.append(y["position"]["name"])
                        if ("location" in y.keys()):
                            newData.append(y["location"])
                        else:
                            newData.append(None)
                        newData.append(y["duration"])

                        if ("pressure" in y.keys()):
                            newData.append(y["pressure"]["counterpress"])
                        else:
                            newData.append(None)
                        
                        cursor.execute(sql, newData)
                        connect.commit()
                        
                    elif (y["type"]["name"] == "Substitution"):
                        sql = '''INSERT INTO events(id, match_id, index, period, timestamp, minute, second, type_id, type_name, possession, possession_team_id,
                        possession_team_name, play_pattern_id, play_pattern_name, team_id, team_name, player_id, player_name, position_id, position_name, location, duration,
                        replacement_id, replacement_name, outcome_id, outcome_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                        
                        newData = []
                        newData.append(y["id"])
                        newData.append(x["match_id"])
                        newData.append(y["index"])
                        newData.append(y["period"])
                        newData.append(y["timestamp"])
                        newData.append(y["minute"])
                        newData.append(y["second"])
                        newData.append(y["type"]["id"])
                        newData.append(y["type"]["name"])
                        newData.append(y["possession"])
                        newData.append(y["possession_team"]["id"])
                        newData.append(y["possession_team"]["name"])
                        newData.append(y["play_pattern"]["id"])
                        newData.append(y["play_pattern"]["name"])
                        newData.append(y["team"]["id"])
                        newData.append(y["team"]["name"])
                        newData.append(y["player"]["id"])
                        newData.append(y["player"]["name"])
                        newData.append(y["position"]["id"])
                        newData.append(y["position"]["name"])
                        
                        if ("location" in y.keys()):
                            newData.append(y["location"])
                        else:
                            newData.append(None)
                            
                        newData.append(y["duration"])
                        newData.append(y["substitution"]["replacement"]["id"])
                        newData.append(y["substitution"]["replacement"]["name"])
                        newData.append(y["substitution"]["outcome"]["id"])
                        newData.append(y["substitution"]["outcome"]["name"])
                        
                        cursor.execute(sql, newData)
                        connect.commit()
    
    sql = '''CREATE INDEX idx_q1_events
    ON events(type_name, statsbomb_xg, player_name, match_id) WHERE type_name = 'Shot' '''
    cursor.execute(sql)
    connect.commit()
    
    sql = '''CREATE INDEX idx_laliga_20_21_matches
    ON matches(season_name, competition_name, match_id) WHERE season_name =  '2020/2021' AND competition_name = 'La Liga' '''
    cursor.execute(sql)
    connect.commit()
    
    sql = '''CREATE INDEX idx_q2_events
    ON events(type_name, player_name, match_id) WHERE type_name = 'Shot' '''
    cursor.execute(sql)
    connect.commit()
    
    sql = '''CREATE INDEX idx_q3_events
    ON events(first_time, player_name, match_id) WHERE first_time = TRUE'''
    cursor.execute(sql)
    connect.commit()
    
    sql = '''CREATE INDEX idx_laliga_18_21_matches
    ON matches(competition_name, season_name, match_id) WHERE competition_name = 'La Liga' AND (season_name = '2020/2021' OR season_name = '2019/2020' OR season_name = '2018/2019') '''
    cursor.execute(sql)
    connect.commit()
    
    sql = '''CREATE INDEX idx_q4_events
    ON events(type_name, team_name, match_id) WHERE type_name = 'Pass' '''
    cursor.execute(sql)
    connect.commit()
    
    sql = '''CREATE INDEX idx_q5_events
    ON events(recipient_name, match_id) WHERE recipient_name IS NOT NULL'''
    cursor.execute(sql)
    connect.commit()
    
    sql = '''CREATE INDEX idx_prem_03_04_matches
    ON matches(season_name, competition_name, match_id) WHERE season_name = '2003/2004' AND competition_name = 'Premier League' '''
    cursor.execute(sql)
    connect.commit()
    
    sql = '''CREATE INDEX idx_q6_events
    ON events(type_name, team_name, match_id) WHERE type_name = 'Shot' '''
    cursor.execute(sql)
    connect.commit()
    
    sql = '''CREATE INDEX idx_q7_events
    ON events(technique_name, player_name, match_id) WHERE technique_name = 'Through Ball' '''
    cursor.execute(sql)
    connect.commit()
    
    sql = '''CREATE INDEX idx_q8_events
    ON events(technique_name, team_name, match_id) WHERE technique_name = 'Through Ball' '''
    cursor.execute(sql)
    connect.commit()
    
    sql = '''CREATE INDEX idx_q9_events
    ON events(outcome_name, type_name, player_name, match_id) WHERE outcome_name = 'Complete' AND type_name = 'Dribble' '''
    cursor.execute(sql)
    connect.commit()
    
    sql = '''CREATE INDEX idx_q10_events
    ON events(type_name, player_name, match_id) WHERE type_name = 'Dribbled Past' '''
    cursor.execute(sql)
    connect.commit()
    
    connect.close()    
                    

                
def dropTables():
    sql = '''DROP TABLE matches, events, game_played
    '''
    cursor.execute(sql)
    connect.commit()
    """
    sql = '''DROP INDEX idx_q1_events'''
    cursor.execute(sql)
    connect.commit()
    
    sql = '''DROP INDEX idx_q1_matches'''
    cursor.execute(sql)
    connect.commit()
    
    sql = '''DROP INDEX idx_q2_events'''
    cursor.execute(sql)
    connect.commit()
    
    sql = '''DROP INDEX idx_q2_matches'''
    cursor.execute(sql)
    connect.commit()
    """
    
def main():
    try:
        createTables()
        # dropTables()

    except psycopg.Error as error:
        print(error)
    print()


if __name__ == "__main__":
    main()
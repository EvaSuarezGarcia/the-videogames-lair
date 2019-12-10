-- Table for all games from Games_2 that appear in App_ID_Info or App_ID_Info_Old
CREATE TABLE all_steam_games_2 (
    steam_id INT UNSIGNED NOT NULL,
    game_name VARCHAR(128) NOT NULL,
    p20 FLOAT,
    p40 FLOAT,
    p60 FLOAT,
    p80 FLOAT,
    players INT UNSIGNED,
    owners INT UNSIGNED,

    CONSTRAINT all_steam_games_2_pk PRIMARY KEY (steam_id)
);

-- Insert all games from Games_2 that appear in App_ID_Info
INSERT INTO all_steam_games_2 (steam_id, game_name)
SELECT appid, Title FROM App_ID_Info
WHERE Type='game' AND appid IN (
    SELECT DISTINCT appid FROM Games_2
);

-- Insert all games from Games_2 that appear in App_ID_Info_Old (most are duplicates, but a few are new)
INSERT IGNORE INTO all_steam_games_2 (steam_id, game_name)
SELECT appid, Title FROM App_ID_Info_Old
WHERE Type='game' AND appid IN (
    SELECT DISTINCT appid FROM Games_2
);

-- Remove ® and ™ symbols from game names
UPDATE all_steam_games_2 SET game_name = REPLACE(game_name,'®','');
UPDATE all_steam_games_2 SET game_name = REPLACE(game_name,'™','');

-- Trim game names
UPDATE all_steam_games_2 SET game_name = TRIM(game_name);

-- Table for playtimes of games in all_steam_games_2
CREATE TABLE filtered_games_2 (
    user_id BIGINT(20) UNSIGNED NOT NULL,
    steam_id INT(10) UNSIGNED NOT NULL,
    playtime_forever INT UNSIGNED NOT NULL,

    CONSTRAINT filtered_games_2_pk PRIMARY KEY (user_id, steam_id),
    CONSTRAINT filtered_games_2_game_fk FOREIGN KEY (steam_id)
        REFERENCES all_steam_games_2 (steam_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

-- Run insert_playtimes.py
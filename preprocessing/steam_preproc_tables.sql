---- Intermediate steam games table ----
CREATE TABLE steam_games (
    steam_id INT(10) UNSIGNED NOT NULL,
    game_name VARCHAR(128) NOT NULL,
    p20 FLOAT,
    p40 FLOAT,
    p60 FLOAT,
    p80 FLOAT,

    CONSTRAINT steam_games_pk PRIMARY KEY (steam_id)
);

-- Insert all games from App_ID_Info that appear on Games_2
INSERT INTO steam_games
SELECT appid, Title FROM App_ID_Info
WHERE Type='game' AND appid IN (
    SELECT DISTINCT appid FROM Games_2
);

---- Intermediate table for steam playtime ----
CREATE TABLE steam_playtime (
    steam_id INT(10) UNSIGNED NOT NULL,
    user_id BIGINT(20) UNSIGNED NOT NULL,
    playtime INT(10) UNSIGNED NOT NULL,
    estimated_rating TINYINT UNSIGNED,

    CONSTRAINT steam_playtime_pk PRIMARY KEY (steam_id, user_id),
    CONSTRAINT steam_playtime_game_fk FOREIGN KEY (steam_id)
        REFERENCES steam_games (steam_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

-- Insert all playtimes for games appearing in steam_games with playtime > 0
INSERT INTO steam_playtime
SELECT appid, steamid, playtime_forever
FROM Games_2
WHERE playtime_forever > 0 AND appid IN (
    SELECT steam_id FROM steam_games
)
GROUP BY steamid
HAVING COUNT(appid) >= 5;

-- Delete all steam games that do not have playtime data
DELETE FROM steam_games 
WHERE steam_id NOT IN (
    SELECT DISTINCT steam_id FROM steam_playtime
);

-- Update steam_games with quintiles
UPDATE steam_games AS s
JOIN (
    SELECT appid, 
        PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY playtime_forever) OVER (PARTITION BY appid) AS p20,
        PERCENTILE_CONT(0.4) WITHIN GROUP (ORDER BY playtime_forever) OVER (PARTITION BY appid) AS p40,
        PERCENTILE_CONT(0.6) WITHIN GROUP (ORDER BY playtime_forever) OVER (PARTITION BY appid) AS p60,
        PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY playtime_forever) OVER (PARTITION BY appid) AS p80
    FROM Games_2 WHERE playtime_forever > 0
) AS g
ON s.steam_id = g.appid
SET s.p20 = g.p20,
    s.p40 = g.p40,
    s.p60 = g.p60,
    s.p80 = g.p80;
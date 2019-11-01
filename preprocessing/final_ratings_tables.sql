--- Intermediate table associating GB users with user ids ---
CREATE TABLE dataset_users (
    user_id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    user_foreign_id VARCHAR(20) NOT NULL,
    steam_user BOOLEAN NOT NULL DEFAULT False,

    CONSTRAINT dataset_users_pk PRIMARY KEY (user_id)
);

--- Final table for game ratings ---
-- estimated column indicates whether the rating was estimated from Steam playtimes or not
CREATE TABLE game_ratings (
    game_id INT UNSIGNED NOT NULL,
    user_id INT UNSIGNED NOT NULL,
    rating TINYINT UNSIGNED NOT NULL,
    estimated BOOLEAN NOT NULL DEFAULT False,

    CONSTRAINT game_ratings_pk PRIMARY KEY (game_id, user_id),
    CONSTRAINT game_ratings_game_fk FOREIGN KEY (game_id)
        REFERENCES games (id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    CONSTRAINT game_ratings_user_fk FOREIGN KEY (user_id)
        REFERENCES dataset_users (user_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

-- Insert GB users data into intermediate table
INSERT INTO dataset_users (user_foreign_id)
SELECT DISTINCT reviewer FROM gb_staff_reviews
UNION SELECT DISTINCT reviewer FROM gb_user_reviews;

-- Transform game ids in gb_staff_games (and gb_user_reviews because there's no foreign key :))
UPDATE gb_staff_games
SET game_id = SUBSTRING(game_id, 6);

UPDATE gb_user_reviews
SET game_id = SUBSTRING(game_id, 6);

-- Drop foreign key constraint that references gb_staff_games
ALTER TABLE gb_staff_reviews DROP FOREIGN KEY gb_staff_reviews_game_fk;

-- Update game ids that changed its id in GB
-- The Legend of Zelda: Ocarina of Time 3D 31776 -> 12572
-- Street Fighter II: Hyper Fighting 22362 -> 8161
-- Halo: Combat Evolved Anniversary 35534 -> 2600
UPDATE gb_staff_reviews
SET game_id = 12572
WHERE game_id = 31776;

UPDATE gb_staff_reviews
SET game_id = 8161
WHERE game_id = 22362;

UPDATE gb_staff_reviews
SET game_id = 2600
WHERE game_id = 35534;

UPDATE gb_user_reviews
SET game_id = 12572
WHERE game_id = 31776;

UPDATE gb_user_reviews
SET game_id = 8161
WHERE game_id = 22362;

UPDATE gb_user_reviews
SET game_id = 2600
WHERE game_id = 35534;

-- Add foreign key constraints
ALTER TABLE gb_staff_reviews MODIFY game_id INT UNSIGNED NOT NULL;
ALTER TABLE gb_staff_reviews
ADD CONSTRAINT gb_staff_reviews_game_fk FOREIGN KEY (game_id)
    REFERENCES games (id)
    ON DELETE CASCADE
    ON UPDATE CASCADE;

ALTER TABLE gb_user_reviews MODIFY game_id INT UNSIGNED NOT NULL;
ALTER TABLE gb_user_reviews
ADD CONSTRAINT gb_user_reviews_game_fk FOREIGN KEY (game_id)
    REFERENCES games (id)
    ON DELETE CASCADE
    ON UPDATE CASCADE;

-- Insert ratings data from GB users into game_ratings table
INSERT INTO game_ratings (game_id, user_id, rating)
SELECT game_id, user_id, score
FROM gb_staff_reviews AS r JOIN dataset_users AS u ON r.reviewer = u.user_foreign_id;

INSERT INTO game_ratings (game_id, user_id, rating)
SELECT game_id, user_id, score
FROM gb_user_reviews AS r JOIN dataset_users AS u ON r.reviewer = u.user_foreign_id;

-- Remove Steam games that did not match
DELETE FROM steam_games WHERE steam_id NOT IN (
    SELECT steam_id FROM game_steam_ids
);

-- Remove Steam users having less than 5 played games
DELETE FROM steam_playtime WHERE user_id IN (
    SELECT user_id FROM steam_playtime
    GROUP BY user_id HAVING COUNT(*) < 5
);

-- Insert remaining Steam users into dataset_users
INSERT INTO dataset_users(user_foreign_id, steam_user)
SELECT DISTINCT user_id, 'True' FROM steam_playtime;

-- Compute estimated ratings for Steam playtimes
UPDATE steam_playtime AS p
JOIN steam_games AS g
ON p.steam_id = g.steam_id
SET p.estimated_rating = 
    CASE   
        WHEN p.playtime < g.p20 THEN 1
        WHEN p.playtime < g.p40 THEN 2
        WHEN p.playtime < g.p60 THEN 3
        WHEN p.playtime < g.p80 THEN 4
        ELSE 5
    END CASE;

-- Insert ratings data from Steam users into game_ratings table
-- game_id tiene que venir de hacer un join con game_steam_ids
-- user_id tiene que venir de hacer un join con dataset_users
-- rating viene de steam_playtime
-- estimated es True
INSERT INTO game_ratings(game_id, user_id, rating, estimated)
SELECT g.gb_id, u.user_id, p.estimated_rating, 'True'
FROM steam_playtime AS p 
JOIN game_steam_ids AS g ON p.steam_id = g.steam_id
JOIN dataset_users AS u ON p.user_id = u.user_foreign_id;
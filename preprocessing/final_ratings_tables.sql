--- Sequence for assigning user IDs ---
CREATE SEQUENCE user_sequence;

--- Table for GB and Steam users (dataset) ---
CREATE TABLE dataset_users (
    user_id INT UNSIGNED NOT NULL DEFAULT (NEXT VALUE FOR user_sequence),
    foreign_user_id VARCHAR(20) NOT NULL UNIQUE,
    steam_user BOOLEAN NOT NULL DEFAULT False,

    CONSTRAINT dataset_users_pk PRIMARY KEY (user_id)
);

--- Add user_id columns to metadata tables ---
ALTER TABLE age_ratings ADD COLUMN user_id INT UNSIGNED NOT NULL DEFAULT (NEXT VALUE FOR user_sequence);
ALTER TABLE companies ADD COLUMN user_id INT UNSIGNED NOT NULL DEFAULT (NEXT VALUE FOR user_sequence);
ALTER TABLE concepts ADD COLUMN user_id INT UNSIGNED NOT NULL DEFAULT (NEXT VALUE FOR user_sequence);
ALTER TABLE franchises ADD COLUMN user_id INT UNSIGNED NOT NULL DEFAULT (NEXT VALUE FOR user_sequence);
ALTER TABLE genres ADD COLUMN user_id INT UNSIGNED NOT NULL DEFAULT (NEXT VALUE FOR user_sequence);
ALTER TABLE platforms ADD COLUMN user_id INT UNSIGNED NOT NULL DEFAULT (NEXT VALUE FOR user_sequence);
ALTER TABLE themes ADD COLUMN user_id INT UNSIGNED NOT NULL DEFAULT (NEXT VALUE FOR user_sequence);

-- Replace primary key with new columns
ALTER TABLE game_age_ratings DROP CONSTRAINT game_age_ratings_age_rating_fk;
UPDATE game_age_ratings AS g JOIN age_ratings AS m ON g.age_rating_id = m.id SET g.age_rating_id = m.user_id;
ALTER TABLE age_ratings DROP PRIMARY KEY, ADD PRIMARY KEY (user_id);
ALTER TABLE game_age_ratings ADD CONSTRAINT game_age_ratings_age_rating_fk 
    FOREIGN KEY (age_rating_id) REFERENCES age_ratings (user_id) 
    ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE age_ratings CHANGE id gb_id INT UNSIGNED NOT NULL;

ALTER TABLE game_developers DROP CONSTRAINT game_developers_developer_fk;
UPDATE game_developers AS g JOIN companies AS m ON g.developer_id = m.id SET g.developer_id = m.user_id;
ALTER TABLE game_publishers DROP CONSTRAINT game_publishers_publisher_fk;
UPDATE game_publishers AS g JOIN companies AS m ON g.publisher_id = m.id SET g.publisher_id = m.user_id;
ALTER TABLE companies DROP PRIMARY KEY, ADD PRIMARY KEY (user_id);
ALTER TABLE game_developers ADD CONSTRAINT game_developers_developer_fk 
    FOREIGN KEY (developer_id) REFERENCES companies (user_id) 
    ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE game_publishers ADD CONSTRAINT game_publishers_publisher_fk 
    FOREIGN KEY (publisher_id) REFERENCES companies (user_id) 
    ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE companies CHANGE id gb_id INT UNSIGNED NOT NULL;

ALTER TABLE game_concepts DROP CONSTRAINT game_concepts_concept_fk;
UPDATE game_concepts AS g JOIN concepts AS m ON g.concept_id = m.id SET g.concept_id = m.user_id;
ALTER TABLE concepts DROP PRIMARY KEY, ADD PRIMARY KEY (user_id);
ALTER TABLE game_concepts ADD CONSTRAINT game_concepts_concept_fk 
    FOREIGN KEY (concept_id) REFERENCES concepts (user_id) 
    ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE concepts CHANGE id gb_id INT UNSIGNED NOT NULL;

ALTER TABLE game_franchises DROP CONSTRAINT game_franchises_franchise_fk;
UPDATE game_franchises AS g JOIN franchises AS m ON g.franchise_id = m.id SET g.franchise_id = m.user_id;
ALTER TABLE franchises DROP PRIMARY KEY, ADD PRIMARY KEY (user_id);
ALTER TABLE game_franchises ADD CONSTRAINT game_franchises_franchise_fk 
    FOREIGN KEY (franchise_id) REFERENCES franchises (user_id) 
    ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE franchises CHANGE id gb_id INT UNSIGNED NOT NULL;

ALTER TABLE game_genres DROP CONSTRAINT game_genres_genre_fk;
UPDATE game_genres AS g JOIN genres AS m ON g.genre_id = m.id SET g.genre_id = m.user_id;
ALTER TABLE genres DROP PRIMARY KEY, ADD PRIMARY KEY (user_id);
ALTER TABLE game_genres ADD CONSTRAINT game_genres_genre_fk 
    FOREIGN KEY (genre_id) REFERENCES genres (user_id) 
    ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE genres CHANGE id gb_id INT UNSIGNED NOT NULL;

ALTER TABLE game_platforms DROP CONSTRAINT game_platforms_platform_fk;
UPDATE game_platforms AS g JOIN platforms AS m ON g.platform_id = m.id SET g.platform_id = m.user_id;
ALTER TABLE platforms DROP PRIMARY KEY, ADD PRIMARY KEY (user_id);
ALTER TABLE game_platforms ADD CONSTRAINT game_platforms_platform_fk 
    FOREIGN KEY (platform_id) REFERENCES platforms (user_id) 
    ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE platforms CHANGE id gb_id INT UNSIGNED NOT NULL;

ALTER TABLE game_themes DROP CONSTRAINT game_themes_theme_fk;
UPDATE game_themes AS g JOIN themes AS m ON g.theme_id = m.id SET g.theme_id = m.user_id;
ALTER TABLE themes DROP PRIMARY KEY, ADD PRIMARY KEY (user_id);
ALTER TABLE game_themes ADD CONSTRAINT game_themes_theme_fk 
    FOREIGN KEY (theme_id) REFERENCES themes (user_id) 
    ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE themes CHANGE id gb_id INT UNSIGNED NOT NULL;

--- Final table for game ratings (GB and Steam users only, not metadata) ---
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
INSERT INTO dataset_users (foreign_user_id)
SELECT DISTINCT reviewer FROM gb_staff_reviews
UNION SELECT DISTINCT reviewer FROM gb_user_reviews;

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

-- Insert ratings data from GB users into game_ratings table
INSERT INTO game_ratings (game_id, user_id, rating)
SELECT game_id, user_id, score
FROM gb_staff_reviews AS r JOIN dataset_users AS u ON r.reviewer = u.foreign_user_id;

INSERT INTO game_ratings (game_id, user_id, rating)
SELECT game_id, user_id, score
FROM gb_user_reviews AS r JOIN dataset_users AS u ON r.reviewer = u.foreign_user_id;

-- Run estimate_steam_ratings.py

-- After inserting ratings data from Steam users, mark valid rows (those of users that played at least 5 games)
-- As some Steam games match several GB games and vice versa, and some Steam games don't match any GB games at all,
-- some user can have less than 5 games
ALTER TABLE game_ratings ADD COLUMN is_valid BOOLEAN NOT NULL DEFAULT TRUE;

-- Run mark_not_valid.py
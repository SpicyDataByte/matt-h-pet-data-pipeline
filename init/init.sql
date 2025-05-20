-- ===============================
-- Drop existing tables in correct order
-- ===============================

DROP TABLE IF EXISTS license;
DROP TABLE IF EXISTS pets;
DROP TABLE IF EXISTS pet_owners;
DROP TABLE IF EXISTS pet_breed;
DROP TABLE IF EXISTS pet_colour;
DROP TABLE IF EXISTS pet_species;

-- ===============================
-- Lookup Tables (Dimension Tables)
-- ===============================

CREATE TABLE pet_species (
    species_id INTEGER PRIMARY KEY,
    species_name TEXT UNIQUE
    -- species_name TEXT NOT NULL UNIQUE
);

CREATE TABLE pet_breed (
    breed_id INTEGER PRIMARY KEY,  
    breed_name TEXT UNIQUE
    -- breed_name TEXT NOT NULL UNIQUE
);

CREATE TABLE pet_colour (
    colour_id INTEGER PRIMARY KEY, 
    colour_name TEXT UNIQUE
    -- colour_name TEXT NOT NULL UNIQUE
);

-- ===============================
-- Main Tables -- NOT NULL constraints to be added. Solution is easier to follow if left out for now. 
-- ===============================

CREATE TABLE pet_owners (
    pet_owner_id BIGINT PRIMARY KEY,
    owner_first_name TEXT,
    owner_last_name TEXT,
    owner_street_number TEXT,
    owner_street_name TEXT,
    owner_unit TEXT,
    owner_city TEXT,
    owner_province TEXT,
    owner_postal_code TEXT,
    owner_phone TEXT,
    owner_email TEXT,
    owner_alternate_phone TEXT
);

CREATE TABLE pets (
    pet_id BIGINT PRIMARY KEY,
    pet_owner_id BIGINT NOT NULL REFERENCES pet_owners(pet_owner_id) ON DELETE CASCADE,
    pet_name TEXT,
    species_id INTEGER REFERENCES pet_species(species_id),
    breed_id INTEGER REFERENCES pet_breed(breed_id),
    colour_id INTEGER REFERENCES pet_colour(colour_id),
    sex TEXT,
    is_fixed TEXT,
    pet_status TEXT
);

CREATE TABLE license (
    license_id SERIAL PRIMARY KEY,
    pet_id BIGINT NOT NULL REFERENCES pets(pet_id) ON DELETE CASCADE,
    license_tag_number TEXT,
    last_date_issued DATE,
    license_expiry DATE,
    tag_status TEXT
);

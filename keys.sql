--Kör härifrån för FKs.
SET foreign_key_checks = 0;

--Börjar med allt till title
ALTER TABLE Directors
ADD CONSTRAINT Directors_title_id_fkey FOREIGN KEY (titleId) REFERENCES Title(titleId);

ALTER TABLE Writers
ADD CONSTRAINT Writers_title_id_fkey FOREIGN KEY (titleId) REFERENCES Title(titleId);

ALTER TABLE Ratings
ADD CONSTRAINT Ratings_title_id_fkey FOREIGN KEY (titleId) REFERENCES Title(titleId);

ALTER TABLE title_spec
ADD CONSTRAINT title_spec_title_id_fkey FOREIGN KEY (titleId) REFERENCES Title(titleId);

ALTER TABLE title_attrib
ADD CONSTRAINT title_attrib_title_id_fkey FOREIGN KEY (titleId) REFERENCES Title(titleId);

ALTER TABLE title_genre
ADD CONSTRAINT title_genre_title_id_fkey FOREIGN KEY (titleId) REFERENCES Title(titleId);

ALTER TABLE episodes
ADD CONSTRAINT episodes_title_id_fkey FOREIGN KEY (titleId) REFERENCES Title(titleId);

ALTER TABLE title_type
ADD CONSTRAINT title_type_title_id_fkey FOREIGN KEY (titleId) REFERENCES Title(titleId);

ALTER TABLE actors
ADD CONSTRAINT actors_title_id_fkey FOREIGN KEY (titleId) REFERENCES Title(titleId);

ALTER TABLE characters
ADD CONSTRAINT characters_id_fkey FOREIGN KEY (titleId, personid) REFERENCES Title(titleId);



ALTER TABLE cast
ADD CONSTRAINT cast_title_id_fkey FOREIGN KEY (titleId) REFERENCES Title(titleId);

--Börjar med allt till person
ALTER TABLE person_profession
ADD CONSTRAINT person_profession_person_id_fkey FOREIGN KEY (personId) REFERENCES person(personId);

ALTER TABLE cast
ADD CONSTRAINT cast_person_id_fkey FOREIGN KEY (personId) REFERENCES person(personId);

ALTER TABLE person_known_for
ADD CONSTRAINT person_known_for_id_fkey FOREIGN KEY (personId) REFERENCES person(personId);




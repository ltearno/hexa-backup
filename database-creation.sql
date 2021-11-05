CREATE EXTENSION pg_trgm;
CREATE EXTENSION fuzzystrmatch;
create table objects (sha char(64), isDirectory boolean, lastWrite bigint, size bigint, name text, mimeType text);
alter table objects add constraint objects_unique primary key (sha, isDirectory, lastWrite, size, name, mimeType);
CREATE INDEX trgm_idx_objects_name ON objects USING gin (name gin_trgm_ops);
CREATE index idx_objects_sha on objects USING btree (sha);
CREATE index idx_objects_mimeType on objects USING btree (mimeType);
create index idx_objects_lastWrite on objects USING btree(lastWrite);
CREATE index idx_objects_size on objects USING btree (size);

/* table des parents */
create table object_parents (sha char(64), parentSha char(64), primary key (sha,parentSha));
create index idx_object_parents_parentSha on object_parents USING btree(parentSha);

/* object_footprints : sha names, and (artist, album, genre,... for audio/ objects) */
/*
    sha names is particular to object (how a sha is referenced in a directory)
    id3 tags are particular to a sha

    => some caracteristics are either related to:
      - the content itself (the sha, sniffed mimeType, id3 tags, exif data)
      - how this content is pointed from a directory (name, mime type from name extension, write date...)
*/
create table object_footprints (sha char(64), footprint text, primary key (sha));
create index idx_object_footprints ON object_footprints USING gin (footprint gin_trgm_ops);
create index idx_object_footprints_sha on objects USING btree (sha);

/* content related metadata */
create table object_audio_tags (sha char(64), tags jsonb, primary key (sha));
create table object_exifs (sha char(64), exif jsonb, primary key (sha));

/* table des sources */
create table object_sources (sha char(64), sourceId char(64), primary key (sha, sourceId));
create index idx_object_sources_sourceId on object_sources USING btree(sourceId);
create index idx_object_shas on object_sources USING btree(sha);
create index idx_object_sources on object_sources USING btree(sourceId, sha);

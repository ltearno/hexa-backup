CREATE EXTENSION pg_trgm;
CREATE EXTENSION fuzzystrmatch;

create table objects_hierarchy (sourceId char(64), parentSha char(64), sha char(64), lastWrite bigint, size bigint, name text, mimeType text);
alter table objects_hierarchy add constraint objects_hierarchy_unique primary key (sourceId, parentSha, sha, lastWrite, size, name, mimeType);
CREATE index idx_objects_hierarchy_sourceId on objects_hierarchy USING btree (sourceId);
CREATE index idx_objects_hierarchy_parentSha on objects_hierarchy USING btree (parentSha);
CREATE index idx_objects_hierarchy_sha on objects_hierarchy USING btree (sha);
create index idx_objects_hierarchy_lastWrite on objects_hierarchy USING btree(lastWrite);
CREATE index idx_objects_hierarchy_size on objects_hierarchy USING btree (size);
CREATE INDEX trgm_idx_objects_hierarchy_name ON objects_hierarchy USING gin (name gin_trgm_ops);
CREATE index idx_objects_hierarchy_mimeType on objects_hierarchy USING btree (mimeType);

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

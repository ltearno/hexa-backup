CREATE EXTENSION pg_trgm;
CREATE EXTENSION fuzzystrmatch;

create table objects_hierarchy (sourceId text, parentSha char(64), sha char(64), lastWrite bigint, size bigint, name text, mimeTypeType text, mimeTypeSubType text);
alter table objects_hierarchy add constraint objects_hierarchy_unique primary key (sourceId, sha, parentSha, name);
CREATE index idx_objects_hierarchy_sourceId on objects_hierarchy USING btree (sourceId);
CREATE index idx_objects_hierarchy_parentSha on objects_hierarchy USING btree (parentSha);
CREATE index idx_objects_hierarchy_sha on objects_hierarchy USING btree (sha);
create index idx_objects_hierarchy_lastWrite on objects_hierarchy USING btree(lastWrite);
CREATE index idx_objects_hierarchy_size on objects_hierarchy USING btree (size);
CREATE INDEX trgm_idx_objects_hierarchy_name ON objects_hierarchy USING gin (name gin_trgm_ops);
CREATE index idx_objects_hierarchy_sourceId_mime on objects_hierarchy USING btree (sourceId, mimeTypeType, mimeTypeSubType);

/* content related metadata */
create table object_audio_tags (sha char(64), tags jsonb, footprint text, primary key (sha));
create index idx_object_audio_tags_footprints ON object_audio_tags USING gin (footprint gin_trgm_ops);
create table object_exifs (sha char(64), exif jsonb, type text, date timestamp, model text, height int, width int, latitude float, latitudeRef text, longitude float, longitudeRef text, primary key (sha));
create index idx_object_exifs_date on object_exifs USING btree (date);
create index idx_object_exifs_model on object_exifs USING btree (model);
create index idx_object_exifs_type on object_exifs USING btree (type);
create index idx_object_exifs_date_height on object_exifs USING btree (date, height);
create index idx_object_exifs_height_date on object_exifs USING btree (height, date);
create index idx_object_exifs_date_model_sha on object_exifs USING btree (date, model, sha);

create materialized view audio_objects as select o.sourceId, o.parentSha, o.sha, o.name, o.mimeTypeType, o.mimeTypeSubType, concat(o.name, ' ', oat.footprint) as footprint from objects_hierarchy o left join object_audio_tags oat on o.sha = oat.sha where o.mimeTypeType = 'audio';
CREATE INDEX idx_audio_objects_footprint ON audio_objects USING gin (footprint gin_trgm_ops);
CREATE index idx_audio_objects_sourceId on audio_objects USING btree (sourceId);

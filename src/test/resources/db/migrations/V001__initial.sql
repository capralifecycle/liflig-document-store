CREATE TABLE example
(
  id          uuid        NOT NULL PRIMARY KEY,
  created_at  timestamptz NOT NULL,
  modified_at timestamptz NOT NULL,
  version     bigint      NOT NULL,
  data        jsonb       NOT NULL
);
CREATE UNIQUE INDEX example_unique_field_index ON "example" ((data ->> 'uniqueField'));
-- Create index on text field to speed up tests
CREATE INDEX example_text_index ON "example" ((data ->> 'text'));

CREATE TABLE example_with_count
(
  id          uuid        NOT NULL PRIMARY KEY,
  created_at  timestamptz NOT NULL,
  modified_at timestamptz NOT NULL,
  version     bigint      NOT NULL,
  data        jsonb       NOT NULL
);
CREATE INDEX example_with_count_text_index ON "example_with_count" ((data ->> 'text'));

CREATE TABLE example_for_list_all
(
  id          uuid        NOT NULL PRIMARY KEY,
  created_at  timestamptz NOT NULL,
  modified_at timestamptz NOT NULL,
  version     bigint      NOT NULL,
  data        jsonb       NOT NULL
);
CREATE INDEX example_for_list_all_text_index ON "example_for_list_all" ((data ->> 'text'));

CREATE TABLE example_with_string_id
(
  id          text        NOT NULL PRIMARY KEY,
  created_at  timestamptz NOT NULL,
  modified_at timestamptz NOT NULL,
  version     bigint      NOT NULL,
  data        jsonb       NOT NULL
);
CREATE INDEX example_with_string_id_text_index ON "example_with_string_id" ((data ->> 'text'));

CREATE TABLE example_for_migration
(
  id          uuid        NOT NULL PRIMARY KEY,
  created_at  timestamptz NOT NULL,
  modified_at timestamptz NOT NULL,
  version     bigint      NOT NULL,
  data        jsonb       NOT NULL
);
CREATE INDEX example_for_migration_text_index ON "example_for_migration" ((data ->> 'text'));

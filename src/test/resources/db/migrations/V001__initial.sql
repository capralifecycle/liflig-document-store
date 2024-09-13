CREATE TABLE example
(
  id          uuid        NOT NULL PRIMARY KEY,
  created_at  timestamptz NOT NULL,
  modified_at timestamptz NOT NULL,
  version     bigint      NOT NULL,
  data        jsonb       NOT NULL
);

CREATE UNIQUE INDEX example_unique_field_index ON "example" ((data ->> 'uniqueField'));

CREATE TABLE example_with_count
(
  id          uuid        NOT NULL PRIMARY KEY,
  created_at  timestamptz NOT NULL,
  modified_at timestamptz NOT NULL,
  version     bigint      NOT NULL,
  data        jsonb       NOT NULL
);

CREATE TABLE example_with_string_id
(
  id          text        NOT NULL PRIMARY KEY,
  created_at  timestamptz NOT NULL,
  modified_at timestamptz NOT NULL,
  version     bigint      NOT NULL,
  data        jsonb       NOT NULL
)

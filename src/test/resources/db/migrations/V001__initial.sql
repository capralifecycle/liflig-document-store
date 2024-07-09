CREATE TABLE example
(
  id          uuid        NOT NULL PRIMARY KEY,
  created_at  timestamptz NOT NULL,
  modified_at timestamptz NOT NULL,
  version     bigint      NOT NULL,
  data        jsonb       NOT NULL
);

-- Separate table to avoid other tests interfering with the count returned by SearchDaoWithCount
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

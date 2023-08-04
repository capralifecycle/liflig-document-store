CREATE TABLE example (
  id uuid NOT NULL PRIMARY KEY,
  created_at timestamptz NOT NULL,
  modified_at timestamptz NOT NULL,
  version bigint NOT NULL,
  data jsonb NOT NULL
);

-- Separate table to make SearchRepositoryWithCount tests independent
CREATE TABLE example_with_count
(
  id          uuid        NOT NULL PRIMARY KEY,
  created_at  timestamptz NOT NULL,
  modified_at timestamptz NOT NULL,
  version     bigint      NOT NULL,
  data        jsonb       NOT NULL
);

CREATE TABLE states (
  state_id   INTEGER,
  state_name VARCHAR(30)
) PROPERTIES (storage='lin-hash', hashkey='0');
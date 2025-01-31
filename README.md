```
cargo build
./target/debug/flight_sql_server &
./target/debug/flight_sql_client --host localhost --port 50051 db-schemas %
```

OR

use a pyarrow client to `list_schemas()`

#[macro_export]
macro_rules! pg_insert {
    ($txn:expr, $table:ident, { $($field:ident : $val:expr),+ $(,)? }) => {{
        let table = stringify!($table);
        let field_names: Vec<&str> = vec![$( stringify!($field) ),+];
        let placeholders: Vec<String> = (1..=field_names.len()).map(|i| format!("${}", i)).collect();
        let values: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![$(&$val),+];
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table,
            field_names.join(", "),
            placeholders.join(", ")
        );
        $txn.execute(sql.as_str(), &values).await
    }};
}

#[macro_export]
macro_rules! pg_select {
     ($txn:expr, $table:ident, { $($field:ident),* $(,)? }) => {{
        let table = stringify!($table);
        let fields: Vec<_> = vec![$( stringify!($field) ),*];
        let state = format!("SELECT {} FROM {}", fields.join(", "), table);

        $txn.query(&state.clone(), &[])
    }};

      ($txn:expr, $table:ident, { $($field:ident),* $(,)? } where $($field_name:ident = $field_value:ident),* $(,)? ) => {{
        let table = stringify!($table);
        let fields: Vec<_> = vec![$( stringify!($field) ),*];
        let mut state = format!("SELECT {} FROM {}", fields.join(", "), table);

        // Append WHERE clause
        state.push_str(" WHERE ");
        $(
            state.push_str(&format!("{} = ", stringify!($field_name)));
            state.push_str(&format!("'{}'", stringify!($field_value)));
            state.push(',');
        )*

        // Remove the trailing comma from the last condition
        state.pop();

        $txn.query(&state.clone(), &[])
    }};
}
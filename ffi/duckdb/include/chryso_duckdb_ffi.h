#pragma once

#ifdef __cplusplus
extern "C" {
#endif

typedef struct DuckDbSession DuckDbSession;

DuckDbSession* chryso_duckdb_session_new();
void chryso_duckdb_session_free(DuckDbSession* session);

int chryso_duckdb_plan_execute(
    DuckDbSession* session,
    const unsigned char* plan_ptr,
    unsigned long long plan_len,
    char** result_out
);

char* chryso_duckdb_last_error();
void chryso_duckdb_string_free(char* value);

#ifdef __cplusplus
}
#endif

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

typedef struct VxSession VxSession;

VxSession* vx_session_new();
void vx_session_free(VxSession* session);

int vx_plan_execute(VxSession* session, const char* plan_json, char** result_out);
int vx_plan_execute_arrow(VxSession* session, const char* plan_json, unsigned char** data_out, unsigned long long* len_out);

const char* vx_last_error();
void vx_string_free(char* value);
void vx_bytes_free(unsigned char* value);

#ifdef __cplusplus
}
#endif

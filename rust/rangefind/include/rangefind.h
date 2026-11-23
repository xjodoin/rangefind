/*
 * Rangefind query engine — C ABI.
 *
 * The same functions are exported by the native static/dynamic library
 * (iOS, Android NDK, macOS, Linux, Windows, Dart FFI) and by the
 * WebAssembly module. The engine performs no I/O: it asks the host for
 * whole objects or byte ranges, and the host answers with bytes.
 *
 * Usage:
 *   uint32_t s  = rf_session_new();
 *   uint32_t op = rf_open(s, url, url_len, "{}", 2);
 *   loop:
 *     const uint8_t *json = rf_poll(s); size_t n = rf_result_len(s);
 *     // json = {"requests":[{"id","url","ranges":[[offset,length],...]}],
 *     //         "done":[{"op","ok","value"|"error","code"}], "pending": N}
 *     // For each request: fetch (ranges empty = whole object), then call
 *     //   rf_complete(s, id, data, data_len, lengths, count)  (count = 0 for whole object)
 *     //   rf_complete_missing(s, id)                          (404)
 *     //   rf_complete_error(s, id, msg, msg_len)
 *     // Opening resolves to {"engine": id, "summary": {...}}; then
 *     //   rf_call(s, engine, "search", 6, params_json, params_len)
 *     // with methods search | count | suggest | vectorSearch |
 *     // authorityLookup | loadFacetValues | loadDocValues | hydrateRows | stats.
 *
 * rf_complete: `lengths` (u32 x count) must sum exactly to `data_len`;
 * inconsistent lengths fail that request with an error instead of being
 * guessed. Freeing a session with requests still outstanding is safe: its
 * pending operations are cancelled and every engine and cache it owns is
 * released.
 *
 * Threading: a session is single-threaded; use one session per thread.
 * The buffer returned by rf_poll stays valid until the next rf_poll on the
 * same session. Strings are UTF-8 and not NUL-terminated (explicit lengths).
 */
#ifndef RANGEFIND_H
#define RANGEFIND_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

uint8_t *rf_alloc(size_t len);
void rf_free(uint8_t *ptr, size_t len);

uint32_t rf_session_new(void);
void rf_session_free(uint32_t session);

uint32_t rf_open(uint32_t session, const uint8_t *url, size_t url_len, const uint8_t *options_json, size_t options_len);
uint32_t rf_call(uint32_t session, uint32_t engine, const uint8_t *method, size_t method_len, const uint8_t *params_json, size_t params_len);
uint32_t rf_close(uint32_t session, uint32_t engine);

void rf_complete(uint32_t session, uint32_t request, const uint8_t *data, size_t data_len, const uint32_t *lengths, size_t count);
void rf_complete_missing(uint32_t session, uint32_t request);
void rf_complete_error(uint32_t session, uint32_t request, const uint8_t *message, size_t message_len);

const uint8_t *rf_poll(uint32_t session);
size_t rf_result_len(uint32_t session);

/* (major << 16) | (minor << 8) | patch */
uint32_t rf_version(void);

#ifdef __cplusplus
}
#endif

#endif /* RANGEFIND_H */

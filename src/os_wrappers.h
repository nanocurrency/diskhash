#ifndef DISKHASH_OS_WRAPPERS_H_INCLUDE_GUARD__
#define DISKHASH_OS_WRAPPERS_H_INCLUDE_GUARD__

/* Wrapper code so it works under Windows and POSIX standards. */

#ifdef _WIN32
// File Options
#define O_RDONLY	     00
#define O_WRONLY	     01
#define O_RDWR		     02
#define O_CREAT	       0100	/* Not fcntl.  */
#define O_EXCL		   0200	/* Not fcntl.  */
// Memory Mapping Protections
#define PROT_READ	0x1		/* Page can be read.  */
#define PROT_WRITE	0x2		/* Page can be written.  */
#define PROT_EXEC	0x4		/* Page can be executed.  */
#define PROT_NONE	0x0		/* Page can not be accessed.  */
#endif

#ifdef _WIN32
typedef void* dht_file_t;  // HANDLE
#else
typedef int dht_file_t;
#endif

#ifdef __cplusplus
extern "C" {
#endif

#ifdef _WIN32
int dht_win32_page_protections(const int protections);
int dht_win32_file_mapping_access(const int page_protections);
bool dht_utf8_to_utf16(const char* src, unsigned short** dst);
#endif
int dht_read_file (dht_file_t file_descriptor, void * buffer, size_t size);
dht_file_t dht_open_file(const char* file_path, const unsigned int flags, const bool limited_access);
void dht_close_file(const dht_file_t file_descriptor);
bool dht_delete_file(const char* file_path);
bool dht_file_size(const dht_file_t file_descriptor, size_t* file_size);
bool dht_truncate_file(const dht_file_t file_descriptor, const size_t file_size);
bool dht_file_sync(const dht_file_t file_descriptor);
bool dht_memory_map_file(const dht_file_t file_descriptor, const void** data_buffer, const size_t data_size, const int protections);
bool dht_memory_unmap_file(void* data, const size_t size);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif //DISKHASH_OS_WRAPPERS_H_INCLUDE_GUARD__

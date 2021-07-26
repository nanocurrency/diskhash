#ifdef _WIN32
#include <Windows.h>
#include <handleapi.h>
#include <windef.h>
#include <memoryapi.h>
#include <fileapi.h>
#include <winnt.h>
#else
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#endif
#include <stdbool.h>

#include "os_wrappers.h"

int dht_read_file (dht_file_t file_descriptor, void *buffer, size_t size)
{
    int read_size = 0;
#ifdef _WIN32
    bool read_status = ReadFile (file_descriptor, buffer, (DWORD) size, (LPDWORD) &read_size, NULL);
	if (!read_status)
	{
		read_size = -1;
	}
#else
    read_size = read(file_descriptor, buffer, size);
#endif
    return read_size;
}

dht_file_t dht_open_file(const char* file_path, int flags, bool limited_access)
{
    dht_file_t file_descriptor = 0;
#ifdef _WIN32
    LPCWSTR wfile_path = NULL;
    LPOFSTRUCT file_info = { 0 };
    DWORD acc = 0;
    DWORD share = FILE_SHARE_READ | FILE_SHARE_WRITE;
    DWORD disp = OPEN_ALWAYS;
    DWORD attrs = FILE_ATTRIBUTE_NORMAL;
    if (!(flags & O_CREAT) || flags == O_RDONLY)
    {
        disp = OPEN_EXISTING;
    }
    if (flags == O_RDONLY)
    {
        acc |= GENERIC_READ;
    }
    if (flags & O_RDWR)
    {
        acc |= GENERIC_READ;
    }
    if (flags & O_WRONLY || flags & O_RDWR)
    {
        acc |= GENERIC_WRITE;
    }
    wfile_path = malloc((strlen(file_path)+1) * sizeof(WCHAR));
    if (!wfile_path || !dht_utf8_to_utf16(file_path, (LPCWSTR*) &wfile_path))
    {
        free(wfile_path);
        return file_descriptor;
    }
    file_descriptor = CreateFileW(wfile_path, acc, share, NULL, disp, attrs, NULL);
    free(wfile_path);
    if (file_descriptor == INVALID_HANDLE_VALUE)
    {
        file_descriptor = 0;
    }
#else
    unsigned int file_access = limited_access ? 0600 : 0644;
    file_descriptor = open(file_path, flags, file_access);
#endif
    return file_descriptor;
}

void dht_close_file(dht_file_t file_descriptor)
{
#ifdef _WIN32
    CloseHandle(file_descriptor);
#else
    close(file_descriptor);
#endif
}

bool dht_delete_file(const char* file_path)
{
    bool success = false;
#ifdef _WIN32
    LPCWSTR wfile_path = NULL;
    wfile_path = malloc((strlen(file_path)+1) * sizeof(WCHAR));
	if (wfile_path && dht_utf8_to_utf16 (file_path, (LPCWSTR*) &wfile_path))
	{
		success = DeleteFileW (wfile_path) != 0;
	}
	free(wfile_path);
#else
    success = unlink(file_path) == 0;
#endif
    return success;
}

bool dht_file_size(dht_file_t file_descriptor, size_t* file_size)
{
    bool success = false;
    *file_size = 0;
#ifdef _WIN32
    LARGE_INTEGER _size = { 0 };
    success = GetFileSizeEx(file_descriptor, &_size) != 0;
    if (success)
    {
        *file_size = (size_t)_size.QuadPart;
    }
#else
    struct stat st;
    success = fstat(file_descriptor, &st) == 0;
    if (success)
    {
        *file_size = st.st_size;
    }
#endif
    return success;
}

bool dht_truncate_file(dht_file_t file_descriptor, size_t file_size)
{
    bool success = false;
#ifdef _WIN32
    DWORD returned_size = SetFilePointer(file_descriptor, (LONG) file_size, NULL, FILE_BEGIN);
    success = (returned_size == (DWORD)file_size);
    if (success)
    {
        success = (SetEndOfFile(file_descriptor) != 0);
    }
#else
    success = (ftruncate(file_descriptor, file_size) == 0);
#endif
    return success;
}

bool dht_file_sync(dht_file_t file_descriptor)
{
    bool success = false;
#ifdef _WIN32
    success = FlushFileBuffers(file_descriptor) != 0;
#else
    success = fsync(file_descriptor) == 0;
#endif
    return success;
}

#ifdef _WIN32
int dht_win32_page_protections(int protections)
{
    int __win_protections = 0;
    if (protections & PROT_READ && protections & PROT_WRITE && protections & PROT_EXEC)
    {
        __win_protections = PAGE_EXECUTE_READWRITE;
    }
    else if ((protections & PROT_READ && protections & PROT_WRITE))
    {
        __win_protections = PAGE_READWRITE;
    }
    else if ((protections & PROT_READ))
    {
        __win_protections = PAGE_READONLY;
    }
    return __win_protections;
}

int dht_win32_file_mapping_access(int page_protections)
{
    int file_mapping_access = (page_protections == PAGE_READONLY) ? FILE_MAP_READ : FILE_MAP_WRITE;
    return file_mapping_access;
}

bool dht_utf8_to_utf16(const char* src, unsigned short** dst)
{
    int need = 0;
    unsigned short* result = NULL;
    for (;;)
    {
        /* malloc result, then fill it in */
        need = MultiByteToWideChar(CP_UTF8, 0, src, -1, result, need);
        if (!need)
        {
            //rc = ErrCode();
            free(result);
            return false;
        }
        if (!result) {
            result = malloc(sizeof(unsigned short) * need);
            if (!result)
            {
                //return ENOMEM;
                return false;
            }
            continue;
        }
        memcpy(*dst, result, sizeof(unsigned short) * need);
        free(result);
        return true;
    }
}
#endif

bool dht_memory_map_file(dht_file_t file_descriptor, void** data_buffer, size_t data_size, int protections)
{
    bool success = false;
#ifdef _WIN32
    HANDLE mh;
    const int win_page_protections = dht_win32_page_protections(protections);
    mh = CreateFileMappingW(file_descriptor, NULL, win_page_protections, 0, 0, NULL);
    if (mh != NULL)
    {
        const DWORD file_map_access = dht_win32_file_mapping_access(win_page_protections);
        *data_buffer = MapViewOfFileEx(mh, file_map_access, 0, 0, data_size, NULL);
        CloseHandle(mh);
        if (*data_buffer != NULL)
        {
            success = true;
        }
    }
    else
    {
        DWORD error = GetLastError();
        char err[256];
        memset(err, 0, 256);
        FormatMessage (FORMAT_MESSAGE_FROM_SYSTEM, NULL, error,
                       MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), err, 255, NULL);
        int x = 0;
    }
#else
    *data_buffer = mmap(NULL,
                     data_size,
                     protections,
                     MAP_SHARED,
                     file_descriptor,
                     0);
    success = (*data_buffer != MAP_FAILED);
#endif
    return success;
}

bool dht_memory_unmap_file(void* data, size_t size)
{
    bool success = false;
#ifdef _WIN32
    success = UnmapViewOfFile(data) != 0;
#else
    success = munmap(data, size) == 0;
#endif
    return success;
}

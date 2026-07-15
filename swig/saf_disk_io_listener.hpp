// saf_disk_io_listener.hpp
//
// disk_io_file_provider is implemented in Java via SWIG's director
// mechanism (org.libtorrent4j.swig.disk_io_file_provider once generated --
// see %feature("director") in libtorrent.i), the same way
// list_files_listener/set_piece_hashes_listener already are. Kept in its
// own header (rather than folded into libtorrent.hpp) so it can be
// %include'd early in libtorrent.i, before session_params.i needs the type
// for its set_saf_disk_io_constructor() %extend method.

#pragma once

#include <string>

// Global scope, not `libtorrent::` -- matches list_files_listener /
// set_piece_hashes_listener in libtorrent.hpp.
struct disk_io_file_provider
{
    virtual ~disk_io_file_provider() {}

    // Opens (creating parent directories if necessary) the file at
    // relative_path under save_path for read/write, returning a native
    // POSIX fd. Ownership of the fd passes to native code -- it will be
    // close()d exactly once, from release_file(), never by the caller of
    // this method.
    virtual long open_file(std::string info_hash, int file_index,
        std::string relative_path, std::string save_path)
    { return -1; }

    // Called exactly once per successful open_file() call, when native
    // code is completely done with that fd (torrent stopped/removed).
    virtual void release_file(std::string info_hash, int file_index,
        long fd, std::string relative_path, std::string save_path)
    {}

    virtual bool delete_file(std::string info_hash, int file_index,
        std::string relative_path, std::string save_path)
    { return false; }

    // Best-effort rename within the same provider root. Returning false
    // surfaces as a failed async_rename_file to the libtorrent client;
    // there is no copy fallback on the native side.
    virtual bool rename_file(std::string info_hash, int file_index,
        std::string relative_path, std::string save_path, std::string new_name)
    { return false; }

    // Moves all of a torrent's files from old_save_path to new_save_path.
    // Returning false keeps the torrent on old_save_path. Any fds already
    // cached by native code for this torrent stay open and valid (they
    // reference inodes, not paths); the new save path only affects files
    // opened afterwards.
    virtual bool move_storage(std::string info_hash,
        std::string old_save_path, std::string new_save_path)
    { return false; }
};

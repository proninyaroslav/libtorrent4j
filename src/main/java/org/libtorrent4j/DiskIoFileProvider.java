/*
 * Copyright (c) 2026, Yaroslav Pronin
 *
 * Licensed under the terms of the MIT license.
 * Copy of the license at https://opensource.org/licenses/MIT
 */

package org.libtorrent4j;

/**
 * Implemented on the Java side and handed to {@code SessionParams} to back
 * torrent storage with something other than plain filesystem paths (e.g.
 * Android SAF, an encrypted container, a RAM disk).
 * <p>
 * Native code (see {@code swig/saf_disk_io.cpp}) treats {@code savePath} as
 * an opaque string it never interprets itself -- it is whatever was passed
 * to {@code SessionManager.download(..., savePath)} / stored in resume
 * data, and is handed back verbatim so the implementation can decode its
 * own scheme.
 * <p>
 * Native code caches the fd returned by {@link #openFile} for the lifetime
 * of the torrent's storage (or until the torrent is stopped/removed); it
 * will call {@link #releaseFile} exactly once per successful
 * {@link #openFile} call before the process could plausibly reuse that fd
 * number, and will not call {@code close()} on it itself.
 */
public interface DiskIoFileProvider {

    /**
     * Opens (creating if necessary, including parent directories) the file
     * at {@code relativePath} under {@code savePath} for read/write, and
     * returns a native POSIX file descriptor. Ownership of the fd passes to
     * native code.
     *
     * @return a valid fd, or -1 on failure (native code reports this to the
     *         client as a FILE_OPEN storage_error).
     */
    long openFile(byte[] infoHash, int fileIndex, String relativePath, String savePath);

    /**
     * Called when native code is completely done with a previously-opened
     * fd for this file. Implementations should not close {@code fd}
     * eagerly on any other event -- native code owns its lifetime once
     * {@link #openFile} hands it over.
     */
    void releaseFile(byte[] infoHash, int fileIndex, long fd, String relativePath, String savePath);

    boolean deleteFile(byte[] infoHash, int fileIndex, String relativePath, String savePath);

    /**
     * Best-effort rename within the same provider root. Returning false
     * surfaces as a failed async_rename_file to the libtorrent client;
     * there is no copy fallback on the native side.
     */
    boolean renameFile(byte[] infoHash, int fileIndex, String relativePath, String savePath, String newName);

    /**
     * Moves all of this torrent's files from {@code oldSavePath} to
     * {@code newSavePath}. Returning false keeps the torrent on
     * {@code oldSavePath}. Any fds already cached by native code for this
     * torrent stay open and valid (they reference inodes, not paths); the
     * new save path only affects files opened afterwards.
     */
    boolean moveStorage(byte[] infoHash, String oldSavePath, String newSavePath);
}

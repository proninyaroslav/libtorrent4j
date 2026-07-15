// saf_disk_io.hpp
//
// A disk_interface implementation that resolves every torrent file to a
// native fd via a JNI callback into a Java-side DiskIoFileProvider
// (src/main/java/org/libtorrent4j/DiskIoFileProvider.java), instead of
// opening paths itself. Written for Android SAF (content:// / DocumentFile)
// as the motivating case, but the JNI contract is deliberately
// SAF-agnostic -- native code only ever asks "give me an fd for this file".
//
// Modeled on posix_disk_io (fd-based pread/pwrite) rather than
// mmap_disk_io: SAF-provided fds are not guaranteed to be safely mmap-able
// (non-local DocumentsProviders can hand back pipe fds), and per
// disk_interface.hpp's own doc comment, posix_disk_io itself is
// single-threaded and runs inline on the network thread -- we don't want
// that here, since first-open-per-file crosses JNI and later
// pread/pwrite may hit non-local storage, so this uses its own small
// worker pool instead of libtorrent's internal aux::disk_io_thread_pool
// (that type is tightly coupled to aux::disk_job/tailqueue, the dispatch
// machinery mmap_disk_io/posix_disk_io use internally, and is not really
// meant for external disk_interface implementations to reuse).
//
// Signatures below verified directly against the vendored libtorrent
// submodule at swig/deps/libtorrent (pinned commit: v2.0.11-674-g24a3adf35)
// on 2026-07-15 -- disk_interface.hpp, storage_defs.hpp, session_params.hpp,
// error_code.hpp (storage_error), file_storage.hpp (file_slice, map_block),
// peer_request.hpp, operations.hpp (operation_t). Re-verify if the submodule
// pin moves.

#pragma once

#include <jni.h>

#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "libtorrent/disk_interface.hpp"
#include "libtorrent/disk_buffer_holder.hpp"
#include "libtorrent/file_storage.hpp"   // torrent_entry::files is a by-value member
#include "libtorrent/io_context.hpp"
#include "libtorrent/session_params.hpp" // disk_io_constructor_type
#include "libtorrent/storage_defs.hpp"   // storage_params, storage_mode_t, move_flags_t

namespace lt = libtorrent;

// Minimal fixed-size worker pool. Deliberately not libtorrent's internal
// aux::disk_io_thread_pool -- see rationale in the file header comment.
class saf_job_queue
{
public:
    explicit saf_job_queue(int num_threads);
    ~saf_job_queue();

    void post(std::function<void()> job);

private:
    void worker_loop();

    std::vector<std::thread> m_threads;
    std::mutex m_mutex;
    std::condition_variable m_cv;
    std::deque<std::function<void()>> m_jobs;
    bool m_stop = false;
};

class saf_disk_io final : public lt::disk_interface, public lt::buffer_allocator_interface
{
public:
    saf_disk_io(lt::io_context& ioc, lt::settings_interface const& settings,
                lt::counters& cnt, JavaVM* jvm, jobject bridge_global_ref);
    ~saf_disk_io() override;

    // --- lt::disk_interface ---

    lt::storage_holder new_torrent(lt::storage_params const& p,
                                    std::shared_ptr<void> const& torrent) override;
    void remove_torrent(lt::storage_index_t idx) override;

    void async_read(lt::storage_index_t storage, lt::peer_request const& r,
                     std::function<void(lt::disk_buffer_holder, lt::storage_error const&)> handler,
                     lt::disk_job_flags_t flags = {}) override;

    bool async_write(lt::storage_index_t storage, lt::peer_request const& r, char const* buf,
                      std::shared_ptr<lt::disk_observer> o,
                      std::function<void(lt::storage_error const&)> handler,
                      lt::disk_job_flags_t flags = {}) override;

    void async_hash(lt::storage_index_t storage, lt::piece_index_t piece,
                     lt::span<lt::sha256_hash> v2, lt::disk_job_flags_t flags,
                     std::function<void(lt::piece_index_t, lt::sha1_hash const&, lt::storage_error const&)> handler) override;

    void async_hash2(lt::storage_index_t storage, lt::piece_index_t piece, int offset,
                      lt::disk_job_flags_t flags,
                      std::function<void(lt::piece_index_t, lt::sha256_hash const&, lt::storage_error const&)> handler) override;

    void async_move_storage(lt::storage_index_t storage, std::string p, lt::move_flags_t flags,
                             std::function<void(lt::status_t, std::string const&, lt::storage_error const&)> handler) override;

    void async_release_files(lt::storage_index_t storage,
                              std::function<void()> handler = std::function<void()>()) override;

    void async_check_files(lt::storage_index_t storage, lt::add_torrent_params const* resume_data,
                            lt::aux::vector<std::string, lt::file_index_t> links,
                            std::function<void(lt::status_t, lt::storage_error const&)> handler) override;

    void async_stop_torrent(lt::storage_index_t storage,
                             std::function<void()> handler = std::function<void()>()) override;

    void async_rename_file(lt::storage_index_t storage, lt::file_index_t index, std::string name,
                            std::function<void(std::string const&, lt::file_index_t, lt::storage_error const&)> handler) override;

    void async_delete_files(lt::storage_index_t storage, lt::remove_flags_t options,
                             std::function<void(lt::storage_error const&)> handler) override;

    void async_set_file_priority(lt::storage_index_t storage,
                                  lt::aux::vector<lt::download_priority_t, lt::file_index_t> prio,
                                  std::function<void(lt::storage_error const&, lt::aux::vector<lt::download_priority_t, lt::file_index_t>)> handler) override;

    void async_clear_piece(lt::storage_index_t storage, lt::piece_index_t index,
                            std::function<void(lt::piece_index_t)> handler) override;

    void update_stats_counters(lt::counters& c) const override;
    std::vector<lt::open_file_state> get_status(lt::storage_index_t) const override;

    void abort(bool wait) override;
    void submit_jobs() override;
    void settings_updated() override;

    // --- lt::buffer_allocator_interface ---
    void free_disk_buffer(char* b) override;

private:
    struct torrent_entry
    {
        lt::file_storage files; // owned copy -- storage_params::files is only
                                 // a reference valid for the new_torrent() call
        std::string save_path;  // owned copy -- storage_params::path is a string_view
        lt::sha1_hash info_hash;
        std::vector<int> fds; // indexed by file_index_t, -1 until opened;
                               // plain ints (not e.g. once_flag/atomic per
                               // element) so this stays default-movable/
                               // copyable for std::vector's own bookkeeping --
                               // fd_for() serializes first-open under `mutex`
                               // instead.
        std::mutex mutex;
    };

    std::shared_ptr<torrent_entry> torrent_for(lt::storage_index_t storage) const;

    // Resolves (and lazily opens via JNI) the fd for a given file. May block
    // the calling worker thread on first access only (JNI round-trip);
    // cached thereafter. Returns -1 on failure.
    int fd_for(torrent_entry& t, lt::file_index_t file_idx);
    void release_all(torrent_entry& t);

    JNIEnv* attach_jni() const;

    lt::io_context& m_ioc;
    lt::counters& m_stats;

    JavaVM* m_jvm;
    jobject m_bridge; // global ref to the Java DiskIoFileProvider, owned by caller
    jclass m_bridge_class;
    jmethodID m_mid_open_file;
    jmethodID m_mid_release_file;
    jmethodID m_mid_delete_file;
    jmethodID m_mid_rename_file;
    jmethodID m_mid_move_storage;

    mutable std::mutex m_torrents_mutex;
    std::map<std::uint32_t, std::shared_ptr<torrent_entry>> m_torrents;
    std::uint32_t m_next_index = 0;

    saf_job_queue m_pool;
};

// Bind this into session_params::disk_io_constructor. `jvm` and
// `bridge_global_ref` must outlive the session (bridge_global_ref should be
// created with NewGlobalRef by the caller and released only after the
// session tied to it is destroyed).
lt::disk_io_constructor_type saf_disk_io_constructor(JavaVM* jvm, jobject bridge_global_ref);

// saf_disk_io.cpp
//
// See saf_disk_io.hpp for the design rationale and what's been verified
// against the vendored libtorrent headers vs. what's still a guess.
//
// Remaining known gaps (fix before this is PR-able upstream):
//  - error_code construction on failure paths uses errno + generic_category(),
//    which is right for POSIX pread/pwrite/ftruncate failures, but the
//    JNI-side "operation failed" (openFile returned -1, deleteFile
//    returned false, ...) has no errno to report -- currently mapped to a
//    generic EIO. A real implementation should let DiskIoFileProvider
//    surface a reason (e.g. an exception message) back through JNI.
//  - async_check_files does not actually verify anything on disk yet
//    (always reports success), so resume data isn't validated.
//  - async_set_file_priority is a no-op: no partfile support, so toggling
//    a file to zero priority does not reclaim space the way the built-in
//    storage backends do.
//  - get_status()/update_stats_counters() are stubs.
//  - v2 (async_hash2) hashing is not implemented (returns a zero hash),
//    only v1 async_hash works. Needed for hybrid/v2 torrents.

#include "saf_disk_io.hpp"

#include "libtorrent/aux_/vector.hpp"
#include "libtorrent/error_code.hpp"
#include "libtorrent/hasher.hpp"
#include "libtorrent/operations.hpp"
#include "libtorrent/peer_request.hpp"

#include <cerrno>
#include <cstring>
#include <unistd.h>

namespace {

lt::storage_error errno_error(lt::file_index_t file, lt::operation_t op)
{
    return lt::storage_error(
        lt::error_code(errno, boost::system::generic_category()), file, op);
}

lt::storage_error generic_error(lt::file_index_t file, lt::operation_t op)
{
    // No errno available (JNI callback returned failure, not a syscall) --
    // EIO is a placeholder; see file-header note about surfacing a real
    // reason from DiskIoFileProvider.
    return lt::storage_error(
        lt::error_code(EIO, boost::system::generic_category()), file, op);
}

} // namespace

// --- saf_job_queue ---

saf_job_queue::saf_job_queue(int num_threads)
{
    for (int i = 0; i < num_threads; i++)
        m_threads.emplace_back([this] { worker_loop(); });
}

saf_job_queue::~saf_job_queue()
{
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_stop = true;
    }
    m_cv.notify_all();
    for (auto& t : m_threads) t.join();
}

void saf_job_queue::post(std::function<void()> job)
{
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_jobs.push_back(std::move(job));
    }
    m_cv.notify_one();
}

void saf_job_queue::worker_loop()
{
    for (;;) {
        std::function<void()> job;
        {
            std::unique_lock<std::mutex> lock(m_mutex);
            m_cv.wait(lock, [this] { return m_stop || !m_jobs.empty(); });
            if (m_stop && m_jobs.empty()) return;
            job = std::move(m_jobs.front());
            m_jobs.pop_front();
        }
        job();
    }
}

// --- saf_disk_io ---

JNIEnv* saf_disk_io::attach_jni() const
{
    JNIEnv* env = nullptr;
    jint r = m_jvm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_6);
    if (r == JNI_EDETACHED) {
        // saf_job_queue's worker threads live for the process lifetime in
        // this skeleton, so never detaching is a bounded, one-time-per-thread
        // leak of a JNIEnv, not an unbounded one. Revisit if the pool is ever
        // made to spin threads up/down dynamically.
        m_jvm->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    }
    return env;
}

saf_disk_io::saf_disk_io(lt::io_context& ioc, lt::settings_interface const&,
                          lt::counters& cnt, JavaVM* jvm, jobject bridge_global_ref)
    : m_ioc(ioc)
    , m_stats(cnt)
    , m_jvm(jvm)
    , m_bridge(bridge_global_ref)
    , m_pool(4) // TODO: derive from settings_pack (aio_threads) instead of hardcoding
{
    JNIEnv* env = attach_jni();
    jclass local = env->GetObjectClass(m_bridge);
    m_bridge_class = static_cast<jclass>(env->NewGlobalRef(local));
    env->DeleteLocalRef(local);

    m_mid_open_file = env->GetMethodID(m_bridge_class, "openFile",
        "([BILjava/lang/String;Ljava/lang/String;)J");
    m_mid_release_file = env->GetMethodID(m_bridge_class, "releaseFile",
        "([BIJLjava/lang/String;Ljava/lang/String;)V");
    m_mid_delete_file = env->GetMethodID(m_bridge_class, "deleteFile",
        "([BILjava/lang/String;Ljava/lang/String;)Z");
    m_mid_rename_file = env->GetMethodID(m_bridge_class, "renameFile",
        "([BILjava/lang/String;Ljava/lang/String;Ljava/lang/String;)Z");
    m_mid_move_storage = env->GetMethodID(m_bridge_class, "moveStorage",
        "([BLjava/lang/String;Ljava/lang/String;)Z");
}

saf_disk_io::~saf_disk_io()
{
    JNIEnv* env = attach_jni();
    env->DeleteGlobalRef(m_bridge_class);
    // m_bridge itself is owned by whoever called saf_disk_io_constructor --
    // released after the session tied to it is destroyed, not here.
}

std::shared_ptr<saf_disk_io::torrent_entry> saf_disk_io::torrent_for(lt::storage_index_t storage) const
{
    std::lock_guard<std::mutex> lock(m_torrents_mutex);
    return m_torrents.at(static_cast<std::uint32_t>(storage));
}

lt::storage_holder saf_disk_io::new_torrent(lt::storage_params const& p,
                                             std::shared_ptr<void> const&)
{
    auto t = std::make_shared<torrent_entry>();
    t->files = p.files; // owned copy; p.files is only a reference valid for this call
    t->save_path.assign(p.path.data(), p.path.size());
    t->info_hash = p.info_hash;
    t->fds.assign(static_cast<std::size_t>(t->files.num_files()), -1);

    std::uint32_t storage_idx;
    {
        std::lock_guard<std::mutex> lock(m_torrents_mutex);
        storage_idx = m_next_index++;
        m_torrents[storage_idx] = t;
    }
    return lt::storage_holder(lt::storage_index_t(storage_idx), *this);
}

void saf_disk_io::remove_torrent(lt::storage_index_t storage)
{
    std::shared_ptr<torrent_entry> t;
    {
        std::lock_guard<std::mutex> lock(m_torrents_mutex);
        auto key = static_cast<std::uint32_t>(storage);
        auto it = m_torrents.find(key);
        if (it == m_torrents.end()) return;
        t = it->second;
        m_torrents.erase(it);
    }
    release_all(*t);
}

void saf_disk_io::release_all(torrent_entry& t)
{
    JNIEnv* env = attach_jni();
    std::lock_guard<std::mutex> lock(t.mutex);
    jbyteArray info_hash = env->NewByteArray(20);
    env->SetByteArrayRegion(info_hash, 0, 20,
        reinterpret_cast<const jbyte*>(t.info_hash.data()));
    jstring save_path = env->NewStringUTF(t.save_path.c_str());

    for (int i = 0; i < static_cast<int>(t.fds.size()); i++) {
        if (t.fds[i] < 0) continue;
        std::string rel_path = t.files.file_path(lt::file_index_t(i));
        jstring rel = env->NewStringUTF(rel_path.c_str());
        env->CallVoidMethod(m_bridge, m_mid_release_file, info_hash, i,
                             static_cast<jlong>(t.fds[i]), rel, save_path);
        ::close(t.fds[i]); // native side owns the fd; bridge impl must NOT close it too
        t.fds[i] = -1;
        env->DeleteLocalRef(rel);
    }
    env->DeleteLocalRef(info_hash);
    env->DeleteLocalRef(save_path);
}

int saf_disk_io::fd_for(torrent_entry& t, lt::file_index_t file_idx)
{
    auto const i = static_cast<std::size_t>(static_cast<int>(file_idx));

    // Fast path: already opened. Double-checked under `mutex` for the
    // first-open race instead of a per-file once_flag/atomic, since those
    // aren't move/copy-constructible and can't live as std::vector elements
    // (see torrent_entry::fds) -- contention only happens on first touch of
    // each file, so a shared per-torrent lock is fine here.
    {
        std::lock_guard<std::mutex> lock(t.mutex);
        if (t.fds[i] >= 0) return t.fds[i];
    }

    JNIEnv* env = attach_jni();
    jbyteArray info_hash = env->NewByteArray(20);
    env->SetByteArrayRegion(info_hash, 0, 20,
        reinterpret_cast<const jbyte*>(t.info_hash.data()));
    std::string rel_path = t.files.file_path(file_idx);
    jstring rel = env->NewStringUTF(rel_path.c_str());
    jstring save_path = env->NewStringUTF(t.save_path.c_str());

    jlong fd = env->CallLongMethod(m_bridge, m_mid_open_file, info_hash,
                                    static_cast<int>(file_idx), rel, save_path);

    env->DeleteLocalRef(info_hash);
    env->DeleteLocalRef(rel);
    env->DeleteLocalRef(save_path);

    std::lock_guard<std::mutex> lock(t.mutex);
    if (t.fds[i] >= 0) {
        // Lost the race to another thread opening the same file concurrently
        // -- keep theirs, close ours.
        if (fd >= 0) ::close(static_cast<int>(fd));
        return t.fds[i];
    }
    t.fds[i] = static_cast<int>(fd); // -1 on failure
    return t.fds[i];
}

void saf_disk_io::free_disk_buffer(char* b)
{
    delete[] b;
}

void saf_disk_io::async_read(lt::storage_index_t storage, lt::peer_request const& r,
                              std::function<void(lt::disk_buffer_holder, lt::storage_error const&)> handler,
                              lt::disk_job_flags_t)
{
    auto t = torrent_for(storage);
    m_pool.post([this, t, r, handler = std::move(handler)]() mutable {
        lt::storage_error err;
        char* data = new char[static_cast<std::size_t>(r.length)];
        std::int64_t pos = 0;

        for (auto const& slice : t->files.map_block(r.piece, r.start, r.length)) {
            int fd = fd_for(*t, slice.file_index);
            if (fd < 0) { err = generic_error(slice.file_index, lt::operation_t::file_open); break; }
            auto n = ::pread(fd, data + pos, static_cast<std::size_t>(slice.size), slice.offset);
            if (n != slice.size) { err = errno_error(slice.file_index, lt::operation_t::file_read); break; }
            pos += slice.size;
        }

        lt::disk_buffer_holder buf(*this, data, static_cast<int>(r.length));
        lt::post(m_ioc, [handler = std::move(handler), buf = std::move(buf), err]() mutable {
            handler(std::move(buf), err);
        });
    });
}

bool saf_disk_io::async_write(lt::storage_index_t storage, lt::peer_request const& r,
                               char const* buf, std::shared_ptr<lt::disk_observer>,
                               std::function<void(lt::storage_error const&)> handler,
                               lt::disk_job_flags_t)
{
    auto t = torrent_for(storage);
    // buf is only guaranteed valid for the duration of this call per
    // disk_interface's contract for async_write (unlike async_read's
    // returned buffer) -- copy it before handing off to the worker pool.
    std::vector<char> owned(buf, buf + r.length);

    m_pool.post([this, t, r, owned = std::move(owned), handler = std::move(handler)]() mutable {
        lt::storage_error err;
        std::int64_t pos = 0;

        for (auto const& slice : t->files.map_block(r.piece, r.start, r.length)) {
            int fd = fd_for(*t, slice.file_index);
            if (fd < 0) { err = generic_error(slice.file_index, lt::operation_t::file_open); break; }
            auto n = ::pwrite(fd, owned.data() + pos, static_cast<std::size_t>(slice.size), slice.offset);
            if (n != slice.size) { err = errno_error(slice.file_index, lt::operation_t::file_write); break; }
            pos += slice.size;
        }

        lt::post(m_ioc, [handler = std::move(handler), err]() { handler(err); });
    });
    return false; // never claim the write queue is full -- no backpressure
                   // implemented yet; fine for correctness, not for a huge
                   // in-flight write queue under a slow SAF provider.
}

void saf_disk_io::async_hash(lt::storage_index_t storage, lt::piece_index_t piece,
                              lt::span<lt::sha256_hash>, lt::disk_job_flags_t,
                              std::function<void(lt::piece_index_t, lt::sha1_hash const&, lt::storage_error const&)> handler)
{
    auto t = torrent_for(storage);
    m_pool.post([this, t, piece, handler = std::move(handler)]() mutable {
        lt::storage_error err;
        lt::hasher h;
        int const piece_size = t->files.piece_size(piece);
        std::vector<char> block(static_cast<std::size_t>(std::min(piece_size, 1 << 20)));

        int offset = 0;
        while (offset < piece_size && !err) {
            int const len = std::min(static_cast<int>(block.size()), piece_size - offset);
            for (auto const& slice : t->files.map_block(piece, offset, len)) {
                int fd = fd_for(*t, slice.file_index);
                if (fd < 0) { err = generic_error(slice.file_index, lt::operation_t::file_open); break; }
                auto n = ::pread(fd, block.data(), static_cast<std::size_t>(slice.size), slice.offset);
                if (n != slice.size) { err = errno_error(slice.file_index, lt::operation_t::file_read); break; }
                h.update({block.data(), static_cast<int>(slice.size)});
            }
            offset += len;
        }

        lt::sha1_hash digest = err ? lt::sha1_hash{} : h.final();
        lt::post(m_ioc, [handler = std::move(handler), piece, digest, err]() {
            handler(piece, digest, err);
        });
    });
}

void saf_disk_io::async_hash2(lt::storage_index_t, lt::piece_index_t piece, int,
                               lt::disk_job_flags_t,
                               std::function<void(lt::piece_index_t, lt::sha256_hash const&, lt::storage_error const&)> handler)
{
    // v2 (per-block SHA-256) not implemented yet -- see file-header note.
    // Reporting success with a zero hash would silently corrupt v2/hybrid
    // torrents' merkle trees, so fail loudly instead until this is real.
    lt::storage_error err(lt::error_code(ENOSYS, boost::system::generic_category()),
                           lt::operation_t::file_read);
    lt::post(m_ioc, [handler = std::move(handler), piece, err]() {
        handler(piece, lt::sha256_hash{}, err);
    });
}

void saf_disk_io::async_move_storage(lt::storage_index_t storage, std::string p, lt::move_flags_t flags,
                                      std::function<void(lt::status_t, std::string const&, lt::storage_error const&)> handler)
{
    auto t = torrent_for(storage);

    if (flags == lt::move_flags_t::reset_save_path
        || flags == lt::move_flags_t::reset_save_path_unchecked) {
        // No data movement requested -- just repoint save_path.
        {
            std::lock_guard<std::mutex> lock(t->mutex);
            t->save_path = p;
        }
        lt::post(m_ioc, [handler = std::move(handler), p]() {
            handler(lt::status_t{}, p, lt::storage_error{});
        });
        return;
    }

    JNIEnv* env = attach_jni();
    jbyteArray info_hash = env->NewByteArray(20);
    env->SetByteArrayRegion(info_hash, 0, 20,
        reinterpret_cast<const jbyte*>(t->info_hash.data()));
    jstring old_path = env->NewStringUTF(t->save_path.c_str());
    jstring new_path = env->NewStringUTF(p.c_str());

    jboolean ok = env->CallBooleanMethod(m_bridge, m_mid_move_storage, info_hash, old_path, new_path);

    env->DeleteLocalRef(info_hash);
    env->DeleteLocalRef(old_path);
    env->DeleteLocalRef(new_path);

    lt::status_t status = ok ? lt::status_t{} : lt::disk_status::fatal_disk_error;
    lt::storage_error err = ok ? lt::storage_error{} : generic_error(lt::file_index_t(-1), lt::operation_t::file_rename);
    std::string result_path = t->save_path; // moveStorage() didn't move fds, only the
                                             // backing files -- next fd_for() call per
                                             // file will re-open lazily under new_path;
                                             // existing open fds stay valid (same inode).
    if (ok) {
        std::lock_guard<std::mutex> lock(t->mutex);
        t->save_path = p;
        result_path = p;
    }

    lt::post(m_ioc, [handler = std::move(handler), status, result_path, err]() {
        handler(status, result_path, err);
    });
}

void saf_disk_io::async_release_files(lt::storage_index_t storage, std::function<void()> handler)
{
    std::shared_ptr<torrent_entry> t;
    {
        std::lock_guard<std::mutex> lock(m_torrents_mutex);
        auto it = m_torrents.find(static_cast<std::uint32_t>(storage));
        if (it != m_torrents.end()) t = it->second;
    }
    if (t) release_all(*t);
    if (handler) lt::post(m_ioc, std::move(handler));
}

void saf_disk_io::async_delete_files(lt::storage_index_t storage, lt::remove_flags_t,
                                      std::function<void(lt::storage_error const&)> handler)
{
    auto t = torrent_for(storage);
    release_all(*t); // fds must be closed before their backing files can be deleted

    JNIEnv* env = attach_jni();
    jbyteArray info_hash = env->NewByteArray(20);
    env->SetByteArrayRegion(info_hash, 0, 20,
        reinterpret_cast<const jbyte*>(t->info_hash.data()));
    jstring save_path = env->NewStringUTF(t->save_path.c_str());

    lt::storage_error err;
    for (int i = 0; i < t->files.num_files(); i++) {
        std::string rel_path = t->files.file_path(lt::file_index_t(i));
        jstring rel = env->NewStringUTF(rel_path.c_str());
        jboolean ok = env->CallBooleanMethod(m_bridge, m_mid_delete_file, info_hash, i, rel, save_path);
        env->DeleteLocalRef(rel);
        if (!ok && !err) err = generic_error(lt::file_index_t(i), lt::operation_t::file_remove);
    }
    env->DeleteLocalRef(info_hash);
    env->DeleteLocalRef(save_path);

    lt::post(m_ioc, [handler = std::move(handler), err]() { handler(err); });
}

void saf_disk_io::async_check_files(lt::storage_index_t storage, lt::add_torrent_params const*,
                                     lt::aux::vector<std::string, lt::file_index_t>,
                                     std::function<void(lt::status_t, lt::storage_error const&)> handler)
{
    // Placeholder: trusts resume data unconditionally instead of verifying
    // file sizes/existence on disk. See file-header note -- this is the
    // biggest correctness gap versus the built-in storage backends.
    (void)storage;
    lt::post(m_ioc, [handler = std::move(handler)]() {
        handler(lt::status_t{}, lt::storage_error{});
    });
}

void saf_disk_io::async_rename_file(lt::storage_index_t storage, lt::file_index_t file_idx, std::string name,
                                     std::function<void(std::string const&, lt::file_index_t, lt::storage_error const&)> handler)
{
    auto t = torrent_for(storage);

    JNIEnv* env = attach_jni();
    jbyteArray info_hash = env->NewByteArray(20);
    env->SetByteArrayRegion(info_hash, 0, 20,
        reinterpret_cast<const jbyte*>(t->info_hash.data()));
    std::string old_rel = t->files.file_path(file_idx);
    jstring rel = env->NewStringUTF(old_rel.c_str());
    jstring save_path = env->NewStringUTF(t->save_path.c_str());
    jstring new_name = env->NewStringUTF(name.c_str());

    jboolean ok = env->CallBooleanMethod(m_bridge, m_mid_rename_file, info_hash,
                                          static_cast<int>(file_idx), rel, save_path, new_name);

    env->DeleteLocalRef(info_hash);
    env->DeleteLocalRef(rel);
    env->DeleteLocalRef(save_path);
    env->DeleteLocalRef(new_name);

    lt::storage_error err = ok ? lt::storage_error{} : generic_error(file_idx, lt::operation_t::file_rename);
    std::string result = ok ? name : old_rel;
    // Note: renaming on the SAF side does not change t->files (the
    // file_storage the torrent believes is authoritative) -- libtorrent
    // itself tracks the rename via add_torrent_params::renamed_files after
    // this handler reports success. Any fd already cached in t->fds for
    // this file_idx stays valid (rename doesn't invalidate an open fd's
    // underlying inode).
    lt::post(m_ioc, [handler = std::move(handler), result, file_idx, err]() {
        handler(result, file_idx, err);
    });
}

void saf_disk_io::async_stop_torrent(lt::storage_index_t storage, std::function<void()> handler)
{
    std::shared_ptr<torrent_entry> t;
    {
        std::lock_guard<std::mutex> lock(m_torrents_mutex);
        auto it = m_torrents.find(static_cast<std::uint32_t>(storage));
        if (it != m_torrents.end()) t = it->second;
    }
    if (t) release_all(*t); // per disk_interface.hpp: "should at least do the
                             // same thing as async_release_files()"
    if (handler) lt::post(m_ioc, std::move(handler));
}

void saf_disk_io::async_set_file_priority(lt::storage_index_t storage,
                                           lt::aux::vector<lt::download_priority_t, lt::file_index_t> prio,
                                           std::function<void(lt::storage_error const&, lt::aux::vector<lt::download_priority_t, lt::file_index_t>)> handler)
{
    // No-op: no partfile support (see file-header note), so zero-priority
    // files simply stay wherever they are instead of having their data
    // reclaimed into a partfile. Piece-picker-level skipping still works
    // since that's handled above disk_interface.
    (void)storage;
    lt::post(m_ioc, [handler = std::move(handler), prio = std::move(prio)]() {
        handler(lt::storage_error{}, prio);
    });
}

void saf_disk_io::async_clear_piece(lt::storage_index_t, lt::piece_index_t index,
                                     std::function<void(lt::piece_index_t)> handler)
{
    // Nothing to synchronize -- every read/write for a given file is
    // already serialized through fd_for()'s single cached fd plus
    // pread/pwrite (no in-memory write-back cache sits in front of it).
    lt::post(m_ioc, [handler = std::move(handler), index]() { handler(index); });
}

void saf_disk_io::update_stats_counters(lt::counters&) const {}

std::vector<lt::open_file_state> saf_disk_io::get_status(lt::storage_index_t storage) const
{
    std::vector<lt::open_file_state> result;
    std::shared_ptr<torrent_entry> t;
    {
        std::lock_guard<std::mutex> lock(m_torrents_mutex);
        auto it = m_torrents.find(static_cast<std::uint32_t>(storage));
        if (it == m_torrents.end()) return result;
        t = it->second;
    }
    std::lock_guard<std::mutex> lock(t->mutex);
    for (int i = 0; i < static_cast<int>(t->fds.size()); i++) {
        if (t->fds[i] < 0) continue;
        lt::open_file_state s;
        s.file_index = lt::file_index_t(i);
        s.open_mode = lt::file_open_mode::read_write;
        s.last_use = lt::clock_type::now(); // not actually tracked per-file -- placeholder
        result.push_back(s);
    }
    return result;
}

void saf_disk_io::abort(bool) {}
void saf_disk_io::submit_jobs() {}
void saf_disk_io::settings_updated() {}

lt::disk_io_constructor_type saf_disk_io_constructor(JavaVM* jvm, jobject bridge_global_ref)
{
    return [jvm, bridge_global_ref](lt::io_context& ioc, lt::settings_interface const& settings,
                                     lt::counters& cnt) -> std::unique_ptr<lt::disk_interface> {
        return std::make_unique<saf_disk_io>(ioc, settings, cnt, jvm, bridge_global_ref);
    };
}

/*
 * Copyright (c) 2026, Yaroslav Pronin
 *
 * Licensed under the terms of the MIT license.
 * Copy of the license at https://opensource.org/licenses/MIT
 */

package org.libtorrent4j;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.libtorrent4j.alerts.Alert;
import org.libtorrent4j.alerts.AlertType;
import org.libtorrent4j.alerts.ListenSucceededAlert;
import org.libtorrent4j.swig.disk_io_file_provider;
import org.libtorrent4j.swig.libtorrent;
import org.libtorrent4j.swig.torrent_flags_t;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertArrayEquals;

/**
 * Exercises saf_disk_io end-to-end: a leecher session backed by a custom
 * disk_io_file_provider (standing in for e.g. Android SAF, which isn't
 * available on a plain JVM) downloads a small torrent from a
 * plain-filesystem seeder over loopback. Proves new_torrent/async_write/
 * async_hash/async_check_files actually move real bytes through
 * provider-supplied fds, not just that the JNI bridge type-checks.
 *
 * @author proninyaroslav
 */
public class SafDiskIoTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testDownloadThroughCustomDiskIo() throws IOException, InterruptedException {
        File seedDir = folder.newFolder("seed");
        File seedFile = new File(seedDir, "test.bin");
        byte[] content = new byte[128 * 1024]; // a handful of pieces
        new Random(42).nextBytes(content);
        Files.write(seedFile.toPath(), content);

        // Build from the file directly (not seedDir) so the resulting
        // torrent is a plain single-file torrent named "test.bin" with no
        // extra directory nesting -- building from a directory names the
        // torrent after the directory's basename and expects the file one
        // level deeper (seedDir/seed/test.bin), which doesn't match where
        // the file actually is (seedDir/test.bin) and made the seeder's
        // initial hash-check find nothing, silently reporting HAVE_NONE.
        // v1-only: async_hash2 (BitTorrent v2 per-block SHA-256) isn't
        // implemented in saf_disk_io yet (see swig/SAF_DISK_IO_NOTES.md) --
        // a v2/hybrid torrent's merkle-tree verification needs it to
        // succeed, and it deliberately fails loudly instead. Leaving this
        // test on a hybrid torrent (TorrentBuilder's default) crashed the
        // JVM inside libtorrent::aux::piece_picker::completed_hash_job.
        TorrentBuilder.Result built = new TorrentBuilder().path(seedFile)
            .flags(TorrentBuilder.V1_ONLY)
            .generate();
        TorrentInfo ti = TorrentInfo.bdecode(built.entry().bencode());

        File leechDir = folder.newFolder("leech");
        disk_io_file_provider provider = new PlainFileDiskIoProvider();

        SessionManager seeder = new SessionManager();
        SessionManager leecher = new SessionManager();
        try {
            AtomicInteger seederPortHolder = new AtomicInteger(-1);
            CountDownLatch seederListening = new CountDownLatch(1);
            CountDownLatch seederTorrentResumed = new CountDownLatch(1);
            seeder.addListener(new AlertListener() {
                @Override
                public int[] types() {
                    return new int[]{AlertType.LISTEN_SUCCEEDED.swig(), AlertType.TORRENT_RESUMED.swig()};
                }

                @Override
                public void alert(Alert<?> alert) {
                    if (alert.type() == AlertType.LISTEN_SUCCEEDED) {
                        seederPortHolder.set(((ListenSucceededAlert) alert).port());
                        seederListening.countDown();
                    } else if (alert.type() == AlertType.TORRENT_RESUMED) {
                        seederTorrentResumed.countDown();
                    }
                }
            });

            SettingsPack seederSettings = new SettingsPack();
            seederSettings.setEnableDht(false);
            seederSettings.setEnableLsd(false);
            seederSettings.listenInterfaces("127.0.0.1:0");
            seeder.start(new SessionParams(seederSettings));
            seeder.download(ti, seedDir);

            if (!seederListening.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("seeder never reported LISTEN_SUCCEEDED");
            }
            int seederPort = seederPortHolder.get();

            // A freshly added torrent is auto-managed and starts paused;
            // incoming connections are rejected ("no active torrents") until
            // the auto-manager gets around to resuming it, which does not
            // happen the instant LISTEN_SUCCEEDED fires. Connecting the
            // leecher before this landed the very first (and in this test,
            // only) connection attempt in a dropped-with-EOF race.
            if (!seederTorrentResumed.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("seeder's torrent never resumed (stayed paused)");
            }

            SettingsPack leechSettings = new SettingsPack();
            leechSettings.setEnableDht(false);
            leechSettings.setEnableLsd(false);
            leechSettings.listenInterfaces("127.0.0.1:0");
            SessionParams leechParams = new SessionParams(leechSettings);
            leechParams.swig().set_saf_disk_io_constructor(provider);
            leecher.start(leechParams);

            leecher.download(ti, leechDir, null, null,
                Collections.singletonList(new TcpEndpoint("127.0.0.1", seederPort)),
                new torrent_flags_t());

            TorrentHandle th = waitForHandle(leecher, ti);
            waitUntilFinished(th);
        } finally {
            leecher.stop();
            seeder.stop();
        }

        byte[] downloaded = Files.readAllBytes(new File(leechDir, "test.bin").toPath());
        assertArrayEquals("bytes written through disk_io_file_provider don't match the original",
            content, downloaded);
    }

    private static TorrentHandle waitForHandle(SessionManager s, TorrentInfo ti) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            TorrentHandle th = s.find(ti.infoHash());
            if (th != null && th.isValid()) return th;
            Thread.sleep(100);
        }
        throw new AssertionError("torrent was never added to the leecher session");
    }

    private static void waitUntilFinished(TorrentHandle th) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            if (th.status().isFinished()) return;
            Thread.sleep(200);
        }
        throw new AssertionError("download did not finish within the timeout"
            + " -- state=" + th.status().state());
    }

    /**
     * Backs disk_io_file_provider with plain java.io.File paths under a
     * given root, using {@link libtorrent#test_open_native_fd} (a
     * test-only native helper, see swig/libtorrent.hpp) to get a real fd
     * without needing a platform-specific source like Android's SAF.
     */
    private static class PlainFileDiskIoProvider extends disk_io_file_provider {

        @Override
        public int open_file(String infoHash, int fileIndex, String relativePath, String savePath) {
            File f = new File(savePath, relativePath);
            File parent = f.getParentFile();
            if (parent != null) parent.mkdirs();
            return libtorrent.test_open_native_fd(f.getAbsolutePath());
        }

        @Override
        public void release_file(String infoHash, int fileIndex, int fd, String relativePath, String savePath) {
            libtorrent.test_close_native_fd(fd);
        }

        @Override
        public boolean delete_file(String infoHash, int fileIndex, String relativePath, String savePath) {
            return new File(savePath, relativePath).delete();
        }

        @Override
        public boolean rename_file(String infoHash, int fileIndex, String relativePath, String savePath, String newName) {
            File src = new File(savePath, relativePath);
            File dst = new File(src.getParentFile(), newName);
            return src.renameTo(dst);
        }

        @Override
        public boolean move_storage(String infoHash, String oldSavePath, String newSavePath) {
            return false; // not exercised by this test
        }
    }
}

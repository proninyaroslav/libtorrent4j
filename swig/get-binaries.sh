#!/usr/bin/env bash

rm -rf bin
mkdir bin

# android
wget --no-check-certificate -P bin/release/android/armeabi-v7a https://s3.amazonaws.com/libtorrent4j/release/android/armeabi-v7a/libtorrent4j.so
wget --no-check-certificate -P bin/release/android/arm64-v8a https://s3.amazonaws.com/libtorrent4j/release/android/arm64-v8a/libtorrent4j.so
wget --no-check-certificate -P bin/release/android/x86 https://s3.amazonaws.com/libtorrent4j/release/android/x86/libtorrent4j.so
wget --no-check-certificate -P bin/release/android/x86_64 https://s3.amazonaws.com/libtorrent4j/release/android/x86_64/libtorrent4j.so

# linux
wget --no-check-certificate -P bin/release/linux/x86 https://s3.amazonaws.com/libtorrent4j/release/linux/x86/libtorrent4j.so
wget --no-check-certificate -P bin/release/linux/x86_64 https://s3.amazonaws.com/libtorrent4j/release/linux/x86_64/libtorrent4j.so

# windows
wget --no-check-certificate -P bin/release/windows/x86 https://s3.amazonaws.com/libtorrent4j/release/windows/x86/libtorrent4j.dll
wget --no-check-certificate -P bin/release/windows/x86_64 https://s3.amazonaws.com/libtorrent4j/release/windows/x86_64/libtorrent4j.dll

# macos
wget --no-check-certificate -P bin/release/macosx/x86_64 https://s3.amazonaws.com/libtorrent4j/release/macosx/x86_64/libtorrent4j.dylib

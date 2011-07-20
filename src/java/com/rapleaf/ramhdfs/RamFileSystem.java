//
// Copyright 2011 Rapleaf
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.rapleaf.ramhdfs;

import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;

import org.apache.commons.vfs.AllFileSelector;
import org.apache.commons.vfs.FileContent;
import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.FileType;
import org.apache.commons.vfs.RandomAccessContent;
import org.apache.commons.vfs.impl.DefaultFileSystemManager;
import org.apache.commons.vfs.provider.ram.RamFileProvider;
import org.apache.commons.vfs.util.RandomAccessMode;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.util.Progressable;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class RamFileSystem extends FileSystem {

  private static DefaultFileSystemManager fsManager = new DefaultFileSystemManager();
  private static URI NAME;
  private static Path workingDir;

  static {
    try {
      NAME = URI.create("ram:///");
      fsManager.addProvider("ram", new RamFileProvider());
      fsManager.init();
      workingDir = getInitialWorkingDirectory();
    } catch (FileSystemException e) {
      throw new RuntimeException(e);
    }
  }

  private static Path makeAbsolute(Path f) {
    if (f.isAbsolute()) {
      return new Path("ram:" + f.toUri().getSchemeSpecificPart());
    } else {
      return new Path(workingDir, f);
    }
  }

  private static Path getInitialWorkingDirectory() {
    return makeAbsolute(new Path(System.getProperty("user.dir")));
  }

  private static FileObject pathToFileObject(Path f) {
    try {
      return fsManager.resolveFile("ram://"
          + makeAbsolute(f).toUri().getSchemeSpecificPart());
    } catch (FileSystemException e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean isFile(FileObject fo) {
    try {
      return fo.getType() == FileType.FILE;
    } catch (FileSystemException e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean isDirectory(FileObject fo) {
    try {
      return fo.getType() == FileType.FOLDER;
    } catch (FileSystemException e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean createDirectory(FileObject fo) {
    try {
      fo.createFolder();
      fo.getContent().setLastModifiedTime(System.currentTimeMillis());
    } catch (FileSystemException e) {
      throw new RuntimeException(e);
    }
    return true;
  }

  @Override
  public boolean exists(Path f) {
    try {
      return pathToFileObject(f).exists();
    } catch (FileSystemException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progressable) throws IOException {
    if (!exists(f)) {
      throw new FileNotFoundException("File " + f + " not found.");
    }
    if (isDirectory(pathToFileObject(f))) {
      throw new IOException("Cannot append to a diretory (=" + f + " ).");
    }
    FileObject fo = pathToFileObject(f);
    return new FSDataOutputStream(new BufferedOutputStream(
        new RamFSOutputStream(fo), bufferSize), statistics);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    if (exists(f) && !overwrite) {
      throw new IOException("File already exists:" + f);
    }
    Path parent = f.getParent();
    if (parent != null && !mkdirs(parent)) {
      throw new IOException("Mkdirs failed to create " + parent.toString());
    }
    FileObject fo = pathToFileObject(f);
    fo.createFile();
    fo.getContent().setLastModifiedTime(System.currentTimeMillis());
    setPermission(f, permission);
    return new FSDataOutputStream(new BufferedOutputStream(
        new RamFSOutputStream(fo), bufferSize), statistics);
  }

  @Override
  public boolean delete(Path f) throws IOException {
    return delete(f, true);
  }

  @Override
  public boolean delete(Path f, boolean arg1) throws IOException {
    FileObject fo = pathToFileObject(f);
    return fo.delete(new AllFileSelector()) > 0;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    FileObject fo = pathToFileObject(f);
    if (fo.exists()) {
      return new RamFileStatus(makeAbsolute(f), getDefaultBlockSize());
    } else {
      throw new FileNotFoundException("File " + f + " does not exist.");
    }
  }

  @Override
  public URI getUri() {
    return NAME;
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {

    FileObject fo = pathToFileObject(f);

    if (!fo.exists()) {
      throw new FileNotFoundException("File " + f + " does not exist.");
    }
    if (isFile(fo)) {
      return new FileStatus[] { getFileStatus(f) };
    }

    FileObject[] children = pathToFileObject(f).getChildren();
    FileStatus[] results = new FileStatus[children.length];
    int j = 0;
    for (int i = 0; i < children.length; i++) {
      FileObject child = children[i];
      try {
        results[j] = getFileStatus(new Path(child.getName().getPath()));
        j++;
      } catch (FileNotFoundException e) {
        // ignore the files not found since the dir list may have have changed
        // since the names[] list was generated.
      }
    }
    if (j == results.length) {
      return results;
    }
    return Arrays.copyOf(results, j);
  }

  @Override
  public boolean mkdirs(Path f) throws IOException {
    if (f == null) {
      throw new IllegalArgumentException("mkdirs path arg is null");
    }
    Path parent = f.getParent();
    FileObject p2f = pathToFileObject(f);
    if (isDirectory(p2f)) {
      return true;
    }
    if (parent != null) {
      FileObject parent2f = pathToFileObject(parent);
      if (parent2f != null && parent2f.exists() && !isDirectory(parent2f)) {
        throw new FileAlreadyExistsException("Parent path is not a directory: "
            + parent);
      }
    }
    return (parent == null || mkdirs(parent))
        && (createDirectory(p2f) || isDirectory(p2f));
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    boolean b = mkdirs(f);
    if (b) {
      setPermission(f, permission);
    }
    return b;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    if (!exists(f)) {
      throw new FileNotFoundException(f.toString());
    }
    return new FSDataInputStream(new BufferedFSInputStream(
        new RamFSInputStream(pathToFileObject(f)), bufferSize));
  }

  @Override
  public boolean rename(Path source, Path target) throws IOException {
    FileObject sourceFo = pathToFileObject(source);
    pathToFileObject(target).copyFrom(sourceFo, new AllFileSelector());
    delete(source);
    return true;
  }

  @Override
  public void setWorkingDirectory(Path f) {
    workingDir = makeAbsolute(f);
  }

  private static class RamFileStatus extends FileStatus {
    public RamFileStatus(Path f, long blockSize) throws FileSystemException {
      super(
          !RamFileSystem.isFile(RamFileSystem.pathToFileObject(f)) ? 0
              : RamFileSystem.pathToFileObject(f).getContent().getSize(),
          RamFileSystem.isDirectory(RamFileSystem.pathToFileObject(f)),
          1, blockSize, RamFileSystem.pathToFileObject(f).getContent()
              .getLastModifiedTime(), RamFileSystem.makeAbsolute(f));
    }
  }

  private static class RamFSOutputStream extends OutputStream {

    FileObject fo;
    FileContent content;
    OutputStream outputStream;

    public RamFSOutputStream(FileObject fo) throws FileSystemException {
      this.fo = fo;
      content = fo.getContent();
      outputStream = content.getOutputStream();
    }

    @Override
    public void flush() {
      try {
        outputStream.flush();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {
      try {
        outputStream.close();
        content.close();
        fo.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void write(byte[] b) {
      try {
        outputStream.write(b);
        content.setLastModifiedTime(System.currentTimeMillis());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void write(byte[] b, int off, int len) {
      try {
        outputStream.write(b, off, len);
        content.setLastModifiedTime(System.currentTimeMillis());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void write(int b) throws IOException {
      outputStream.write(b);
      content.setLastModifiedTime(System.currentTimeMillis());
    }

  }

  private static class RamFSInputStream extends FSInputStream {

    FileObject fo;
    FileContent content;
    RandomAccessContent randomAccessContent;

    public RamFSInputStream(FileObject fo) throws FileSystemException {
      this.fo = fo;
      content = fo.getContent();
      randomAccessContent = fo.getContent().getRandomAccessContent(
          RandomAccessMode.READ);
    }

    @Override
    public long getPos() throws IOException {
      return randomAccessContent.getFilePointer();
    }

    @Override
    public void seek(long pos) throws IOException {
      randomAccessContent.seek(pos);
    }

    @Override
    public boolean seekToNewSource(long arg0) throws IOException {
      return false;
    }

    @Override
    public int available() {
      try {
        return randomAccessContent.getInputStream().available();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {
      try {
        randomAccessContent.close();
        content.close();
        fo.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean markSupported() {
      return false;
    }

    @Override
    public int read() throws IOException {
      throw new NotImplementedException();
    }

    @Override
    public int read(byte[] b) {
      throw new NotImplementedException();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (getPos() < fo.getContent().getSize()) {
        int value = randomAccessContent.getInputStream().read(b, off, len);
        return value;
      }
      return -1;
    }

    @Override
    public int read(long position, byte[] b, int off, int len)
        throws IOException {
      synchronized (this) {
        long oldPos = getPos();
        int nread = -1;
        try {
          seek(position);
          nread = read(b, off, len);
        } finally {
          seek(oldPos);
        }
        return nread;
      }
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
        throws IOException {
      int nread = 0;
      while (nread < length) {
        int nbytes = read(position + nread, buffer, offset + nread, length
            - nread);
        if (nbytes < 0) {
          throw new EOFException("End of file reached before reading fully.");
        }
        nread += nbytes;
      }
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      readFully(position, buffer, 0, buffer.length);
    }

    @Override
    public long skip(long n) throws IOException {
      return randomAccessContent.getInputStream().skip(n);
    }
  }
}

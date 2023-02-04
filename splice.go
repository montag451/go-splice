//go:build linux

package splice

import (
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

type FD interface {
	SyscallConn() (syscall.RawConn, error)
}

type ProgressHandler func(n int64)

type Option func(opts *Options)

type Options struct {
	bufSize  int
	progress ProgressHandler
}

func WithBufSize(size int) Option {
	return func(opts *Options) {
		opts.bufSize = size
	}
}

func WithProgressHandler(h ProgressHandler) Option {
	return func(opts *Options) {
		opts.progress = h
	}
}

func splice(rfd uintptr, wfd uintptr, len int) (int64, error) {
	return unix.Splice(int(rfd), nil, int(wfd), nil, len, unix.SPLICE_F_NONBLOCK)
}

func copyWithOpts(dst, src FD, opts Options) (int64, error) {
	pr, pw, err := os.Pipe()
	if err != nil {
		return 0, fmt.Errorf("failed to create splice pipes: %v", err)
	}
	defer pr.Close()
	defer pw.Close()
	sc, err := src.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("failed to get raw conn for src FD: %v", err)
	}
	dc, err := dst.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("failed to get raw conn for dst FD: %v", err)
	}
	prc, err := pr.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("failed to get raw conn for pipe reader: %v", err)
	}
	pwc, err := pw.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("failed to get raw conn for pipe writer: %v", err)
	}
	var maxLen int
	cmd := unix.F_GETPIPE_SZ
	if opts.bufSize > 0 {
		cmd = unix.F_SETPIPE_SZ
	}
	if cerr := prc.Control(func(fd uintptr) {
		maxLen, err = unix.FcntlInt(fd, cmd, opts.bufSize)
	}); cerr != nil || err != nil {
		if err == nil {
			err = cerr
		}
		return 0, fmt.Errorf("failed to get/set splice pipes size: %v", err)
	}
	var written int64
	for {
		var err error
		var inPipe int64
		if serr := sc.Read(func(rfd uintptr) bool {
			for {
				if werr := pwc.Write(func(wfd uintptr) bool {
					inPipe, err = splice(rfd, wfd, maxLen)
					return true
				}); werr != nil {
					err = werr
					return true
				}
				if err == unix.EINTR {
					continue
				}
				if err == unix.EAGAIN {
					return false
				}
				return true
			}
		}); serr != nil {
			err = serr
		}
		if err != nil {
			return written, err
		}
		if inPipe == 0 {
			return written, nil
		}
		if opts.progress != nil {
			opts.progress(inPipe)
		}
		rem := inPipe
		for rem > 0 {
			var n int64
			if werr := dc.Write(func(wfd uintptr) bool {
				for {
					if serr := prc.Read(func(rfd uintptr) bool {
						n, err = splice(rfd, wfd, maxLen)
						return true
					}); serr != nil {
						err = serr
						return true
					}
					if err == unix.EINTR {
						continue
					}
					if err == unix.EAGAIN {
						return false
					}
					return true
				}
			}); werr != nil {
				err = werr
			}
			if err != nil {
				return written, err
			}
			rem -= n
			written += n
		}
	}
}

func Copy(dst, src FD, opts ...Option) (int64, error) {
	var o Options
	for _, opt := range opts {
		opt(&o)
	}
	return copyWithOpts(dst, src, o)
}

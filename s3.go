package storage

import (
	"errors"
	"github.com/aaronland/go-storage"	
	"github.com/whosonfirst/go-whosonfirst-aws/s3"
	"io"
)

/*
type FSFile struct {
	io.WriteCloser
	fh *os.File
}

func (f *FSFile) Write(b []byte) (int, error) {
	return f.fh.Write(b)
}

func (f *FSFile) WriteString(b string) (int, error) {
	return f.fh.Write([]byte(b))
}

func (f *FSFile) Close() error {
	return f.fh.Close()
}
*/

type S3Store struct {
	storage.Store
	config *s3.S3Config
	conn *s3.S3Connection
}

func NewS3Store(dsn string) (Store, error) {

	cfg, err := s3.NewS3ConfigFromString(dsn)

	if err != nil {
		return nil, err
	}

	conn, err := s3.NewS3Connection(cfg)

	if err != nil {
		return nil, err
	}

	s := S3Store{
		config: cfg,
		conn:       conn,
	}

	return &s, nil
}

func (s *S3Store) URI(k string) string {
	return c.conn.URI(key)
}

func (s *S3Store) Get(k string) (io.ReadCloser, error) {
	return s.conn.Get(k)
}

func (s *S3Store) Open(k string) (io.WriteCloser, error) {

	return nil, errors.New("Please write me")
}

func (s *S3Store) Put(k string, in io.ReadCloser) error {

	return s.conn.Put(k, in)
}

func (s *S3Store) Delete(k string) error {

	return s.conn.Delete(k)
}

func (s *S3Store) Exists(k string) (bool, error) {

	_, err := s.conn.Head(k)

	if err != nil {
		return false, err
	}

	return true, nil
}

func (s *S3Store) Walk(user_cb storage.WalkFunc) error {
	
	list_cb := func(obj *S3Object) error {
		user_cb(obj.Key, obj)
	}

	list_opts := s3.DefaultS3ListOptions()
	
	return s.conn.List(list_cb, list_opts)
}

package listener

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	M "github.com/sagernet/sing/common/metadata"
)

type testPreprocessor struct {
	matches bool
	process func(conn net.Conn) (M.Socksaddr, error)
	calls   atomic.Int32
}

func (p *testPreprocessor) Match(net.Conn) bool {
	return p.matches
}

func (p *testPreprocessor) Process(ctx context.Context, conn net.Conn) (context.Context, net.Conn, M.Socksaddr, error) {
	p.calls.Add(1)
	addr, err := p.process(conn)
	return ContextWithClientAddr(ctx, addr), conn, addr, err
}

func acceptAndDial(t *testing.T, wrapper func(context.Context, net.Listener) net.Listener) (net.Conn, net.Conn) {
	t.Helper()
	rawListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = rawListener.Close() })
	listener := wrapper(context.Background(), rawListener)
	accepted := make(chan net.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- conn
	}()
	client, err := net.Dial("tcp", rawListener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	select {
	case conn := <-accepted:
		t.Cleanup(func() { _ = conn.Close() })
		return client, conn
	case err := <-acceptErr:
		t.Fatalf("accept: %v", err)
		return nil, nil
	case <-time.After(5 * time.Second):
		t.Fatal("accept timed out")
		return nil, nil
	}
}

func TestPreprocessListenerSkipsUntrustedConnections(t *testing.T) {
	preprocessor := &testPreprocessor{matches: false, process: func(net.Conn) (M.Socksaddr, error) {
		t.Error("untrusted connection must not be preprocessed")
		return M.Socksaddr{}, nil
	}}
	ctx := WithConnectionPreprocessor(context.Background(), preprocessor)
	_, server := acceptAndDial(t, func(_ context.Context, l net.Listener) net.Listener {
		return WrapTransportListener(ctx, l)
	})
	if _, ok := server.(*pendingConn); ok {
		t.Fatal("untrusted connection was wrapped")
	}
	if preprocessor.calls.Load() != 0 {
		t.Fatalf("process calls = %d, want 0", preprocessor.calls.Load())
	}
}

func TestPreprocessListenerAttachesClientAddr(t *testing.T) {
	clientAddr := M.ParseSocksaddr("192.0.2.10:54321")
	preprocessor := &testPreprocessor{matches: true, process: func(conn net.Conn) (M.Socksaddr, error) {
		var head [4]byte
		if _, err := io.ReadFull(conn, head[:]); err != nil {
			return M.Socksaddr{}, err
		}
		if string(head[:]) != "PXY\n" {
			return M.Socksaddr{}, errors.New("unexpected header")
		}
		return clientAddr, nil
	}}
	ctx := WithConnectionPreprocessor(context.Background(), preprocessor)
	client, server := acceptAndDial(t, func(_ context.Context, l net.Listener) net.Listener {
		return WrapTransportListener(ctx, l)
	})
	if _, err := client.Write([]byte("PXY\npayload")); err != nil {
		t.Fatalf("write: %v", err)
	}
	ctx, addr := PreprocessConnContext(context.Background(), server)
	if addr != clientAddr {
		t.Fatalf("addr = %v, want %v", addr, clientAddr)
	}
	if got := ClientAddrFromContext(ctx); got != clientAddr {
		t.Fatalf("context addr = %v, want %v", got, clientAddr)
	}
	payload := make([]byte, len("payload"))
	if _, err := io.ReadFull(server, payload); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if string(payload) != "payload" {
		t.Fatalf("payload = %q", payload)
	}
}

func TestPreprocessListenerRejectsInvalidHeader(t *testing.T) {
	preprocessor := &testPreprocessor{matches: true, process: func(net.Conn) (M.Socksaddr, error) {
		return M.Socksaddr{}, errors.New("bad header")
	}}
	ctx := WithConnectionPreprocessor(context.Background(), preprocessor)
	_, server := acceptAndDial(t, func(_ context.Context, l net.Listener) net.Listener {
		return WrapTransportListener(ctx, l)
	})
	if _, addr := PreprocessConnContext(context.Background(), server); addr.IsValid() {
		t.Fatalf("unexpected client address %v", addr)
	}
	if _, err := server.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected read to fail after a preprocessing error")
	}
}

func TestPreprocessListenerDoesNotBlockAccept(t *testing.T) {
	release := make(chan struct{})
	preprocessor := &testPreprocessor{matches: true, process: func(net.Conn) (M.Socksaddr, error) {
		<-release
		return M.Socksaddr{}, nil
	}}
	ctx := WithConnectionPreprocessor(context.Background(), preprocessor)
	rawListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer rawListener.Close()
	listener := WrapTransportListener(ctx, rawListener)
	client, err := net.Dial("tcp", rawListener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()
	server, err := listener.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	// The first connection is still waiting for its header, but Accept has
	// returned and the next connection can arrive.
	secondClient, err := net.Dial("tcp", rawListener.Addr().String())
	if err != nil {
		t.Fatalf("second dial: %v", err)
	}
	defer secondClient.Close()
	secondAccepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			secondAccepted <- conn
		}
	}()
	close(release)
	select {
	case second := <-secondAccepted:
		_ = second.Close()
	case <-time.After(2 * time.Second):
		t.Fatal("second Accept was blocked behind the slow header")
	}
	if _, err := client.Write([]byte("x")); err != nil {
		t.Fatalf("write: %v", err)
	}
	data := make([]byte, 1)
	if _, err := io.ReadFull(server, data); err != nil {
		t.Fatalf("read: %v", err)
	}
	if data[0] != 'x' {
		t.Fatalf("data = %q", data)
	}
	_ = server.Close()
}

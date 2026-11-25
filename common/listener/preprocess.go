package listener

import (
	"context"
	"net"
	"sync"

	M "github.com/sagernet/sing/common/metadata"
)

// ConnectionPreprocessor inspects a freshly accepted TCP connection before the
// protocol handler sees it. A nil address means the connection is a direct
// client and the peer address stays authoritative.
//
// Implementations must not block the accept loop: the caller runs this in the
// connection's own goroutine.
type ConnectionPreprocessor interface {
	Process(ctx context.Context, conn net.Conn) (context.Context, net.Conn, M.Socksaddr, error)
}

// ConnectionPreprocessorMatcher is an optional fast path. When the accepted
// connection will not be preprocessed (for example a direct client rather than
// a trusted relay), the listener skips the wrapper and its goroutine entirely.
type ConnectionPreprocessorMatcher interface {
	Match(conn net.Conn) bool
}

type preprocessorKey struct{}

type clientAddrKey struct{}

// WithConnectionPreprocessor installs p for every listener created from ctx.
// There is a single instance per process; per-inbound policy is the
// implementation's concern, keyed by the connection's local address.
func WithConnectionPreprocessor(ctx context.Context, p ConnectionPreprocessor) context.Context {
	return context.WithValue(ctx, preprocessorKey{}, p)
}

// ConnectionPreprocessorFromContext returns the installed preprocessor, if
// any.
func ConnectionPreprocessorFromContext(ctx context.Context) ConnectionPreprocessor {
	p, _ := ctx.Value(preprocessorKey{}).(ConnectionPreprocessor)
	return p
}

// ContextWithClientAddr annotates ctx with the verified client address.
func ContextWithClientAddr(ctx context.Context, addr M.Socksaddr) context.Context {
	if !addr.IsValid() {
		return ctx
	}
	return context.WithValue(ctx, clientAddrKey{}, addr)
}

// ClientAddrFromContext returns the verified client address, or an invalid
// address when the connection is direct or was never preprocessed.
func ClientAddrFromContext(ctx context.Context) M.Socksaddr {
	addr, _ := ctx.Value(clientAddrKey{}).(M.Socksaddr)
	return addr
}

// PreprocessConn runs the configured preprocessor in the current goroutine.
// It is the entry point for the plain TCP listener loop.
func PreprocessConn(ctx context.Context, conn net.Conn) (context.Context, net.Conn, M.Socksaddr, error) {
	preprocessor := ConnectionPreprocessorFromContext(ctx)
	if preprocessor == nil {
		return ctx, conn, M.Socksaddr{}, nil
	}
	return preprocessor.Process(ctx, conn)
}

type connKey struct {
	local  string
	remote string
}

func keyOf(conn net.Conn) connKey {
	key := connKey{}
	if addr := conn.LocalAddr(); addr != nil {
		key.local = addr.String()
	}
	if addr := conn.RemoteAddr(); addr != nil {
		key.remote = addr.String()
	}
	return key
}

// pendingConnection is shared between Accept, the preprocessor goroutine and
// ConnContext. The context handed to an HTTP transport only carries the raw
// connection, so the result is looked up by its address pair; TLS wrapping
// leaves RemoteAddr and LocalAddr unchanged.
type pendingConnection struct {
	done chan struct{}
	conn net.Conn
	addr M.Socksaddr
	err  error
	key  connKey
}

var pendingConnections sync.Map

func storePending(entry *pendingConnection) {
	pendingConnections.Store(entry.key, entry)
}

func loadPending(conn net.Conn) (*pendingConnection, bool) {
	return loadPendingKey(keyOf(conn))
}

func loadPendingKey(key connKey) (*pendingConnection, bool) {
	value, loaded := pendingConnections.Load(key)
	if !loaded {
		return nil, false
	}
	entry, ok := value.(*pendingConnection)
	return entry, ok
}

func addrKey(local, remote net.Addr) connKey {
	key := connKey{}
	if local != nil {
		key.local = local.String()
	}
	if remote != nil {
		key.remote = remote.String()
	}
	return key
}

// PreprocessConnContext is called from a transport's ConnContext hook. It
// waits for the in-flight preprocessing, attaches the verified address to ctx
// and closes the connection if the header was required but invalid.
func PreprocessConnContext(ctx context.Context, conn net.Conn) (context.Context, M.Socksaddr) {
	return PreprocessAddrContext(ctx, conn.LocalAddr(), conn.RemoteAddr())
}

// PreprocessAddrContext is the address-based variant used by gRPC, whose
// stats handler exposes the peer addresses rather than the connection.
func PreprocessAddrContext(ctx context.Context, local, remote net.Addr) (context.Context, M.Socksaddr) {
	entry, loaded := loadPendingKey(addrKey(local, remote))
	if !loaded {
		return ctx, M.Socksaddr{}
	}
	<-entry.done
	if entry.err != nil {
		_ = entry.conn.Close()
		return ctx, M.Socksaddr{}
	}
	if !entry.addr.IsValid() {
		return ctx, entry.addr
	}
	return ContextWithClientAddr(ctx, entry.addr), entry.addr
}

// preprocessListener performs the PROXY read in its own goroutine, so a slow
// client cannot stall Accept, while the returned connection blocks on first
// use until the header has been consumed.
type preprocessListener struct {
	net.Listener
	ctx          context.Context
	preprocessor ConnectionPreprocessor
}

func (l *preprocessListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if matcher, isMatcher := l.preprocessor.(ConnectionPreprocessorMatcher); isMatcher && !matcher.Match(conn) {
		return conn, nil
	}
	entry := &pendingConnection{
		done: make(chan struct{}),
		conn: conn,
		key:  keyOf(conn),
	}
	storePending(entry)
	go func() {
		_, processed, addr, err := l.preprocessor.Process(l.ctx, conn)
		entry.conn = processed
		entry.addr = addr
		entry.err = err
		close(entry.done)
	}()
	return &pendingConn{Conn: conn, entry: entry}, nil
}

type pendingConn struct {
	net.Conn
	entry *pendingConnection
}

func (c *pendingConn) wait() error {
	<-c.entry.done
	return c.entry.err
}

func (c *pendingConn) Read(p []byte) (int, error) {
	if err := c.wait(); err != nil {
		return 0, err
	}
	return c.entry.conn.Read(p)
}

func (c *pendingConn) Write(p []byte) (int, error) {
	if err := c.wait(); err != nil {
		return 0, err
	}
	return c.entry.conn.Write(p)
}

func (c *pendingConn) Close() error {
	pendingConnections.Delete(c.entry.key)
	return c.entry.conn.Close()
}

func (c *pendingConn) Upstream() any {
	return c.entry.conn
}

// WrapTransportListener installs the preprocessing fast path for transports
// that accept connections themselves (HTTP, WebSocket, gRPC). It is a no-op
// when no preprocessor is configured.
func WrapTransportListener(ctx context.Context, tcpListener net.Listener) net.Listener {
	preprocessor := ConnectionPreprocessorFromContext(ctx)
	if preprocessor == nil {
		return tcpListener
	}
	return &preprocessListener{
		Listener:     tcpListener,
		ctx:          ctx,
		preprocessor: preprocessor,
	}
}
